"""Tests for production NHSN Hubverse nowcast ingestion."""

import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest

from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.hubverse_nowcast import HubversePointerNowcast


def _spec(
    *,
    disease: str = "COVID-19",
    loc: str = "CA",
    report_date: dt.date = dt.date(2026, 3, 14),
) -> ForecastSpec:
    return ForecastSpec(
        disease=disease,
        loc=loc,
        report_date=report_date,
        target="nhsn",
        frequency="epiweekly",
        ed_visit_type="observed",
    )


def _hubverse_rows(
    *,
    target: str = "wk inc covid hosp",
    location: str = "ca",
    origin_date: dt.date = dt.date(2026, 3, 14),
) -> list[dict]:
    dates = [
        dt.date(2026, 2, 28),
        dt.date(2026, 3, 7),
        dt.date(2026, 3, 14),
    ]
    horizons = [-2, -1, 0]
    values_by_sample = {
        "covid_ca_1": [100.0, 110.0, 120.0],
        "covid_ca_2": [200.0, 210.0, 220.0],
    }
    rows = []
    for sample_id, values in values_by_sample.items():
        for target_end_date, horizon, value in zip(
            dates, horizons, values, strict=True
        ):
            rows.append(
                {
                    "origin_date": origin_date,
                    "target_end_date": target_end_date,
                    "horizon": horizon,
                    "target": target,
                    "location": location,
                    "output_type": "sample",
                    "output_type_id": sample_id,
                    "value": value,
                }
            )
    return rows


def _write_hubverse(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path)
    return path


def _write_pointer(
    path: Path,
    *,
    model_output_uri: str,
    target: str = "covid",
    report_date: str = "2026-03-14",
    validation_status: str = "passed",
) -> Path:
    path.write_text(
        json.dumps(
            {
                "artifact_type": "handoff_pointer",
                "contract_version": "v2alpha1",
                "target": target,
                "report_date": report_date,
                "run_id": "weekly-2026-03-14-primary",
                "generated_at": "2026-03-14T09:12:05Z",
                "run_manifest_uri": "run_manifest.json",
                "selection_manifest_uri": "selection_manifest.json",
                "run_status_uri": "run_status.json",
                "status": "success",
                "decision_counts": {
                    "total": 1,
                    "primary_success": 1,
                    "fallback_success": 0,
                    "failed_no_output": 0,
                },
                "nowcast_artifacts": [],
                "hubverse": {
                    "hub_root_uri": str(path.parent),
                    "model_output_uri": model_output_uri,
                    "model_id": "CFA-nowcastNHSN",
                    "round_id": report_date,
                    "validation_status": validation_status,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _source_for_rows(
    tmp_path: Path,
    rows: list[dict],
    *,
    forecast_spec: ForecastSpec | None = None,
    pointer_disease: str = "covid",
    pointer_report_date: str = "2026-03-14",
    validation_status: str = "passed",
    model_output_uri: str | None = None,
) -> HubversePointerNowcast:
    spec = forecast_spec or _spec()
    hubverse_path = _write_hubverse(tmp_path / "hubverse.parquet", rows)
    pointer_path = _write_pointer(
        tmp_path / "latest.json",
        model_output_uri=model_output_uri or hubverse_path.name,
        target=pointer_disease,
        report_date=pointer_report_date,
        validation_status=validation_status,
    )
    return HubversePointerNowcast(
        pointer_path=pointer_path,
        forecast_spec=spec,
        source_label="NHSN Hubverse nowcast",
    )


def test_reads_hubverse_samples_and_excludes_horizon_zero(tmp_path):
    """Strictly negative horizons become EpiAutoGP nowcast samples."""
    source = _source_for_rows(tmp_path, _hubverse_rows())

    data = source.get_nowcast_data(
        dates=[
            dt.date(2026, 2, 28),
            dt.date(2026, 3, 7),
            dt.date(2026, 3, 14),
        ],
        reports=[90.0, 100.0, 120.0],
    )

    assert data.dates == [dt.date(2026, 2, 28), dt.date(2026, 3, 7)]
    assert data.reports == [[100.0, 110.0], [200.0, 210.0]]


@pytest.mark.parametrize(
    ("disease", "producer_target", "hubverse_target"),
    [
        ("Influenza", "flu", "wk inc flu hosp"),
        ("RSV", "rsv", "wk inc rsv hosp"),
    ],
)
def test_maps_supported_nhsn_diseases(
    tmp_path, disease, producer_target, hubverse_target
):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(
            target=hubverse_target,
            location="ny",
        ),
        forecast_spec=_spec(disease=disease, loc="NY"),
        pointer_disease=producer_target,
    )

    data = source.get_nowcast_data(dates=[], reports=[])

    assert data.dates == [dt.date(2026, 2, 28), dt.date(2026, 3, 7)]
    assert data.reports == [[100.0, 110.0], [200.0, 210.0]]


def test_unsupported_disease_fails_fast(tmp_path):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(),
        forecast_spec=_spec(disease="Parainfluenza"),
    )

    with pytest.raises(ValueError, match="Unsupported Hubverse nowcast disease"):
        source.get_nowcast_data(dates=[], reports=[])


def test_pointer_disease_mismatch_fails_fast(tmp_path):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(),
        pointer_disease="flu",
    )

    with pytest.raises(ValueError, match="pointer target \\(disease\\) mismatch"):
        source.get_nowcast_data(dates=[], reports=[])


def test_pointer_report_date_mismatch_fails_fast(tmp_path):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(),
        pointer_report_date="2026-03-21",
    )

    with pytest.raises(ValueError, match="pointer report_date mismatch"):
        source.get_nowcast_data(dates=[], reports=[])


def test_unvalidated_hubverse_pointer_fails_fast(tmp_path):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(),
        validation_status="failed",
    )

    with pytest.raises(ValueError, match="validation_status mismatch"):
        source.get_nowcast_data(dates=[], reports=[])


def test_no_matching_rows_fails_fast(tmp_path):
    source = _source_for_rows(
        tmp_path,
        _hubverse_rows(location="ny"),
    )

    with pytest.raises(ValueError, match="No matching NHSN Hubverse nowcast"):
        source.get_nowcast_data(dates=[], reports=[])


def test_missing_required_column_fails_fast(tmp_path):
    rows = _hubverse_rows()
    for row in rows:
        del row["value"]
    source = _source_for_rows(tmp_path, rows)

    with pytest.raises(ValueError, match="missing required columns: value"):
        source.get_nowcast_data(dates=[], reports=[])


def test_duplicate_sample_date_rows_fail_fast(tmp_path):
    rows = _hubverse_rows()
    rows.append(dict(rows[0]))
    source = _source_for_rows(tmp_path, rows)

    with pytest.raises(ValueError, match="duplicate rows"):
        source.get_nowcast_data(dates=[], reports=[])


def test_incomplete_sample_grid_fails_fast(tmp_path):
    rows = [
        row
        for row in _hubverse_rows()
        if not (
            row["output_type_id"] == "covid_ca_2"
            and row["target_end_date"] == dt.date(2026, 3, 7)
        )
    ]
    source = _source_for_rows(tmp_path, rows)

    with pytest.raises(ValueError, match="same target_end_date values"):
        source.get_nowcast_data(dates=[], reports=[])


@pytest.mark.parametrize("bad_value", [-1.0, float("inf"), None])
def test_bad_nowcast_values_fail_fast(tmp_path, bad_value):
    rows = _hubverse_rows()
    rows[0]["value"] = bad_value
    source = _source_for_rows(tmp_path, rows)

    with pytest.raises(ValueError, match="missing, non-finite, or negative"):
        source.get_nowcast_data(dates=[], reports=[])


def test_missing_negative_horizons_fails_fast(tmp_path):
    rows = [row for row in _hubverse_rows() if row["horizon"] == 0]
    source = _source_for_rows(tmp_path, rows)

    with pytest.raises(ValueError, match="strictly negative-horizon"):
        source.get_nowcast_data(dates=[], reports=[])
