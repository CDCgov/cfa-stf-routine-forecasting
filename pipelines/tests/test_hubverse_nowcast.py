"""Tests for generic Hubverse nowcast ingestion."""

import datetime as dt
import json
from pathlib import Path

import polars as pl

from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.hubverse_nowcast import (
    HubversePointerNowcast,
    hubverse_samples_to_nowcast_data,
)


def _rows(*, target: str = "wk inc covid hosp") -> list[dict]:
    return [
        {
            "origin_date": dt.date(2026, 4, 4),
            "target_end_date": target_end_date,
            "horizon": horizon,
            "target": target,
            "location": "us",
            "output_type": "sample",
            "output_type_id": sample_id,
            "value": value,
        }
        for sample_id, values in {
            "1": [1.0, 2.0, 3.0],
            "2": [4.0, 5.0, 6.0],
        }.items()
        for target_end_date, horizon, value in zip(
            [
                dt.date(2026, 3, 21),
                dt.date(2026, 3, 28),
                dt.date(2026, 4, 4),
            ],
            [-2, -1, 0],
            values,
            strict=True,
        )
    ]


def _spec(**kwargs) -> ForecastSpec:
    return ForecastSpec(
        disease=kwargs.get("disease", "COVID-19"),
        loc=kwargs.get("loc", "US"),
        report_date=kwargs.get("report_date", dt.date(2026, 4, 4)),
        target=kwargs.get("target", "nhsn"),
        frequency=kwargs.get("frequency", "epiweekly"),
        ed_visit_type=kwargs.get("ed_visit_type", "observed"),
    )


def _write_pointer(path: Path, *, model_output_uri: str) -> Path:
    path.write_text(
        json.dumps(
            {
                "artifact_type": "handoff_pointer",
                "target": "covid",
                "report_date": "2026-04-04",
                "hubverse": {
                    "model_output_uri": model_output_uri,
                    "round_id": "2026-04-04",
                    "validation_status": "passed",
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_hubverse_samples_to_nowcast_data_is_target_agnostic():
    data = hubverse_samples_to_nowcast_data(
        pl.DataFrame(_rows(target="wk inc example hosp")),
        forecast_spec=_spec(),
        hubverse_target="wk inc example hosp",
    )

    assert data.dates == [dt.date(2026, 3, 21), dt.date(2026, 3, 28)]
    assert data.reports == [[1.0, 2.0], [4.0, 5.0]]


def test_hubverse_samples_accepts_hewr_reference_date_column():
    rows = [
        {"reference_date": row["origin_date"]}
        | {key: value for key, value in row.items() if key != "origin_date"}
        for row in _rows(target="wk inc example hosp")
    ]

    data = hubverse_samples_to_nowcast_data(
        pl.DataFrame(rows),
        forecast_spec=_spec(),
        hubverse_target="wk inc example hosp",
    )

    assert data.dates == [dt.date(2026, 3, 21), dt.date(2026, 3, 28)]
    assert data.reports == [[1.0, 2.0], [4.0, 5.0]]


def test_hubverse_pointer_nowcast_reads_generic_handoff(tmp_path):
    hubverse_path = tmp_path / "model-output.parquet"
    pl.DataFrame(_rows()).write_parquet(hubverse_path)
    pointer_path = _write_pointer(
        tmp_path / "latest.json",
        model_output_uri=hubverse_path.name,
    )
    source = HubversePointerNowcast(
        pointer_uri=pointer_path,
        forecast_spec=_spec(),
    )

    data = source.get_nowcast_data(dates=[], reports=[])

    assert data.dates == [dt.date(2026, 3, 21), dt.date(2026, 3, 28)]
    assert data.reports == [[1.0, 2.0], [4.0, 5.0]]


def test_hubverse_pointer_nowcast_applies_to_supported_spec():
    spec = ForecastSpec(
        disease="COVID-19",
        loc="CA",
        report_date=dt.date(2026, 4, 4),
        target="nssp",
        frequency="daily",
        ed_visit_type="pct",
    )

    assert HubversePointerNowcast.applies_to(forecast_spec=spec)


def test_hubverse_pointer_nowcast_applies_to_rejects_unmapped_spec():
    assert not HubversePointerNowcast.applies_to(
        forecast_spec=_spec(disease="Parainfluenza")
    )
