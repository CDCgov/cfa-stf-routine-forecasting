"""Tests for converting Hubverse sample output to EpiAutoGP nowcasts."""

import datetime as dt

import polars as pl
import pytest

from pipelines.epiautogp.epiautogp_forecast_utils import _resolve_nowcast_source
from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.hubverse_nowcast import HubverseNowcast
from pipelines.epiautogp.nowcast import NowcastData

ORIGIN = dt.date(2026, 7, 18)
NOWCAST_DATES = [dt.date(2026, 7, 4), dt.date(2026, 7, 11)]


def _spec(
    *,
    disease: str = "COVID-19",
    loc: str = "CA",
    report_date: dt.date = ORIGIN,
    target: str = "nhsn",
    frequency: str = "epiweekly",
    ed_visit_type: str = "observed",
) -> ForecastSpec:
    return ForecastSpec(
        disease=disease,
        loc=loc,
        report_date=report_date,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
    )


def _sample_rows(
    *,
    target: str = "wk inc covid hosp",
    origin_date: dt.date = ORIGIN,
) -> list[dict]:
    rows = [
        {
            "origin_date": origin_date,
            "target_end_date": date,
            "horizon": -2 + date_index,
            "target": target,
            "location": location,
            "output_type": "sample",
            "output_type_id": sample_id,
            "value": value,
        }
        for location, sample_id, values in [
            ("ca", "sample_2", [20.0, 21.0]),
            ("ca", "sample_1", [10.0, 11.0]),
            ("ny", "sample_1", [100.0, 101.0]),
        ]
        for date_index, (date, value) in enumerate(zip(NOWCAST_DATES, values))
    ]
    return list(reversed(rows))


def _write_artifact(
    tmp_path,
    rows: list[dict] | None = None,
    *,
    filename: str = "2026-07-18-CFA-nowcastNHSN.parquet",
):
    output_dir = tmp_path / "model-output" / "CFA-nowcastNHSN"
    output_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(_sample_rows() if rows is None else rows).write_parquet(
        output_dir / filename
    )
    return tmp_path


def _get_nowcast(source: HubverseNowcast) -> NowcastData:
    return source.get_nowcast_data(
        dates=[dt.date(2026, 6, 28), *NOWCAST_DATES],
        reports=[5.0, 6.0, 7.0],
    )


def test_converts_shuffled_rows_to_complete_ordered_trajectories(tmp_path):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path),
        forecast_spec=_spec(loc="CA"),
    )

    result = _get_nowcast(source)

    assert result.dates == NOWCAST_DATES
    assert result.reports == [[10.0, 11.0], [20.0, 21.0]]


@pytest.mark.parametrize(
    ("disease", "target"),
    [
        ("COVID-19", "wk inc covid hosp"),
        ("Influenza", "wk inc flu hosp"),
        ("RSV", "wk inc rsv hosp"),
    ],
)
def test_maps_supported_diseases_and_location_case_insensitively(
    tmp_path, disease, target
):
    source = HubverseNowcast(
        containing_dir=_write_artifact(
            tmp_path,
            _sample_rows(target=target),
        ),
        forecast_spec=_spec(disease=disease, loc="cA"),
    )

    assert _get_nowcast(source).reports == [[10.0, 11.0], [20.0, 21.0]]


def test_resolver_builds_source_for_materialized_directory(tmp_path):
    result = _resolve_nowcast_source(
        forecast_spec=_spec(),
        nowcast_source_name="hubverse",
        hubverse_nowcast_dir=tmp_path,
    )

    assert isinstance(result, HubverseNowcast)
    assert result.containing_dir == tmp_path


@pytest.mark.parametrize(
    "spec",
    [
        _spec(target="nssp"),
        _spec(frequency="daily"),
        _spec(ed_visit_type="pct"),
    ],
)
def test_resolver_rejects_inapplicable_model_configuration(tmp_path, spec):
    with pytest.raises(ValueError, match="only applicable"):
        _resolve_nowcast_source(
            forecast_spec=spec,
            nowcast_source_name="hubverse",
            hubverse_nowcast_dir=tmp_path,
        )


def test_resolver_requires_containing_directory():
    with pytest.raises(ValueError, match="hubverse_nowcast_dir is required"):
        _resolve_nowcast_source(
            forecast_spec=_spec(),
            nowcast_source_name="hubverse",
            hubverse_nowcast_dir=None,
        )


def test_resolver_rejects_reporting_pmf_with_hubverse_directory(tmp_path):
    with pytest.raises(ValueError, match="mutually exclusive"):
        _resolve_nowcast_source(
            forecast_spec=_spec(),
            nowcast_source_name="hubverse",
            reporting_delay_pmf=[0.5, 0.5],
            hubverse_nowcast_dir=tmp_path,
        )


@pytest.mark.parametrize("artifact_count", [0, 2])
def test_requires_exactly_one_parquet(tmp_path, artifact_count):
    if artifact_count:
        _write_artifact(tmp_path, filename="one.parquet")
        _write_artifact(tmp_path, filename="two.parquet")
    source = HubverseNowcast(containing_dir=tmp_path, forecast_spec=_spec())

    with pytest.raises(ValueError, match="exactly one Hubverse Parquet"):
        _get_nowcast(source)


def test_rejects_missing_required_column(tmp_path):
    rows = _sample_rows()
    for row in rows:
        del row["output_type_id"]
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, rows),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="missing required columns"):
        _get_nowcast(source)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (_sample_rows(origin_date=dt.date(2026, 7, 12)), "run vintage"),
        (_sample_rows(target="wk inc flu hosp"), "no rows for target"),
        (
            [
                {**row, "location": "ny"}
                for row in _sample_rows()
                if row["location"] == "ca"
            ],
            "no rows for location",
        ),
        (
            [{**row, "output_type": "quantile"} for row in _sample_rows()],
            "no sample rows",
        ),
    ],
)
def test_rejects_unmatched_vintage_target_location_or_output_type(
    tmp_path, rows, message
):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, rows),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match=message):
        _get_nowcast(source)


def test_rejects_duplicate_sample_date(tmp_path):
    rows = _sample_rows()
    rows.append(next(row.copy() for row in rows if row["location"] == "ca"))
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, rows),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="duplicate"):
        _get_nowcast(source)


def test_rejects_incomplete_trajectory(tmp_path):
    rows = [
        row
        for row in _sample_rows()
        if not (
            row["location"] == "ca"
            and row["output_type_id"] == "sample_2"
            and row["target_end_date"] == NOWCAST_DATES[1]
        )
    ]
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, rows),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="incomplete"):
        _get_nowcast(source)


@pytest.mark.parametrize("invalid_value", [-1.0, float("inf"), float("nan")])
def test_rejects_invalid_sample_values(tmp_path, invalid_value):
    rows = _sample_rows()
    next(row for row in rows if row["location"] == "ca")["value"] = invalid_value
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, rows),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="finite and non-negative"):
        _get_nowcast(source)


def test_rejects_nowcast_date_absent_from_observations(tmp_path):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="absent from"):
        source.get_nowcast_data(
            dates=[NOWCAST_DATES[0]],
            reports=[1.0],
        )


def test_rejects_mismatched_observation_vectors(tmp_path):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="same length"):
        source.get_nowcast_data(
            dates=NOWCAST_DATES,
            reports=[1.0],
        )


def test_rejects_unsupported_disease(tmp_path):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path),
        forecast_spec=_spec(disease="Measles"),
    )

    with pytest.raises(ValueError, match="No Hubverse NHSN target mapping"):
        _get_nowcast(source)
