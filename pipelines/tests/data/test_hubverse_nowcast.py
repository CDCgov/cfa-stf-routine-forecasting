"""Tests for converting Hubverse sample output to EpiAutoGP nowcasts."""

import datetime as dt

import polars as pl
import pytest

from pipelines.data.hubverse_nowcast import HubverseNowcast
from pipelines.data.nowcast import NowcastData
from pipelines.epiautogp.epiautogp_forecast_utils import _resolve_nowcast_source
from pipelines.epiautogp.forecast_spec import ForecastSpec

ORIGIN = dt.date(2026, 7, 18)
NOWCAST_DATES = [dt.date(2026, 7, 4), dt.date(2026, 7, 11)]
HUBVERSE_SCHEMA = {
    "origin_date": pl.Date,
    "target_end_date": pl.Date,
    "horizon": pl.Int32,
    "target": pl.String,
    "location": pl.String,
    "output_type": pl.String,
    "output_type_id": pl.String,
    "value": pl.Float64,
}


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


def _sample_frame(
    *,
    target: str = "wk inc covid hosp",
    origin_date: dt.date = ORIGIN,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
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
        ],
        schema=HUBVERSE_SCHEMA,
    ).reverse()


def _write_artifact(
    tmp_path,
    frame: pl.DataFrame | None = None,
    *,
    filename: str = "2026-07-18-CFA-nowcastNHSN.parquet",
):
    output_dir = tmp_path / "model-output" / "CFA-nowcastNHSN"
    output_dir.mkdir(parents=True, exist_ok=True)
    (_sample_frame() if frame is None else frame).write_parquet(output_dir / filename)
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
            _sample_frame(target=target),
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
    ("spec", "invalid_setting"),
    [
        (_spec(target="nssp"), "target='nssp'"),
        (_spec(frequency="daily"), "frequency='daily'"),
        (_spec(ed_visit_type="pct"), "ed_visit_type='pct'"),
    ],
)
def test_validation_explains_inapplicable_model_configuration(spec, invalid_setting):
    with pytest.raises(ValueError, match=invalid_setting):
        HubverseNowcast.ensure_applicable(forecast_spec=spec)


@pytest.mark.parametrize(
    "spec",
    [
        _spec(target="nssp"),
        _spec(frequency="daily"),
        _spec(ed_visit_type="pct"),
    ],
)
def test_resolver_propagates_applicability_error(tmp_path, spec):
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
    source = HubverseNowcast(
        containing_dir=_write_artifact(
            tmp_path, _sample_frame().drop("output_type_id")
        ),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="missing required columns"):
        _get_nowcast(source)


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (_sample_frame(origin_date=dt.date(2026, 7, 12)), "run vintage"),
        (_sample_frame(target="wk inc flu hosp"), "no rows for target"),
        (
            _sample_frame()
            .filter(pl.col("location") == "ca")
            .with_columns(location=pl.lit("ny")),
            "no rows for location",
        ),
        (
            _sample_frame().with_columns(output_type=pl.lit("quantile")),
            "no sample rows",
        ),
    ],
)
def test_rejects_unmatched_vintage_target_location_or_output_type(
    tmp_path, frame, message
):
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, frame),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match=message):
        _get_nowcast(source)


def test_rejects_duplicate_sample_date(tmp_path):
    frame = _sample_frame()
    frame = pl.concat([frame, frame.filter(pl.col("location") == "ca").head(1)])
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, frame),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="duplicate"):
        _get_nowcast(source)


def test_rejects_incomplete_trajectory(tmp_path):
    frame = _sample_frame().filter(
        ~(
            (pl.col("location") == "ca")
            & (pl.col("output_type_id") == "sample_2")
            & (pl.col("target_end_date") == NOWCAST_DATES[1])
        )
    )
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, frame),
        forecast_spec=_spec(),
    )

    with pytest.raises(ValueError, match="incomplete"):
        _get_nowcast(source)


@pytest.mark.parametrize("invalid_value", [-1.0, float("inf"), float("nan")])
def test_rejects_invalid_sample_values(tmp_path, invalid_value):
    frame = _sample_frame().with_columns(
        value=pl.when(pl.col("location") == "ca")
        .then(pl.lit(invalid_value))
        .otherwise(pl.col("value"))
    )
    source = HubverseNowcast(
        containing_dir=_write_artifact(tmp_path, frame),
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
