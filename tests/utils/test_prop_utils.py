"""Tests for proportion fusion model utilities."""

import datetime as dt

import polars as pl
from cfa.stf.forecasttools import read_tabular, write_tabular
from polars.testing import assert_frame_equal

from cfa.stf.routine.utils.prop_utils import create_prop_fusion_model


def _model_frame(
    dates,
    values,
    *,
    variable: str,
    draws=None,
    resolution: str = "daily",
    data_type="forecast",
) -> pl.DataFrame:
    columns = {"date": dates, ".value": values, "data_type": data_type}
    if draws is not None:
        columns[".draw"] = draws
    return pl.DataFrame(columns).with_columns(
        pl.lit("CA").alias("geo_value"),
        pl.lit("flu").alias("disease"),
        pl.lit(variable).alias(".variable"),
        pl.lit(resolution).alias("resolution"),
    )


def _write_model_outputs(
    model_dir,
    samples: pl.DataFrame,
    data: pl.DataFrame,
) -> None:
    (model_dir / "data").mkdir(parents=True)
    write_tabular(samples, model_dir / "samples.parquet")
    write_tabular(data, model_dir / "data" / "combined_data.tsv")


def test_create_prop_fusion_model_aggregates_with_forecasttools(tmp_path):
    daily_dates = pl.date_range(dt.date(2025, 1, 5), dt.date(2025, 1, 18), eager=True)
    num_samples = (
        _model_frame(
            daily_dates,
            pl.int_range(1, 15, eager=True),
            variable="observed_ed_visits",
            draws=1,
        )
        .with_row_index(".iteration", offset=1)
        .with_columns(pl.lit(1).alias(".chain"))
    )
    num_data = num_samples.drop(".draw", ".chain", ".iteration").with_columns(
        pl.lit("train").alias("data_type")
    )
    weekly_dates = [dt.date(2025, 1, 11), dt.date(2025, 1, 18)]
    other_samples = _model_frame(
        weekly_dates,
        [63, 70],
        variable="other_ed_visits",
        draws=[1, 1],
        resolution="epiweekly",
    )
    other_data = other_samples.drop(".draw").with_columns(
        pl.lit("train").alias("data_type")
    )
    _write_model_outputs(tmp_path / "num_model", num_samples, num_data)
    _write_model_outputs(tmp_path / "other_model", other_samples, other_data)

    create_prop_fusion_model(
        model_run_dir=tmp_path,
        num_model_name="num_model",
        other_model_name="other_model",
        aggregate_num=True,
        augment_other_with_obs=False,
    )

    output_dir = tmp_path / "prop_epiweekly_aggregated_num_model_other_model"
    samples = read_tabular(output_dir / "samples.parquet").sort("date")
    data = read_tabular(output_dir / "data" / "combined_data.tsv").sort("date")
    expected = pl.DataFrame(
        {
            "date": weekly_dates,
            "resolution": ["epiweekly"] * 2,
            ".variable": ["prop_disease_ed_visits"] * 2,
            ".value": [28 / (28 + 63), 77 / (77 + 70)],
        }
    )
    assert_frame_equal(samples.select(expected.columns), expected, check_exact=False)
    assert ".chain" not in samples.columns
    assert ".iteration" not in samples.columns
    assert_frame_equal(data.select(expected.columns), expected, check_exact=False)


def test_create_prop_fusion_model_augments_samples_with_observations(tmp_path):
    training_date = dt.date(2025, 1, 1)
    forecast_date = dt.date(2025, 1, 2)
    num_samples = _model_frame(
        [training_date] * 2 + [forecast_date] * 2,
        [2, 3, 4, 5],
        variable="observed_ed_visits",
        draws=[1, 2, 1, 2],
        data_type=["train", "train", "forecast", "forecast"],
    )
    other_samples = _model_frame(
        [forecast_date] * 2,
        [5, 6],
        variable="other_ed_visits",
        draws=[2, 1],
    )
    num_data = num_samples.filter(pl.col("date") == training_date).drop(".draw")
    other_data = _model_frame(
        [training_date],
        [8],
        variable="other_ed_visits",
        data_type="train",
    )
    _write_model_outputs(tmp_path / "num_model", num_samples, num_data)
    _write_model_outputs(tmp_path / "other_model", other_samples, other_data)

    create_prop_fusion_model(
        model_run_dir=tmp_path,
        num_model_name="num_model",
        other_model_name="other_model",
    )

    samples = read_tabular(tmp_path / "prop_num_model_other_model" / "samples.parquet")
    expected = pl.DataFrame(
        {
            "date": [training_date] * 2 + [forecast_date] * 2,
            ".draw": [1, 2, 1, 2],
            ".value": [2 / 10, 3 / 11, 4 / 10, 5 / 10],
        }
    )
    assert_frame_equal(samples.select(expected.columns), expected, check_exact=False)
