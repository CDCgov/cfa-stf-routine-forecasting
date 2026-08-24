"""Tests for proportion fusion model utilities."""

import datetime as dt

import polars as pl
import pytest
from cfa.stf.forecasttools import read_tabular, write_tabular

from cfa.stf.routine.utils.prop_utils import create_prop_fusion_model


def _write_model_outputs(
    model_dir,
    samples: pl.DataFrame,
    data: pl.DataFrame,
) -> None:
    (model_dir / "data").mkdir(parents=True)
    write_tabular(samples, model_dir / "samples.parquet")
    write_tabular(data, model_dir / "data" / "combined_data.tsv")


def test_create_prop_fusion_model_aggregates_with_forecasttools(tmp_path):
    daily_dates = [dt.date(2025, 1, 5) + dt.timedelta(days=day) for day in range(14)]
    num_samples = pl.DataFrame(
        {
            "date": daily_dates,
            "geo_value": ["CA"] * 14,
            "disease": ["flu"] * 14,
            ".variable": ["observed_ed_visits"] * 14,
            ".value": list(range(1, 15)),
            ".draw": [1] * 14,
            ".chain": [1] * 14,
            ".iteration": list(range(1, 15)),
            "resolution": ["daily"] * 14,
            "data_type": ["forecast"] * 14,
        }
    )
    num_data = num_samples.drop(".draw", ".chain", ".iteration").with_columns(
        pl.lit("train").alias("data_type")
    )
    weekly_dates = [dt.date(2025, 1, 11), dt.date(2025, 1, 18)]
    other_samples = pl.DataFrame(
        {
            "date": weekly_dates,
            "geo_value": ["CA"] * 2,
            "disease": ["flu"] * 2,
            ".variable": ["other_ed_visits"] * 2,
            ".value": [63, 70],
            ".draw": [1, 1],
            "resolution": ["epiweekly"] * 2,
            "data_type": ["forecast"] * 2,
        }
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
    expected_values = [28 / (28 + 63), 77 / (77 + 70)]
    assert samples.get_column("date").to_list() == weekly_dates
    assert samples.get_column("resolution").unique().to_list() == ["epiweekly"]
    assert samples.get_column(".variable").unique().to_list() == [
        "prop_disease_ed_visits"
    ]
    assert samples.get_column(".value").to_list() == pytest.approx(expected_values)
    assert ".chain" not in samples.columns
    assert ".iteration" not in samples.columns
    assert data.get_column(".value").to_list() == pytest.approx(expected_values)


def test_create_prop_fusion_model_augments_samples_with_observations(tmp_path):
    training_date = dt.date(2025, 1, 1)
    forecast_date = dt.date(2025, 1, 2)
    num_samples = pl.DataFrame(
        {
            "date": [training_date] * 2 + [forecast_date] * 2,
            "geo_value": ["CA"] * 4,
            "disease": ["flu"] * 4,
            ".draw": [1, 2, 1, 2],
            "resolution": ["daily"] * 4,
            "data_type": ["train", "train", "forecast", "forecast"],
            ".variable": ["observed_ed_visits"] * 4,
            ".value": [2, 3, 4, 5],
        }
    )
    other_samples = pl.DataFrame(
        {
            "date": [forecast_date] * 2,
            "geo_value": ["CA"] * 2,
            "disease": ["flu"] * 2,
            ".draw": [2, 1],
            "resolution": ["daily"] * 2,
            "data_type": ["forecast"] * 2,
            ".variable": ["other_ed_visits"] * 2,
            ".value": [5, 6],
        }
    )
    num_data = num_samples.filter(pl.col("date") == training_date).drop(".draw")
    other_data = pl.DataFrame(
        {
            "date": [training_date],
            "geo_value": ["CA"],
            "disease": ["flu"],
            "resolution": ["daily"],
            "data_type": ["train"],
            ".variable": ["other_ed_visits"],
            ".value": [8],
        }
    )
    _write_model_outputs(tmp_path / "num_model", num_samples, num_data)
    _write_model_outputs(tmp_path / "other_model", other_samples, other_data)

    create_prop_fusion_model(
        model_run_dir=tmp_path,
        num_model_name="num_model",
        other_model_name="other_model",
    )

    samples = read_tabular(tmp_path / "prop_num_model_other_model" / "samples.parquet")
    assert samples.select("date", ".draw").rows() == [
        (training_date, 1),
        (training_date, 2),
        (forecast_date, 1),
        (forecast_date, 2),
    ]
    assert samples.get_column(".value").to_list() == pytest.approx(
        [2 / 10, 3 / 11, 4 / 10, 5 / 10]
    )
