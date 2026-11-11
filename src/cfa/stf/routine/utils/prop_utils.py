"""Utilities for creating and appending disease proportion data."""

import datetime as dt
from pathlib import Path

import polars as pl
from cfa.stf.forecasttools import (
    append_prop_data,
    augment_samples_with_observations,
    ceiling_mmwr_epiweek,
    create_proportions,
    read_tabular,
    write_tabular,
)

from cfa.stf.routine.utils.data_utils import aggregate_long_to_epiweekly


def validate_forecast_date_coverage(
    samples: pl.DataFrame, origin: dt.date, through: dt.date
) -> None:
    """Require every draw to contain all forecast dates at its output resolution."""
    resolutions = samples["resolution"].unique().to_list()
    if len(resolutions) != 1 or resolutions[0] not in ("daily", "epiweekly"):
        raise ValueError("Expected one daily or epiweekly fusion resolution")
    weekly = resolutions[0] == "epiweekly"
    start = ceiling_mmwr_epiweek(origin) if weekly else origin
    if through < start:
        raise ValueError("Forecast horizon precedes the first required forecast date")
    required = set(
        pl.date_range(start, through, interval="1w" if weekly else "1d", eager=True)
    )
    groups = samples.partition_by(".draw") if ".draw" in samples.columns else [samples]
    if not groups or any(not required <= set(group["date"]) for group in groups):
        raise ValueError("Fusion samples do not cover the required forecast dates")


def create_prop_fusion(
    *,
    numerator_dir: Path,
    other_dir: Path,
    destination_dir: Path,
    num_var_name: str = "observed_ed_visits",
    other_var_name: str = "other_ed_visits",
    prop_var_name: str = "prop_disease_ed_visits",
    augment_num_with_obs: bool = False,
    augment_other_with_obs: bool = True,
    aggregate_num: bool = False,
    aggregate_other: bool = False,
) -> pl.DataFrame:
    """Save a proportion fusion model and return its constructed samples."""

    def read_model_output(
        model_dir: Path, var_name: str, filename: str
    ) -> pl.DataFrame:
        data = read_tabular(model_dir / filename)
        data = (
            data.filter(pl.col(".variable") == var_name)
            .drop(".variable")
            .rename({".value": var_name})
            .drop(".chain", ".iteration", strict=False)
        )
        populated_columns = [
            column
            for column in data.columns
            if not data.get_column(column).is_null().all()
        ]
        return data.select(populated_columns)

    num_samples = read_model_output(
        numerator_dir, num_var_name, "samples.parquet"
    ).drop("data_type", strict=False)
    other_samples = read_model_output(
        other_dir, other_var_name, "samples.parquet"
    ).drop("data_type", strict=False)
    num_data = read_model_output(numerator_dir, num_var_name, "data/combined_data.tsv")
    other_data = read_model_output(other_dir, other_var_name, "data/combined_data.tsv")

    if augment_num_with_obs:
        num_samples = augment_samples_with_observations(
            num_samples,
            num_data.drop("data_type"),
        )
    if augment_other_with_obs:
        other_samples = augment_samples_with_observations(
            other_samples,
            other_data.drop("data_type"),
        )
    if aggregate_num:
        num_samples = aggregate_long_to_epiweekly(num_samples, value_col=num_var_name)
        num_data = aggregate_long_to_epiweekly(num_data, value_col=num_var_name)
    if aggregate_other:
        other_samples = aggregate_long_to_epiweekly(
            other_samples, value_col=other_var_name
        )
        other_data = aggregate_long_to_epiweekly(other_data, value_col=other_var_name)

    if set(num_samples["resolution"]) != set(other_samples["resolution"]):
        raise ValueError("Fusion input resolutions differ")
    common_dates = set(num_samples["date"]) & set(other_samples["date"])
    num_samples = num_samples.filter(pl.col("date").is_in(sorted(common_dates)))
    other_samples = other_samples.filter(pl.col("date").is_in(sorted(common_dates)))

    prop_samples = create_proportions(
        numerator_df=num_samples,
        other_df=other_samples,
        num_val_col=num_var_name,
        other_val_col=other_var_name,
        prop_var=prop_var_name,
    ).sort("date", ".draw")
    if (
        prop_samples[".value"].null_count()
        or not prop_samples[".value"].is_finite().all()
    ):
        raise ValueError("Fusion output contains missing or nonfinite proportions")
    prop_data = (
        create_proportions(
            numerator_df=num_data.rename({"data_type": "num_data_type"}),
            other_df=other_data.rename({"data_type": "other_data_type"}),
            num_val_col=num_var_name,
            other_val_col=other_var_name,
            prop_var=prop_var_name,
        )
        .with_columns(
            pl.when(
                (pl.col("num_data_type") == "eval")
                & (pl.col("other_data_type") == "eval")
            )
            .then(pl.lit("eval"))
            .otherwise(pl.lit("train"))
            .alias("data_type")
        )
        .drop(
            "num_data_type",
            "other_data_type",
        )
    )

    prop_model_dir = destination_dir
    data_dir = prop_model_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    write_tabular(prop_samples, prop_model_dir / "samples.parquet")
    write_tabular(prop_data, data_dir / "combined_data.tsv")
    return prop_samples


def append_prop_ed_data(
    data: pl.DataFrame,
    observed_var: str = "observed_ed_visits",
    other_var: str = "other_ed_visits",
    prop_var: str = "prop_disease_ed_visits",
) -> pl.DataFrame:
    """Append disease ED visit proportion rows when both inputs are available."""
    required_vars = {observed_var, other_var}
    available_vars = set(data.get_column(".variable").unique().to_list())
    missing_vars = required_vars - available_vars
    if missing_vars:
        missing_vars_text = ", ".join(sorted(missing_vars))
        raise ValueError(
            "Cannot append ED visit proportions from incomplete NSSP data; "
            f"missing variable(s): {missing_vars_text}"
        )

    return append_prop_data(
        data,
        observed_var=observed_var,
        other_var=other_var,
        prop_var=prop_var,
    )
