"""Utilities for creating and appending disease proportion data."""

import datetime as dt
from pathlib import Path

import polars as pl
from cfa.stf.forecasttools import (
    append_prop_data,
    augment_samples_with_observations,
    create_proportions,
    read_tabular,
    write_tabular,
)

from cfa.stf.routine.utils.data_utils import aggregate_long_to_epiweekly


def _latest_training_date(data: pl.DataFrame, *, model_name: str) -> dt.date:
    if "data_type" not in data.columns:
        raise ValueError(f"{model_name} data is missing required column: data_type")
    latest_training_date = (
        data.filter(pl.col("data_type") == "train").get_column("date").max()
    )
    if latest_training_date is None:
        raise ValueError(f"{model_name} data contains no training observations")
    return latest_training_date


def _drop_nonpreferred_data_type(
    *,
    num_data: pl.DataFrame,
    other_data: pl.DataFrame,
    num_model_name: str,
    other_model_name: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    num_last_training_date = _latest_training_date(
        num_data,
        model_name=num_model_name,
    )
    other_last_training_date = _latest_training_date(
        other_data,
        model_name=other_model_name,
    )
    if num_last_training_date >= other_last_training_date:
        return num_data, other_data.drop("data_type")
    return num_data.drop("data_type"), other_data


def create_prop_fusion_model(
    model_run_dir: Path | str,
    num_model_name: str,
    other_model_name: str,
    num_var_name: str = "observed_ed_visits",
    other_var_name: str = "other_ed_visits",
    prop_var_name: str = "prop_disease_ed_visits",
    augment_num_with_obs: bool = False,
    augment_other_with_obs: bool = True,
    aggregate_num: bool = False,
    aggregate_other: bool = False,
) -> None:
    """Create and save a proportion fusion model from two model outputs."""
    model_run_dir = Path(model_run_dir)

    def read_model_output(
        model_name: str, var_name: str, filename: str
    ) -> pl.DataFrame:
        data = read_tabular(model_run_dir / model_name / filename)
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
        num_model_name, num_var_name, "samples.parquet"
    ).drop("data_type", strict=False)
    other_samples = read_model_output(
        other_model_name, other_var_name, "samples.parquet"
    ).drop("data_type", strict=False)
    num_data = read_model_output(num_model_name, num_var_name, "data/combined_data.tsv")
    other_data = read_model_output(
        other_model_name, other_var_name, "data/combined_data.tsv"
    )

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

    prop_samples = create_proportions(
        numerator_df=num_samples.drop("data_type", strict=False),
        other_df=other_samples.drop("data_type", strict=False),
        num_val_col=num_var_name,
        other_val_col=other_var_name,
        prop_var=prop_var_name,
    ).sort("date", ".draw")
    num_prop_data, other_prop_data = _drop_nonpreferred_data_type(
        num_data=num_data,
        other_data=other_data,
        num_model_name=num_model_name,
        other_model_name=other_model_name,
    )
    prop_data = create_proportions(
        numerator_df=num_prop_data,
        other_df=other_prop_data,
        num_val_col=num_var_name,
        other_val_col=other_var_name,
        prop_var=prop_var_name,
    )

    def aggregated_model_name(model_name: str, aggregate: bool) -> str:
        return f"epiweekly_aggregated_{model_name}" if aggregate else model_name

    prop_model_name = "_".join(
        [
            "prop",
            aggregated_model_name(num_model_name, aggregate_num),
            aggregated_model_name(other_model_name, aggregate_other),
        ]
    )
    prop_model_dir = model_run_dir / prop_model_name
    data_dir = prop_model_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    write_tabular(prop_samples, prop_model_dir / "samples.parquet")
    write_tabular(prop_data, data_dir / "combined_data.tsv")


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
