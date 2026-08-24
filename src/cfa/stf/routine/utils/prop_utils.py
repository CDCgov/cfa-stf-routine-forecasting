"""Utilities for creating and appending disease proportion data."""

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

    num_samples = read_model_output(num_model_name, num_var_name, "samples.parquet")
    other_samples = read_model_output(
        other_model_name, other_var_name, "samples.parquet"
    )
    num_data = read_model_output(num_model_name, num_var_name, "data/combined_data.tsv")
    other_data = read_model_output(
        other_model_name, other_var_name, "data/combined_data.tsv"
    )

    if augment_num_with_obs:
        num_samples = augment_samples_with_observations(num_samples, num_data)
    if augment_other_with_obs:
        other_samples = augment_samples_with_observations(other_samples, other_data)
    if aggregate_num:
        num_samples = aggregate_long_to_epiweekly(num_samples, value_col=num_var_name)
        num_data = aggregate_long_to_epiweekly(num_data, value_col=num_var_name)
    if aggregate_other:
        other_samples = aggregate_long_to_epiweekly(
            other_samples, value_col=other_var_name
        )
        other_data = aggregate_long_to_epiweekly(other_data, value_col=other_var_name)

    prop_samples = create_proportions(
        numerator_df=num_samples,
        other_df=other_samples,
        num_val_col=num_var_name,
        other_val_col=other_var_name,
        prop_var=prop_var_name,
    ).sort("date", ".draw")
    prop_data = create_proportions(
        numerator_df=num_data,
        other_df=other_data,
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


def append_prop_data_to_combined_data(
    data: pl.DataFrame,
    observed_var: str = "observed_ed_visits",
    other_var: str = "other_ed_visits",
    prop_var: str = "prop_disease_ed_visits",
) -> pl.DataFrame:
    """Append disease ED visit proportion rows when both inputs are available."""
    required_vars = {observed_var, other_var}
    available_vars = set(data.get_column(".variable").unique().to_list())
    present_required_vars = required_vars & available_vars
    if not present_required_vars:
        return data
    if present_required_vars != required_vars:
        missing_vars = ", ".join(sorted(required_vars - available_vars))
        raise ValueError(
            "Cannot append ED visit proportions from incomplete NSSP data; "
            f"missing variable(s): {missing_vars}"
        )

    return append_prop_data(
        data,
        observed_var=observed_var,
        other_var=other_var,
        prop_var=prop_var,
    )
