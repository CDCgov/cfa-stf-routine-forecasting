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


def create_prop_fusion_model(
    model_run_dir: Path | str | None = None,
    num_model_name: str = "",
    other_model_name: str = "",
    num_var_name: str = "observed_ed_visits",
    other_var_name: str = "other_ed_visits",
    prop_var_name: str = "prop_disease_ed_visits",
    augment_num_with_obs: bool = False,
    augment_other_with_obs: bool = True,
    aggregate_num: bool = False,
    aggregate_other: bool = False,
    *,
    numerator_dir: Path | None = None,
    other_dir: Path | None = None,
    destination_dir: Path | None = None,
    forecast_date: dt.date | None = None,
    forecast_through: dt.date | None = None,
) -> None:
    """Create and save a proportion fusion model from two model outputs."""
    model_run_dir = Path(model_run_dir) if model_run_dir is not None else None
    input_dirs = {
        num_model_name: numerator_dir or model_run_dir / num_model_name,
        other_model_name: other_dir or model_run_dir / other_model_name,
    }

    def read_model_output(
        model_name: str, var_name: str, filename: str
    ) -> pl.DataFrame:
        data = read_tabular(input_dirs[model_name] / filename)
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

    common_dates = set(num_samples["date"]) & set(other_samples["date"])
    if forecast_date is not None and forecast_through is not None:
        from cfa.stf.forecasttools import ceiling_mmwr_epiweek

        weekly = aggregate_num or aggregate_other
        start = ceiling_mmwr_epiweek(forecast_date) if weekly else forecast_date
        step = dt.timedelta(days=7 if weekly else 1)
        required = set()
        while start <= forecast_through:
            required.add(start)
            start += step
        for frame in (num_samples, other_samples):
            groups = (
                frame.partition_by(".draw") if ".draw" in frame.columns else [frame]
            )
            if not groups or any(
                not required <= set(group["date"]) for group in groups
            ):
                raise ValueError(
                    "Fusion inputs do not cover the required forecast dates"
                )
    num_samples = num_samples.filter(pl.col("date").is_in(sorted(common_dates)))
    other_samples = other_samples.filter(pl.col("date").is_in(sorted(common_dates)))

    prop_samples = create_proportions(
        numerator_df=num_samples,
        other_df=other_samples,
        num_val_col=num_var_name,
        other_val_col=other_var_name,
        prop_var=prop_var_name,
    ).sort("date", ".draw")
    if forecast_date is not None and forecast_through is not None:
        groups = (
            prop_samples.partition_by(".draw")
            if ".draw" in prop_samples.columns
            else [prop_samples]
        )
        if not groups or any(not required <= set(group["date"]) for group in groups):
            raise ValueError("Fusion output does not cover the required forecast dates")
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

    def aggregated_model_name(model_name: str, aggregate: bool) -> str:
        return f"epiweekly_aggregated_{model_name}" if aggregate else model_name

    prop_model_name = "_".join(
        [
            "prop",
            aggregated_model_name(num_model_name, aggregate_num),
            aggregated_model_name(other_model_name, aggregate_other),
        ]
    )
    prop_model_dir = destination_dir or model_run_dir / prop_model_name
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
