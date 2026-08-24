"""Utilities for preparing forecast pipeline datasets."""

import polars as pl
import polars.selectors as cs
from cfa.stf.forecasttools import daily_to_weekly


def aggregate_long_to_epiweekly(
    data: pl.DataFrame,
    date_col: str = "date",
    value_col: str = ".value",
) -> pl.DataFrame:
    """Aggregate daily values into complete MMWR weeks."""
    id_columns = list(cs.expand_selector(data, cs.exclude(date_col, value_col)))
    return (
        daily_to_weekly(
            data,
            value_col=value_col,
            date_col=date_col,
            id_cols=id_columns,
            weekly_value_name=value_col,
            standard="MMWR",
            with_week_end_date=True,
            week_end_date_name=date_col,
            strict=True,
        )
        .with_columns(pl.lit("epiweekly").alias("resolution"))
        .drop("week", "weekyear")
    )


def aggregate_nssp_to_epiweekly(data: pl.DataFrame) -> pl.DataFrame:
    """Aggregate wide daily NSSP data to complete MMWR weeks."""
    value_columns = ["observed_ed_visits", "other_ed_visits"]
    id_columns = [column for column in data.columns if column not in value_columns]
    long_data = data.unpivot(
        on=value_columns,
        index=id_columns,
        variable_name=".variable",
        value_name=".value",
    )
    return (
        aggregate_long_to_epiweekly(long_data)
        .pivot(on=".variable", index=id_columns, values=".value")
        .select(data.columns)
        .sort("date", "state_abb")
    )
