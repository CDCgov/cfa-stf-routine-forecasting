"""Shared aggregation helpers for routine forecasting data."""

import polars as pl
import polars.selectors as cs
from cfa.stf.forecasttools import daily_to_weekly


def aggregate_long_to_epiweekly(data: pl.DataFrame) -> pl.DataFrame:
    """Aggregate long-form daily values into complete MMWR weeks."""
    id_columns = list(cs.expand_selector(data, cs.exclude("date", ".value")))
    return (
        daily_to_weekly(
            data,
            value_col=".value",
            date_col="date",
            id_cols=id_columns,
            weekly_value_name=".value",
            standard="MMWR",
            with_week_end_date=True,
            week_end_date_name="date",
            strict=True,
        )
        .with_columns(pl.lit("epiweekly").alias("resolution"))
        .drop("week", "weekyear")
    )
