"""Utilities for preparing forecast pipeline datasets."""

from pathlib import Path

import polars as pl
import polars.selectors as cs
from cfa.stf.forecasttools import daily_to_weekly, read_tabular, write_tabular


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


def generate_epiweekly_data(data_dir: Path, overwrite_daily: bool = False) -> None:
    """Aggregate daily ED-visit data and write a combined epiweekly dataset."""
    data_dir = Path(data_dir)
    data_path = data_dir / "combined_data.tsv"
    output_path = (
        data_path if overwrite_daily else data_dir / "epiweekly_combined_data.tsv"
    )

    daily_data = read_tabular(data_path)
    ed_visit_filter = pl.col(".variable").str.ends_with("_ed_visits")
    daily_ed_data = daily_data.filter(ed_visit_filter)
    other_data = daily_data.filter(~ed_visit_filter)

    epiweekly_data = pl.concat(
        [aggregate_long_to_epiweekly(daily_ed_data), other_data],
        how="diagonal_relaxed",
    ).select(daily_data.columns)
    epiweekly_data = epiweekly_data.sort("date", ".variable")

    write_tabular(epiweekly_data, output_path)
