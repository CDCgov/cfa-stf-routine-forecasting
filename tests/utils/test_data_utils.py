"""Tests for forecast dataset aggregation utilities."""

import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal

from cfa.stf.routine.utils.data_utils import (
    aggregate_long_to_epiweekly,
    aggregate_nssp_to_epiweekly,
)


def test_aggregate_long_to_epiweekly_accepts_date_and_value_columns():
    data = pl.DataFrame(
        {
            "observation_date": pl.date_range(
                dt.date(2025, 1, 5), dt.date(2025, 1, 11), eager=True
            ),
            "visits": pl.int_range(1, 8, eager=True),
        }
    ).with_columns(
        pl.lit("CA").alias("location"),
        pl.lit("daily").alias("resolution"),
    )

    result = aggregate_long_to_epiweekly(
        data,
        date_col="observation_date",
        value_col="visits",
    )

    expected = pl.DataFrame(
        {
            "observation_date": [dt.date(2025, 1, 11)],
            "visits": [28],
            "resolution": ["epiweekly"],
        }
    )
    assert_frame_equal(
        result.select("observation_date", "visits", "resolution"), expected
    )


def test_aggregate_nssp_to_epiweekly_preserves_long_source_schema():
    dates = pl.date_range(dt.date(2025, 1, 5), dt.date(2025, 1, 18), eager=True)
    variables = ("observed_ed_visits", "other_ed_visits")
    observed_ed_visits = range(1, len(dates) + 1)
    other_ed_visits = 9
    data = pl.DataFrame(
        {
            "date": date,
            "state_abb": "CA",
            ".variable": variable,
            ".value": value,
            "data_type": "train",
            "resolution": "daily",
        }
        for date, observed in zip(dates, observed_ed_visits, strict=True)
        for variable, value in zip(
            variables,
            (observed, other_ed_visits),
            strict=True,
        )
    )

    result = aggregate_nssp_to_epiweekly(data)
    week_end_dates = (dt.date(2025, 1, 11), dt.date(2025, 1, 18))
    weekly_values = ((28, 63), (77, 63))
    expected = pl.DataFrame(
        {
            "date": date,
            "state_abb": "CA",
            ".variable": variable,
            ".value": value,
            "data_type": "train",
            "resolution": "epiweekly",
        }
        for date, values in zip(week_end_dates, weekly_values, strict=True)
        for variable, value in zip(variables, values, strict=True)
    )
    assert_frame_equal(result, expected)
