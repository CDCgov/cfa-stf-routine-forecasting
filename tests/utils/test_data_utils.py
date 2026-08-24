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


def test_aggregate_nssp_to_epiweekly_preserves_wide_source_schema():
    data = pl.DataFrame(
        {
            "date": pl.date_range(
                dt.date(2025, 1, 5), dt.date(2025, 1, 18), eager=True
            ),
            "state_abb": ["CA"] * 14,
            "observed_ed_visits": pl.int_range(1, 15, eager=True),
            "other_ed_visits": [9] * 14,
            "data_type": ["train"] * 14,
            "resolution": ["daily"] * 14,
        }
    )

    result = aggregate_nssp_to_epiweekly(data)
    expected = pl.DataFrame(
        {
            "date": [dt.date(2025, 1, 11), dt.date(2025, 1, 18)],
            "state_abb": ["CA", "CA"],
            "observed_ed_visits": [28, 77],
            "other_ed_visits": [63, 63],
            "data_type": ["train", "train"],
            "resolution": ["epiweekly", "epiweekly"],
        }
    )
    assert_frame_equal(result, expected)
