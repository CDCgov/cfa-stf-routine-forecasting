"""Tests for forecast dataset aggregation utilities."""

import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal

from cfa.stf.routine.utils.data_utils import (
    aggregate_ed_visits_to_epiweekly,
    aggregate_long_to_epiweekly,
)


def _with_metadata(data: pl.DataFrame, *, resolution: str) -> pl.DataFrame:
    return data.with_columns(
        geo_value=pl.lit("CA"),
        disease=pl.lit("flu"),
        data_type=pl.lit("train"),
        resolution=pl.lit(resolution),
    ).select(
        "date",
        "geo_value",
        "disease",
        "data_type",
        "resolution",
        ".variable",
        ".value",
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


def test_aggregate_ed_visits_to_epiweekly_preserves_other_data():
    daily_ed_data = _with_metadata(
        pl.DataFrame(
            {
                "date": pl.date_range(
                    dt.date(2025, 1, 5), dt.date(2025, 1, 18), eager=True
                ),
                "observed_ed_visits": pl.int_range(1, 15, eager=True),
                "other_ed_visits": 9,
            }
        ).unpivot(
            on=["observed_ed_visits", "other_ed_visits"],
            index="date",
            variable_name=".variable",
            value_name=".value",
        ),
        resolution="daily",
    )
    hospital_data = _with_metadata(
        pl.DataFrame(
            {
                "date": [dt.date(2025, 1, 11)],
                ".variable": ["observed_hospital_admissions"],
                ".value": [3],
            }
        ),
        resolution="epiweekly",
    )
    result = aggregate_ed_visits_to_epiweekly(pl.concat([daily_ed_data, hospital_data]))
    expected = _with_metadata(
        pl.DataFrame(
            [
                (dt.date(2025, 1, 11), "observed_ed_visits", 28),
                (dt.date(2025, 1, 11), "observed_hospital_admissions", 3),
                (dt.date(2025, 1, 11), "other_ed_visits", 63),
                (dt.date(2025, 1, 18), "observed_ed_visits", 77),
                (dt.date(2025, 1, 18), "other_ed_visits", 63),
            ],
            schema={"date": pl.Date, ".variable": pl.String, ".value": pl.Int64},
            orient="row",
        ),
        resolution="epiweekly",
    )
    assert_frame_equal(result, expected)
