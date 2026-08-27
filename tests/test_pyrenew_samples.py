import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal

from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import format_pyrenew_samples


def test_format_pyrenew_samples_matches_previous_r_output():
    variables = [
        "observed_ed_visits",
        "other_ed_visits",
        "observed_hospital_admissions",
    ]
    dates = [
        dt.datetime(2024, 2, 4),
        dt.datetime(2024, 2, 5),
        dt.datetime(2024, 2, 6),
    ]
    posterior_predictive = pl.DataFrame(
        {
            "chain": [0] * 6 + [1] * 6,
            "draw": ([0] * 3 + [1] * 3) * 2,
            "variable": variables * 4,
            "value": [float(value) for value in range(12)],
            "date": dates * 4,
        }
    )
    actual = format_pyrenew_samples(
        posterior_predictive,
        geo_value="CA",
        disease="covid",
    )
    expected = pl.DataFrame(
        {
            ".chain": pl.Series([1.0] * 6 + [2.0] * 6, dtype=pl.Float64),
            ".iteration": pl.Series(
                ([1.0] * 3 + [2.0] * 3) * 2,
                dtype=pl.Float64,
            ),
            ".draw": pl.Series(
                [1] * 3 + [2] * 3 + [3] * 3 + [4] * 3,
                dtype=pl.Int32,
            ),
            "date": pl.Series(
                [value.date() for value in dates] * 4,
                dtype=pl.Date,
            ),
            "geo_value": ["CA"] * 12,
            "disease": ["covid"] * 12,
            ".variable": variables * 4,
            ".value": [float(value) for value in range(12)],
            "resolution": ["daily", "daily", "epiweekly"] * 4,
        }
    )
    assert_frame_equal(actual, expected)
