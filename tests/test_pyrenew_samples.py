import datetime as dt

import polars as pl
from polars.testing import assert_frame_equal
from pyrenew_multisignal.hew import PyrenewHEWData

from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import format_pyrenew_samples
from cfa.stf.routine.pyrenew_hew.generate_predictive import (
    _build_forecast_data_through,
)


def test_build_forecast_data_reaches_final_epiweek():
    training_dates = [
        dt.date(2026, 8, 1) + dt.timedelta(weeks=week) for week in range(5)
    ]
    data = PyrenewHEWData(
        nhsn_training_data=pl.DataFrame(
            {
                "weekendingdate": training_dates,
                "jurisdiction": ["CA"] * len(training_dates),
                "hospital_admissions": [1.0] * len(training_dates),
            }
        ),
        nhsn_step_size=7,
    )
    forecast_through = dt.date(2026, 9, 26)

    forecast_data = _build_forecast_data_through(
        data,
        forecast_through,
        include_epiweekly=True,
    )

    assert forecast_data.last_hospital_admissions_date.astype(dt.date) == (
        forecast_through
    )


def test_build_forecast_data_ends_daily_spine_on_requested_date():
    training_dates = pl.date_range(
        dt.date(2026, 8, 1),
        dt.date(2026, 8, 29),
        eager=True,
    )
    data = PyrenewHEWData(
        nssp_training_data=pl.DataFrame(
            {
                "date": training_dates,
                "geo_value": ["CA"] * len(training_dates),
                "observed_ed_visits": [1.0] * len(training_dates),
                "other_ed_visits": [2.0] * len(training_dates),
            }
        ),
        nssp_step_size=1,
    )
    forecast_through = dt.date(2026, 9, 26)

    forecast_data = _build_forecast_data_through(
        data,
        forecast_through,
        include_epiweekly=False,
    )

    assert forecast_data.last_ed_visits_date.astype(dt.date) == forecast_through


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
