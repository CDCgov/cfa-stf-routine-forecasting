"""Unit tests for the shared lag-aware reporting-delay correction."""

import datetime as dt

import jax.numpy as jnp
import pytest
from pyrenew.convolve import compute_prop_already_reported

from cfa.stf.routine.data.reporting_delay import (
    correct_reports_by_lag,
    inflate_report,
    reporting_fractions_by_lag,
    reporting_inflation_factors,
)
from cfa.stf.routine.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast

# CDF 0.6, 0.8, 0.9, 0.99, 1.0: four incomplete lags.
PMF = [0.6, 0.2, 0.1, 0.09, 0.01]
REPORT_DATE = dt.date(2026, 9, 30)


def _dates_ending(last: dt.date, n: int) -> list[dt.date]:
    return [last - dt.timedelta(days=i) for i in reversed(range(n))]


@pytest.mark.parametrize("exclude_last_n_days", [0, 1, 2])
def test_each_report_uses_its_true_lag(exclude_last_n_days):
    last = REPORT_DATE - dt.timedelta(days=exclude_last_n_days + 1)
    dates = _dates_ending(last, 8)
    reports = [10.0 + i for i in range(8)]
    factors = reporting_inflation_factors(PMF)

    corrected, fractions = correct_reports_by_lag(
        dates=dates, reports=reports, pmf=PMF, report_date=REPORT_DATE
    )

    lags = [(REPORT_DATE - date).days for date in dates]
    expected_fractions = [
        factors[lag - 1] if lag <= len(factors) else 1.0 for lag in lags
    ]
    assert fractions == pytest.approx(expected_fractions)
    assert corrected == pytest.approx(
        [
            inflate_report(report, fraction)
            for report, fraction in zip(reports, expected_fractions, strict=True)
        ]
    )
    # The newest report sits at lag exclude_last_n_days + 1 and gets that CDF entry,
    # not the first one.
    assert fractions[-1] == pytest.approx(factors[exclude_last_n_days])


def test_matches_positional_nowcast_when_series_ends_the_day_before_report():
    dates = _dates_ending(REPORT_DATE - dt.timedelta(days=1), 10)
    reports = [float(20 + i) for i in range(10)]

    corrected, _ = correct_reports_by_lag(
        dates=dates, reports=reports, pmf=PMF, report_date=REPORT_DATE
    )
    positional = ReportingDelayNowcast(reporting_delay_pmf=PMF).get_nowcast_data(
        dates=dates, reports=reports
    )

    n = len(positional.dates)
    assert dates[-n:] == positional.dates
    assert corrected[-n:] == pytest.approx(positional.reports[0])
    assert corrected[:-n] == pytest.approx(reports[:-n])


def test_series_shorter_than_incomplete_tail():
    dates = _dates_ending(REPORT_DATE - dt.timedelta(days=1), 2)
    corrected, fractions = correct_reports_by_lag(
        dates=dates, reports=[5.0, 3.0], pmf=PMF, report_date=REPORT_DATE
    )
    assert fractions == pytest.approx([0.8, 0.6])
    assert corrected == pytest.approx([(5.0 + 0.2) / 0.8, (3.0 + 0.4) / 0.6])


def test_dates_beyond_the_pmf_are_fully_reported():
    dates = _dates_ending(REPORT_DATE - dt.timedelta(days=30), 3)
    corrected, fractions = correct_reports_by_lag(
        dates=dates, reports=[1.0, 2.0, 3.0], pmf=PMF, report_date=REPORT_DATE
    )
    assert fractions == [1.0, 1.0, 1.0]
    assert corrected == [1.0, 2.0, 3.0]


def test_zero_reporting_fraction():
    pmf = [0.0, 1.0]
    dates = _dates_ending(REPORT_DATE - dt.timedelta(days=1), 1)
    corrected, fractions = correct_reports_by_lag(
        dates=dates, reports=[0.0], pmf=pmf, report_date=REPORT_DATE
    )
    assert fractions == [0.0]
    assert corrected == [0.0]
    with pytest.raises(ValueError, match="zero reporting fraction"):
        correct_reports_by_lag(
            dates=dates, reports=[2.0], pmf=pmf, report_date=REPORT_DATE
        )


def test_rejects_reference_dates_on_or_after_the_report_date():
    with pytest.raises(ValueError, match="not before report date"):
        correct_reports_by_lag(
            dates=[REPORT_DATE], reports=[1.0], pmf=PMF, report_date=REPORT_DATE
        )


def test_rejects_length_mismatch():
    with pytest.raises(ValueError, match="same length"):
        correct_reports_by_lag(
            dates=_dates_ending(REPORT_DATE - dt.timedelta(days=1), 2),
            reports=[1.0],
            pmf=PMF,
            report_date=REPORT_DATE,
        )


@pytest.mark.parametrize("exclude_last_n_days", [0, 1, 2, 6])
def test_fractions_match_pyrenew_prop_already_reported(exclude_last_n_days):
    last = REPORT_DATE - dt.timedelta(days=exclude_last_n_days + 1)
    dates = _dates_ending(last, 8)
    # ForecastRun.right_truncation_offset for a series ending at `last`.
    right_truncation_offset = (REPORT_DATE - last).days - 1

    fractions = reporting_fractions_by_lag(
        dates=dates, pmf=PMF, report_date=REPORT_DATE
    )

    expected = compute_prop_already_reported(
        jnp.array(PMF), len(dates), right_truncation_offset
    )
    assert fractions == pytest.approx(expected.tolist())
