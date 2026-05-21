"""Unit tests for nowcast sources and reporting-delay helpers."""

import datetime as dt
import math

import pytest

from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.nowcast import FixedNowcast, NowcastData
from pipelines.epiautogp.reporting_delay import (
    inflate_report,
    reporting_inflation_factors,
)
from pipelines.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast


def _spec(*, target: str = "nssp", ed_visit_type: str = "observed",
          frequency: str = "daily") -> ForecastSpec:
    """Build a ForecastSpec varying only the fields applies_to cares about."""
    return ForecastSpec(
        disease="COVID-19",
        loc="CA",
        report_date=dt.date(2024, 1, 1),
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
    )


class TestReportingInflationFactors:
    def test_drops_fully_reported_tail(self):
        # CDF = [0.4, 0.9, 1.0]; the 1.0 is fully reported and dropped.
        assert reporting_inflation_factors([0.4, 0.5, 0.1]) == pytest.approx([0.4, 0.9])

    def test_all_partial(self):
        assert reporting_inflation_factors([0.3, 0.4]) == pytest.approx([0.3, 0.7])

    def test_single_complete_bin_yields_empty(self):
        assert reporting_inflation_factors([1.0]) == []

    def test_empty_pmf(self):
        assert reporting_inflation_factors([]) == []


class TestInflateReport:
    def test_basic_inflation(self):
        # (report + 1 - fraction) / fraction; report=4, fraction=0.5
        assert inflate_report(4.0, 0.5) == pytest.approx((4.0 + 0.5) / 0.5)

    def test_zero_report_with_zero_fraction(self):
        assert inflate_report(0.0, 0.0) == 0.0

    def test_positive_report_with_zero_fraction_raises(self):
        with pytest.raises(ValueError, match="zero reporting fraction"):
            inflate_report(3.0, 0.0)

    def test_negative_fraction_raises(self):
        with pytest.raises(ValueError, match="nonnegative"):
            inflate_report(1.0, -0.1)


class TestReportingDelayNowcastAppliesTo:
    @pytest.mark.parametrize(
        ("target", "ed_visit_type", "expected"),
        [
            ("nssp", "observed", True),
            ("nssp", "other", True),
            ("nssp", "pct", False),
            # NHSN has no ed_visit_type concept; the parameter defaults to
            # "observed" upstream and the source happily applies to its counts.
            ("nhsn", "observed", True),
        ],
    )
    def test_applies_to(self, target, ed_visit_type, expected):
        # Only ed_visit_type is gating; target and frequency are carried in the
        # spec for protocol compatibility but ignored here. The resolver
        # enforces daily cadence via a soft warning.
        spec = _spec(target=target, ed_visit_type=ed_visit_type, frequency="daily")
        assert ReportingDelayNowcast.applies_to(forecast_spec=spec) is expected


class TestReportingDelayNowcastGetNowcastData:
    def test_returns_inflated_estimates_for_partial_tail(self):
        # PMF = [0.5, 0.5] -> CDF = [0.5, 1.0] -> incomplete = [0.5];
        # only the most-recent observation gets inflated.
        source = ReportingDelayNowcast(reporting_delay_pmf=[0.5, 0.5])
        dates = [dt.date(2024, 1, 1), dt.date(2024, 1, 2), dt.date(2024, 1, 3)]
        reports = [10.0, 20.0, 4.0]

        result = source.get_nowcast_data(dates=dates, reports=reports)

        assert result.dates == [dt.date(2024, 1, 3)]
        assert len(result.reports) == 1
        # inflate_report(4, 0.5) = (4 + 0.5) / 0.5 = 9.0
        assert result.reports[0] == pytest.approx([9.0])

    def test_two_partial_rows(self):
        # PMF = [0.4, 0.5, 0.1] -> CDF = [0.4, 0.9, 1.0] -> incomplete = [0.4, 0.9]
        # Most-recent observation pairs with the smallest fraction (0.4).
        source = ReportingDelayNowcast(reporting_delay_pmf=[0.4, 0.5, 0.1])
        dates = [
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 2),
            dt.date(2024, 1, 3),
            dt.date(2024, 1, 4),
        ]
        reports = [10.0, 20.0, 8.0, 4.0]

        result = source.get_nowcast_data(dates=dates, reports=reports)

        assert result.dates == [dt.date(2024, 1, 3), dt.date(2024, 1, 4)]
        # The older partial row (Jan 3) uses fraction=0.9; the newest (Jan 4) uses 0.4.
        expected = [
            (8.0 + 1.0 - 0.9) / 0.9,
            (4.0 + 1.0 - 0.4) / 0.4,
        ]
        assert result.reports[0] == pytest.approx(expected)

    def test_fully_reported_pmf_returns_empty(self):
        source = ReportingDelayNowcast(reporting_delay_pmf=[1.0])
        result = source.get_nowcast_data(
            dates=[dt.date(2024, 1, 1)],
            reports=[5.0],
        )
        assert result == NowcastData()

    def test_length_mismatch_raises(self):
        source = ReportingDelayNowcast(reporting_delay_pmf=[0.5, 0.5])
        with pytest.raises(ValueError, match="same length"):
            source.get_nowcast_data(
                dates=[dt.date(2024, 1, 1)],
                reports=[1.0, 2.0],
            )

    def test_zero_report_with_zero_fraction_returns_zero(self):
        # If the most-recent reporting fraction were ever 0 with a 0 report,
        # inflate_report returns 0 rather than dividing by zero.
        # Construct a PMF whose first incomplete fraction is exactly 0.
        # This is contrived; the safety path matters more than realism.
        source = ReportingDelayNowcast(reporting_delay_pmf=[0.0, 1.0])
        result = source.get_nowcast_data(
            dates=[dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
            reports=[5.0, 0.0],
        )
        assert result.dates == [dt.date(2024, 1, 2)]
        assert result.reports[0] == [0.0]


class TestFixedNowcast:
    def test_applies_to_always_true(self):
        assert FixedNowcast.applies_to(forecast_spec=_spec()) is True

    def test_returns_stored_data(self):
        data = NowcastData(dates=[dt.date(2024, 1, 1)], reports=[[1.0]])
        source = FixedNowcast(data=data)
        out = source.get_nowcast_data(dates=[dt.date(2024, 5, 1)], reports=[42.0])
        assert out is data


def test_no_nan_in_inflate():
    # Sanity: typical numbers should not produce NaN/inf.
    value = inflate_report(7.0, 0.25)
    assert math.isfinite(value)
