"""
Reporting-delay nowcasting for NowcastAutoGP.

The estimator inflates the most-recent observations of a daily count series by
the inverse of the reporting CDF. It is only meaningful for *count* targets:
applied to a percentage (numerator / denominator) the same inflation factor
would multiply both terms and cancel out, so the source declares itself
inapplicable to `ed_visit_type="pct"`. Producing useful percentage nowcasts
would require distinct PMFs for the numerator and denominator series (see
#1058).
"""

import datetime as dt
import logging
from dataclasses import dataclass
from itertools import accumulate

from cfa.stf.routine.data.nowcast import NowcastData
from cfa.stf.routine.nowcastautogp.config import NowcastAutoGPConfig

logger = logging.getLogger(__name__)
REPORTING_FRACTION_TOL = 1e-9


def reporting_inflation_factors(pmf: list[float]) -> list[float]:
    """Return incomplete reporting CDF entries, ordered by increasing delay."""
    return [
        fraction
        for fraction in accumulate(pmf)
        if fraction < 1.0 - REPORTING_FRACTION_TOL
    ]


def inflate_report(report: float, fraction: float) -> float:
    """Inflate one partial report using its expected reporting fraction."""
    if fraction < 0.0:
        raise ValueError(f"Reporting fraction must be nonnegative: {fraction}")
    if fraction == 0.0:
        if report == 0.0:
            return 0.0
        raise ValueError(
            "Cannot inflate a positive report with zero reporting fraction"
        )
    return (report + 1.0 - fraction) / fraction


@dataclass(frozen=True)
class ReportingDelayNowcast:
    """
    Estimate nowcasts by inflating recent observations with a reporting-delay PMF.

    The PMF support is daily reporting delay by convention; the resolver logs a
    soft warning if used with a non-daily series.
    """

    reporting_delay_pmf: list[float]
    reference_date: dt.date | None = None
    step_days: int = 1

    @staticmethod
    def ensure_applicable(
        *,
        config: NowcastAutoGPConfig,
    ) -> None:
        # The estimator multiplies recent observations by 1/reporting_fraction.
        # For a percentage (numerator / denominator) the same factor applies to
        # both terms and cancels, so reject ed_visit_type="pct". Target and
        # frequency are not gating conditions: any count series can be
        # corrected when paired with a PMF on its native cadence.
        if config.frequency != "daily":
            logger.warning(
                "Using reporting-delay nowcasting for frequency=%r. Confirm "
                "the reporting-delay PMF support matches the model cadence.",
                config.frequency,
            )

        if config.ed_visit_type == "pct":
            raise ValueError(
                "Reporting-delay nowcasting is not applicable when "
                "ed_visit_type='pct': applying the same reporting-delay "
                "inflation factor to the numerator and denominator would "
                "cancel out."
            )

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """
        Apply reporting-delay inflation to one daily time series.
        """
        if len(dates) != len(reports):
            raise ValueError("dates and reports must have the same length")

        incomplete_fractions = reporting_inflation_factors(self.reporting_delay_pmf)

        if not dates or not incomplete_fractions:
            return NowcastData()
        # PMF bin zero describes reference_date, normally report_date - 1.
        # Keep the anchor fixed when observations are missing or omitted.
        reference_date = self.reference_date or max(dates)
        nowcast_dates = []
        nowcast_estimates = []
        for date, report in zip(dates, reports, strict=True):
            age, remainder = divmod((reference_date - date).days, self.step_days)
            if age < 0 or remainder:
                raise ValueError(
                    "Observation dates must precede and align with reference_date"
                )
            if age < len(incomplete_fractions):
                nowcast_dates.append(date)
                nowcast_estimates.append(
                    inflate_report(float(report), incomplete_fractions[age])
                )
        if not nowcast_dates:
            return NowcastData()

        return NowcastData(dates=nowcast_dates, reports=[nowcast_estimates])
