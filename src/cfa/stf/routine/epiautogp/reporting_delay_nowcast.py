"""
Reporting-delay nowcasting for EpiAutoGP.

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
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig

logger = logging.getLogger(__name__)
REPORTING_FRACTION_TOL = 1e-9


def reporting_inflation_factors(pmf: list[float]) -> list[float]:
    """Return incomplete reporting CDF entries, ordered oldest first."""
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

    @staticmethod
    def ensure_applicable(
        *,
        config: EpiAutoGPConfig,
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

        n_nowcast = min(len(reports), len(incomplete_fractions))
        if n_nowcast == 0:
            return NowcastData()

        nowcast_dates = dates[-n_nowcast:]
        nowcast_estimates = [
            inflate_report(float(report), fraction)
            for report, fraction in zip(
                reports[-n_nowcast:],
                reversed(incomplete_fractions[-n_nowcast:]),
            )
        ]

        return NowcastData(dates=nowcast_dates, reports=[nowcast_estimates])
