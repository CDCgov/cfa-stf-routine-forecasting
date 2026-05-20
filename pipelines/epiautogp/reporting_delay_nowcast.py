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
from dataclasses import dataclass

from pipelines.epiautogp.nowcast import NowcastData
from pipelines.epiautogp.reporting_delay import (
    inflate_report,
    reporting_inflation_factors,
)


@dataclass(frozen=True)
class ReportingDelayNowcast:
    """
    Estimate nowcasts by inflating recent observations with a reporting-delay PMF.

    The PMF support is daily reporting delay by convention; the resolver logs a
    soft warning if used with a non-daily series.
    """

    reporting_delay_pmf: list[float]

    @staticmethod
    def applies_to(
        *,
        target: str = "nssp",
        ed_visit_type: str = "observed",
        frequency: str = "daily",
    ) -> bool:
        # The estimator multiplies recent observations by 1/reporting_fraction.
        # For a percentage (numerator / denominator) the same factor applies to
        # both terms and cancels, so reject ed_visit_type="pct". Target and
        # frequency are not gating conditions: any count series can be
        # corrected when paired with a PMF on its native cadence.
        return ed_visit_type != "pct"

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
