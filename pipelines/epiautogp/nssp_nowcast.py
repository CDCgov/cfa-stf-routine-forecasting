"""
NSSP right-truncation nowcasting for EpiAutoGP.

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
class NsspRightTruncationNowcast:
    """
    Estimate NSSP nowcasts from PyRenew-style reporting-delay PMFs.

    The PMF support is daily reporting delay by convention; that is the source
    of the `frequency="daily"` requirement in `applies_to`.
    """

    reporting_delay_pmf: list[float]

    @staticmethod
    def applies_to(
        *,
        target: str,
        ed_visit_type: str,
        frequency: str,
    ) -> bool:
        return (
            target == "nssp"
            and ed_visit_type in ("observed", "other")
        )

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """
        Apply right-truncation correction to one daily NSSP time series.
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
