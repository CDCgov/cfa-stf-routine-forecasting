"""
NSSP right-truncation nowcasting for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass
from itertools import accumulate

from pipelines.epiautogp.nowcast import NowcastData

REPORTING_FRACTION_TOL = 1e-9


@dataclass(frozen=True)
class NsspRightTruncationNowcast:
    """
    Estimate NSSP nowcasts from PyRenew-style right-truncation PMFs.
    """

    right_truncation_pmf: list[float]

    def estimate(
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

        # The right-truncation PMF has no explicit resolution metadata;
        # its support is daily reporting delay by convention.
        right_truncation_cdf = list(accumulate(self.right_truncation_pmf))
        incomplete_cdf = [
            fraction
            for fraction in right_truncation_cdf
            if fraction < 1.0 - REPORTING_FRACTION_TOL
        ]

        n_nowcast = min(len(reports), len(incomplete_cdf))
        if n_nowcast == 0:
            return NowcastData()

        # Well behaved estimate when cases are zero, see:
        # https://baselinenowcast.epinowcast.org/articles/model_definition.html#point-nowcast-generation
        nowcast_dates = dates[-n_nowcast:]
        nowcast_estimates = []
        for report, fraction in zip(
            reports[-n_nowcast:],
            reversed(incomplete_cdf[-n_nowcast:]),
        ):
            report = float(report)
            if fraction < 0.0:
                raise ValueError(
                    "Reporting fraction must be nonnegative: "
                    f"{fraction}"
                )
            if fraction == 0.0:
                if report == 0.0:
                    nowcast_estimates.append(0.0)
                    continue
                raise ValueError(
                    "Cannot nowcast a positive report with zero reporting fraction"
                )
            nowcast_estimates.append(
                (report + 1.0 - fraction) / fraction
            )

        return NowcastData(dates=nowcast_dates, reports=[nowcast_estimates])
