"""
Generic nowcast objects for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass

from pipelines.data.nowcast import NowcastData
from pipelines.epiautogp.forecast_spec import ForecastSpec


@dataclass(frozen=True)
class FixedNowcast:
    """
    Simple nowcast source that just returns a fixed set of nowcast data
    """

    data: NowcastData

    @staticmethod
    def applies_to(
        *,
        forecast_spec: ForecastSpec,
    ) -> bool:
        return True

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        return self.data
