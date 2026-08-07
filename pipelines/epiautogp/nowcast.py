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
    def ensure_applicable(
        *,
        forecast_spec: ForecastSpec,
    ) -> None:
        pass

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        return self.data
