"""
Generic nowcast objects for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass

from cfa.stf.routine.data.nowcast import NowcastData
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig


@dataclass(frozen=True)
class FixedNowcast:
    """
    Simple nowcast source that just returns a fixed set of nowcast data
    """

    data: NowcastData

    @staticmethod
    def ensure_applicable(
        *,
        config: EpiAutoGPConfig,
    ) -> None:
        pass

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        return self.data
