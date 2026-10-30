"""Simple nowcast source implementations for NowcastAutoGP."""

import datetime as dt
from dataclasses import dataclass

from cfa.stf.routine.data.nowcast import NowcastData
from cfa.stf.routine.nowcastautogp.config import NowcastAutoGPConfig


@dataclass(frozen=True)
class FixedNowcast:
    """Return precomputed nowcast data unchanged."""

    data: NowcastData

    @staticmethod
    def ensure_applicable(*, config: NowcastAutoGPConfig) -> None:
        """Accept every supported NowcastAutoGP configuration."""

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """Return the fixed nowcast without consulting observed data."""
        return self.data
