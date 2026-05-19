"""
Generic nowcast objects for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class NowcastData:
    """
    Dates and report series for nowcasting.
    """

    dates: list[dt.date] = field(default_factory=list)
    reports: list[list[float]] = field(default_factory=list)


class NowcastSource(Protocol):
    """
    Interface to getting nowcast data, whether from a fixed source or some estimation procedure.
    """

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """
        Estimate nowcast data for one model run.
        """
        ...


@dataclass(frozen=True)
class FixedNowcast:
    data: NowcastData

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        return self.data
