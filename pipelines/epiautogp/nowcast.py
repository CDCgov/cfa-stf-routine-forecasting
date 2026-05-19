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


class NowcastModel(Protocol):
    """
    Interface for models that estimate nowcast data.
    """

    def estimate(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """
        Estimate nowcast data for one model run.
        """
        ...
