"""Shared nowcast data objects."""

import datetime as dt
from dataclasses import dataclass, field


@dataclass(frozen=True)
class NowcastData:
    """Dates and report series for nowcasting."""

    dates: list[dt.date] = field(default_factory=list)
    reports: list[list[float]] = field(default_factory=list)
