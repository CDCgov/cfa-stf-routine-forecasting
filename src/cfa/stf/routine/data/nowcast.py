"""Shared nowcast data objects and source protocol."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from cfa.stf.routine.epiautogp.forecast_spec import ForecastSpec


@dataclass(frozen=True)
class NowcastData:
    """Dates and report series for nowcasting."""

    dates: list[dt.date] = field(default_factory=list)
    reports: list[list[float]] = field(default_factory=list)


class NowcastSource(Protocol):
    """Interface for producing nowcast data for a forecast configuration."""

    def ensure_applicable(
        self,
        *,
        forecast_spec: ForecastSpec,
    ) -> None:
        """Ensure this source applies to a forecast configuration."""
        ...

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """Estimate nowcast data for one model run."""
        ...
