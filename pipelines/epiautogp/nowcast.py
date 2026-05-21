"""
Generic nowcast objects for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass, field
from typing import Protocol

from pipelines.epiautogp.forecast_spec import ForecastSpec


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

    A source declares which pipeline configurations it can meaningfully nowcast with a particular nowcasting
    approach via `applies_to`. The resolver in `epiautogp_forecast_utils` uses this to
    decide whether to wire the source into the pipeline (under "auto") or to
    error out (under explicit selection).
    """

    def applies_to(
        self,
        *,
        forecast_spec: ForecastSpec,
    ) -> bool:
        """
        Whether this source is applicable to a particular pipeline configuration.
        """
        ...

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
