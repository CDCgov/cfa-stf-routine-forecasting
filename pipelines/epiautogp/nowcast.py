"""
Generic nowcast objects for EpiAutoGP.
"""

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class NowcastData:
    """
    Dates and report series for EpiAutoGP nowcasting.
    """

    dates: list[dt.date] = field(default_factory=list)
    reports: list[list[float]] = field(default_factory=list)


class NowcastModel(Protocol):
    """
    Interface for models that estimate EpiAutoGP nowcast data.
    """

    def estimate(
        self,
        *,
        combined_data_path: Path | str,
        disease: str,
        loc: str,
        report_date: dt.date,
        frequency: str,
        ed_visit_type: str,
        exclude_date_ranges: list[tuple[dt.date, dt.date]] | None = None,
    ) -> NowcastData:
        """
        Estimate nowcast data for one EpiAutoGP model run.
        """
        ...

