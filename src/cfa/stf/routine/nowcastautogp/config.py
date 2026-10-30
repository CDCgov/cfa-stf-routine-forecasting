"""NowcastAutoGP-specific forecast configuration."""

import datetime as dt
from dataclasses import dataclass

from cfa.stf.routine.data.data_access import DataResolution, ForecastSourceName


@dataclass(frozen=True)
class NowcastAutoGPConfig:
    """Options that define how NowcastAutoGP consumes a shared forecast run."""

    target: ForecastSourceName
    frequency: DataResolution
    ed_visit_type: str
    exclude_date_ranges: list[tuple[dt.date, dt.date]] | None = None
