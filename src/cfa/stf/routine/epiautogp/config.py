"""EpiAutoGP-specific forecast configuration."""

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class EpiAutoGPConfig:
    """Options that define how EpiAutoGP consumes a shared forecast run."""

    target: str
    frequency: str
    ed_visit_type: str
    exclude_date_ranges: list[tuple[dt.date, dt.date]] | None = None
