"""NowcastAutoGP-specific forecast configuration."""

from dataclasses import dataclass

from cfa.stf.routine.data.data_access import DataResolution, ForecastSourceName


@dataclass(frozen=True)
class NowcastAutoGPConfig:
    """Options that define how NowcastAutoGP consumes a shared forecast run."""

    target: ForecastSourceName
    frequency: DataResolution
    ed_visit_type: str
