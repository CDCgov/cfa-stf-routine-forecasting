"""Canonical state for one materialized forecast run."""

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from cfa.stf.routine.data.data_access import (
    DataFreshness,
    NHSNData,
    NSSPData,
    SurveillanceInputs,
)


@dataclass(frozen=True)
class ForecastRun:
    """Canonical shared state for one model, location, and forecast vintage."""

    disease: str
    loc: str
    report_date: dt.date
    first_training_date: dt.date
    last_training_date: dt.date
    n_forecast_days: int
    exclude_last_n_days: int
    model_name: str
    model_batch_dir: Path
    surveillance: SurveillanceInputs

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_batch_dir", Path(self.model_batch_dir))

    @property
    def model_run_dir(self) -> Path:
        return self.model_batch_dir / "model_runs" / self.loc

    @property
    def model_dir(self) -> Path:
        return self.model_run_dir / self.model_name

    @property
    def data_dir(self) -> Path:
        return self.model_dir / "data"

    @property
    def loc_pop(self) -> int:
        return self.surveillance.loc_pop

    @property
    def right_truncation_offset(self) -> int:
        # The first entry of a source right-truncation PMF corresponds to reports
        # for reference_date = report_date - 1 as of report_date.
        return (self.report_date - self.last_training_date).days - 1

    @property
    def nssp(self) -> NSSPData | None:
        return self.surveillance.nssp

    @property
    def nhsn(self) -> NHSNData | None:
        return self.surveillance.nhsn

    @property
    def freshness(self) -> tuple[DataFreshness, ...]:
        return self.surveillance.freshness

    @property
    def is_stale(self) -> bool:
        return self.surveillance.is_stale
