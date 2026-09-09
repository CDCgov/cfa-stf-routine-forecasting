"""Canonical state for one materialized forecast run."""

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from cfa.stf.forecasttools import ceiling_mmwr_epiweek

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
    model_name: str
    model_batch_dir: Path
    surveillance: SurveillanceInputs

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_batch_dir", Path(self.model_batch_dir))

    @property
    def forecast_through(self) -> dt.date:
        """Last target date, three MMWR epiweeks beyond the report date."""
        return ceiling_mmwr_epiweek(self.report_date + dt.timedelta(weeks=3))

    @property
    def last_training_date(self) -> dt.date:
        """Latest observed training date across the run's data sources."""
        return max(source.last_training_date for source in self.surveillance.sources)

    @property
    def n_forecast_days(self) -> int:
        """Number of days after the last training date through the last target date."""
        return (self.forecast_through - self.last_training_date).days

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
