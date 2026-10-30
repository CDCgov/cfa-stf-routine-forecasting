"""Date boundaries and output identity for a routine forecast."""

import datetime as dt
from dataclasses import dataclass

from cfa.stf.forecasttools import ceiling_mmwr_epiweek

from cfa.stf.routine.lookback import format_lookback_days

EARLIEST_TRAINING_DATE = dt.date(2022, 3, 1)


@dataclass(frozen=True)
class ForecastWindow:
    """Report-anchored training and forecast boundaries."""

    report_date: dt.date
    n_lookback_days: int | None = None

    def __post_init__(self) -> None:
        if self.n_lookback_days is not None and self.n_lookback_days <= 0:
            raise ValueError("n_lookback_days must be positive.")

    @property
    def min_allowed_training_date(self) -> dt.date:
        """Earliest date that may be retained for training."""
        requested_start = (
            dt.date.min
            if self.n_lookback_days is None
            else self.report_date - dt.timedelta(days=self.n_lookback_days)
        )
        return max(requested_start, EARLIEST_TRAINING_DATE)

    @property
    def max_allowed_training_date(self) -> dt.date:
        """Latest date that may be retained for training."""
        # The latest available observation is normally report_date - 1.
        return self.report_date - dt.timedelta(days=1)

    @property
    def forecast_through(self) -> dt.date:
        """Last target date, three MMWR epiweeks beyond the report date."""
        return ceiling_mmwr_epiweek(self.report_date + dt.timedelta(weeks=3))

    def model_batch_dir_name(self, disease: str) -> str:
        """Return the batch directory name for this window and disease."""
        lookback = format_lookback_days(self.n_lookback_days)
        return f"{disease}_lookback-{lookback}"
