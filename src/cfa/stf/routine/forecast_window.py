"""Date boundaries and output identity for a routine forecast."""

import datetime as dt
from dataclasses import dataclass

from cfa.stf.forecasttools import ceiling_mmwr_epiweek


@dataclass(frozen=True)
class ForecastWindow:
    """Report-anchored training and forecast boundaries."""

    report_date: dt.date
    n_lookback_days: int | None = None
    exclude_last_n_days: int = 0

    def __post_init__(self) -> None:
        if self.n_lookback_days is not None and self.n_lookback_days <= 0:
            raise ValueError("n_lookback_days must be positive.")
        if self.exclude_last_n_days < 0:
            raise ValueError("exclude_last_n_days must be nonnegative.")
        if (
            self.n_lookback_days is not None
            and self.exclude_last_n_days >= self.n_lookback_days
        ):
            raise ValueError("exclude_last_n_days must be less than n_lookback_days.")

    @property
    def min_allowed_training_date(self) -> dt.date | None:
        """Earliest date that may be retained for training."""
        if self.n_lookback_days is None:
            return None
        return self.report_date - dt.timedelta(days=self.n_lookback_days)

    @property
    def max_allowed_training_date(self) -> dt.date:
        """Latest date that may be retained for training."""
        # The latest available observation is normally report_date - 1.
        return self.report_date - dt.timedelta(days=self.exclude_last_n_days + 1)

    @property
    def forecast_through(self) -> dt.date:
        """Last target date, three MMWR epiweeks beyond the report date."""
        return ceiling_mmwr_epiweek(self.report_date + dt.timedelta(weeks=3))

    def model_batch_dir_name(self, disease: str) -> str:
        """Return the batch directory name for this window and disease."""
        lookback = self.n_lookback_days if self.n_lookback_days is not None else "all"
        return f"{disease}_lookback-{lookback}_omit-{self.exclude_last_n_days}"
