"""Mechanistic daily NSSP trend model (basic_seir_daily_nssp_trend) pipeline."""

import datetime as dt
import logging
from pathlib import Path

from cfa.stf.data import get_nnh_right_truncation_pmf

from cfa.stf.routine._paths import MECH_NSSP_DAILY_DIR
from cfa.stf.routine.data.data_access import ForecastSourceName
from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.mech_nssp_daily.prep_mech_nssp_daily_data import (
    convert_to_mech_nssp_daily_json,
)
from cfa.stf.routine.utils.language_utils import run_julia_script

_FIT_SCRIPT = MECH_NSSP_DAILY_DIR / "fit_mech_nssp_daily.jl"
SUPPORTED_DISEASES = frozenset({"covid"})
# The Julia weekday estimator needs at least five weeks in its window.
MIN_DOW_WINDOW_DAYS = 35


def run_mech_nssp_daily_forecast(
    json_input_path: Path,
    model_dir: Path,
    *,
    n_particles: int,
    n_forecast_draws: int,
    seed: int,
    min_observations: int,
    dow_window_days: int,
    dow_min_exclude_days: int,
    n_threads: int,
) -> None:
    """Run the repository's Julia runner for the daily NSSP trend model."""
    model_dir.mkdir(parents=True, exist_ok=True)
    args = [
        f"--json-input={json_input_path}",
        f"--output-dir={model_dir}",
        f"--n-particles={n_particles}",
        f"--n-forecast-draws={n_forecast_draws}",
        f"--seed={seed}",
        f"--min-observations={min_observations}",
        f"--dow-window-days={dow_window_days}",
        f"--dow-min-exclude-days={dow_min_exclude_days}",
    ]
    run_julia_script(
        _FIT_SCRIPT,
        args,
        executor_flags=[
            f"--project={MECH_NSSP_DAILY_DIR}",
            f"--threads={n_threads}",
        ],
        function_name="run_mech_nssp_daily_forecast",
        capture_output=False,
    )


class MechNSSPDailyPipeline(ForecastPipeline):
    """Single-location pipeline for the mechanistic daily NSSP trend model."""

    def __init__(
        self,
        *,
        n_particles: int = 3000,
        n_forecast_draws: int = 2000,
        seed: int = 2026,
        n_threads: int = 4,
        min_observations: int = 84,
        dow_window_days: int = 365,
        dow_min_exclude_days: int = 14,
        reporting_delay_pmf: list[float] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.n_particles = n_particles
        self.n_forecast_draws = n_forecast_draws
        self.seed = seed
        # A fixed thread count, never "auto": seeded particle-filter runs only
        # reproduce at a fixed number of threads.
        self.n_threads = n_threads
        self.min_observations = min_observations
        self.dow_window_days = dow_window_days
        self.dow_min_exclude_days = dow_min_exclude_days
        self.reporting_delay_pmf = reporting_delay_pmf

    @property
    def model_name(self) -> str:
        return "mech_nssp_daily"

    @property
    def sources(self) -> set[ForecastSourceName]:
        return {"nssp"}

    @property
    def minimum_exclude_last_n_days(self) -> int:
        # The backtest fitted through report_date - 1; the runner nowcasts the
        # provisional tail by lag, so no omitted days are required.
        return 0

    def validate_configuration(self) -> None:
        if self.disease not in SUPPORTED_DISEASES:
            raise ValueError(
                f"mech_nssp_daily supports diseases {sorted(SUPPORTED_DISEASES)}; "
                f"got {self.disease!r} (its priors are COVID-informed)"
            )
        for name in (
            "n_particles",
            "n_forecast_draws",
            "n_threads",
            "min_observations",
            "dow_window_days",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer, got {value!r}")
        if self.dow_min_exclude_days < 0:
            raise ValueError(
                "dow_min_exclude_days must be non-negative, "
                f"got {self.dow_min_exclude_days!r}"
            )
        if self.dow_window_days < MIN_DOW_WINDOW_DAYS:
            raise ValueError(
                f"dow_window_days must be at least {MIN_DOW_WINDOW_DAYS} days, "
                f"got {self.dow_window_days}"
            )
        if self.reporting_delay_pmf is not None:
            if not self.reporting_delay_pmf or any(
                weight < 0 for weight in self.reporting_delay_pmf
            ):
                raise ValueError(
                    "reporting_delay_pmf must be a non-empty list of "
                    "non-negative weights"
                )

    def _resolve_reporting_delay_pmf(self, run: ForecastRun) -> list[float]:
        if self.reporting_delay_pmf is not None:
            return list(self.reporting_delay_pmf)
        return get_nnh_right_truncation_pmf(
            state_abb=run.loc,
            disease=run.disease,
            as_of=run.report_date,
            reference_date=run.report_date,
        )

    def prepare_model_artifacts(self, run: ForecastRun) -> None:
        if run.is_stale:
            self.logger.warning(
                "NSSP data for %s is stale relative to run date %s; reporting lags "
                "are measured from the run date, so the nowcast will under-correct.",
                run.loc,
                run.report_date,
            )
        self.logger.info("Converting data to mech_nssp_daily JSON format...")
        convert_to_mech_nssp_daily_json(
            forecast_run=run,
            reporting_delay_pmf=self._resolve_reporting_delay_pmf(run),
            logger=self.logger,
        )

    def run_model(self, run: ForecastRun) -> None:
        self.logger.info("Fitting the mechanistic daily NSSP trend model...")
        run_mech_nssp_daily_forecast(
            json_input_path=run.model_dir / f"{run.model_name}_input.json",
            model_dir=run.model_dir,
            n_particles=self.n_particles,
            n_forecast_draws=self.n_forecast_draws,
            seed=self.seed,
            min_observations=self.min_observations,
            dow_window_days=self.dow_window_days,
            dow_min_exclude_days=self.dow_min_exclude_days,
            n_threads=self.n_threads,
        )


def main(
    disease: str,
    loc: str,
    output_dir: Path | str,
    n_lookback_days: int | None,
    run_date: dt.date,
    exclude_last_n_days: int = 0,
    fail_on_stale_data: bool = False,
    n_particles: int = 3000,
    n_forecast_draws: int = 2000,
    seed: int = 2026,
    n_threads: int = 4,
    min_observations: int = 84,
    dow_window_days: int = 365,
    dow_min_exclude_days: int = 14,
    reporting_delay_pmf: list[float] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Run the complete mech_nssp_daily pipeline for one location."""
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    MechNSSPDailyPipeline(
        disease=disease,
        loc=loc,
        output_dir=output_dir,
        n_lookback_days=n_lookback_days,
        run_date=run_date,
        exclude_last_n_days=exclude_last_n_days,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
        n_particles=n_particles,
        n_forecast_draws=n_forecast_draws,
        seed=seed,
        n_threads=n_threads,
        min_observations=min_observations,
        dow_window_days=dow_window_days,
        dow_min_exclude_days=dow_min_exclude_days,
        reporting_delay_pmf=reporting_delay_pmf,
    ).execute()
