"""Shared lifecycle for routine forecast pipelines."""

import datetime as dt
import logging
from abc import ABC, abstractmethod
from collections.abc import Collection
from dataclasses import asdict, replace
from pathlib import Path

from pydantic_core import to_jsonable_python

from cfa.stf.routine.data.data_access import (
    DataResolution,
    ForecastSourceName,
    load_surveillance_inputs,
)
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.run_storage import standalone_directory, write_json
from cfa.stf.routine.utils.r_utils import (
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)


class ForecastPipeline(ABC):
    """Template lifecycle shared by all single-location forecast pipelines."""

    def __init__(
        self,
        *,
        disease: str,
        loc: str,
        output_dir: Path | str,
        n_lookback_days: int | None = None,
        forecast_date: dt.date,
        exclude_last_n_days: int = 0,
        fail_on_stale_data: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.disease = disease
        self.loc = loc
        self.output_dir = Path(output_dir)
        self.requested_window = ForecastWindow(
            forecast_date=forecast_date,
            n_lookback_days=n_lookback_days,
            exclude_last_n_days=exclude_last_n_days,
        )
        self.fail_on_stale_data = fail_on_stale_data
        self.logger = logger or logging.getLogger(type(self).__module__)

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Name of the model output directory."""

    @property
    @abstractmethod
    def sources(self) -> Collection[ForecastSourceName]:
        """Surveillance sources required by this model configuration."""

    @property
    def ed_visit_input_resolution(self) -> DataResolution:
        """Resolution to use for serialized ED-visit inputs."""
        return "daily"

    @property
    def minimum_exclude_last_n_days(self) -> int:
        """Minimum recent calendar days to exclude from model training."""
        return 0

    def validate_configuration(self) -> None:
        """Validate model-specific configuration before loading data."""

    def invocation_config(self) -> dict:
        """Serializable parameters for standalone execution provenance."""
        return to_jsonable_python(
            {key: value for key, value in vars(self).items() if key != "logger"}
        )

    def build_forecast_run(self) -> ForecastRun:
        """Calculate shared run state and load the requested forecast inputs."""
        effective_exclusion = max(
            self.requested_window.exclude_last_n_days,
            self.minimum_exclude_last_n_days,
        )
        if effective_exclusion != self.requested_window.exclude_last_n_days:
            effective_window = replace(
                self.requested_window,
                exclude_last_n_days=effective_exclusion,
            )
            self.logger.info(
                "Increasing excluded training tail from %s to %s days for model %s.",
                self.requested_window.exclude_last_n_days,
                effective_window.exclude_last_n_days,
                self.model_name,
            )
        else:
            effective_window = self.requested_window
        self.logger.info(
            "Minimum allowed training date: %s",
            effective_window.min_allowed_training_date,
        )
        self.logger.info(
            "Maximum allowed training date: %s",
            effective_window.max_allowed_training_date,
        )
        surveillance = load_surveillance_inputs(
            disease=self.disease,
            loc_abb=self.loc,
            forecast_date=effective_window.forecast_date,
            min_allowed_training_date=effective_window.min_allowed_training_date,
            max_allowed_training_date=effective_window.max_allowed_training_date,
            sources=self.sources,
            ed_visit_input_resolution=self.ed_visit_input_resolution,
            fail_on_stale_data=self.fail_on_stale_data,
            logger=self.logger,
        )
        self.output_dir = standalone_directory(
            self.output_dir, self.invocation_config()
        )
        run = ForecastRun(
            disease=self.disease,
            loc=self.loc,
            forecast_window=effective_window,
            model_name=self.model_name,
            output_dir=self.output_dir,
            surveillance=surveillance,
            requested_window=self.requested_window,
        )
        self.logger.info("Model batch directory: %s", run.model_batch_dir)
        self.logger.info("Model run directory: %s", run.model_run_dir)
        return run

    def prepare_model_artifacts(self, run: ForecastRun) -> None:
        """Create additional files required to run the model."""

    def prepare_input_artifacts(self, run: ForecastRun) -> None:
        """Create all common and model-specific input artifacts."""
        run.data_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info("Processing data for %s", run.loc)
        serialize_data(
            forecast_run=run,
            logger=self.logger,
        )
        self.prepare_model_artifacts(run)
        self.logger.info("Data preparation complete.")

    @abstractmethod
    def run_model(self, run: ForecastRun) -> None:
        """Run the configured model and write standardized forecast samples."""

    def publish_outputs(self, run: ForecastRun) -> None:
        """Generate standard plots and the model-level Hubverse table."""
        make_figures_from_model_fit_dir(
            model_fit_dir=run.model_dir,
            save_figs=True,
            save_ci=True,
        )
        model_fit_dir_to_hub_tbl(run.model_dir, forecast_date=run.forecast_date)
        self.logger.info("Postprocessing complete.")

    def execute(self) -> None:
        """Execute the complete forecast pipeline lifecycle."""
        self.logger.info(
            "Starting single-location pipeline for model %s, location %s, and run "
            "date %s.",
            self.model_name,
            self.loc,
            self.requested_window.forecast_date,
        )
        self.validate_configuration()
        run = self.build_forecast_run()
        run.model_dir.mkdir(parents=True, exist_ok=True)
        (run.model_dir / "complete.json").unlink(missing_ok=True)
        write_json(
            run.model_dir / "metadata.json",
            {
                "model": run.model_name,
                "location": run.loc,
                "disease": run.disease,
                "forecast_date": run.forecast_date,
                "forecast_through": run.forecast_through,
                "requested_window": asdict(run.requested_window),
                "effective_window": asdict(run.forecast_window),
                "first_training_date": run.first_training_date,
                "last_training_date": run.last_training_date,
            },
        )
        self.prepare_input_artifacts(run)
        self.run_model(run)
        self.publish_outputs(run)
        write_json(run.model_dir / "complete.json", {"complete": True})
        self.logger.info(
            "Single-location pipeline complete for model %s, location %s, and run "
            "date %s.",
            run.model_name,
            run.loc,
            run.forecast_date,
        )
