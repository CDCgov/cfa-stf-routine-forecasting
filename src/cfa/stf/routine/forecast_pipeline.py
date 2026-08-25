"""Shared lifecycle for routine forecast pipelines."""

import datetime as dt
import logging
from abc import ABC, abstractmethod
from collections.abc import Collection
from pathlib import Path

from cfa.stf.routine.data.data_access import (
    DataResolution,
    ForecastSourceName,
    load_surveillance_inputs,
)
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.date_utils import calculate_training_dates
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
        n_training_days: int,
        n_forecast_days: int,
        run_date: dt.date,
        exclude_last_n_days: int = 0,
        fail_on_stale_data: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.disease = disease
        self.loc = loc
        self.output_dir = Path(output_dir)
        self.n_training_days = n_training_days
        self.n_forecast_days = n_forecast_days
        self.run_date = run_date
        self.exclude_last_n_days = exclude_last_n_days
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

    def validate_configuration(self) -> None:
        """Validate model-specific configuration before loading data."""

    def build_forecast_run(self) -> ForecastRun:
        """Calculate shared run state and load the requested forecast inputs."""
        first_training_date, last_training_date = calculate_training_dates(
            self.run_date,
            self.n_training_days,
            self.exclude_last_n_days,
            self.logger,
        )
        surveillance = load_surveillance_inputs(
            disease=self.disease,
            loc_abb=self.loc,
            run_date=self.run_date,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
            sources=self.sources,
            ed_visit_input_resolution=self.ed_visit_input_resolution,
            fail_on_stale_data=self.fail_on_stale_data,
            logger=self.logger,
        )
        run = ForecastRun(
            disease=self.disease,
            loc=self.loc,
            report_date=self.run_date,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
            n_forecast_days=self.n_forecast_days,
            exclude_last_n_days=self.exclude_last_n_days,
            model_name=self.model_name,
            output_dir=self.output_dir,
            surveillance=surveillance,
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
            ed_visit_input_resolution=self.ed_visit_input_resolution,
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
        model_fit_dir_to_hub_tbl(run.model_dir)
        self.logger.info("Postprocessing complete.")

    def execute(self) -> None:
        """Execute the complete forecast pipeline lifecycle."""
        self.logger.info(
            "Starting single-location pipeline for model %s, location %s, and run "
            "date %s.",
            self.model_name,
            self.loc,
            self.run_date,
        )
        self.validate_configuration()
        run = self.build_forecast_run()
        self.prepare_input_artifacts(run)
        self.run_model(run)
        self.publish_outputs(run)
        self.logger.info(
            "Single-location pipeline complete for model %s, location %s, and run "
            "date %s.",
            run.model_name,
            run.loc,
            run.report_date,
        )
