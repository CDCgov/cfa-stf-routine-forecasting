"""Shared lifecycle for routine forecast pipelines."""

import datetime as dt
import logging
from abc import ABC, abstractmethod
from collections.abc import Collection
from pathlib import Path

from cfa.stf.routine.data.data_access import (
    ForecastSourceName,
    load_surveillance_inputs,
)
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)


class ForecastPipeline[ModelInputsT](ABC):
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

    @abstractmethod
    def resolve_model_inputs(self, run: ForecastRun) -> ModelInputsT:
        """Resolve model-specific inputs that require the materialized run state."""

    def before_data_preparation(self, run: ForecastRun) -> None:
        """Run model-specific work before common data serialization."""

    def after_data_serialization(
        self,
        run: ForecastRun,
        model_inputs: ModelInputsT,
    ) -> None:
        """Run model-specific work after common data serialization."""

    def prepare_model_inputs(
        self,
        run: ForecastRun,
        model_inputs: ModelInputsT,
    ) -> None:
        """Serialize common inputs and apply model-specific preparation hooks."""
        run.data_dir.mkdir(parents=True, exist_ok=True)
        self.before_data_preparation(run)
        self.logger.info("Processing data for %s", run.loc)
        serialize_data(
            forecast_run=run,
            save_dir=run.data_dir,
            logger=self.logger,
        )
        self.after_data_serialization(run, model_inputs)
        append_prop_data_to_combined_data(run.data_dir / "combined_data.tsv")
        self.logger.info("Data preparation complete.")

    @abstractmethod
    def fit_and_forecast(
        self,
        run: ForecastRun,
        model_inputs: ModelInputsT,
    ) -> None:
        """Fit the configured model and write its forecast samples."""

    def before_post_process(self, run: ForecastRun) -> None:
        """Run model-specific output conversion before common post-processing."""

    def post_process(self, run: ForecastRun) -> None:
        """Generate standard plots and the model-level Hubverse table."""
        self.before_post_process(run)
        make_figures_from_model_fit_dir(
            model_fit_dir=run.model_dir,
            save_figs=True,
            save_ci=True,
        )
        model_fit_dir_to_hub_tbl(run.model_dir)
        self.logger.info("Postprocessing complete.")

    def execute(self) -> None:
        """Execute the complete forecast pipeline lifecycle."""
        self.validate_configuration()
        run = self.build_forecast_run()
        model_inputs = self.resolve_model_inputs(run)
        self.prepare_model_inputs(run, model_inputs)
        self.fit_and_forecast(run, model_inputs)
        self.post_process(run)
        self.logger.info(
            "Single-location pipeline complete for model %s, location %s, and run "
            "date %s.",
            run.model_name,
            run.loc,
            run.report_date,
        )
