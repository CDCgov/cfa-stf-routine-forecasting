"""
Shared utilities for forecast pipeline scripts.

This module contains common functionality used across different forecast
pipelines (pyrenew, timeseries, epiautogp, etc.).
"""

import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, get_args

from pipelines.data.data_access import (
    ForecastData,
    load_forecast_data,
    resolve_nssp_report_date,
)
from pipelines.data.prep_data import get_pmfs, process_and_save_loc_data
from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.nowcast import NowcastSource
from pipelines.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast
from pipelines.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    generate_epiweekly_data,
    get_model_batch_dir_name,
    load_credentials,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)

NowcastSourceName = Literal["none", "reporting-delay"]
VALID_NOWCAST_SOURCE_NAMES: tuple[str, ...] = get_args(NowcastSourceName)


@dataclass
class ModelPaths:
    """
    Container for model output directory structure and file paths.

    This class holds all the computed output paths for a specific model run,
    making it easier to track where data and results are stored.
    """

    model_output_dir: Path
    data_dir: Path
    training_data: Path


@dataclass
class ForecastPipelineContext:
    """
    Container for common forecast pipeline data, input configurations and
    the logger.

    This class holds all the shared state that gets passed around during
    a forecast pipeline run, reducing the number of parameters that need
    to be passed between functions.
    """

    forecast_spec: ForecastSpec
    model_name: str
    first_training_date: date
    last_training_date: date
    n_forecast_days: int
    exclude_last_n_days: int
    exclude_date_ranges: list[tuple[date, date]] | None
    model_batch_dir: Path
    model_run_dir: Path
    credentials_dict: dict[str, Any]
    forecast_data: ForecastData
    logger: logging.Logger
    nowcast_source: NowcastSource | None = None

    def prepare_model_data(self) -> ModelPaths:
        """
        Prepare training data for a model.

        This function performs the data preparation steps that are common across
        all forecast pipelines:
        1. Create model output directory
        2. Process and save location data
        3. Generate epiweekly datasets

        Returns
        -------
        ModelPaths
            Object containing all model output directory and file paths
        """
        # Create model output directory
        model_output_dir = Path(self.model_run_dir, self.model_name)
        data_dir = Path(model_output_dir, "data")
        os.makedirs(data_dir, exist_ok=True)

        self.logger.info(f"Processing data for {self.forecast_spec.loc}")

        # Process and save location data
        process_and_save_loc_data(
            loc_abb=self.forecast_spec.loc,
            disease=self.forecast_spec.disease,
            forecast_data=self.forecast_data,
            first_training_date=self.first_training_date,
            last_training_date=self.last_training_date,
            save_dir=data_dir,
            logger=self.logger,
        )

        # Generate epiweekly datasets
        # only do this if we're fitting an epiweekly model
        if self.forecast_spec.frequency == "epiweekly":
            self.logger.info("Generating epiweekly datasets from daily datasets...")
            generate_epiweekly_data(data_dir, overwrite_daily=True)

        append_prop_data_to_combined_data(Path(data_dir, "combined_data.tsv"))
        self.logger.info("Data preparation complete.")

        # Return structured paths object
        return ModelPaths(
            model_output_dir=model_output_dir,
            data_dir=data_dir,
            training_data=Path(data_dir, "combined_data.tsv"),
        )

    def post_process_forecast(self) -> None:
        """
        Post-process forecast outputs: create hubverse table and generate plots.

        This function performs the final post-processing steps:
        1. Process model output samples
        2. Generate plots
        3. Create hubverse table from processed outputs

        Returns
        -------
        None
        """
        self.logger.info("Processing forecast and generating plots...")
        model_fit_dir = Path(self.model_run_dir, self.model_name)

        make_figures_from_model_fit_dir(
            model_fit_dir=model_fit_dir,
            save_figs=True,
            save_ci=True,
        )

        self.logger.info("Processing and plotting complete.")

        # Create hubverse table from processed outputs
        self.logger.info("Creating hubverse table...")
        model_fit_dir_to_hub_tbl(model_fit_dir)
        self.logger.info("Postprocessing complete.")


def _build_reporting_delay_nowcast(
    *,
    forecast_spec: ForecastSpec,
    reporting_delay_pmf: list[float] | None,
) -> ReportingDelayNowcast:
    """Build a ReportingDelayNowcast, fetching the PMF if not supplied."""
    if reporting_delay_pmf is None:
        reporting_delay_pmf = get_pmfs(
            param_estimates=None,
            loc_abb=forecast_spec.loc,
            disease=forecast_spec.disease,
            as_of=forecast_spec.report_date,
        )["right_truncation_pmf"]

    return ReportingDelayNowcast(reporting_delay_pmf=reporting_delay_pmf)


def _resolve_nowcast_source(
    *,
    forecast_spec: ForecastSpec,
    nowcast_source_name: NowcastSourceName,
    **options: Any,
) -> NowcastSource | None:
    """
    Resolve the requested nowcast source for one EpiAutoGP run.

    `forecast_spec` and `nowcast_source_name` are universal; remaining keyword
    arguments are forwarded as-is to the chosen source's builder, which is
    responsible for validating them. New nowcast approaches plug in by adding
    a new case here and a builder that defines its own required kwargs.
    """
    match nowcast_source_name:
        case "none":
            return None
        case "reporting-delay":
            if not ReportingDelayNowcast.applies_to(forecast_spec=forecast_spec):
                raise ValueError(
                    f"reporting-delay nowcasting is not applicable to "
                    f"target={forecast_spec.target!r}, ed_visit_type={forecast_spec.ed_visit_type!r}."
                )
            return _build_reporting_delay_nowcast(
                forecast_spec=forecast_spec, **options
            )
        case _:
            raise ValueError(
                f"nowcast_source_name must be one of {list(VALID_NOWCAST_SOURCE_NAMES)}, "
                f"got {nowcast_source_name!r}"
            )


def setup_forecast_pipeline(
    disease: str,
    loc: str,
    target: str,
    frequency: str,
    ed_visit_type: str,
    model_name: str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    exclude_last_n_days: int = 0,
    exclude_date_ranges: list[tuple[date, date]] | None = None,
    credentials_path: Path | None = None,
    logger: logging.Logger | None = None,
    nowcast_source_name: NowcastSourceName = "none",
    reporting_delay_pmf: list[float] | None = None,
    run_date: date | None = None,
    fail_on_stale_data: bool = False,
) -> ForecastPipelineContext:
    """
    Set up common forecast pipeline infrastructure.

    This function performs the initial setup steps that are common across
    all forecast pipelines:
    1. Load credentials
    2. Get available report dates
    3. Parse and validate the report date
    4. Calculate training dates
    5. Load forecast input data
    6. Create batch directory structure
    7. Resolve nowcasting source based on input parameters and available data

    Parameters
    ----------
    disease : str
        Disease to model (e.g., "COVID-19", "Influenza", "RSV")
    loc : str
        Two-letter USPS location abbreviation (e.g., "CA", "NY")
    target : str
        Target data type: "nssp" or "nhsn"
    frequency : str
        Data frequency: "daily" or "epiweekly"
    ed_visit_type : str
        Type of ED visits: "observed" or "other" (NSSP only)
    model_name : str
        Name of the model configuration
    output_dir : Path | str
        Root directory for output
    n_training_days : int
        Number of days of training data
    n_forecast_days : int
        Number of days ahead to forecast
    exclude_last_n_days : int, default=0
        Number of recent days to exclude from training
    exclude_date_ranges : list[tuple[date, date]] | None, default=None
        List of date ranges to exclude from training data (inclusive).
        Each tuple contains (start_date, end_date).
    credentials_path : Path | None, default=None
        Path to credentials file
    logger : logging.Logger | None, default=None
        Logger instance. If None, creates a new logger
    nowcast_source_name : {"none", "reporting-delay"}, default="none"
        Nowcast source selection for EpiAutoGP input.
    reporting_delay_pmf : list[float] | None, default=None
        Directly supplied reporting-delay PMF. If omitted, the PMF is loaded
        through `cfa-stf-data`.

    Returns
    -------
    ForecastPipelineContext
        Context object containing all setup information
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(
        f"Setting up forecast pipeline for {disease}, "
        f"location {loc}, latest report date."
    )

    # Load credentials
    credentials_dict = load_credentials(credentials_path, logger)

    report_date_parsed = resolve_nssp_report_date(run_date)

    # Gather the forecast specification input parameters into a single cohesive unit
    forecast_spec = ForecastSpec(
        disease=disease,
        loc=loc,
        report_date=report_date_parsed,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
    )

    # Calculate training dates
    first_training_date, last_training_date = calculate_training_dates(
        report_date_parsed,
        n_training_days,
        exclude_last_n_days,
        logger,
    )

    forecast_data = load_forecast_data(
        disease=disease,
        loc_abb=loc,
        report_date=report_date_parsed,
        first_training_date=first_training_date,
        run_date=run_date,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    )

    # Create model batch directory structure
    model_batch_dir_name = get_model_batch_dir_name(
        disease=disease,
        report_date=report_date_parsed,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
    )
    model_batch_dir = Path(output_dir, model_batch_dir_name)
    model_run_dir = Path(model_batch_dir, "model_runs", loc)

    logger.info(f"Model batch directory: {model_batch_dir}")
    logger.info(f"Model run directory: {model_run_dir}")

    # Resolve nowcast source
    resolved_nowcast_source = _resolve_nowcast_source(
        forecast_spec=forecast_spec,
        nowcast_source_name=nowcast_source_name,
        reporting_delay_pmf=reporting_delay_pmf,
    )

    return ForecastPipelineContext(
        forecast_spec=forecast_spec,
        model_name=model_name,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        n_forecast_days=n_forecast_days,
        exclude_last_n_days=exclude_last_n_days,
        exclude_date_ranges=exclude_date_ranges,
        model_batch_dir=model_batch_dir,
        model_run_dir=model_run_dir,
        credentials_dict=credentials_dict,
        forecast_data=forecast_data,
        logger=logger,
        nowcast_source=resolved_nowcast_source,
    )
