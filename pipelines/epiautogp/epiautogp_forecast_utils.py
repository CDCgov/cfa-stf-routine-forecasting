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
from typing import Any, Literal

import polars as pl

from pipelines.data.prep_data import get_pmfs, process_and_save_loc_data
from pipelines.epiautogp.nssp_nowcast import NsspRightTruncationNowcast
from pipelines.epiautogp.nowcast import NowcastSource
from pipelines.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    generate_epiweekly_data,
    get_available_reports,
    get_model_batch_dir_name,
    load_credentials,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)

NowcastSourceName = Literal["auto", "none", "right-truncation"]


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

    disease: str
    loc: str
    target: str
    frequency: str
    ed_visit_type: str
    model_name: str
    nhsn_data_path: Path | None
    report_date: date
    first_training_date: date
    last_training_date: date
    n_forecast_days: int
    exclude_last_n_days: int
    exclude_date_ranges: list[tuple[date, date]] | None
    model_batch_dir: Path
    model_run_dir: Path
    credentials_dict: dict[str, Any]
    facility_level_nssp_data: pl.LazyFrame
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

        self.logger.info(f"Processing data for {self.loc}")

        # Process and save location data
        process_and_save_loc_data(
            loc_abb=self.loc,
            disease=self.disease,
            facility_level_nssp_data=self.facility_level_nssp_data,
            report_date=self.report_date,
            first_training_date=self.first_training_date,
            last_training_date=self.last_training_date,
            save_dir=data_dir,
            logger=self.logger,
            credentials_dict=self.credentials_dict,
            nhsn_data_path=self.nhsn_data_path,
        )

        # Generate epiweekly datasets
        # only do this if we're fitting an epiweekly model
        if self.frequency == "epiweekly":
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

def _use_right_truncation_nowcasting_auto(
    *,
    target: str,
    ed_visit_type: str,
) -> bool:
    """
    Determine whether right-truncation nowcasting should be used based on the
    target and ED visit type when nowcast_source_name="auto".

    Right-truncation nowcasting is currently only supported for NSSP targets with
    "observed" or "other" ED visit types.

    Parameters
    ----------
    target : str
        Target data type (e.g., "nssp", "nhsn")
    ed_visit_type : str
        Type of ED visits (e.g., "observed", "other", "pct")

    Returns
    -------
    bool
        True if right-truncation nowcasting should be used, False otherwise
    """
    return target == "nssp" and ed_visit_type in ["observed", "other"]

def _use_right_truncation_nowcasting_right_truncation(
    *,
    target: str,
    ed_visit_type: str,
) -> bool:
    """
    Determine whether right-truncation nowcasting should be used based on the
    target and ED visit type when nowcast_source_name="right-truncation".

    Right-truncation nowcasting is currently only supported for NSSP targets with
    "observed" or "other" ED visit types.

    Parameters
    ----------
    target : str
        Target data type (e.g., "nssp", "nhsn")
    ed_visit_type : str
        Type of ED visits (e.g., "observed", "other", "pct")

    Returns
    -------
    bool
        True if right-truncation nowcasting should be used, False otherwise
    """
    if target != "nssp":
        raise ValueError(
            "right-truncation nowcasting is currently supported only "
            "for target='nssp'."
        )
    if ed_visit_type == "pct":
        raise ValueError(
            "right-truncation nowcasting is not supported for "
            "ed_visit_type='pct'."
        )
    return True

def _generate_right_truncation_nowcast(
    *,
    right_truncation_pmf: list[float] | None,
    disease: str,
    loc: str,
    frequency: str,
    report_date: date,
    param_data_dir: Path | str | None,
    logger: logging.Logger,
) -> NsspRightTruncationNowcast:
    """Generate a right-truncation nowcast source for EpiAutoGP.
    """ 
    if frequency != "daily":
        logger.warning(
            "Using right-truncation nowcasting for frequency=%r. Confirm that "
            "the right-truncation PMF is indexed to the cadence of the model "
            "series being nowcast.",
            frequency,
        )

    if right_truncation_pmf is None:
        if param_data_dir is None:
            raise ValueError(
                "param_data_dir is required when right-truncation nowcasting "
                "is requested without a directly supplied right_truncation_pmf."
            )
        param_estimates = pl.scan_parquet(Path(param_data_dir, "prod.parquet"))
        right_truncation_pmf = get_pmfs(
            param_estimates=param_estimates,
            loc_abb=loc,
            disease=disease,
            as_of=report_date,
        )["right_truncation_pmf"]

    return NsspRightTruncationNowcast(right_truncation_pmf=right_truncation_pmf)

def _resolve_nowcast_source(
    *,
    disease: str,
    loc: str,
    target: str,
    frequency: str,
    ed_visit_type: str,
    report_date: date,
    param_data_dir: Path | str | None,
    nowcast_source_name: NowcastSourceName,
    right_truncation_pmf: list[float] | None,
    logger: logging.Logger,
) -> NowcastSource | None:
    """
    Resolve the requested nowcast source for one EpiAutoGP run.
    """
    valid_sources = ["auto", "none", "right-truncation"]
    if nowcast_source_name not in valid_sources:
        raise ValueError(
            f"nowcast_source_name must be one of {valid_sources}, "
            f"got {nowcast_source_name!r}"
        )

    if nowcast_source_name == "none":
        return None

    use_right_truncation = False
    if nowcast_source_name == "auto":
        use_right_truncation = _use_right_truncation_nowcasting_auto(
            target=target,
            ed_visit_type=ed_visit_type,
        )
    elif nowcast_source_name == "right-truncation":
        use_right_truncation = _use_right_truncation_nowcasting_right_truncation(
            target=target,
            ed_visit_type=ed_visit_type,
        )

    if use_right_truncation:
        return _generate_right_truncation_nowcast(
            right_truncation_pmf=right_truncation_pmf,
            disease=disease,
            loc=loc,
            frequency=frequency,
            report_date=report_date,
            param_data_dir=param_data_dir,
            logger=logger,
        )
    else:
        return None


def setup_forecast_pipeline(
    disease: str,
    loc: str,
    target: str,
    frequency: str,
    ed_visit_type: str,
    model_name: str,
    nhsn_data_path: Path | None,
    facility_level_nssp_data_dir: Path | str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    exclude_last_n_days: int = 0,
    exclude_date_ranges: list[tuple[date, date]] | None = None,
    credentials_path: Path | None = None,
    logger: logging.Logger | None = None,
    param_data_dir: Path | str | None = None,
    nowcast_source_name: NowcastSourceName = "auto",
    right_truncation_pmf: list[float] | None = None,
) -> ForecastPipelineContext:
    """
    Set up common forecast pipeline infrastructure.

    This function performs the initial setup steps that are common across
    all forecast pipelines:
    1. Load credentials
    2. Get available report dates
    3. Parse and validate the report date
    4. Calculate training dates
    5. Load NSSP data
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
    nhsn_data_path : Path | None
        Path to NHSN hospital admission data
    facility_level_nssp_data_dir : Path | str
        Directory containing facility-level NSSP ED visit data
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
    param_data_dir : Path | str | None, default=None
        Directory containing parameter estimates such as right-truncation PMFs.
        Required when right-truncation nowcasting is selected and no PMF is
        directly supplied.
    nowcast_source_name : {"auto", "none", "right-truncation"}, default="auto"
        Nowcast source selection for EpiAutoGP input.
    right_truncation_pmf : list[float] | None, default=None
        Directly supplied right-truncation PMF. Takes precedence over
        param_data_dir when right-truncation nowcasting is selected.

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

    # Get available reports
    available_facility_level_reports = get_available_reports(
        facility_level_nssp_data_dir
    )

    report_date_parsed = max(available_facility_level_reports)

    # Calculate training dates
    first_training_date, last_training_date = calculate_training_dates(
        report_date_parsed,
        n_training_days,
        exclude_last_n_days,
        logger,
    )

    # Load NSSP data
    facility_datafile = f"{report_date_parsed}.parquet"
    facility_level_nssp_data = pl.scan_parquet(
        Path(facility_level_nssp_data_dir, facility_datafile)
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
        disease=disease,
        loc=loc,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
        report_date=report_date_parsed,
        param_data_dir=param_data_dir,
        nowcast_source_name=nowcast_source_name,
        right_truncation_pmf=right_truncation_pmf,
        logger=logger,
    )

    return ForecastPipelineContext(
        disease=disease,
        loc=loc,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
        model_name=model_name,
        nhsn_data_path=nhsn_data_path,
        report_date=report_date_parsed,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        n_forecast_days=n_forecast_days,
        exclude_last_n_days=exclude_last_n_days,
        exclude_date_ranges=exclude_date_ranges,
        model_batch_dir=model_batch_dir,
        model_run_dir=model_run_dir,
        credentials_dict=credentials_dict,
        facility_level_nssp_data=facility_level_nssp_data,
        logger=logger,
        nowcast_source=resolved_nowcast_source,
    )
