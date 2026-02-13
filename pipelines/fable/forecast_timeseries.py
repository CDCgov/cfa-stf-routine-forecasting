import argparse
import logging
import os
from pathlib import Path

import polars as pl

from pipelines.data.prep_data import process_and_save_loc_data
from pipelines.utils.cli_utils import add_common_forecast_arguments
from pipelines.utils.common_utils import (
    calculate_training_dates,
    create_hubverse_table,
    get_available_reports,
    load_credentials,
    make_figures_from_model_fit_dir,
    run_r_script,
)


def generate_epiweekly_data(data_dir: Path, overwrite_daily: bool = False) -> None:
    """Generate epiweekly datasets from daily datasets using an R script."""
    args = [str(data_dir)]
    if overwrite_daily:
        args.append("--overwrite-daily")

    run_r_script(
        "pipelines/data/generate_epiweekly_data.R",
        args,
        function_name="generate_epiweekly_data",
    )
    return None


def timeseries_ensemble_forecasts(
    model_dir: Path, n_forecast_days: int, n_samples: int, epiweekly: bool = False
) -> None:
    script_args = [
        "--model-dir",
        f"{model_dir}",
        "--n-forecast-days",
        f"{n_forecast_days}",
        "--n-samples",
        f"{n_samples}",
    ]
    if epiweekly:
        script_args.append("--epiweekly")
    run_r_script(
        "pipelines/fable/fit_timeseries.R",
        script_args,
        function_name="timeseries_ensemble_forecasts",
    )
    return None


def main(
    disease: str,
    loc: str,
    facility_level_nssp_data_dir: Path | str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    n_samples: int,
    exclude_last_n_days: int = 0,
    epiweekly: bool = False,
    credentials_path: Path | None = None,
    nhsn_data_path: Path | None = None,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    prefix = "epiweekly" if epiweekly else "daily"
    ensemble_model_name = f"{prefix}_ts_ensemble_e"

    logger.info(
        "Starting single-location timeseries forecasting pipeline for "
        f"location {loc}, and latest report date."
    )

    credentials_dict = load_credentials(credentials_path, logger)

    available_facility_level_reports = get_available_reports(
        facility_level_nssp_data_dir
    )

    report_date = max(available_facility_level_reports)
    facility_datafile = f"{report_date}.parquet"

    first_training_date, last_training_date = calculate_training_dates(
        report_date,
        n_training_days,
        exclude_last_n_days,
        logger,
    )

    facility_level_nssp_data = pl.scan_parquet(
        Path(facility_level_nssp_data_dir, facility_datafile)
    )

    model_batch_dir_name = (
        f"{disease.lower()}_r_{report_date}_f_"
        f"{first_training_date}_t_{last_training_date}"
    )
    model_batch_dir = Path(output_dir, model_batch_dir_name)

    model_run_dir = Path(model_batch_dir, "model_runs", loc)
    ensemble_model_output_dir = Path(model_run_dir, ensemble_model_name)

    os.makedirs(model_run_dir, exist_ok=True)
    os.makedirs(ensemble_model_output_dir, exist_ok=True)

    logger.info(f"Processing {loc}")
    process_and_save_loc_data(
        loc_abb=loc,
        disease=disease,
        facility_level_nssp_data=facility_level_nssp_data,
        loc_level_nwss_data=None,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        save_dir=Path(ensemble_model_output_dir, "data"),
        logger=logger,
        credentials_dict=credentials_dict,
        nhsn_data_path=nhsn_data_path,
    )
    if epiweekly:
        logger.info("Generating epiweekly datasets from daily datasets...")
        generate_epiweekly_data(
            Path(ensemble_model_output_dir, "data"), overwrite_daily=True
        )

    logger.info("Data preparation complete.")

    n_days_past_last_training = n_forecast_days + exclude_last_n_days

    logger.info("Performing timeseries ensemble forecasting")
    timeseries_ensemble_forecasts(
        ensemble_model_output_dir,
        n_days_past_last_training,
        n_samples,
        epiweekly=epiweekly,
    )

    make_figures_from_model_fit_dir(
        Path(
            ensemble_model_output_dir,
        ),
        save_figs=True,
        save_ci=True,
    )

    create_hubverse_table(ensemble_model_output_dir)

    logger.info("Postprocessing complete.")

    logger.info(
        "Single-location timeseries pipeline complete "
        f"for location {loc}, and "
        f"report date {report_date}."
    )
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create fit data for disease modeling."
    )

    # Add common arguments
    add_common_forecast_arguments(parser)

    # Add timeseries-specific arguments
    parser.add_argument(
        "--n-samples",
        type=int,
        default=1000,
        help=("Number of samples to draw (default: 1000)."),
    )
    parser.add_argument(
        "--epiweekly",
        action="store_true",
        help=(
            "Whether to generate epiweekly forecasts in addition to daily. "
            "If set, will generate epiweekly datasets and forecasts, and "
            "append 'epiweekly' to the model name."
        ),
    )
    args = parser.parse_args()
    main(**vars(args))
