import argparse
import datetime as dt
import logging
import os
from pathlib import Path

from pipelines.data.data_access import load_forecast_data, resolve_nssp_report_date
from pipelines.data.prep_data import process_and_save_loc_data
from pipelines.utils.cli_utils import add_common_forecast_arguments
from pipelines.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    generate_epiweekly_data,
    get_model_batch_dir_name,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
    run_r_script,
)


def fable_e_other_forecasts(
    model_dir: Path, n_forecast_days: int, n_samples: int
) -> None:
    script_args = [
        "--model-dir",
        f"{model_dir}",
        "--n-forecast-days",
        f"{n_forecast_days}",
        "--n-samples",
        f"{n_samples}",
    ]
    run_r_script(
        "pipelines/fable/fit_fable.R",
        script_args,
        function_name="fit_fable",
    )
    return None


def main(
    disease: str,
    loc: str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    n_samples: int,
    exclude_last_n_days: int = 0,
    epiweekly: bool = False,
    run_date: dt.date | None = None,
    fail_on_stale_data: bool = False,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    prefix = "epiweekly" if epiweekly else "daily"
    ensemble_model_name = f"{prefix}_fable_e_other"

    logger.info(
        "Starting single-location fable E-other forecasting pipeline for "
        f"location {loc}, and latest report date."
    )

    report_date = resolve_nssp_report_date()
    first_training_date, last_training_date = calculate_training_dates(
        report_date,
        n_training_days,
        exclude_last_n_days,
        logger,
    )

    forecast_data = load_forecast_data(
        disease=disease,
        loc_abb=loc,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        run_date=run_date,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    )

    model_batch_dir_name = get_model_batch_dir_name(
        disease=disease,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
    )
    model_batch_dir = Path(output_dir, model_batch_dir_name)

    model_run_dir = Path(model_batch_dir, "model_runs", loc)
    ensemble_model_output_dir = Path(model_run_dir, ensemble_model_name)

    os.makedirs(model_run_dir, exist_ok=True)
    os.makedirs(ensemble_model_output_dir, exist_ok=True)
    data_dir = Path(ensemble_model_output_dir, "data")
    logger.info(f"Processing {loc}")
    process_and_save_loc_data(
        forecast_data=forecast_data,
        save_dir=data_dir,
        logger=logger,
    )
    if epiweekly:
        logger.info("Generating epiweekly datasets from daily datasets...")
        generate_epiweekly_data(data_dir, overwrite_daily=True)

    append_prop_data_to_combined_data(Path(data_dir, "combined_data.tsv"))
    logger.info("Data preparation complete.")

    n_days_past_last_training = n_forecast_days + exclude_last_n_days

    logger.info("Performing fable E-other forecasting")
    fable_e_other_forecasts(
        ensemble_model_output_dir, n_days_past_last_training, n_samples
    )

    make_figures_from_model_fit_dir(
        Path(
            ensemble_model_output_dir,
        ),
        save_figs=True,
        save_ci=True,
    )

    model_fit_dir_to_hub_tbl(ensemble_model_output_dir)

    logger.info("Postprocessing complete.")

    logger.info(
        "Single-location fable E-other pipeline complete "
        f"for location {loc}, and "
        f"report date {report_date}."
    )
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create fable E-other forecasts.")

    # Add common arguments
    add_common_forecast_arguments(parser)

    # Add fable-specific arguments
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
            "Whether to generate epiweekly fable E-other forecasts. "
            "If set, will generate epiweekly datasets and forecasts, and "
            "append 'epiweekly' to the model name."
        ),
    )
    parser.add_argument(
        "--fail-on-stale-data",
        action="store_true",
        help="Fail instead of warning when selected input data is stale.",
    )
    args = parser.parse_args()
    main(**vars(args))
