import argparse
import datetime as dt
import logging
import os
import shutil
import tomllib
from pathlib import Path

import tomli_w
from pyrenew_multisignal.hew.utils import (
    flags_from_hew_letters,
    pyrenew_model_name_from_flags,
)

from pipelines.data.data_access import load_forecast_data, resolve_nssp_report_date
from pipelines.data.prep_data import (
    process_and_save_loc_data,
    process_and_save_loc_param,
)
from pipelines.pyrenew_hew.fit_pyrenew_model import fit_and_save_model
from pipelines.pyrenew_hew.generate_predictive import generate_and_save_predictions
from pipelines.utils.cli_utils import add_common_forecast_arguments
from pipelines.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    get_model_batch_dir_name,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
    run_r_script,
)


def copy_and_record_priors(priors_path: Path, model_dir: Path):
    metadata_file = Path(model_dir, "metadata.toml")
    shutil.copyfile(priors_path, Path(model_dir, "priors.py"))

    if metadata_file.exists():
        with open(metadata_file, "rb") as file:
            metadata = tomllib.load(file)
    else:
        metadata = {}

    new_metadata = {
        "priors_path": str(priors_path),
    }

    metadata.update(new_metadata)

    with open(metadata_file, "wb") as file:
        tomli_w.dump(metadata, file)


def create_samples_from_pyrenew_fit_dir(model_fit_dir: Path) -> None:
    """Create samples.parquet from a PyRenew model fit directory using R."""
    run_r_script(
        "pipelines/pyrenew_hew/create_samples_from_pyrenew_fit_dir.R",
        [str(model_fit_dir)],
        function_name="create_samples_from_pyrenew_fit_dir",
    )
    return None


def main(
    disease: str,
    loc: str,
    priors_path: Path,
    output_dir: Path,
    n_training_days: int,
    n_forecast_days: int,
    n_chains: int,
    n_warmup: int,
    n_samples: int,
    exclude_last_n_days: int = 0,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
    fit_wastewater: bool = False,
    forecast_ed_visits: bool = False,
    forecast_hospital_admissions: bool = False,
    forecast_wastewater: bool = False,
    rng_key: int | None = None,
    run_date: dt.date | None = None,
    fail_on_stale_data: bool = False,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if fit_wastewater or forecast_wastewater:
        raise ValueError(
            "Wastewater data loading is no longer supported in this pipeline."
        )

    pyrenew_model_name = pyrenew_model_name_from_flags(
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
    )

    logger.info(
        "Starting single-location forecasting pipeline for "
        f"model {pyrenew_model_name}, location {loc}, "
        f"and latest NSSP report date."
    )
    signals = ["ed_visits", "hospital_admissions"]

    for signal in signals:
        fit = locals().get(f"fit_{signal}", False)
        forecast = locals().get(f"forecast_{signal}", False)
        if fit and not forecast:
            raise ValueError(
                "This pipeline does not currently support "
                "fitting to but not forecasting a signal. "
                f"Asked to fit but not forecast {signal}."
            )
    any_fit = any([locals().get(f"fit_{signal}", False) for signal in signals])
    if not any_fit:
        raise ValueError(
            "pyrenew_null (fitting to no signals) is not supported by this pipeline"
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
    model_dir = Path(model_run_dir, pyrenew_model_name)
    data_dir = Path(model_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    logger.info(f"Copying and recording priors from {priors_path}...")
    copy_and_record_priors(priors_path, model_dir)

    logger.info(f"Processing {loc}")
    process_and_save_loc_data(
        forecast_data=forecast_data,
        save_dir=data_dir,
        logger=logger,
    )

    process_and_save_loc_param(
        loc_abb=loc,
        disease=disease,
        param_estimates=None,
        fit_ed_visits=fit_ed_visits,
        save_dir=data_dir,
        as_of=report_date,
    )
    append_prop_data_to_combined_data(Path(data_dir, "combined_data.tsv"))
    logger.info("Data preparation complete.")

    logger.info("Fitting model...")

    fit_and_save_model(
        model_dir,
        n_warmup=n_warmup,
        n_samples=n_samples,
        n_chains=n_chains,
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
        rng_key=rng_key,
    )
    logger.info("Model fitting complete")

    logger.info("Performing posterior prediction / forecasting...")

    n_days_past_last_training = n_forecast_days + exclude_last_n_days
    generate_and_save_predictions(
        model_run_dir,
        pyrenew_model_name,
        n_days_past_last_training,
        predict_ed_visits=forecast_ed_visits,
        predict_hospital_admissions=forecast_hospital_admissions,
        predict_wastewater=forecast_wastewater,
        rng_key=rng_key,
    )
    # pipe this into something that creates samples.parquet
    logger.info("All forecasting complete.")

    logger.info("Postprocessing forecast...")

    # Create daily counts
    logger.info("Creating daily counts...")
    create_samples_from_pyrenew_fit_dir(model_dir)
    make_figures_from_model_fit_dir(
        model_dir,
        save_figs=True,
        save_ci=True,
    )
    model_fit_dir_to_hub_tbl(model_dir)

    logger.info(
        "Single-location pipeline complete "
        f"for model {pyrenew_model_name}, "
        f"location {loc}, and "
        f"report date {report_date}."
    )
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create fit data for disease modeling."
    )

    # Add common arguments
    add_common_forecast_arguments(parser)

    # Add pyrenew-specific arguments
    parser.add_argument(
        "--model-letters",
        type=str,
        help=(
            "Fit the model corresponding to the provided model letters "
            "(e.g. 'he', 'e', 'hew')."
        ),
        required=True,
    )

    parser.add_argument(
        "--priors-path",
        type=Path,
        help=(
            "Path to an executable python file defining random variables "
            "that require priors as pyrenew RandomVariable objects."
        ),
        required=True,
    )

    parser.add_argument(
        "--n-warmup",
        type=int,
        default=1000,
        help="Number of warmup iterations per chain for NUTS (default: 1000).",
    )

    parser.add_argument(
        "--n-samples",
        type=int,
        default=1000,
        help=(
            "Number of posterior samples to draw per chain using NUTS (default: 1000)."
        ),
    )

    parser.add_argument(
        "--n-chains",
        type=int,
        default=4,
        help="Number of MCMC chains to run (default: 4).",
    )

    parser.add_argument(
        "--additional-forecast-letters",
        type=str,
        help=(
            "Forecast the following signals even if they were not fit. "
            "Fit signals are always forecast."
        ),
        default=None,
    )

    parser.add_argument(
        "--rng-key",
        type=int,
        help=(
            "Integer seed for a JAX random number generator. "
            "If not provided, a random integer will be chosen."
        ),
        default=None,
    )
    parser.add_argument(
        "--fail-on-stale-data",
        action="store_true",
        help="Fail instead of warning when selected input data is stale.",
    )

    args = parser.parse_args()
    fit_flags = flags_from_hew_letters(args.model_letters)
    forecast_flags = flags_from_hew_letters(
        args.model_letters + (args.additional_forecast_letters or ""),
        flag_prefix="forecast",
    )
    delattr(args, "additional_forecast_letters")
    delattr(args, "model_letters")
    main(**vars(args), **fit_flags, **forecast_flags)
