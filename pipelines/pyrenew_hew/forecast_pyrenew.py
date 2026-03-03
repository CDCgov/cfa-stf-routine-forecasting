import argparse
import datetime as dt
import logging
import os
import shutil
import tomllib
from pathlib import Path

import polars as pl
import tomli_w
from pygit2.repository import Repository
from pyrenew_multisignal.hew.utils import (
    flags_from_hew_letters,
    pyrenew_model_name_from_flags,
)

from pipelines.data.prep_data import (
    process_and_save_loc_data,
    process_and_save_loc_param,
)
from pipelines.data.prep_ww_data import clean_nwss_data, preprocess_ww_data
from pipelines.pyrenew_hew.fit_pyrenew_model import fit_and_save_model
from pipelines.pyrenew_hew.generate_predictive import generate_and_save_predictions
from pipelines.utils.cli_utils import add_common_forecast_arguments
from pipelines.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    create_hubverse_table,
    create_prop_samples,
    get_available_reports,
    load_credentials,
    make_figures_from_model_fit_dir,
    run_r_script,
)


def record_git_info(model_dir: Path):
    metadata_file = Path(model_dir, "metadata.toml")

    if metadata_file.exists():
        with open(metadata_file, "rb") as file:
            metadata = tomllib.load(file)
    else:
        metadata = {}

    try:
        repo = Repository(os.getcwd())
        branch_name = repo.head.shorthand
        commit_sha = str(repo.head.target)
    except Exception:
        branch_name = os.environ.get("GIT_BRANCH_NAME", "unknown")
        commit_sha = os.environ.get("GIT_COMMIT_SHA", "unknown")

    new_metadata = {
        "branch_name": branch_name,
        "commit_sha": commit_sha,
    }

    metadata.update(new_metadata)

    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "wb") as file:
        tomli_w.dump(metadata, file)


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
    facility_level_nssp_data_dir: Path,
    nwss_data_dir: Path,
    param_data_dir: Path,
    priors_path: Path,
    output_dir: Path,
    n_training_days: int,
    n_forecast_days: int,
    n_chains: int,
    n_warmup: int,
    n_samples: int,
    nhsn_data_path: Path | None = None,
    exclude_last_n_days: int = 0,
    credentials_path: Path | None = None,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
    fit_wastewater: bool = False,
    forecast_ed_visits: bool = False,
    forecast_hospital_admissions: bool = False,
    forecast_wastewater: bool = False,
    rng_key: int | None = None,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

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
    signals = ["ed_visits", "hospital_admissions", "wastewater"]

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

    nwss_data_disease_map = {
        "COVID-19": "covid",
        "Influenza": "flu",
        "RSV": "rsv",
    }

    def get_available_nwss_reports(
        data_dir: str | Path,
        glob_pattern: str = f"NWSS-ETL-{nwss_data_disease_map[disease]}-",
    ):
        return [
            dt.datetime.strptime(f.stem.removeprefix(glob_pattern), "%Y-%m-%d").date()
            for f in Path(data_dir).glob(f"{glob_pattern}*")
        ]

    if fit_wastewater:
        available_nwss_reports = get_available_nwss_reports(nwss_data_dir)
        if report_date in available_nwss_reports:
            nwss_data_raw = pl.scan_parquet(
                Path(
                    nwss_data_dir,
                    f"NWSS-ETL-{nwss_data_disease_map[disease]}-{report_date}",
                    "bronze.parquet",
                )
            )
            nwss_data_cleaned = clean_nwss_data(nwss_data_raw).filter(
                (pl.col("location") == loc) & (pl.col("date") >= first_training_date)
            )
            loc_level_nwss_data = preprocess_ww_data(nwss_data_cleaned.collect())
        else:
            raise ValueError(
                f"NWSS data not available for the requested report date {report_date}"
            )
    else:
        loc_level_nwss_data = None

    param_estimates = pl.scan_parquet(Path(param_data_dir, "prod.parquet"))

    model_batch_dir_name = (
        f"{disease.lower()}_r_{report_date}_f_"
        f"{first_training_date}_t_{last_training_date}"
    )

    model_batch_dir = Path(output_dir, model_batch_dir_name)

    model_run_dir = Path(model_batch_dir, "model_runs", loc)
    model_dir = Path(model_run_dir, pyrenew_model_name)
    data_dir = Path(model_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    logger.info("Recording git info...")
    record_git_info(model_dir)

    logger.info(f"Copying and recording priors from {priors_path}...")
    copy_and_record_priors(priors_path, model_dir)

    logger.info(f"Processing {loc}")
    process_and_save_loc_data(
        loc_abb=loc,
        disease=disease,
        facility_level_nssp_data=facility_level_nssp_data,
        loc_level_nwss_data=loc_level_nwss_data,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        save_dir=data_dir,
        logger=logger,
        credentials_dict=credentials_dict,
        nhsn_data_path=nhsn_data_path,
    )

    process_and_save_loc_param(
        loc_abb=loc,
        disease=disease,
        loc_level_nwss_data=loc_level_nwss_data,
        param_estimates=param_estimates,
        fit_ed_visits=fit_ed_visits,
        save_dir=data_dir,
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
    create_hubverse_table(model_dir)

    if fit_ed_visits:
        if Path(model_run_dir, "daily_ts_ensemble_e").exists():
            logger.info("Creating daily proportions from daily_ts_ensemble_e...")
            create_prop_samples(
                model_run_dir=model_run_dir,
                num_model_name=pyrenew_model_name,
                other_model_name="daily_ts_ensemble_e",
                aggregate_num=False,
                aggregate_other=False,
                save=True,
            )
            tmp_model_name = f"prop_{pyrenew_model_name}_daily_ts_ensemble_e"
            tmp_model_fit_dir = Path(model_run_dir, tmp_model_name)
            make_figures_from_model_fit_dir(
                tmp_model_fit_dir,
                save_figs=True,
                save_ci=True,
            )
            create_hubverse_table(tmp_model_fit_dir)

        else:
            logger.warning(
                "daily_ts_ensemble_e not found, skipping creation of daily proportions for comparison with pyrenew model."
            )
        if Path(model_run_dir, "epiweekly_ts_ensemble_e").exists():
            logger.info(
                "Creating epiweekly proportions from epiweekly_ts_ensemble_e..."
            )
            create_prop_samples(
                model_run_dir=model_run_dir,
                num_model_name=pyrenew_model_name,
                other_model_name="epiweekly_ts_ensemble_e",
                aggregate_num=True,
                aggregate_other=False,
                save=True,
            )
            tmp_model_name = f"prop_epiweekly_aggregated_{pyrenew_model_name}_epiweekly_ts_ensemble_e"
            tmp_model_fit_dir = Path(model_run_dir, tmp_model_name)

            make_figures_from_model_fit_dir(
                tmp_model_fit_dir,
                save_figs=True,
                save_ci=True,
            )
            create_hubverse_table(tmp_model_fit_dir)

        else:
            logger.warning(
                "epiweekly_ts_ensemble_e not found, skipping creation of epiweekly proportions for comparison with pyrenew model."
            )
        if Path(model_run_dir, "epiautogp_nssp_daily_other").exists():
            logger.info("Creating daily proportions from epiautogp_nssp_daily_other...")
            create_prop_samples(
                model_run_dir=model_run_dir,
                num_model_name=pyrenew_model_name,
                other_model_name="epiautogp_nssp_daily_other",
                aggregate_num=False,
                aggregate_other=False,
                save=True,
            )
            tmp_model_name = f"prop_{pyrenew_model_name}_epiautogp_nssp_daily_other"
            tmp_model_fit_dir = Path(model_run_dir, tmp_model_name)
            make_figures_from_model_fit_dir(
                tmp_model_fit_dir,
                save_figs=True,
                save_ci=True,
            )
            create_hubverse_table(tmp_model_fit_dir)

            logger.info(
                "Creating epiweekly proportions from epiautogp_nssp_daily_other..."
            )
            create_prop_samples(
                model_run_dir=model_run_dir,
                num_model_name=pyrenew_model_name,
                other_model_name="epiautogp_nssp_daily_other",
                aggregate_num=True,
                aggregate_other=True,
                save=True,
            )
            tmp_model_name = f"prop_epiweekly_aggregated_{pyrenew_model_name}_epiweekly_aggregated_epiautogp_nssp_daily_other"
            tmp_model_fit_dir = Path(model_run_dir, tmp_model_name)
            make_figures_from_model_fit_dir(
                tmp_model_fit_dir,
                save_figs=True,
                save_ci=True,
            )
            create_hubverse_table(tmp_model_fit_dir)
        else:
            logger.warning(
                "epiautogp_nssp_daily_other not found, skipping creation of proportions for comparison with epiautogp model."
            )

    logger.info("Postprocessing complete.")

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
        "--nwss-data-dir",
        type=Path,
        default=Path("private_data", "nwss_vintages"),
        help="Directory in which to look for NWSS data.",
    )

    parser.add_argument(
        "--priors-path",
        type=Path,
        help=(
            "Path to an executible python file defining random variables "
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
        "--param-data-dir",
        type=Path,
        default=Path("private_data", "prod_param_estimates"),
        help="Directory in which to look for parameter estimates such as delay PMFs.",
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
