"""
Set up a multi-location, multi-disease run of cfa-stf-routine-forecasting
with the EpiAutoGP model family on Azure Batch.
"""

import argparse
import itertools
from pathlib import Path

from rich.console import Console

from pipelines.batch.common_batch_utils import (
    DEFAULT_EXCLUDED_LOCATIONS,
    create_job_details_table,
    get_filtered_locations,
    get_pyrenew_container_settings,
    print_job_header,
    string_to_boolean,
    submit_batch_job,
    validate_diseases,
)


def main(
    job_id: str,
    pool_id: str,
    diseases: str | list[str],
    output_subdir: str | Path = "./",
    container_image_name: str = "cfa-stf-routine-forecasting",
    container_image_version: str = "latest",
    n_training_days: int = 150,
    exclude_last_n_days: int = 1,
    locations_include: list[str] | None = None,
    locations_exclude: list[str] | None = None,
    test: bool = False,
    dry_run: bool = False,
    # EpiAutoGP-specific parameters
    target: str = "nssp",
    frequency: str = "epiweekly",
    ed_visit_type: str = "observed",
    n_particles: int = 24,
    n_mcmc: int = 100,
    n_hmc: int = 50,
    n_forecast_draws: int = 2000,
    smc_data_proportion: float = 0.1,
    n_threads: int | str = "auto",
    n_forecast_days: int = 28,
) -> None:
    """
    Set up and submit an EpiAutoGP model job to Azure Batch.

    Parameters
    ----------
    job_id
        Name for the Batch job.
    pool_id
        Azure Batch pool on which to run the job.
    diseases
        Name(s) of disease(s) to run as part of the job,
        as a list of strings.
        Supported values are 'COVID-19', 'Influenza', and 'RSV'.
    output_subdir
        Subdirectory of the output blob storage container
        in which to save results.
    container_image_name
        Name of the container to use for the job.
        Default 'cfa-stf-routine-forecasting'.
    container_image_version
        Version of the container to use. Default 'latest'.
    n_training_days
        Number of training days of data to use for model fitting.
        Default 150.
    exclude_last_n_days
        Number of days of available data to exclude from fitting.
        Default 1.
    locations_include
        List of two-letter USPS location abbreviations for locations
        to include in the job. If None, use all available locations.
    locations_exclude
        List of additional two letter USPS location abbreviations to
        exclude from the job. If None, only default locations excluded.
    test
        Is this a testing run? Default False.
    dry_run
        If set, do not submit tasks to Azure Batch. Default False.
    target
        Target data type for EpiAutoGP: 'nssp' for ED visit data or
        'nhsn' for hospital admission counts. Default 'nssp'.
    frequency
        Data frequency for EpiAutoGP: 'daily' or 'epiweekly'. Default 'epiweekly'.
    ed_visit_type
        Type of ED visits for EpiAutoGP: 'observed', 'other', or 'pct'. Default 'observed'.
    n_particles
        Number of particles for SMC in EpiAutoGP. Default 24.
    n_mcmc
        Number of MCMC steps for GP kernel structure in EpiAutoGP. Default 100.
    n_hmc
        Number of HMC steps for GP kernel hyperparameters in EpiAutoGP. Default 50.
    n_forecast_draws
        Number of forecast draws for EpiAutoGP. Default 2000.
    smc_data_proportion
        Proportion of data used in each SMC step in EpiAutoGP. Default 0.1.
    n_threads
        Number of threads for EpiAutoGP Julia execution. Default "auto".
    n_forecast_days
        Number of days ahead to forecast. Default 28.

    Returns
    -------
    None
    """
    # Validate inputs
    validate_diseases(diseases)

    # Validate EpiAutoGP-specific parameters
    if target not in ["nssp", "nhsn"]:
        raise ValueError(f"Invalid target: {target}. Must be 'nssp' or 'nhsn'.")
    if frequency not in ["daily", "epiweekly"]:
        raise ValueError(
            f"Invalid frequency: {frequency}. Must be 'daily' or 'epiweekly'."
        )
    if ed_visit_type not in ["observed", "other", "pct"]:
        raise ValueError(
            f"Invalid ed_visit_type: {ed_visit_type}. Must be 'observed', 'other', or 'pct'."
        )

    # Get filtered locations
    all_locations = get_filtered_locations(locations_include, locations_exclude)
    all_exclusions = list(set(DEFAULT_EXCLUDED_LOCATIONS + (locations_exclude or [])))

    # Container setup
    container_image = f"ghcr.io/cdcgov/{container_image_name}:{container_image_version}"
    container_settings = get_pyrenew_container_settings(container_image, test)

    # Build command
    run_script = "epiautogp/forecast_epiautogp.py"
    additional_args = (
        f"--target {target} "
        f"--frequency {frequency} "
        f"--ed-visit-type {ed_visit_type} "
        f"--n-particles {n_particles} "
        f"--n-mcmc {n_mcmc} "
        f"--n-hmc {n_hmc} "
        f"--n-forecast-draws {n_forecast_draws} "
        f"--smc-data-proportion {smc_data_proportion} "
        f"--n-threads {n_threads} "
        f"--n-forecast-days {n_forecast_days} "
    )

    base_call = (
        "/bin/bash -c '"
        f"uv run python pipelines/{run_script} "
        "--disease {disease} "
        "--loc {loc} "
        f"--n-training-days {n_training_days} "
        "--facility-level-nssp-data-dir nssp-etl/gold "
        "--param-data-dir params "
        "--output-dir {output_dir} "
        "--credentials-path config/creds.toml "
        f"--exclude-last-n-days {exclude_last_n_days} "
        f"{additional_args}"
        "'"
    )

    # Print job details
    print_job_header("epiautogp")
    console = Console()

    details = {
        "job_id": job_id,
        "pool_id": pool_id,
        "model_family": "epiautogp",
        "diseases": ", ".join(diseases),
        "output_subdir": str(output_subdir),
        "container_image": container_image,
        "training_days": n_training_days,
        "exclude_last_n_days": exclude_last_n_days,
        "target": target,
        "frequency": frequency,
        "ed_visit_type": ed_visit_type,
        "n_particles": n_particles,
        "n_mcmc": n_mcmc,
        "n_hmc": n_hmc,
        "n_forecast_draws": n_forecast_draws,
        "smc_data_proportion": smc_data_proportion,
        "n_threads": n_threads,
        "n_forecast_days": n_forecast_days,
        "locations_included": all_locations,
        "excluded_locations": ", ".join(all_exclusions),
        "test": test,
        "dry_run": dry_run,
    }

    table = create_job_details_table(details)
    console.print(table)

    # Create tasks
    tasks = []
    for disease, loc in itertools.product(diseases, all_locations):
        task_id = f"{job_id}-{loc}-{disease}-prod"
        command = base_call.format(
            loc=loc,
            disease=disease,
            output_dir=str(Path("output", output_subdir)),
        )
        tasks.append((task_id, command, container_settings))

    # Submit job
    submit_batch_job(job_id, pool_id, tasks, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set up EpiAutoGP model job on Azure Batch"
    )
    parser.add_argument(
        "--job-id", type=str, required=True, help="Name for the Azure batch job"
    )
    parser.add_argument(
        "--pool-id",
        type=str,
        help="Name of the Azure batch pool on which to run the job",
        default="pyrenew-pool",
    )
    parser.add_argument(
        "--diseases",
        type=str,
        default="COVID-19 Influenza RSV",
        help=(
            "Name(s) of disease(s) to run as part of the job, "
            "as a whitespace-separated string. Default 'COVID-19 Influenza RSV'."
        ),
    )
    parser.add_argument(
        "--output-subdir",
        type=str,
        help="Subdirectory of the output blob storage container in which to save results.",
        default="./",
    )
    parser.add_argument(
        "--container-image-name",
        type=str,
        help="Name of the container to use for the job.",
        default="cfa-stf-routine-forecasting",
    )
    parser.add_argument(
        "--container-image-version",
        type=str,
        help="Version of the container to use for the job.",
        default="latest",
    )
    parser.add_argument(
        "--n-training-days",
        type=int,
        help="Number of 'training days' of observed data to use for model fitting.",
        default=150,
    )
    parser.add_argument(
        "--exclude-last-n-days",
        type=int,
        help="Number of days to drop from the end of the timeseries of observed data.",
        default=1,
    )
    parser.add_argument(
        "--locations-include",
        type=str,
        help=(
            "Two-letter USPS location abbreviations to include in the job, "
            "as a whitespace-separated string."
        ),
        default=None,
    )
    parser.add_argument(
        "--locations-exclude",
        type=str,
        help=(
            f"Additional two-letter USPS location abbreviations to exclude from the job, "
            f"as a whitespace-separated string. These will be combined with the default "
            f"exclusions ({' '.join(DEFAULT_EXCLUDED_LOCATIONS)})."
        ),
        default=None,
    )
    parser.add_argument(
        "--target",
        type=str,
        default="nssp",
        choices=["nssp", "nhsn"],
        help="Target data type: 'nssp' for ED visit data or 'nhsn' for hospital admissions.",
    )
    parser.add_argument(
        "--frequency",
        type=str,
        default="epiweekly",
        choices=["daily", "epiweekly"],
        help="Data frequency: 'daily' or 'epiweekly' (default: epiweekly).",
    )
    parser.add_argument(
        "--ed-visit-type",
        type=str,
        default="observed",
        choices=["observed", "other", "pct"],
        help="Type of ED visits: 'observed', 'other', or 'pct' (default: observed).",
    )
    parser.add_argument(
        "--n-particles",
        type=int,
        default=24,
        help="Number of particles for SMC (default: 24).",
    )
    parser.add_argument(
        "--n-mcmc",
        type=int,
        default=100,
        help="Number of MCMC steps for GP kernel structure (default: 100).",
    )
    parser.add_argument(
        "--n-hmc",
        type=int,
        default=50,
        help="Number of HMC steps for GP kernel hyperparameters (default: 50).",
    )
    parser.add_argument(
        "--n-forecast-draws",
        type=int,
        default=2000,
        help="Number of forecast draws (default: 2000).",
    )
    parser.add_argument(
        "--smc-data-proportion",
        type=float,
        default=0.1,
        help="Proportion of data used in each SMC step (default: 0.1).",
    )
    parser.add_argument(
        "--n-threads",
        type=lambda v: int(v) if v.isdigit() else v,
        default="auto",
        help="Number of threads for Julia execution (integer or 'auto'; default: auto).",
    )
    parser.add_argument(
        "--n-forecast-days",
        type=int,
        default=28,
        help="Number of days ahead to forecast (default: 28).",
    )
    parser.add_argument(
        "--test",
        type=string_to_boolean,
        nargs="?",
        const=True,
        default=False,
        help="Run in test mode (default: False).",
    )
    parser.add_argument(
        "--dry-run",
        type=string_to_boolean,
        nargs="?",
        const=True,
        default=False,
        help="If set to True, do not submit tasks to Azure Batch. Only print what would be done.",
    )
    args = parser.parse_args()
    args.diseases = args.diseases.split()
    args.locations_include = (
        args.locations_include.split() if args.locations_include else None
    )
    args.locations_exclude = (
        args.locations_exclude.split() if args.locations_exclude else None
    )

    main(**vars(args))
