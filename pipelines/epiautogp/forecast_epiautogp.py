import argparse
import logging
from pathlib import Path

from pipelines.epiautogp import (
    convert_to_epiautogp_json,
    setup_forecast_pipeline,
)
from pipelines.utils.cli_utils import add_common_forecast_arguments
from pipelines.utils.common_utils import (
    parse_exclude_date_ranges,
    run_julia_code,
    run_julia_script,
)


def run_epiautogp_forecast(
    json_input_path: Path,
    model_dir: Path,
    params: dict,
    execution_settings: dict,
) -> None:
    """
    Run EpiAutoGP forecasting model using Julia.

    Parameters
    ----------
    json_input_path : Path
        Path to the JSON input file for EpiAutoGP.
    model_dir : Path
        Directory to save model outputs.
    params : dict
        Parameters to pass to EpiAutoGP. Expected keys:
        - n_ahead: Number of time steps to forecast
        - n_particles: Number of particles for SMC
        - n_mcmc: Number of MCMC steps for GP kernel structure
        - n_hmc: Number of HMC steps for GP kernel hyperparameters
        - n_forecast_draws: Number of forecast draws to generate
        - transformation: Type of transformation ("percentage" or "boxcox")
        - smc_data_proportion: Proportion of data used in each SMC step
    execution_settings : dict
        Execution settings for the Julia environment. Expected keys:
        - project: Julia project name
        - threads: Number of threads to use or "auto"

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        If Julia environment setup or script execution fails

    Notes
    -----
    This function sets up the EpiAutoGP Julia environment and runs the
    forecasting script. The output is saved to model_dir.
    """
    # Ensure output directory exists
    model_dir.mkdir(parents=True, exist_ok=True)

    # Instantiate julia environment for EpiAutoGP
    run_julia_code(
        """
        using Pkg
        Pkg.activate("EpiAutoGP")
        Pkg.instantiate()
        """,
        function_name="setup_epiautogp_environment",
    )

    # Add path arguments to pass to EpiAutoGP
    params["json-input"] = str(json_input_path)
    params["output-dir"] = str(model_dir)

    # Convert Python dict keys (with underscores) to Julia CLI args (with hyphens)
    args_to_epiautogp = [
        f"--{key.replace('_', '-')}={value}" for key, value in params.items()
    ]
    executor_flags = [f"--{key}={value}" for key, value in execution_settings.items()]

    # Run Julia script
    run_julia_script(
        "EpiAutoGP/run.jl",
        args_to_epiautogp,
        executor_flags=executor_flags,
        function_name="run_epiautogp_forecast",
    )
    return None


def main(
    disease: str,
    report_date: str,
    loc: str,
    facility_level_nssp_data_dir: Path | str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    target: str,
    frequency: str,
    use_percentage: bool = False,
    ed_visit_type: str = "observed",
    exclude_last_n_days: int = 0,
    credentials_path: Path = None,
    nhsn_data_path: Path = None,
    exclude_date_ranges: str = None,
    n_particles: int = 24,
    n_mcmc: int = 100,
    n_hmc: int = 50,
    n_forecast_draws: int = 2000,
    smc_data_proportion: float = 0.1,
    n_threads: int | str = "auto",
) -> None:
    """
    Run the complete EpiAutoGP forecasting pipeline for a single location.

    This function orchestrates the full EpiAutoGP forecasting pipeline:
    1. Sets up logging and generates model name
    2. Loads and validates data
    3. Prepares training and evaluation datasets
    4. Converts data to EpiAutoGP JSON format
    5. Runs EpiAutoGP forecasting model
    6. Post-processes forecast outputs and generates plots

    Parameters
    ----------
    disease : str
        Disease to model (e.g., "COVID-19", "Influenza", "RSV")
    report_date : str
        Report date in YYYY-MM-DD format or "latest"
    loc : str
        Two-letter USPS location abbreviation (e.g., "CA", "NY")
    facility_level_nssp_data_dir : Path | str
        Directory containing facility-level NSSP ED visit data
    output_dir : Path | str
        Root directory for output
    n_training_days : int
        Number of days of training data
    n_forecast_days : int
        Number of days ahead to forecast
    target : str
        Target data type: "nssp" for ED visit data or
        "nhsn" for hospital admission counts
    frequency : str
        Data frequency: "daily" or "epiweekly"
    use_percentage : bool, default=False
        If True, convert ED visits to percentage of total ED visits
        (only applicable for NSSP target)
    ed_visit_type : str, default="observed"
        Type of ED visits to model: "observed" (disease-related) or
        "other" (non-disease background). Only applicable for NSSP target
    exclude_last_n_days : int, default=0
        Number of recent days to exclude from training
    credentials_path : Path | None, default=None
        Path to credentials file for data access
    nhsn_data_path : Path | None, default=None
        Path to NHSN hospital admission data
    exclude_date_ranges : str | None, default=None
        Comma-separated list of date ranges to exclude from training data.
        Format: 'YYYY-MM-DD:YYYY-MM-DD,YYYY-MM-DD' for ranges and single dates.
        Example: '2024-01-15:2024-01-20,2024-03-01'
    n_particles : int, default=24
        Number of particles for Sequential Monte Carlo (SMC)
    n_mcmc : int, default=100
        Number of MCMC steps for GP kernel structure learning
    n_hmc : int, default=50
        Number of Hamiltonian Monte Carlo steps for GP hyperparameters
    n_forecast_draws : int, default=2000
        Number of forecast draws to generate
    smc_data_proportion : float, default=0.1
        Proportion of data used in each SMC step
    n_threads : int | str, default="auto"
        Number of threads for Julia execution (integer or "auto")

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If invalid parameter combinations are provided (e.g., use_percentage=True
        with target="nhsn", or frequency="daily" with target="nhsn")
    FileNotFoundError
        If required data files or directories don't exist
    RuntimeError
        If Julia execution or R plotting fails

    Notes
    -----
    For epiweekly forecasts, n_forecast_days is converted to weeks by dividing
    by 7 and rounding up. The transformation type is set to "percentage" if
    use_percentage=True, otherwise "boxcox" is used.

    The model name is automatically generated based on target, frequency,
    use_percentage, and ed_visit_type parameters.
    """
    # Step 0: Set up logging, model name and params to pass to epiautogp
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Parse exclude_date_ranges
    parsed_exclude_date_ranges = parse_exclude_date_ranges(exclude_date_ranges)
    if parsed_exclude_date_ranges:
        logger.info(
            f"Excluding {len(parsed_exclude_date_ranges)} date range(s): "
            f"{parsed_exclude_date_ranges}"
        )

    # Generate model name
    model_name = f"epiautogp_{target}_{frequency}"
    if use_percentage:
        model_name += "_pct"
    if ed_visit_type == "other":
        model_name += "_other"

    # Declare transformation type
    if use_percentage:
        transformation = "percentage"
    else:
        transformation = "boxcox"

    # Calculate n_ahead based on frequency
    # For epiweekly data, convert days to weeks (rounded up)
    if frequency == "epiweekly":
        n_ahead = (n_forecast_days + 6) // 7  # Round up to nearest week
    else:
        n_ahead = n_forecast_days

    # Epiautogp params and execution settings
    params = {
        "n_ahead": n_ahead,
        "n_particles": n_particles,
        "n_mcmc": n_mcmc,
        "n_hmc": n_hmc,
        "n_forecast_draws": n_forecast_draws,
        "transformation": transformation,
        "smc_data_proportion": smc_data_proportion,
    }
    execution_settings = {
        "project": "EpiAutoGP",
        "threads": n_threads,
    }

    logger.info(
        "Starting single-location EpiAutoGP forecasting pipeline for "
        f"location {loc}, and report date {report_date}"
    )

    # Step 1: Setup pipeline (loads data, validates dates, creates directories)
    # This is the context of the forecast pipeline
    context = setup_forecast_pipeline(
        disease=disease,
        loc=loc,
        target=target,
        frequency=frequency,
        use_percentage=use_percentage,
        ed_visit_type=ed_visit_type,
        model_name=model_name,
        nhsn_data_path=nhsn_data_path,
        facility_level_nssp_data_dir=facility_level_nssp_data_dir,
        output_dir=output_dir,
        n_training_days=n_training_days,
        n_forecast_days=n_forecast_days,
        exclude_last_n_days=exclude_last_n_days,
        exclude_date_ranges=parsed_exclude_date_ranges,
        credentials_path=credentials_path,
        logger=logger,
    )

    # Step 2: Prepare data for modelling (process location data, epiweekly data)
    # returns paths to prepared data files and directories
    paths = context.prepare_model_data()

    # Step 3: Convert data to EpiAutoGP JSON format
    logger.info("Converting data to EpiAutoGP JSON format...")
    epiautogp_input_json_path = convert_to_epiautogp_json(
        context=context,
        paths=paths,
    )

    # Step 4: Run EpiAutoGP forecast
    logger.info("Performing EpiAutoGP forecasting...")
    run_epiautogp_forecast(
        json_input_path=epiautogp_input_json_path,
        model_dir=paths.model_output_dir,
        params=params,
        execution_settings=execution_settings,
    )

    # Step 5: Post-process forecast outputs
    context.post_process_forecast()

    logger.info(
        "Single-location EpiAutoGP pipeline complete "
        f"for location {loc}, and "
        f"report date {report_date}."
    )
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run EpiAutoGP forecasting pipeline for disease modeling."
    )

    # Add common arguments
    add_common_forecast_arguments(parser)

    # Add EpiAutoGP-specific arguments
    parser.add_argument(
        "--report-date",
        type=str,
        default="latest",
        help=(
            "Report date in YYYY-MM-DD format or 'latest' to use "
            "the most recent available data (default: latest)."
        ),
    )

    parser.add_argument(
        "--target",
        type=str,
        required=True,
        choices=["nssp", "nhsn"],
        help=(
            "Target data type: 'nssp' for ED visit data or "
            "'nhsn' for hospital admission counts."
        ),
    )

    parser.add_argument(
        "--frequency",
        type=str,
        default="epiweekly",
        choices=["daily", "epiweekly"],
        help="Data frequency: 'daily' or 'epiweekly' (default: epiweekly).",
    )

    parser.add_argument(
        "--use-percentage",
        action="store_true",
        help=(
            "Convert ED visits to percentage of total ED visits "
            "(only applicable for NSSP target)."
        ),
    )

    parser.add_argument(
        "--ed-visit-type",
        type=str,
        default="observed",
        choices=["observed", "other"],
        help=(
            "Type of ED visits to model: 'observed' (disease-related) or "
            "'other' (non-disease background). Only applicable for NSSP target "
            "(default: observed)."
        ),
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
        help="Number of threads to use for EpiAutoGP computations (integer or 'auto'; default: auto).",
    )

    parser.add_argument(
        "--exclude-date-ranges",
        type=str,
        default=None,
        help=(
            "Comma-separated list of date ranges to exclude from training data "
            "due to known reporting problems. "
            "Format: 'YYYY-MM-DD:YYYY-MM-DD' for ranges or 'YYYY-MM-DD' for single dates. "
            "Example: '2024-01-15:2024-01-20,2024-03-01' excludes Jan 15-20 and Mar 1."
        ),
    )

    args = parser.parse_args()
    main(**vars(args))
