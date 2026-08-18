import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast, get_args

from cfa.stf.data import get_nnh_right_truncation_pmf

from cfa.stf.routine._paths import EPIAUTOGP_DIR
from cfa.stf.routine.data.data_access import ForecastSourceName
from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.data.nowcast import NowcastSource
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig
from cfa.stf.routine.epiautogp.prep_epiautogp_data import (
    _validate_epiautogp_parameters,
    convert_to_epiautogp_json,
)
from cfa.stf.routine.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast
from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.common_utils import (
    generate_epiweekly_data,
    parse_exclude_date_ranges,
    run_julia_script,
)

_FIT_SCRIPT = Path(__file__).parent / "fit_epiautogp.jl"
NowcastSourceName = Literal["none", "reporting-delay", "hubverse"]
VALID_NOWCAST_SOURCE_NAMES: tuple[str, ...] = get_args(NowcastSourceName)


@dataclass(frozen=True)
class EpiAutoGPDependencies:
    """Run-dependent resources resolved for an EpiAutoGP forecast."""

    nowcast_source: NowcastSource | None


def run_epiautogp_forecast(
    json_input_path: Path,
    model_dir: Path,
    params: dict,
    execution_settings: dict,
) -> None:
    """
    Run the EpiAutoGP forecasting model using the direct NowcastAutoGP Julia script.

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
    This function runs the direct NowcastAutoGP Julia script. The output is
    saved to model_dir.
    """
    # Ensure output directory exists
    model_dir.mkdir(parents=True, exist_ok=True)

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
        f"{_FIT_SCRIPT}",
        args_to_epiautogp,
        executor_flags=executor_flags,
        function_name="run_epiautogp_forecast",
    )
    return None


def _build_reporting_delay_nowcast(
    *,
    forecast_run: ForecastRun,
    reporting_delay_pmf: list[float] | None,
) -> ReportingDelayNowcast:
    """Build a reporting-delay source, fetching the PMF when necessary."""
    if reporting_delay_pmf is None:
        reporting_delay_pmf = get_nnh_right_truncation_pmf(
            state_abb=forecast_run.loc,
            disease=forecast_run.disease,
            as_of=forecast_run.report_date,
            reference_date=forecast_run.report_date,
        )
    return ReportingDelayNowcast(reporting_delay_pmf=reporting_delay_pmf)


def _resolve_nowcast_source(
    *,
    forecast_run: ForecastRun,
    config: EpiAutoGPConfig,
    nowcast_source_name: NowcastSourceName,
    reporting_delay_pmf: list[float] | None = None,
    hubverse_nowcast_dir: Path | str | None = None,
) -> NowcastSource | None:
    """Resolve the requested nowcast source for one EpiAutoGP run."""
    if reporting_delay_pmf is not None and hubverse_nowcast_dir is not None:
        raise ValueError(
            "reporting_delay_pmf and hubverse_nowcast_dir are mutually exclusive."
        )

    match nowcast_source_name:
        case "none":
            return None
        case "reporting-delay":
            ReportingDelayNowcast.ensure_applicable(config=config)
            return _build_reporting_delay_nowcast(
                forecast_run=forecast_run,
                reporting_delay_pmf=reporting_delay_pmf,
            )
        case "hubverse":
            HubverseNowcast.ensure_applicable(config=config)
            if hubverse_nowcast_dir is None:
                raise ValueError(
                    "hubverse_nowcast_dir is required when Hubverse nowcasting is "
                    "requested."
                )
            return HubverseNowcast(
                containing_dir=Path(hubverse_nowcast_dir),
                forecast_run=forecast_run,
                config=config,
            )
        case _:
            raise ValueError(
                f"nowcast_source_name must be one of "
                f"{list(VALID_NOWCAST_SOURCE_NAMES)}, got {nowcast_source_name!r}"
            )


class EpiAutoGPPipeline(ForecastPipeline[EpiAutoGPDependencies]):
    """Single-location EpiAutoGP forecast pipeline."""

    def __init__(
        self,
        *,
        target: str,
        frequency: str,
        ed_visit_type: str = "observed",
        exclude_date_ranges: list[tuple[dt.date, dt.date]] | None = None,
        n_particles: int = 24,
        n_mcmc: int = 100,
        n_hmc: int = 50,
        n_forecast_draws: int = 2000,
        smc_data_proportion: float = 0.1,
        n_threads: int | str = "auto",
        nowcast_source_name: NowcastSourceName = "none",
        reporting_delay_pmf: list[float] | None = None,
        hubverse_nowcast_dir: Path | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config = EpiAutoGPConfig(
            target=target,
            frequency=frequency,
            ed_visit_type=ed_visit_type,
            exclude_date_ranges=exclude_date_ranges,
        )
        self.n_particles = n_particles
        self.n_mcmc = n_mcmc
        self.n_hmc = n_hmc
        self.n_forecast_draws = n_forecast_draws
        self.smc_data_proportion = smc_data_proportion
        self.n_threads = n_threads
        self.nowcast_source_name = nowcast_source_name
        self.reporting_delay_pmf = reporting_delay_pmf
        self.hubverse_nowcast_dir = hubverse_nowcast_dir

    @property
    def model_name(self) -> str:
        model_name = f"epiautogp_{self.config.target}_{self.config.frequency}"
        if self.config.ed_visit_type == "pct":
            model_name += "_pct"
        if self.config.ed_visit_type == "other":
            model_name += "_other"
        return model_name

    @property
    def sources(self) -> set[ForecastSourceName]:
        return {cast(ForecastSourceName, self.config.target)}

    def validate_configuration(self) -> None:
        _validate_epiautogp_parameters(
            self.config.target,
            self.config.frequency,
            self.config.ed_visit_type,
        )

    def resolve_run_dependencies(self, run: ForecastRun) -> EpiAutoGPDependencies:
        return EpiAutoGPDependencies(
            nowcast_source=_resolve_nowcast_source(
                forecast_run=run,
                config=self.config,
                nowcast_source_name=self.nowcast_source_name,
                reporting_delay_pmf=self.reporting_delay_pmf,
                hubverse_nowcast_dir=self.hubverse_nowcast_dir,
            )
        )

    def after_data_serialization(self, run: ForecastRun) -> None:
        if self.config.frequency == "epiweekly":
            self.logger.info("Generating epiweekly datasets from daily datasets...")
            generate_epiweekly_data(run.data_dir, overwrite_daily=True)

    def fit_and_forecast(
        self,
        run: ForecastRun,
        dependencies: EpiAutoGPDependencies,
    ) -> None:
        self.logger.info("Converting data to EpiAutoGP JSON format...")
        input_json_path = convert_to_epiautogp_json(
            forecast_run=run,
            config=self.config,
            nowcast_source=dependencies.nowcast_source,
            logger=self.logger,
        )

        n_ahead = (
            (run.n_forecast_days + 6) // 7
            if self.config.frequency == "epiweekly"
            else run.n_forecast_days
        )
        transformation = (
            "percentage" if self.config.ed_visit_type == "pct" else "boxcox"
        )
        params = {
            "n_ahead": n_ahead,
            "n_particles": self.n_particles,
            "n_mcmc": self.n_mcmc,
            "n_hmc": self.n_hmc,
            "n_forecast_draws": self.n_forecast_draws,
            "transformation": transformation,
            "smc_data_proportion": self.smc_data_proportion,
        }
        execution_settings = {
            "project": str(EPIAUTOGP_DIR),
            "threads": self.n_threads,
        }

        self.logger.info("Performing EpiAutoGP forecasting...")
        run_epiautogp_forecast(
            json_input_path=input_json_path,
            model_dir=run.model_dir,
            params=params,
            execution_settings=execution_settings,
        )


def main(
    disease: str,
    run_date: dt.date,
    loc: str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    target: str,
    frequency: str,
    ed_visit_type: str = "observed",
    exclude_last_n_days: int = 0,
    exclude_date_ranges: str = None,
    n_particles: int = 24,
    n_mcmc: int = 100,
    n_hmc: int = 50,
    n_forecast_draws: int = 2000,
    smc_data_proportion: float = 0.1,
    n_threads: int | str = "auto",
    nowcast_source_name: str = "none",
    reporting_delay_pmf: list[float] | None = None,
    hubverse_nowcast_dir: Path | str | None = None,
    fail_on_stale_data: bool = False,
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
        Disease to model ("covid", "flu", or "rsv")
    run_date : datetime.date
        Date of the forecast run
    loc : str
        Two-letter USPS location abbreviation (e.g., "CA", "NY")
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
    ed_visit_type : str, default="observed"
        Type of ED visits to model: "observed" (disease-related), "other" (non-disease background), or "pct" (percentage of total ED visits). Only applicable for NSSP target
    exclude_last_n_days : int, default=0
        Number of recent days to exclude from training
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
    nowcast_source_name : str, default="none"
        Nowcast source to use: "none", "reporting-delay", or "hubverse"
    reporting_delay_pmf : list[float] | None, default=None
        Directly supplied reporting-delay PMF. Python API only.
    hubverse_nowcast_dir : Path | str | None, default=None
        Local directory containing a materialized Hubverse model-output asset.
        Required when nowcast_source_name="hubverse".

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If invalid parameter combinations are provided (e.g., frequency="daily" with target="nhsn")
    FileNotFoundError
        If required data files or directories don't exist
    RuntimeError
        If Julia execution or R plotting fails

    Notes
    -----
    For epiweekly forecasts, n_forecast_days is converted to weeks by dividing
    by 7 and rounding up. The transformation type is set to "percentage" if
    ed_visit_type=="pct", otherwise "boxcox" is used.

    The model name is automatically generated based on target, frequency, and ed_visit_type parameters.
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

    logger.info(
        "Starting single-location EpiAutoGP forecasting pipeline for "
        f"location {loc}, and run date {run_date}"
    )

    EpiAutoGPPipeline(
        disease=disease,
        loc=loc,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
        output_dir=output_dir,
        n_training_days=n_training_days,
        n_forecast_days=n_forecast_days,
        exclude_last_n_days=exclude_last_n_days,
        exclude_date_ranges=parsed_exclude_date_ranges,
        logger=logger,
        nowcast_source_name=nowcast_source_name,
        reporting_delay_pmf=reporting_delay_pmf,
        hubverse_nowcast_dir=hubverse_nowcast_dir,
        run_date=run_date,
        fail_on_stale_data=fail_on_stale_data,
        n_particles=n_particles,
        n_mcmc=n_mcmc,
        n_hmc=n_hmc,
        n_forecast_draws=n_forecast_draws,
        smc_data_proportion=smc_data_proportion,
        n_threads=n_threads,
    ).execute()
    return None
