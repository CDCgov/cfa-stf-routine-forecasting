import datetime as dt
import logging
import shutil
import tomllib
from pathlib import Path

import tomli_w
from pyrenew_multisignal.hew.utils import pyrenew_model_name_from_flags

from cfa.stf.routine._paths import PYRENEW_HEW_DIR
from cfa.stf.routine.data.data_access import ForecastSourceName
from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.pyrenew_hew.fit_pyrenew_model import fit_and_save_model
from cfa.stf.routine.pyrenew_hew.generate_predictive import (
    generate_and_save_predictions,
)
from cfa.stf.routine.pyrenew_hew.model_inputs import (
    PyRenewModelInputs,
    resolve_pyrenew_model_inputs,
    serialize_pyrenew_model_params,
)
from cfa.stf.routine.utils.common_utils import run_r_script


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
        PYRENEW_HEW_DIR / "create_samples_from_pyrenew_fit_dir.R",
        [str(model_fit_dir)],
        function_name="create_samples_from_pyrenew_fit_dir",
    )
    return None


class PyRenewPipeline(ForecastPipeline[PyRenewModelInputs]):
    """Single-location PyRenew HEW forecast pipeline."""

    def __init__(
        self,
        *,
        priors_path: Path,
        n_chains: int,
        n_warmup: int,
        n_samples: int,
        fit_ed_visits: bool = False,
        fit_hospital_admissions: bool = False,
        fit_wastewater: bool = False,
        forecast_ed_visits: bool = False,
        forecast_hospital_admissions: bool = False,
        forecast_wastewater: bool = False,
        rng_key: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.priors_path = priors_path
        self.n_chains = n_chains
        self.n_warmup = n_warmup
        self.n_samples = n_samples
        self.fit_ed_visits = fit_ed_visits
        self.fit_hospital_admissions = fit_hospital_admissions
        self.fit_wastewater = fit_wastewater
        self.forecast_ed_visits = forecast_ed_visits
        self.forecast_hospital_admissions = forecast_hospital_admissions
        self.forecast_wastewater = forecast_wastewater
        self.rng_key = rng_key

    @property
    def model_name(self) -> str:
        return pyrenew_model_name_from_flags(
            fit_ed_visits=self.fit_ed_visits,
            fit_hospital_admissions=self.fit_hospital_admissions,
            fit_wastewater=self.fit_wastewater,
        )

    @property
    def sources(self) -> set[ForecastSourceName]:
        sources: set[ForecastSourceName] = set()
        if self.fit_ed_visits:
            sources.add("nssp")
        if self.fit_hospital_admissions:
            sources.add("nhsn")
        return sources

    def validate_configuration(self) -> None:
        if self.fit_wastewater or self.forecast_wastewater:
            raise ValueError(
                "Wastewater data loading is no longer supported in this pipeline."
            )

        signals = ["ed_visits", "hospital_admissions"]
        for signal in signals:
            fit = getattr(self, f"fit_{signal}")
            forecast = getattr(self, f"forecast_{signal}")
            if fit and not forecast:
                raise ValueError(
                    "This pipeline does not currently support "
                    "fitting to but not forecasting a signal. "
                    f"Asked to fit but not forecast {signal}."
                )
        if not any(getattr(self, f"fit_{signal}") for signal in signals):
            raise ValueError(
                "pyrenew_null (fitting to no signals) is not supported by this pipeline"
            )

    def resolve_model_inputs(self, run: ForecastRun) -> PyRenewModelInputs:
        return resolve_pyrenew_model_inputs(
            run=run,
            fit_ed_visits=self.fit_ed_visits,
        )

    def before_data_preparation(self, run: ForecastRun) -> None:
        self.logger.info("Copying and recording priors from %s...", self.priors_path)
        copy_and_record_priors(self.priors_path, run.model_dir)

    def after_data_serialization(
        self,
        run: ForecastRun,
        model_inputs: PyRenewModelInputs,
    ) -> None:
        serialize_pyrenew_model_params(
            run=run,
            model_inputs=model_inputs,
            save_dir=run.data_dir,
        )

    def fit_and_forecast(
        self,
        run: ForecastRun,
        model_inputs: PyRenewModelInputs,
    ) -> None:
        self.logger.info("Fitting model...")
        fit_and_save_model(
            run.model_dir,
            n_warmup=self.n_warmup,
            n_samples=self.n_samples,
            n_chains=self.n_chains,
            fit_ed_visits=self.fit_ed_visits,
            fit_hospital_admissions=self.fit_hospital_admissions,
            fit_wastewater=self.fit_wastewater,
            rng_key=self.rng_key,
        )
        self.logger.info("Model fitting complete")

        self.logger.info("Performing posterior prediction / forecasting...")
        n_days_past_last_training = run.n_forecast_days + run.exclude_last_n_days
        generate_and_save_predictions(
            run.model_run_dir,
            run.model_name,
            n_days_past_last_training,
            predict_ed_visits=self.forecast_ed_visits,
            predict_hospital_admissions=self.forecast_hospital_admissions,
            predict_wastewater=self.forecast_wastewater,
            rng_key=self.rng_key,
        )
        self.logger.info("All forecasting complete.")

    def before_post_process(self, run: ForecastRun) -> None:
        self.logger.info("Creating daily counts...")
        create_samples_from_pyrenew_fit_dir(run.model_dir)


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
    run_date: dt.date,
    exclude_last_n_days: int = 0,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
    fit_wastewater: bool = False,
    forecast_ed_visits: bool = False,
    forecast_hospital_admissions: bool = False,
    forecast_wastewater: bool = False,
    rng_key: int | None = None,
    fail_on_stale_data: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    PyRenewPipeline(
        disease=disease,
        loc=loc,
        priors_path=priors_path,
        output_dir=output_dir,
        n_training_days=n_training_days,
        n_forecast_days=n_forecast_days,
        n_chains=n_chains,
        n_warmup=n_warmup,
        n_samples=n_samples,
        run_date=run_date,
        exclude_last_n_days=exclude_last_n_days,
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
        forecast_ed_visits=forecast_ed_visits,
        forecast_hospital_admissions=forecast_hospital_admissions,
        forecast_wastewater=forecast_wastewater,
        rng_key=rng_key,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    ).execute()
    return None
