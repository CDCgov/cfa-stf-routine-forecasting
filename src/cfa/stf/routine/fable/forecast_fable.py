import datetime as dt
import logging
from pathlib import Path

from cfa.stf.forecasttools import write_tabular

from cfa.stf.routine._paths import FABLE_DIR
from cfa.stf.routine.data.data_access import ForecastSourceName
from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.data_utils import generate_epiweekly_data
from cfa.stf.routine.utils.language_utils import run_r_script


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
        FABLE_DIR / "fit_fable.R",
        script_args,
        function_name="fit_fable",
        capture_output=False,
    )
    return None


class FablePipeline(ForecastPipeline):
    """Single-location Fable E-other forecast pipeline."""

    def __init__(self, *, n_samples: int, epiweekly: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n_samples = n_samples
        self.epiweekly = epiweekly

    @property
    def model_name(self) -> str:
        prefix = "epiweekly" if self.epiweekly else "daily"
        return f"{prefix}_fable_e_other"

    @property
    def sources(self) -> set[ForecastSourceName]:
        return {"nssp"}

    def transform_serialized_data(self, run: ForecastRun) -> None:
        if self.epiweekly:
            self.logger.info("Generating epiweekly datasets from daily datasets...")
            data_path = run.data_dir / "combined_data.tsv"
            epiweekly_data = generate_epiweekly_data(data_path)
            write_tabular(epiweekly_data, data_path)

    def run_model(self, run: ForecastRun) -> None:
        n_days_past_last_training = run.n_forecast_days + run.exclude_last_n_days
        self.logger.info("Performing fable E-other forecasting")
        fable_e_other_forecasts(
            run.model_dir,
            n_days_past_last_training,
            self.n_samples,
        )


def main(
    disease: str,
    loc: str,
    output_dir: Path | str,
    n_training_days: int,
    n_forecast_days: int,
    n_samples: int,
    run_date: dt.date,
    exclude_last_n_days: int = 0,
    epiweekly: bool = False,
    fail_on_stale_data: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    FablePipeline(
        disease=disease,
        loc=loc,
        output_dir=output_dir,
        n_training_days=n_training_days,
        n_forecast_days=n_forecast_days,
        run_date=run_date,
        exclude_last_n_days=exclude_last_n_days,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
        n_samples=n_samples,
        epiweekly=epiweekly,
    ).execute()
    return None
