import datetime as dt
import logging
from dataclasses import replace
from math import isclose
from pathlib import Path

import polars as pl
from cfa.stf.forecasttools import ceiling_mmwr_epiweek
from pyrenew_multisignal.hew.utils import flags_from_hew_letters
from tests.integration.generate_test_data import (
    REPORT_DATE,
    REPORTING_DELAY_PMF,
    make_surveillance_inputs,
)

from cfa.stf.routine import forecast_pipeline as forecast_pipeline_module
from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.data.data_access import DataResolution
from cfa.stf.routine.fable import forecast_fable as fable_module
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.nowcastautogp import forecast_nowcastautogp as nowcastautogp_module
from cfa.stf.routine.pyrenew_hew import forecast_pyrenew as pyrenew_module
from cfa.stf.routine.pyrenew_hew import model_inputs as pyrenew_inputs_module
from cfa.stf.routine.utils.data_utils import aggregate_nssp_to_epiweekly

FORECAST_DIR_NAME = f"{REPORT_DATE.isoformat()}_forecasts"
N_LOOKBACK_DAYS = 42
MOCK_DATA_MODE = "mock"
REAL_DATA_MODE = "real"


def _normalize_pmf(weights: list[float]) -> list[float]:
    total = sum(weights)
    pmf = [weight / total for weight in weights]
    assert isclose(sum(pmf), 1.0)
    return pmf


GENERATION_INTERVAL_PMF = _normalize_pmf([64, 23, 9, 3, 1])
DELAY_PMF = _normalize_pmf([0, 2, 17, 24, 20, 14, 9, 6, 4, 2, 1, 1])
RIGHT_TRUNCATION_PMF = _normalize_pmf(REPORTING_DELAY_PMF.tolist())


def resolve_data_mode(request) -> str:
    requested_mode = request.config.getoption("--e2e-data-mode")
    if requested_mode != "auto":
        return requested_mode

    try:
        from cfa.cloudops.util import check_ext_env
    except ImportError:
        print(
            "[pipeline-test] cfa.cloudops.util.check_ext_env is unavailable; "
            "using mocked data.",
            flush=True,
        )
        return MOCK_DATA_MODE

    return REAL_DATA_MODE if check_ext_env() else MOCK_DATA_MODE


def patch_dataops_with_mock_data(monkeypatch) -> None:
    def load_surveillance_inputs(
        *,
        disease,
        loc_abb,
        run_date,
        min_allowed_training_date,
        max_allowed_training_date,
        sources,
        ed_visit_input_resolution="daily",
        **kwargs,
    ):
        surveillance = make_surveillance_inputs(
            location=loc_abb,
            disease=disease,
            sources=sources,
            min_allowed_training_date=min_allowed_training_date,
            max_allowed_training_date=max_allowed_training_date,
        )
        if surveillance.nssp is not None and ed_visit_input_resolution == "epiweekly":
            surveillance = replace(
                surveillance,
                nssp=replace(
                    surveillance.nssp,
                    data=aggregate_nssp_to_epiweekly(surveillance.nssp.data),
                    resolution="epiweekly",
                ),
            )
        return surveillance

    monkeypatch.setattr(
        forecast_pipeline_module,
        "load_surveillance_inputs",
        load_surveillance_inputs,
    )
    monkeypatch.setattr(
        pyrenew_inputs_module,
        "get_nnh_generation_interval_pmf",
        lambda **kwargs: GENERATION_INTERVAL_PMF.copy(),
    )
    monkeypatch.setattr(
        pyrenew_inputs_module,
        "get_nnh_delay_pmf",
        lambda **kwargs: DELAY_PMF.copy(),
    )
    monkeypatch.setattr(
        pyrenew_inputs_module,
        "get_nnh_right_truncation_pmf",
        lambda **kwargs: RIGHT_TRUNCATION_PMF.copy(),
    )
    monkeypatch.setattr(
        nowcastautogp_module,
        "get_nnh_right_truncation_pmf",
        lambda **kwargs: RIGHT_TRUNCATION_PMF.copy(),
    )


def configure_data_mode(request, monkeypatch) -> str:
    data_mode = resolve_data_mode(request)
    print(f"[pipeline-test] Using {data_mode} data.", flush=True)
    if data_mode == MOCK_DATA_MODE:
        patch_dataops_with_mock_data(monkeypatch)
    return data_mode


def patch_mock_nssp_with_missing_values(
    monkeypatch,
) -> tuple[list[dt.date], dt.date]:
    """Null two days and drop a third within one mid-training MMWR week.

    Daily runs then see explicit ``NA`` values plus an implicit date gap, and
    epiweekly runs see one dropped week. Returns the nulled and dropped dates.
    """
    window = ForecastWindow(
        report_date=REPORT_DATE,
        n_lookback_days=N_LOOKBACK_DAYS,
    )
    midpoint = window.min_allowed_training_date + (
        (window.max_allowed_training_date - window.min_allowed_training_date) / 2
    )
    sunday = midpoint - dt.timedelta(days=(midpoint.weekday() + 1) % 7)
    null_dates = [sunday + dt.timedelta(days=offset) for offset in (1, 2)]
    dropped_date = sunday + dt.timedelta(days=3)
    original = make_surveillance_inputs

    def with_missing_values(**kwargs):
        surveillance = original(**kwargs)
        data = surveillance.nssp.data.filter(
            pl.col("date") != dropped_date
        ).with_columns(
            pl.when(~pl.col("date").is_in(null_dates)).then(pl.col(".value"))
        )
        return replace(surveillance, nssp=replace(surveillance.nssp, data=data))

    monkeypatch.setitem(globals(), "make_surveillance_inputs", with_missing_values)
    return null_dates, dropped_date


def selected_nhsn_observations(
    disease: str,
    location: str,
) -> pl.DataFrame:
    """Load the NHSN training observations selected for an integration run."""
    logger = logging.getLogger(__name__)
    forecast_window = ForecastWindow(
        report_date=REPORT_DATE,
        n_lookback_days=N_LOOKBACK_DAYS,
    )
    surveillance = forecast_pipeline_module.load_surveillance_inputs(
        disease=disease,
        loc_abb=location,
        run_date=REPORT_DATE,
        min_allowed_training_date=forecast_window.min_allowed_training_date,
        max_allowed_training_date=forecast_window.max_allowed_training_date,
        sources={"nhsn"},
        ed_visit_input_resolution="epiweekly",
        logger=logger,
    )
    if surveillance.nhsn is None:
        raise AssertionError(
            "Expected selected surveillance inputs to contain NHSN data"
        )
    return (
        surveillance.nhsn.data.filter(
            (pl.col("data_type") == "train")
            & pl.col("value").is_not_null()
            & pl.col("value").is_finite()
        )
        .select("date", "value")
        .sort("date")
    )


def run_fable(
    workspace: Path,
    disease: str,
    location: str,
    *,
    ed_visit_input_resolution: DataResolution = "daily",
) -> None:
    fable_module.main(
        disease=disease,
        loc=location,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_lookback_days=N_LOOKBACK_DAYS,
        n_samples=40,
        run_date=REPORT_DATE,
        ed_visit_input_resolution=ed_visit_input_resolution,
    )


def run_pyrenew(
    workspace: Path, disease: str, location: str, *, model_letters: str = "e"
) -> None:
    fit_flags = flags_from_hew_letters(model_letters)
    forecast_flags = flags_from_hew_letters(
        model_letters,
        flag_prefix="forecast",
    )
    pyrenew_module.main(
        disease=disease,
        loc=location,
        priors_path=PRODUCTION_PRIORS,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_lookback_days=N_LOOKBACK_DAYS,
        n_chains=1,
        n_samples=40,
        n_warmup=40,
        run_date=REPORT_DATE,
        rng_key=12345,
        **fit_flags,
        **forecast_flags,
    )


def run_nowcastautogp(
    workspace: Path,
    disease: str,
    location: str,
    *,
    target: str = "nssp",
    frequency: str = "daily",
    ed_visit_type: str = "other",
    nowcast_source_name: str = "none",
    hubverse_nowcast_dir: Path | None = None,
    n_forecast_draws: int = 40,
) -> None:
    nowcastautogp_module.main(
        disease=disease,
        run_date=REPORT_DATE,
        loc=location,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_lookback_days=N_LOOKBACK_DAYS,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
        n_particles=2,
        n_mcmc=2,
        n_hmc=2,
        n_forecast_draws=n_forecast_draws,
        smc_data_proportion=0.1,
        n_threads=2,
        nowcast_source_name=nowcast_source_name,
        hubverse_nowcast_dir=hubverse_nowcast_dir,
    )


def model_batch_dir(workspace: Path, disease: str) -> Path:
    candidates = list((workspace / FORECAST_DIR_NAME).glob(f"{disease}_lookback-*"))
    assert len(candidates) == 1, (
        f"Expected one batch directory for {disease}, "
        f"found {len(candidates)} in {workspace / FORECAST_DIR_NAME}"
    )
    return candidates[0]


def assert_model_outputs(
    workspace: Path, disease: str, location: str, model_names: list[str]
) -> None:
    model_run_dir = model_batch_dir(workspace, disease) / "model_runs" / location
    expected_forecast_through = ceiling_mmwr_epiweek(
        REPORT_DATE + dt.timedelta(weeks=3)
    )
    for model_name in model_names:
        model_dir = model_run_dir / model_name
        assert model_dir.is_dir(), f"Missing model directory: {model_dir}"
        assert (model_dir / "samples.parquet").is_file(), (
            f"Missing samples parquet: {model_dir}"
        )
        assert (model_dir / "hubverse_table.parquet").is_file(), (
            f"Missing hubverse table: {model_dir}"
        )
        samples = pl.read_parquet(model_dir / "samples.parquet")
        assert samples.get_column("date").max() == expected_forecast_through, (
            f"{model_name} forecasts do not end on {expected_forecast_through}"
        )
