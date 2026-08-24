from math import isclose
from pathlib import Path

from pyrenew_multisignal.hew.utils import flags_from_hew_letters

from cfa.stf.routine import forecast_pipeline as forecast_pipeline_module
from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.data.data_access import DataResolution
from cfa.stf.routine.data.generate_test_data import (
    REPORT_DATE,
    REPORTING_DELAY_PMF,
    make_surveillance_inputs,
)
from cfa.stf.routine.epiautogp import forecast_epiautogp as epiautogp_module
from cfa.stf.routine.fable import forecast_fable as fable_module
from cfa.stf.routine.pyrenew_hew import forecast_pyrenew as pyrenew_module
from cfa.stf.routine.pyrenew_hew import model_inputs as pyrenew_inputs_module

FORECAST_DIR_NAME = f"{REPORT_DATE.isoformat()}_forecasts"
N_TRAINING_DAYS = 42
N_FORECAST_DAYS = 14
EXCLUDE_LAST_N_DAYS = 1
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
        first_training_date,
        last_training_date,
        sources,
        **kwargs,
    ):
        return make_surveillance_inputs(
            location=loc_abb,
            disease=disease,
            sources=sources,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
        )

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
        epiautogp_module,
        "get_nnh_right_truncation_pmf",
        lambda **kwargs: RIGHT_TRUNCATION_PMF.copy(),
    )


def configure_data_mode(request, monkeypatch) -> str:
    data_mode = resolve_data_mode(request)
    print(f"[pipeline-test] Using {data_mode} data.", flush=True)
    if data_mode == MOCK_DATA_MODE:
        patch_dataops_with_mock_data(monkeypatch)
    return data_mode


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
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
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
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        n_chains=1,
        n_samples=40,
        n_warmup=40,
        run_date=REPORT_DATE,
        rng_key=12345,
        **fit_flags,
        **forecast_flags,
    )


def run_epiautogp(
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
    epiautogp_module.main(
        disease=disease,
        run_date=REPORT_DATE,
        loc=location,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
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
    candidates = list((workspace / FORECAST_DIR_NAME).glob(f"{disease}_r_*"))
    assert len(candidates) == 1, (
        f"Expected one batch directory for {disease}, "
        f"found {len(candidates)} in {workspace / FORECAST_DIR_NAME}"
    )
    return candidates[0]


def assert_model_outputs(
    workspace: Path, disease: str, location: str, model_names: list[str]
) -> None:
    model_run_dir = model_batch_dir(workspace, disease) / "model_runs" / location
    for model_name in model_names:
        model_dir = model_run_dir / model_name
        assert model_dir.is_dir(), f"Missing model directory: {model_dir}"
        assert (model_dir / "samples.parquet").is_file(), (
            f"Missing samples parquet: {model_dir}"
        )
        assert (model_dir / "hubverse_table.parquet").is_file(), (
            f"Missing hubverse table: {model_dir}"
        )
