import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest

from pipelines.data import prep_data
from pipelines.data.generate_test_data import (
    DEFAULT_DISEASES,
    DEFAULT_LOCATIONS,
    REPORT_DATE,
    make_forecast_data,
    make_param_estimates,
)
from pipelines.epiautogp import epiautogp_forecast_utils as epiautogp_utils
from pipelines.epiautogp import forecast_epiautogp as epiautogp_module
from pipelines.fable import forecast_fable as fable_module
from pipelines.pyrenew_hew import forecast_pyrenew as pyrenew_module
from pipelines.utils.common_utils import (
    create_prop_samples,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
    parse_model_batch_dir_name,
)
from pipelines.utils.postprocess_forecast_batches import main as postprocess_batches

FORECAST_DIR_NAME = "2024-12-21_forecasts"
N_TRAINING_DAYS = 42
N_FORECAST_DAYS = 14
EXCLUDE_LAST_N_DAYS = 1
EXPECTED_MODELS = [
    "daily_fable_e_other",
    "epiweekly_fable_e_other",
    "pyrenew_e",
    "epiautogp_nssp_daily_other",
    "prop_pyrenew_e_daily_fable_e_other",
    "prop_epiweekly_aggregated_pyrenew_e_epiweekly_fable_e_other",
    "prop_pyrenew_e_epiautogp_nssp_daily_other",
]
MOCK_DATA_MODE = "mock"
REAL_DATA_MODE = "real"


@contextmanager
def _status_step(message: str):
    started_at = time.perf_counter()
    print(f"[pipeline-e2e] {message}...", flush=True)
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - started_at
        print(f"[pipeline-e2e] {message} failed after {elapsed:.1f}s", flush=True)
        raise
    else:
        elapsed = time.perf_counter() - started_at
        print(f"[pipeline-e2e] {message} complete in {elapsed:.1f}s", flush=True)


@pytest.fixture
def pipeline_workspace(request, tmp_path, monkeypatch):
    retained_dir = request.config.getoption("--e2e-output-dir")
    force = request.config.getoption("--e2e-force")

    if retained_dir is None:
        workspace = tmp_path / "pipeline-e2e"
    else:
        workspace = Path(retained_dir)
        if workspace.exists():
            if not force:
                pytest.fail(
                    f"Retained workspace already exists: {workspace}. "
                    "Pass --e2e-force to remove it before running."
                )
            shutil.rmtree(workspace)

    workspace.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("MPLCONFIGDIR", str(workspace / ".matplotlib"))
    monkeypatch.setenv(
        "XLA_FLAGS",
        os.environ.get("XLA_FLAGS", "--xla_force_host_platform_device_count=2"),
    )
    return workspace


def _run_fable(
    workspace: Path, disease: str, location: str, *, epiweekly: bool = False
) -> None:
    fable_module.main(
        disease=disease,
        loc=location,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        n_samples=40,
        epiweekly=epiweekly,
    )


def _run_pyrenew(workspace: Path, disease: str, location: str) -> None:
    pyrenew_module.main(
        disease=disease,
        loc=location,
        priors_path=Path("pipelines/pyrenew_hew/priors/prod_priors.py"),
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        n_chains=1,
        n_samples=40,
        n_warmup=40,
        rng_key=12345,
        fit_ed_visits=True,
        forecast_ed_visits=True,
    )


def _run_epiautogp(workspace: Path, disease: str, location: str) -> None:
    epiautogp_module.main(
        disease=disease,
        report_date="latest",
        loc=location,
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        target="nssp",
        frequency="daily",
        ed_visit_type="other",
        n_particles=2,
        n_mcmc=2,
        n_hmc=2,
        n_forecast_draws=40,
        smc_data_proportion=0.1,
        n_threads=2,
    )


def _model_batch_dir(workspace: Path, disease: str) -> Path:
    candidates = list((workspace / FORECAST_DIR_NAME).glob(f"{disease.lower()}_r_*"))
    assert len(candidates) == 1, (
        f"Expected one batch directory for {disease}, "
        f"found {len(candidates)} in {workspace / FORECAST_DIR_NAME}"
    )
    return candidates[0]


def _run_fusions(model_run_dir: Path) -> None:
    fusion_specs = [
        {
            "num_model_name": "pyrenew_e",
            "other_model_name": "daily_fable_e_other",
            "aggregate_num": False,
            "fusion_model_name": "prop_pyrenew_e_daily_fable_e_other",
        },
        {
            "num_model_name": "pyrenew_e",
            "other_model_name": "epiweekly_fable_e_other",
            "aggregate_num": True,
            "fusion_model_name": (
                "prop_epiweekly_aggregated_pyrenew_e_epiweekly_fable_e_other"
            ),
        },
        {
            "num_model_name": "pyrenew_e",
            "other_model_name": "epiautogp_nssp_daily_other",
            "aggregate_num": False,
            "fusion_model_name": "prop_pyrenew_e_epiautogp_nssp_daily_other",
        },
    ]
    for fusion_spec in fusion_specs:
        create_prop_samples(
            model_run_dir=model_run_dir,
            num_model_name=fusion_spec["num_model_name"],
            other_model_name=fusion_spec["other_model_name"],
            aggregate_num=fusion_spec["aggregate_num"],
            save=True,
        )
        fusion_model_dir = model_run_dir / fusion_spec["fusion_model_name"]
        make_figures_from_model_fit_dir(
            fusion_model_dir,
            save_figs=True,
            save_ci=True,
        )
        model_fit_dir_to_hub_tbl(fusion_model_dir)


def _assert_model_outputs(model_run_dir: Path) -> None:
    for model_name in EXPECTED_MODELS:
        model_dir = model_run_dir / model_name
        assert model_dir.is_dir(), f"Missing model directory: {model_dir}"
        assert (model_dir / "samples.parquet").is_file(), (
            f"Missing samples parquet: {model_dir}"
        )
        assert (model_dir / "hubverse_table.parquet").is_file(), (
            f"Missing hubverse table: {model_dir}"
        )


def _patch_dataops(monkeypatch) -> None:
    param_estimates = make_param_estimates().lazy()

    def resolve_report_date(run_date=None):
        return REPORT_DATE

    def load_forecast_data(
        *,
        disease,
        loc_abb,
        report_date,
        first_training_date,
        last_training_date,
        **kwargs,
    ):
        return make_forecast_data(
            location=loc_abb,
            disease=disease,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
        )

    def process_and_save_loc_param(
        *,
        loc_abb,
        disease,
        param_estimates=None,
        fit_ed_visits,
        save_dir,
        as_of=None,
    ):
        return prep_data.process_and_save_loc_param(
            loc_abb=loc_abb,
            disease=disease,
            param_estimates=param_estimates,
            fit_ed_visits=fit_ed_visits,
            save_dir=save_dir,
            as_of=as_of,
        )

    for module in (fable_module, pyrenew_module):
        monkeypatch.setattr(module, "resolve_nssp_report_date", resolve_report_date)
        monkeypatch.setattr(module, "load_forecast_data", load_forecast_data)
    monkeypatch.setattr(
        epiautogp_utils, "resolve_nssp_report_date", resolve_report_date
    )
    monkeypatch.setattr(epiautogp_utils, "load_forecast_data", load_forecast_data)
    monkeypatch.setattr(
        pyrenew_module,
        "process_and_save_loc_param",
        lambda **kwargs: process_and_save_loc_param(
            **{**kwargs, "param_estimates": param_estimates}
        ),
    )


def _has_external_dataops_env() -> bool:
    try:
        from cfa.cloudops.util import check_ext_env
    except ImportError:
        print(
            "[pipeline-e2e] cfa.cloudops.util.check_ext_env is unavailable; "
            "using mocked DataOps data.",
            flush=True,
        )
        return False

    return check_ext_env()


def _data_mode(request) -> str:
    requested_mode = request.config.getoption("--e2e-data-mode")
    if requested_mode == "auto":
        return REAL_DATA_MODE if _has_external_dataops_env() else MOCK_DATA_MODE
    return requested_mode


@pytest.mark.pipeline_e2e
def test_reduced_pipeline_end_to_end(pipeline_workspace, monkeypatch, request):
    workspace = pipeline_workspace
    data_mode = _data_mode(request)
    print(f"[pipeline-e2e] Using {data_mode} DataOps data.", flush=True)

    if data_mode == MOCK_DATA_MODE:
        _patch_dataops(monkeypatch)

    for disease in DEFAULT_DISEASES:
        for location in DEFAULT_LOCATIONS:
            with _status_step(f"Running Fable for {disease}, {location}"):
                _run_fable(workspace, disease, location)

            with _status_step(f"Running epiweekly Fable for {disease}, {location}"):
                _run_fable(workspace, disease, location, epiweekly=True)

            with _status_step(f"Running PyRenew for {disease}, {location}"):
                _run_pyrenew(workspace, disease, location)

            with _status_step(f"Running EpiAutoGP for {disease}, {location}"):
                _run_epiautogp(workspace, disease, location)

            model_run_dir = (
                _model_batch_dir(workspace, disease) / "model_runs" / location
            )
            with _status_step(f"Running fusion models for {disease}, {location}"):
                _run_fusions(model_run_dir)

            with _status_step(f"Checking model outputs for {disease}, {location}"):
                _assert_model_outputs(model_run_dir)

    with _status_step("Postprocessing forecast batches"):
        postprocess_batches(
            workspace / FORECAST_DIR_NAME,
            diseases=DEFAULT_DISEASES,
            skip_existing=False,
            local_copy_dir=workspace / FORECAST_DIR_NAME,
        )

    for disease in DEFAULT_DISEASES:
        with _status_step(f"Checking postprocessed outputs for {disease}"):
            batch_dir = _model_batch_dir(workspace, disease)
            batch_info = parse_model_batch_dir_name(batch_dir.name)
            postprocessed_path = (
                batch_dir
                / f"{batch_info['report_date']}-{disease.lower()}-hubverse-table.parquet"
            )
            assert postprocessed_path.is_file(), (
                f"Missing postprocessed hubverse table: {postprocessed_path}"
            )
            postprocessed = pl.read_parquet(postprocessed_path)
            assert postprocessed.height > 0
            assert set(postprocessed["location"].unique().to_list()) == set(
                DEFAULT_LOCATIONS
            )

            copied_figures_dir = (
                workspace
                / FORECAST_DIR_NAME
                / f"lookback-{N_TRAINING_DAYS}-omit-{EXCLUDE_LAST_N_DAYS}"
                / disease
            )
            assert copied_figures_dir.is_dir(), (
                f"Missing postprocessed figures directory: {copied_figures_dir}"
            )
            assert list(copied_figures_dir.rglob("*.pdf")), (
                f"Missing postprocessed figure PDFs under: {copied_figures_dir}"
            )
