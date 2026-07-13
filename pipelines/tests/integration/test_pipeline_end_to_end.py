import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest

from pipelines.data.generate_test_data import (
    DEFAULT_DISEASES,
    DEFAULT_LOCATIONS,
)
from pipelines.data.generate_test_data import (
    main as generate_test_data,
)
from pipelines.epiautogp.forecast_epiautogp import main as forecast_epiautogp
from pipelines.fable.forecast_fable import main as forecast_fable
from pipelines.pyrenew_hew.forecast_pyrenew import main as forecast_pyrenew
from pipelines.utils.common_utils import (
    create_prop_samples,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)
from pipelines.utils.postprocess_forecast_batches import main as postprocess_batches

FORECAST_DIR_NAME = "2024-12-21_forecasts"
N_TRAINING_DAYS = 42
N_FORECAST_DAYS = 14
EXCLUDE_LAST_N_DAYS = 1
EXPECTED_OMIT_DAYS = EXCLUDE_LAST_N_DAYS
EXPECTED_MODELS = [
    "daily_fable_e_other",
    "epiweekly_fable_e_other",
    "pyrenew_e",
    "epiautogp_nssp_daily_other",
    "prop_pyrenew_e_daily_fable_e_other",
    "prop_epiweekly_aggregated_pyrenew_e_epiweekly_fable_e_other",
    "prop_pyrenew_e_epiautogp_nssp_daily_other",
]


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
    forecast_fable(
        disease=disease,
        loc=location,
        facility_level_nssp_data_dir=workspace / "private_data" / "nssp_etl_gold",
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        n_samples=40,
        epiweekly=epiweekly,
        nhsn_data_path=(
            workspace
            / "private_data"
            / "nhsn_test_data"
            / f"{disease}_{location}.parquet"
        ),
    )


def _run_pyrenew(workspace: Path, disease: str, location: str) -> None:
    forecast_pyrenew(
        disease=disease,
        loc=location,
        facility_level_nssp_data_dir=workspace / "private_data" / "nssp_etl_gold",
        priors_path=Path("pipelines/pyrenew_hew/priors/prod_priors.py"),
        param_data_dir=workspace / "private_data" / "prod_param_estimates",
        nwss_data_dir=workspace / "private_data" / "nwss_vintages",
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
        nhsn_data_path=(
            workspace
            / "private_data"
            / "nhsn_test_data"
            / f"{disease}_{location}.parquet"
        ),
    )


def _run_epiautogp(workspace: Path, disease: str, location: str) -> None:
    forecast_epiautogp(
        disease=disease,
        report_date="latest",
        loc=location,
        facility_level_nssp_data_dir=workspace / "private_data" / "nssp_etl_gold",
        param_data_dir=workspace / "private_data" / "prod_param_estimates",
        output_dir=workspace / FORECAST_DIR_NAME,
        n_training_days=N_TRAINING_DAYS,
        n_forecast_days=N_FORECAST_DAYS,
        exclude_last_n_days=EXCLUDE_LAST_N_DAYS,
        target="nssp",
        frequency="daily",
        ed_visit_type="other",
        nhsn_data_path=(
            workspace
            / "private_data"
            / "nhsn_test_data"
            / f"{disease}_{location}.parquet"
        ),
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


@pytest.mark.pipeline_e2e
def test_reduced_pipeline_end_to_end(pipeline_workspace):
    workspace = pipeline_workspace
    with _status_step(f"Generating test data in {workspace}"):
        generate_test_data(
            workspace,
            locations=DEFAULT_LOCATIONS,
            diseases=DEFAULT_DISEASES,
        )

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
            postprocessed_path = (
                batch_dir / f"2024-12-21-{disease.lower()}-hubverse-table.parquet"
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
                / f"lookback-{N_TRAINING_DAYS}-omit-{EXPECTED_OMIT_DAYS}"
                / disease
            )
            assert copied_figures_dir.is_dir(), (
                f"Missing postprocessed figures directory: {copied_figures_dir}"
            )
            assert list(copied_figures_dir.rglob("*.pdf")), (
                f"Missing postprocessed figure PDFs under: {copied_figures_dir}"
            )
