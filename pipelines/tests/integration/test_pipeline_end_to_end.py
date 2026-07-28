import os
import shutil
import time
from contextlib import contextmanager
from math import isclose
from pathlib import Path

import polars as pl
import pytest

from pipelines.data import prep_data
from pipelines.data.generate_test_data import (
    DEFAULT_DISEASES,
    DEFAULT_LOCATIONS,
    HUBVERSE_N_SAMPLES,
    HUBVERSE_NOWCAST_DIR_NAME,
    REPORT_DATE,
    REPORTING_DELAY_PMF,
    make_forecast_data,
    write_hubverse_nowcasts,
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

FORECAST_DIR_NAME = f"{REPORT_DATE.isoformat()}_forecasts"
N_TRAINING_DAYS = 42
N_FORECAST_DAYS = 14
EXCLUDE_LAST_N_DAYS = 1
HUBVERSE_DISEASE = "COVID-19"
HUBVERSE_LOCATION = "CA"
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


def _normalize_pmf(weights: list[float]) -> list[float]:
    total = sum(weights)
    pmf = [weight / total for weight in weights]
    assert isclose(sum(pmf), 1.0)
    return pmf


GENERATION_INTERVAL_PMF = _normalize_pmf([64, 23, 9, 3, 1])
DELAY_PMF = _normalize_pmf([0, 2, 17, 24, 20, 14, 9, 6, 4, 2, 1, 1])
RIGHT_TRUNCATION_PMF = _normalize_pmf(REPORTING_DELAY_PMF.tolist())


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
        run_date=REPORT_DATE,
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
        run_date=REPORT_DATE,
        rng_key=12345,
        fit_ed_visits=True,
        forecast_ed_visits=True,
    )


def _run_epiautogp(
    workspace: Path,
    disease: str,
    location: str,
    *,
    target: str,
    frequency: str,
    nowcast_source_name: str,
    ed_visit_type: str = "observed",
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


def _assert_model_outputs(model_run_dir: Path, *, expect_hubverse: bool) -> None:
    expected_models = EXPECTED_MODELS.copy()
    if expect_hubverse:
        expected_models.append("epiautogp_nhsn_epiweekly")

    for model_name in expected_models:
        model_dir = model_run_dir / model_name
        assert model_dir.is_dir(), f"Missing model directory: {model_dir}"
        assert (model_dir / "samples.parquet").is_file(), (
            f"Missing samples parquet: {model_dir}"
        )
        assert (model_dir / "hubverse_table.parquet").is_file(), (
            f"Missing hubverse table: {model_dir}"
        )


def _patch_dataops(monkeypatch) -> None:
    def load_forecast_data(
        *,
        disease,
        loc_abb,
        run_date,
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

    for module in (fable_module, pyrenew_module):
        monkeypatch.setattr(module, "load_forecast_data", load_forecast_data)
    monkeypatch.setattr(epiautogp_utils, "load_forecast_data", load_forecast_data)
    monkeypatch.setattr(
        prep_data,
        "get_nnh_generation_interval_pmf",
        lambda **kwargs: GENERATION_INTERVAL_PMF.copy(),
    )
    monkeypatch.setattr(
        prep_data,
        "get_nnh_delay_pmf",
        lambda **kwargs: DELAY_PMF.copy(),
    )
    monkeypatch.setattr(
        prep_data,
        "get_nnh_right_truncation_pmf",
        lambda **kwargs: RIGHT_TRUNCATION_PMF.copy(),
    )
    monkeypatch.setattr(
        epiautogp_utils,
        "get_nnh_right_truncation_pmf",
        lambda **kwargs: RIGHT_TRUNCATION_PMF.copy(),
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
    with _status_step(f"Generating Hubverse test data in {workspace}"):
        write_hubverse_nowcasts(
            workspace,
            locations=DEFAULT_LOCATIONS,
            diseases=DEFAULT_DISEASES,
        )

    data_mode = _data_mode(request)
    print(f"[pipeline-e2e] Using {data_mode} DataOps data.", flush=True)

    if data_mode == MOCK_DATA_MODE:
        _patch_dataops(monkeypatch)

    for disease in DEFAULT_DISEASES:
        for location in DEFAULT_LOCATIONS:
            # One generated combination is enough to exercise Hubverse.
            expect_hubverse = (
                disease == HUBVERSE_DISEASE and location == HUBVERSE_LOCATION
            )
            with _status_step(f"Running Fable for {disease}, {location}"):
                _run_fable(workspace, disease, location)

            with _status_step(f"Running epiweekly Fable for {disease}, {location}"):
                _run_fable(workspace, disease, location, epiweekly=True)

            with _status_step(f"Running PyRenew for {disease}, {location}"):
                _run_pyrenew(workspace, disease, location)

            with _status_step(
                f"Running reporting-delay EpiAutoGP for {disease}, {location}"
            ):
                _run_epiautogp(
                    workspace,
                    disease,
                    location,
                    target="nssp",
                    frequency="daily",
                    ed_visit_type="other",
                    nowcast_source_name="reporting-delay",
                )

            if expect_hubverse:
                # In this situation we also check the NHSN ("H") signal
                # and that the EpiAutoGP model is run with the Hubverse nowcast source.
                with _status_step(
                    f"Running Hubverse EpiAutoGP for {disease}, {location}"
                ):
                    _run_epiautogp(
                        workspace,
                        disease,
                        location,
                        target="nhsn",
                        frequency="epiweekly",
                        nowcast_source_name="hubverse",
                        hubverse_nowcast_dir=(
                            workspace
                            / "private_data"
                            / HUBVERSE_NOWCAST_DIR_NAME
                            / disease.lower()
                        ),
                        n_forecast_draws=HUBVERSE_N_SAMPLES,
                    )

            model_run_dir = (
                _model_batch_dir(workspace, disease) / "model_runs" / location
            )
            with _status_step(f"Running fusion models for {disease}, {location}"):
                _run_fusions(model_run_dir)

            with _status_step(f"Checking model outputs for {disease}, {location}"):
                _assert_model_outputs(model_run_dir, expect_hubverse=expect_hubverse)

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
