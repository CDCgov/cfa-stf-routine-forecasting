import time
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
from tests.integration.model_test_utils import (
    EXCLUDE_LAST_N_DAYS,
    FORECAST_DIR_NAME,
    N_TRAINING_DAYS,
    assert_model_outputs,
    configure_data_mode,
    model_batch_dir,
    run_epiautogp,
    run_fable,
    run_pyrenew,
)

from cfa.stf.routine.data.generate_test_data import (
    DEFAULT_DISEASES,
    DEFAULT_LOCATIONS,
)
from cfa.stf.routine.utils.directory_utils import parse_model_batch_dir_name
from cfa.stf.routine.utils.postprocess_forecast_batches import (
    main as postprocess_batches,
)
from cfa.stf.routine.utils.prop_utils import create_prop_samples
from cfa.stf.routine.utils.r_utils import (
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)

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


@pytest.mark.pipeline_e2e
def test_reduced_pipeline_end_to_end(pipeline_workspace, monkeypatch, request):
    workspace = pipeline_workspace
    configure_data_mode(request, monkeypatch)

    for disease in DEFAULT_DISEASES:
        for location in DEFAULT_LOCATIONS:
            with _status_step(f"Running Fable for {disease}, {location}"):
                run_fable(workspace, disease, location)

            with _status_step(f"Running epiweekly Fable for {disease}, {location}"):
                run_fable(workspace, disease, location, epiweekly=True)

            with _status_step(f"Running PyRenew for {disease}, {location}"):
                run_pyrenew(workspace, disease, location)

            with _status_step(f"Running EpiAutoGP for {disease}, {location}"):
                run_epiautogp(workspace, disease, location)

            model_run_dir = (
                model_batch_dir(workspace, disease) / "model_runs" / location
            )
            with _status_step(f"Running fusion models for {disease}, {location}"):
                _run_fusions(model_run_dir)

            with _status_step(f"Checking model outputs for {disease}, {location}"):
                assert_model_outputs(
                    workspace,
                    disease,
                    location,
                    EXPECTED_MODELS,
                )

    with _status_step("Postprocessing forecast batches"):
        postprocess_batches(
            workspace / FORECAST_DIR_NAME,
            diseases=DEFAULT_DISEASES,
            skip_existing=False,
            local_copy_dir=workspace / FORECAST_DIR_NAME,
        )

    for disease in DEFAULT_DISEASES:
        with _status_step(f"Checking postprocessed outputs for {disease}"):
            batch_dir = model_batch_dir(workspace, disease)
            batch_info = parse_model_batch_dir_name(batch_dir.name)
            postprocessed_path = (
                batch_dir
                / f"{batch_info['report_date']}-{disease}-hubverse-table.parquet"
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
