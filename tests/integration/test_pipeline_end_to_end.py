import time
from contextlib import contextmanager

import polars as pl
import pytest
from tests.integration.generate_test_data import (
    DEFAULT_DISEASES,
    DEFAULT_LOCATIONS,
    FORECAST_DATE,
)
from tests.integration.model_test_utils import (
    FORECAST_DIR_NAME,
    assert_model_outputs,
    configure_data_mode,
    run_fable,
    run_nowcastautogp,
    run_pyrenew,
)

from cfa.stf.routine.utils.postprocess_forecast_batches import (
    main as postprocess_batches,
)

EXPECTED_MODELS = [
    "daily_fable_e_other",
    "epiweekly_fable_e_other",
    "pyrenew_e",
    "nowcastautogp_nssp_daily_other",
    "prop_pyrenew_e_daily_fable_e_other",
    "prop_epiweekly_aggregated_pyrenew_e_epiweekly_fable_e_other",
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


@pytest.mark.pipeline_e2e
def test_reduced_pipeline_end_to_end(pipeline_workspace, monkeypatch, request):
    workspace = pipeline_workspace
    configure_data_mode(request, monkeypatch)

    for disease in DEFAULT_DISEASES:
        for location in DEFAULT_LOCATIONS:
            with _status_step(f"Running Fable for {disease}, {location}"):
                run_fable(workspace, disease, location)

            with _status_step(f"Running epiweekly Fable for {disease}, {location}"):
                run_fable(
                    workspace,
                    disease,
                    location,
                    ed_visit_input_resolution="epiweekly",
                )

            with _status_step(f"Running PyRenew for {disease}, {location}"):
                run_pyrenew(workspace, disease, location)

            with _status_step(f"Running NowcastAutoGP for {disease}, {location}"):
                run_nowcastautogp(workspace, disease, location)

    from cfa.stf.routine.fusion import FUSION_SPECS, materialize_fusion
    from cfa.stf.routine.run_storage import find_fits, standalone_directory

    base = workspace / FORECAST_DIR_NAME
    fusion_execution = standalone_directory(base, {"operation": "fusion"})
    inputs = find_fits(base)
    for disease in DEFAULT_DISEASES:
        for location in DEFAULT_LOCATIONS:
            for spec in FUSION_SPECS:
                if spec.numerator == "pyrenew_e":
                    materialize_fusion(
                        spec, inputs, fusion_execution, location, disease
                    )

    with _status_step("Postprocessing forecast batches"):
        postprocess_batches(
            workspace / FORECAST_DIR_NAME,
        )

    for disease in DEFAULT_DISEASES:
        with _status_step(f"Checking postprocessed outputs for {disease}"):
            summaries = list(
                (workspace / FORECAST_DIR_NAME).glob(f"*/00_summary/{disease}")
            )
            assert len(summaries) == 1
            postprocessed_path = (
                summaries[0] / f"{FORECAST_DATE}-{disease}-hubverse-table.parquet"
            )
            for location in DEFAULT_LOCATIONS:
                assert_model_outputs(workspace, disease, location, EXPECTED_MODELS)
            assert postprocessed_path.is_file(), (
                f"Missing postprocessed hubverse table: {postprocessed_path}"
            )
            postprocessed = pl.read_parquet(postprocessed_path)
            assert postprocessed.height > 0
            assert set(postprocessed["location"].unique().to_list()) == set(
                DEFAULT_LOCATIONS
            )

            copied_figures_dir = summaries[0] / "figures"
            assert copied_figures_dir.is_dir(), (
                f"Missing postprocessed figures directory: {copied_figures_dir}"
            )
            assert list(copied_figures_dir.rglob("*.pdf")), (
                f"Missing postprocessed figure PDFs under: {copied_figures_dir}"
            )
