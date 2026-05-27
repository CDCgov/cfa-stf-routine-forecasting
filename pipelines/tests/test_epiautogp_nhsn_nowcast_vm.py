"""VM-only smoke test for a future Dagster EpiAutoGP NHSN nowcast route."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest

from pipelines.epiautogp import convert_to_epiautogp_json
from pipelines.epiautogp.epiautogp_forecast_utils import (
    setup_forecast_pipeline,
)
from pipelines.epiautogp.forecast_epiautogp import run_epiautogp_forecast
from pipelines.epiautogp.hubverse_pointer import resolve_artifact_path

RUN_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_TEST"
TARGET_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_TARGET"
LOCATION_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_LOCATION"
CONTAINER_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_CONTAINER"
POINTER_URI_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_POINTER_URI"
FACILITY_NSSP_DIR_ENV = "EPIAUTOGP_NHSN_NOWCAST_VM_NSSP_DATA_DIR"

_TARGET_TO_DISEASE = {
    "covid": "COVID-19",
    "flu": "Influenza",
    "rsv": "RSV",
}


def _skip_unless_enabled() -> None:
    if os.environ.get(RUN_ENV) != "1":
        pytest.skip(f"Set {RUN_ENV}=1 on a VM with nowcastnhsn-prod mounted to run.")


def _require_r_packages(*packages: str) -> None:
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.fail("Rscript is required for the enabled NHSN nowcast smoke test.")

    package_vector = ", ".join(json.dumps(package) for package in packages)
    result = subprocess.run(
        [
            rscript,
            "-e",
            (
                f"pkgs <- c({package_vector}); "
                "missing <- pkgs[!vapply(pkgs, requireNamespace, "
                "logical(1), quietly = TRUE)]; "
                "if (length(missing)) { writeLines(missing); quit(status = 42) }"
            ),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode == 0:
        return

    missing = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    detail = ", ".join(missing) if missing else result.stderr.strip()
    pytest.fail(
        "R packages required for EpiAutoGP data preparation are unavailable: "
        f"{detail}. Install the repo R dependencies or run this smoke test in "
        "the production container image."
    )


def _run_epiautogp_nhsn_nowcast_model(
    *,
    disease: str,
    loc: str,
    output_dir: Path,
    facility_level_nssp_data_dir: Path,
    hubverse_nowcast_pointer_uri: str,
) -> tuple[Path, Path]:
    """Run the Dagster-shaped EpiAutoGP NHSN nowcast route.

    This helper is intentionally shaped like the core of the `_run_pyrenew_model`
    pattern in `dagster_defs.py`, without taking a
    `DynamicGraphAssetExecutionContext` or adding a Dagster asset in this issue.
    A future Dagster wrapper would get `disease` and `loc` from graph dimensions,
    build an output directory for the partition, and then call logic like this
    with the mounted `nssp-etl/gold` path. We leave `nhsn_data_path=None` so NHSN
    HRD data is read through the same forecasttools route as PyRenew-H, and point
    EpiAutoGP at the mounted `nowcastnhsn-prod/latest/<target>.json` Hubverse
    handoff pointer.

    The tiny sampler settings are smoke-test only; the data-prep and nowcast
    ingestion path is the production path.
    """
    context = setup_forecast_pipeline(
        disease=disease,
        loc=loc,
        target="nhsn",
        frequency="epiweekly",
        ed_visit_type="observed",
        model_name="epiautogp_nhsn_epiweekly",
        nhsn_data_path=None,
        facility_level_nssp_data_dir=facility_level_nssp_data_dir,
        output_dir=output_dir,
        n_training_days=20 * 7,
        n_forecast_days=28,
        exclude_last_n_days=0,
        exclude_date_ranges=None,
        credentials_path=None,
        logger=logging.getLogger(__name__),
        nowcast_source_name="hubverse",
        hubverse_nowcast_pointer_uri=hubverse_nowcast_pointer_uri,
    )

    paths = context.prepare_model_data()
    input_json_path = convert_to_epiautogp_json(context=context, paths=paths)

    run_epiautogp_forecast(
        json_input_path=input_json_path,
        model_dir=paths.model_output_dir,
        params={
            "n_ahead": 4,
            "n_particles": 2,
            "n_mcmc": 1,
            "n_hmc": 1,
            "n_forecast_draws": 20,
            "transformation": "boxcox",
            "smc_data_proportion": 0.25,
        },
        execution_settings={
            "project": "pipelines/epiautogp",
            "threads": "1",
        },
    )
    return input_json_path, paths.model_output_dir / "samples.parquet"


def test_epiautogp_runs_with_mounted_prod_nhsn_nowcasts(tmp_path):
    """Smoke the future Dagster EpiAutoGP NHSN-nowcast helper shape.

    The enabled test uses the same mounted `nssp-etl/gold` input convention as
    `_run_pyrenew_model`, deliberately leaves `nhsn_data_path=None` to follow
    PyRenew-H's forecasttools NHSN HRD route, and reads the production-shaped
    nowcast pointer from mounted `nowcastnhsn-prod/latest`.
    """
    _skip_unless_enabled()
    if shutil.which("julia") is None:
        pytest.skip("Julia is not available on PATH.")
    _require_r_packages(
        "argparser",
        "dplyr",
        "forecasttools",
        "fs",
        "lubridate",
        "readr",
        "stringr",
        "tidyr",
    )

    target = os.environ.get(TARGET_ENV, "covid").lower()
    try:
        disease = _TARGET_TO_DISEASE[target]
    except KeyError as exc:
        valid_targets = ", ".join(sorted(_TARGET_TO_DISEASE))
        raise ValueError(f"{TARGET_ENV} must be one of {valid_targets}") from exc

    loc = os.environ.get(LOCATION_ENV, "CA").upper()
    container = os.environ.get(CONTAINER_ENV, "nowcastnhsn-prod")
    pointer_uri = os.environ.get(
        POINTER_URI_ENV,
        f"az://{container}/latest/{target}.json",
    )
    try:
        resolve_artifact_path(
            pointer_uri,
            source_label="production NHSN nowcast pointer",
        )
    except (FileNotFoundError, PermissionError) as exc:
        pytest.skip(
            "Production NHSN nowcast Blob is not mounted/readable. "
            "Run `make mount` on the VM, or make sure /mnt/nowcastnhsn-prod "
            f"is available. Details: {exc}"
        )

    facility_nssp_data_dir = Path(
        os.environ.get(FACILITY_NSSP_DIR_ENV, "nssp-etl/gold")
    )
    if not facility_nssp_data_dir.exists():
        pytest.skip(
            "Mounted facility-level NSSP data directory is not available. "
            f"Set {FACILITY_NSSP_DIR_ENV} if needed: {facility_nssp_data_dir}"
        )
    if not any(facility_nssp_data_dir.glob("*.parquet")):
        pytest.skip(
            "Mounted facility-level NSSP data directory has no report parquet "
            f"files: {facility_nssp_data_dir}"
        )

    input_json_path, samples_path = _run_epiautogp_nhsn_nowcast_model(
        disease=disease,
        loc=loc,
        output_dir=tmp_path,
        facility_level_nssp_data_dir=facility_nssp_data_dir,
        hubverse_nowcast_pointer_uri=pointer_uri,
    )

    epiautogp_input = json.loads(input_json_path.read_text(encoding="utf-8"))
    assert len(epiautogp_input["dates"]) >= 8
    assert epiautogp_input["nowcast_dates"]
    assert epiautogp_input["nowcast_reports"]
    assert set(epiautogp_input["nowcast_dates"]).issubset(epiautogp_input["dates"])

    assert samples_path.exists()
    samples = pl.read_parquet(samples_path)
    assert samples.height > 0
    assert samples.select(pl.col(".value").is_not_null().all()).item()
