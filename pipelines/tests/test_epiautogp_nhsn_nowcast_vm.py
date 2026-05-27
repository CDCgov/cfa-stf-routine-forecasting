"""VM-only smoke test for a future Dagster EpiAutoGP NHSN nowcast route."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl
import pytest

from pipelines.epiautogp import convert_to_epiautogp_json
from pipelines.epiautogp.epiautogp_forecast_utils import (
    setup_forecast_pipeline,
)
from pipelines.epiautogp.forecast_epiautogp import run_epiautogp_forecast
from tests.cfa.stf.data.data_test_utils import requires_ext_catalog

_TARGET_TO_DISEASE = {
    "covid": "COVID-19",
    "flu": "Influenza",
    "rsv": "RSV",
}

NOWCAST_CONTAINER = "nowcastnhsn-prod"
FACILITY_NSSP_DIR = Path("nssp-etl/gold")


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


@requires_ext_catalog
def test_epiautogp_runs_with_mounted_prod_nhsn_nowcasts(tmp_path):
    """Smoke the future Dagster EpiAutoGP NHSN-nowcast helper shape.

    The enabled test uses the same mounted `nssp-etl/gold` input convention as
    `_run_pyrenew_model`, deliberately leaves `nhsn_data_path=None` to follow
    PyRenew-H's forecasttools NHSN HRD route, and reads the production-shaped
    nowcast pointer from mounted `nowcastnhsn-prod/latest`.
    """
    target = "covid"
    disease = _TARGET_TO_DISEASE[target]
    loc = "CA"

    pointer_uri = str(Path(NOWCAST_CONTAINER) / "latest" / f"{target}.json")
    if not Path(pointer_uri).exists():
        pytest.skip(
            f"Production NHSN nowcast pointer not found at {pointer_uri}. "
            "Ensure the nowcastnhsn-prod container is mounted."
        )

    if not FACILITY_NSSP_DIR.exists():
        pytest.skip(
            "Mounted facility-level NSSP data directory is not available: "
            f"{FACILITY_NSSP_DIR}"
        )
    if not any(FACILITY_NSSP_DIR.glob("*.parquet")):
        pytest.skip(
            "Mounted facility-level NSSP data directory has no report parquet "
            f"files: {FACILITY_NSSP_DIR}"
        )

    input_json_path, samples_path = _run_epiautogp_nhsn_nowcast_model(
        disease=disease,
        loc=loc,
        output_dir=tmp_path,
        facility_level_nssp_data_dir=FACILITY_NSSP_DIR,
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
