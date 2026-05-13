"""Parquet checks for the direct juliacall/NowcastAutoGP EpiAutoGP runner."""

import datetime as dt
import importlib.util
import json
from pathlib import Path

import polars as pl
import pytest

from pipelines.epiautogp.juliacall_runner import run_nowcastautogp_forecast


def _write_pct_input(path: Path) -> None:
    dates = [dt.date(2024, 1, 1) + dt.timedelta(days=index) for index in range(20)]
    reports = [20.0 + index for index in range(20)]
    payload = {
        "dates": [date.isoformat() for date in dates],
        "reports": reports,
        "pathogen": "COVID-19",
        "location": "US",
        "target": "nssp",
        "frequency": "daily",
        "ed_visit_type": "pct",
        "forecast_date": dates[-1].isoformat(),
        "nowcast_dates": [],
        "nowcast_reports": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_juliacall_nowcastautogp_runner_writes_pipeline_samples_parquet(
    tmp_path,
):
    if importlib.util.find_spec("juliacall") is None:
        pytest.skip("juliacall is not installed")

    input_path = tmp_path / "epiautogp_input.json"
    model_dir = tmp_path / "model"
    _write_pct_input(input_path)

    run_nowcastautogp_forecast(
        json_input_path=input_path,
        model_dir=model_dir,
        params={
            "n_ahead": 2,
            "n_particles": 1,
            "n_mcmc": 3,
            "n_hmc": 3,
            "n_forecast_draws": 4,
            "transformation": "percentage",
            "smc_data_proportion": 0.5,
        },
        execution_settings={"threads": "1"},
    )

    samples_path = model_dir / "samples.parquet"
    assert samples_path.is_file()

    samples = pl.read_parquet(samples_path)
    expected_dates = [
        dt.date(2024, 1, 20),
        dt.date(2024, 1, 21),
        dt.date(2024, 1, 22),
    ] * 4
    expected_draws = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]

    assert samples.schema["date"] == pl.Date
    assert samples.schema[".draw"] == pl.Int32
    assert samples["date"].to_list() == expected_dates
    assert samples[".draw"].to_list() == expected_draws
    assert samples[".variable"].unique().to_list() == ["prop_disease_ed_visits"]
    assert samples["resolution"].unique().to_list() == ["daily"]
    assert samples["geo_value"].unique().to_list() == ["US"]
    assert samples["disease"].unique().to_list() == ["COVID-19"]
    assert samples[".value"].is_between(0, 1).all()

    lazy_schema = (
        pl.scan_parquet(samples_path).select(pl.col("date").min()).collect().schema
    )
    assert lazy_schema["date"] == pl.Date
