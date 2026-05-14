"""Checks for the direct NowcastAutoGP Julia runner parquet output."""

import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest

from pipelines.utils.common_utils import run_julia_script


def _write_synthetic_input(path: Path) -> None:
    start_date = dt.date(2024, 1, 1)
    dates = [start_date + dt.timedelta(days=i) for i in range(36)]
    reports = [12.0 + (i % 7) * 0.4 + i * 0.05 for i in range(len(dates))]
    input_data = {
        "dates": [date.isoformat() for date in dates],
        "reports": reports,
        "pathogen": "COVID-19",
        "location": "US",
        "target": "nssp",
        "frequency": "daily",
        "ed_visit_type": "pct",
        "forecast_date": dt.date(2024, 2, 6).isoformat(),
        "nowcast_dates": [],
        "nowcast_reports": [],
    }
    path.write_text(json.dumps(input_data), encoding="utf-8")


def test_direct_nowcastautogp_runner_writes_pipeline_parquet(tmp_path) -> None:
    input_path = tmp_path / "epiautogp-input.json"
    output_dir = tmp_path / "model-fit"
    _write_synthetic_input(input_path)

    try:
        run_julia_script(
            "pipelines/epiautogp/fit_epiautogp.jl",
            [
                f"--json-input={input_path}",
                f"--output-dir={output_dir}",
                "--n-ahead=2",
                "--n-particles=2",
                "--n-mcmc=1",
                "--n-hmc=1",
                "--n-forecast-draws=4",
                "--transformation=percentage",
                "--smc-data-proportion=0.5",
            ],
            executor_flags=["--project=pipelines/epiautogp", "--startup-file=no"],
            function_name="test_direct_nowcastautogp_runner_writes_pipeline_parquet",
            text=True,
        )
    except FileNotFoundError:
        pytest.skip("julia is not available")

    samples_path = output_dir / "samples.parquet"
    assert samples_path.is_file()

    samples = pl.read_parquet(samples_path)
    assert samples.schema["date"] == pl.Date
    assert samples.schema[".draw"] == pl.Int32
    assert samples["date"].to_list() == [
        dt.date(2024, 2, 6),
        dt.date(2024, 2, 7),
        dt.date(2024, 2, 8),
        dt.date(2024, 2, 6),
        dt.date(2024, 2, 7),
        dt.date(2024, 2, 8),
        dt.date(2024, 2, 6),
        dt.date(2024, 2, 7),
        dt.date(2024, 2, 8),
        dt.date(2024, 2, 6),
        dt.date(2024, 2, 7),
        dt.date(2024, 2, 8),
    ]
    assert samples[".draw"].to_list() == [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]
    assert samples[".variable"].unique().to_list() == ["prop_disease_ed_visits"]
    assert samples["resolution"].unique().to_list() == ["daily"]
    assert samples["geo_value"].unique().to_list() == ["US"]
    assert samples["disease"].unique().to_list() == ["COVID-19"]
    assert samples[".value"].min() >= 0.0
    assert samples[".value"].max() <= 1.0
