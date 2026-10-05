"""Checks for the mech_nssp_daily Julia runner's outputs (requires Julia)."""

import datetime as dt
import json
import math
from pathlib import Path

import polars as pl
import pytest

from cfa.stf.routine._paths import MECH_NSSP_DAILY_DIR
from cfa.stf.routine.utils.language_utils import run_julia_script, run_r_code
from cfa.stf.routine.utils.r_utils import model_fit_dir_to_hub_tbl

REPORT_DATE = dt.date(2026, 9, 30)  # a Wednesday
FORECAST_THROUGH = dt.date(2026, 10, 24)  # the Saturday ending the third week out
CDF = [0.6, 0.8, 0.9, 0.99]
WEEKDAY_WEIGHTS = [1.05, 1.02, 1.0, 0.98, 0.95, 0.9, 1.1]
RUNNER = MECH_NSSP_DAILY_DIR / "fit_mech_nssp_daily.jl"
EXECUTOR_FLAGS = [
    f"--project={MECH_NSSP_DAILY_DIR}",
    "--startup-file=no",
    "--threads=1",
]


def _synthetic_input(
    n_days: int = 200, *, gap: tuple[int, ...] = (), zero_weekday: int | None = None
) -> dict:
    """A wave with weekday structure, a provisional tail and optional gaps."""
    last = REPORT_DATE - dt.timedelta(days=1)
    dates = [last - dt.timedelta(days=n_days - 1 - i) for i in range(n_days)]
    raw: list[float | None] = []
    observations: list[float | None] = []
    fractions: list[float | None] = []
    for index, date in enumerate(dates):
        if index in gap:
            raw.append(None)
            observations.append(None)
            fractions.append(None)
            continue
        level = 40 + 30 * math.exp(-(((index - n_days * 0.55) / 25) ** 2))
        count = round(level * WEEKDAY_WEIGHTS[date.weekday()])
        if zero_weekday is not None and date.weekday() == zero_weekday:
            count = 0
        lag = (REPORT_DATE - date).days
        fraction = CDF[lag - 1] if lag <= len(CDF) else 1.0
        reported = round(count * fraction)
        raw.append(float(reported))
        fractions.append(fraction)
        observations.append(float(round((reported + 1 - fraction) / fraction)))
    return {
        "dates": [date.isoformat() for date in dates],
        "observations": observations,
        "raw_observations": raw,
        "reporting_fractions": fractions,
        "location": "CA",
        "disease": "covid",
        "population": 39_000_000,
        "report_date": REPORT_DATE.isoformat(),
        "forecast_through": FORECAST_THROUGH.isoformat(),
    }


def _run_runner(tmp_path: Path, payload: dict, *args: str, name: str) -> Path:
    input_path = tmp_path / f"{name}-input.json"
    output_dir = tmp_path / name
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    try:
        run_julia_script(
            RUNNER,
            [f"--json-input={input_path}", f"--output-dir={output_dir}", *args],
            executor_flags=EXECUTOR_FLAGS,
            function_name=name,
            text=True,
        )
    except FileNotFoundError:
        pytest.skip("julia is not available")
    return output_dir


SMALL = ("--n-particles=40", "--n-forecast-draws=6", "--seed=7")


def test_runner_writes_pipeline_parquet_and_diagnostics(tmp_path) -> None:
    output_dir = _run_runner(
        tmp_path, _synthetic_input(gap=(90, 91)), *SMALL, name="fit"
    )

    samples = pl.read_parquet(output_dir / "samples.parquet")
    assert samples.columns == [
        "date",
        ".value",
        ".draw",
        ".variable",
        "resolution",
        "geo_value",
        "disease",
    ]
    assert samples.schema["date"] == pl.Date
    assert samples.schema[".draw"] == pl.Int32
    assert samples.schema[".value"] == pl.Float64
    n_ahead = (FORECAST_THROUGH - REPORT_DATE).days + 1
    expected_dates = [REPORT_DATE + dt.timedelta(days=i) for i in range(n_ahead)]
    assert samples["date"].to_list() == expected_dates * 6
    assert samples[".draw"].to_list() == [
        draw for draw in range(1, 7) for _ in range(n_ahead)
    ]
    assert samples[".variable"].unique().to_list() == ["observed_ed_visits"]
    assert samples["resolution"].unique().to_list() == ["daily"]
    assert samples["geo_value"].unique().to_list() == ["CA"]
    assert samples["disease"].unique().to_list() == ["covid"]
    assert samples[".value"].min() >= 0.0

    fitted = pl.read_csv(output_dir / "mech_nssp_daily_fitted.csv")
    assert fitted.height == 200
    assert fitted["observation"].null_count() == 2
    assert fitted["count"].null_count() == 0

    hyper = pl.read_csv(output_dir / "mech_nssp_daily_hyperparameters.csv")
    multipliers = hyper.filter(pl.col("parameter").str.starts_with("dow_multiplier_"))
    assert multipliers.height == 7
    assert multipliers["value"].to_list() == pytest.approx(WEEKDAY_WEIGHTS, abs=0.03)

    quantiles = pl.read_csv(output_dir / "mech_nssp_daily_quantiles.csv")
    assert quantiles.height == 7 * n_ahead

    metadata = json.loads(
        (output_dir / "mech_nssp_daily_run_metadata.json").read_text()
    )
    assert metadata["n_observed"] == 198
    assert metadata["n_ahead"] == n_ahead
    assert metadata["day_of_week"]["exclude_recent_days"] == 14
    assert metadata["n_particles"] == 40
    assert metadata["inference"] == {
        "filter": "pf",
        "hyperparameter_method": "liu_west",
        "step_days": 1.0,
        "supersample": 2,
        "liu_west_discount": 0.95,
        "filter_threads": True,
        "learned_parameters": [
            "Rt_sigma_stat",
            "R0_baseline",
            "ascertainment_trend_wander",
        ],
    }


def test_trailing_gap_is_a_filter_step_not_a_forecast(tmp_path) -> None:
    output_dir = _run_runner(
        tmp_path, _synthetic_input(gap=(198, 199)), *SMALL, name="trailing"
    )

    samples = pl.read_parquet(output_dir / "samples.parquet")
    assert samples["date"].min() == REPORT_DATE
    assert samples["date"].max() == FORECAST_THROUGH
    fitted = pl.read_csv(output_dir / "mech_nssp_daily_fitted.csv")
    assert fitted.height == 200
    assert fitted["observation"].null_count() == 2


def test_same_seed_reproduces_draws(tmp_path) -> None:
    payload = _synthetic_input()
    first = _run_runner(tmp_path, payload, *SMALL, name="first")
    second = _run_runner(tmp_path, payload, *SMALL, name="second")
    assert (
        pl.read_parquet(first / "samples.parquet")[".value"].to_list()
        == pl.read_parquet(second / "samples.parquet")[".value"].to_list()
    )


def test_rejects_too_short_history(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="observed days"):
        _run_runner(tmp_path, _synthetic_input(50), *SMALL, name="short")


def test_rejects_weekday_fallback(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="fell back to no effect"):
        _run_runner(tmp_path, _synthetic_input(zero_weekday=6), *SMALL, name="fallback")


def _skip_if_r_packages_missing(*packages: str) -> None:
    package_list = ", ".join(json.dumps(package) for package in packages)
    code = (
        f"pkgs <- c({package_list})\n"
        "missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]\n"
        "if (length(missing) > 0) { cat(paste(missing, collapse = ',')); quit(status = 1) }\n"
    )
    try:
        run_r_code(code, executor_flags=["--vanilla"], text=True)
    except (FileNotFoundError, RuntimeError) as exc:
        pytest.skip(f"R packages are not available: {exc}")


def test_runner_samples_convert_to_hubverse_table(tmp_path) -> None:
    _skip_if_r_packages_missing(
        "argparser", "dplyr", "forecasttools", "fs", "stfroutineforecasting"
    )
    batch_dir = tmp_path / "covid_lookback-all_omit-0"
    model_fit_dir = batch_dir / "model_runs" / "CA" / "mech_nssp_daily"
    model_fit_dir.parent.mkdir(parents=True)
    output_dir = _run_runner(tmp_path, _synthetic_input(), *SMALL, name="hub")
    output_dir.rename(model_fit_dir)

    model_fit_dir_to_hub_tbl(model_fit_dir, report_date=REPORT_DATE)

    hub = pl.read_parquet(model_fit_dir / "hubverse_table.parquet")
    assert hub.schema["output_type_id"] == pl.Int32
    assert hub["model_id"].unique().to_list() == ["mech_nssp_daily"]
    assert hub["target_end_date"].max() == FORECAST_THROUGH
    assert hub["horizon"].min() == 0
