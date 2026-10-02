#!/usr/bin/env python3
"""
Developer check: the production runner reproduces the backtest runner bit for bit.

Runs the same synthetic series through `cfa-mech-experiment`'s unmodified
`run_model.jl` (one `independent` origin) and through `fit_mech_nssp_daily.jl`,
with the same seed, weekday settings, `[filter.pf] threads` flag and a single
Julia thread, and compares the forecast draws exactly. Needs a local checkout of
the private repository with its Julia environment instantiated; it is not part
of the test suite.

IMPORTANT: This script cannot run from `cfa-stf-routine-forecasting` alone. It requires access
to a local clone of the private `cdcent/cfa-mech-experiment` repository with its Julia environment
instantiated. The example below assumes that clone is a sibling of this repository.

    uv run src/cfa/stf/routine/mech_nssp_daily/parity_check.py --mech-experiment ../cfa-mech-experiment --work-dir /tmp/mech-parity
"""

import argparse
import csv
import datetime as dt
import json
import math
import shlex
import subprocess
import sys
from pathlib import Path

import polars as pl

RUNNER_DIR = Path(__file__).resolve().parent
REPORT_DATE = dt.date(2026, 9, 30)  # a Wednesday: the backtest's origin weekday
FORECAST_THROUGH = dt.date(2026, 10, 24)
TRUTH_AS_OF = dt.date(2026, 11, 5)  # a Thursday, so it is never an origin
CDF = [0.6, 0.8, 0.9, 0.99]
WEEKDAY_WEIGHTS = [1.05, 1.02, 1.0, 0.98, 0.95, 0.9, 1.1]
POPULATION = 39_512_223  # CA in the private repo's locations table
N_DAYS = 200
GAP = {90, 91}
SEED = 7
N_PARTICLES = 40
N_DRAWS = 6
DOW_WINDOW_DAYS = 182
DOW_EXCLUDE_DAYS = 14


def _final_count(index: int, weekday: int) -> int:
    level = 40 + 30 * math.exp(-(((index - N_DAYS * 0.55) / 25) ** 2))
    return round(level * WEEKDAY_WEIGHTS[weekday])


def write_inputs(work_dir: Path) -> None:
    last = REPORT_DATE - dt.timedelta(days=1)
    first = last - dt.timedelta(days=N_DAYS - 1)
    dates = [first + dt.timedelta(days=i) for i in range(N_DAYS)]
    observations: list[float | None] = []
    raw: list[float | None] = []
    fractions: list[float | None] = []
    triangle: list[tuple] = []
    for index, date in enumerate(dates):
        if index in GAP:
            observations.append(None)
            raw.append(None)
            fractions.append(None)
            continue
        lag = (REPORT_DATE - date).days
        fraction = CDF[lag - 1] if lag <= len(CDF) else 1.0
        reported = round(_final_count(index, date.weekday()) * fraction)
        nowcast = round((reported + 1 - fraction) / fraction)
        observations.append(float(nowcast))
        raw.append(float(reported))
        fractions.append(fraction)
        triangle.append((date, REPORT_DATE, nowcast, reported, fraction))
    # The backtest only scores an origin whose targets have settled truth.
    for index in range(N_DAYS + (FORECAST_THROUGH - REPORT_DATE).days + 1):
        date = first + dt.timedelta(days=index)
        count = _final_count(index, date.weekday())
        triangle.append((date, TRUTH_AS_OF, count, count, 1.0))

    with (work_dir / "triangle.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "date",
                "as_of",
                "location",
                "pathogen",
                "age_group",
                "strain",
                "target",
                "observation",
                "raw_observation",
                "reporting_fraction",
            ]
        )
        for date, as_of, obs, rep, fraction in triangle:
            writer.writerow(
                [
                    date,
                    as_of,
                    "ca",
                    "Covid",
                    "",
                    "",
                    "ed_visit_count",
                    obs,
                    rep,
                    fraction,
                ]
            )
    (work_dir / "input.json").write_text(
        json.dumps(
            {
                "dates": [date.isoformat() for date in dates],
                "observations": observations,
                "raw_observations": raw,
                "reporting_fractions": fractions,
                "location": "CA",
                "disease": "covid",
                "population": POPULATION,
                "report_date": REPORT_DATE.isoformat(),
                "forecast_through": FORECAST_THROUGH.isoformat(),
            }
        )
    )
    (work_dir / "run.toml").write_text(
        f"""n_ahead = {(FORECAST_THROUGH - REPORT_DATE).days + 1}
n_draws = {N_DRAWS}
burnin_observations = 84
drop_recent_observations = 0
step_days = 1.0
supersample = 2
seed = {SEED}
origin_mode = "independent"
forecast_start = "{REPORT_DATE.isoformat()}"

[io]
data = "{work_dir / "triangle.csv"}"
model_id = "parity"
forecast_df = "forecast.csv"
loc = "ca"

[input.counts]

[epi.basic_seir_daily_nssp_trend.day_of_week.plugin]
fit_policy = "first_vintage"
window_days = {DOW_WINDOW_DAYS}
exclude_recent_days = {DOW_EXCLUDE_DAYS}

[filter.pf]
n_particles = {N_PARTICLES}
threads = true

[hyper.liu_west]
discount = 0.95
replay_on_revision = false
"""
    )


def run(command: list[str], log: Path) -> None:
    with log.open("w") as handle:
        subprocess.run(command, check=True, stdout=handle, stderr=subprocess.STDOUT)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mech-experiment",
        type=Path,
        required=True,
        help="path to a local clone of the private cdcent/cfa-mech-experiment repository",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="directory for temporary parity outputs",
    )
    parser.add_argument(
        "--julia", default="julia", help="Julia command, e.g. 'julia +1.11'"
    )
    args = parser.parse_args()

    upstream = (args.mech_experiment / "models" / "ConfigurableEpi").resolve()
    if not (upstream / "run_model.jl").is_file():
        parser.error(
            "--mech-experiment must point to a local clone of the private "
            "cdcent/cfa-mech-experiment repository"
        )
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    write_inputs(work_dir)
    julia = shlex.split(args.julia)

    print("running upstream run_model.jl ...", flush=True)
    run(
        [
            *julia,
            "-t1",
            f"--project={upstream}",
            str(upstream / "run_model.jl"),
            f"--config={work_dir / 'run.toml'}",
        ],
        work_dir / "upstream.log",
    )
    print("running fit_mech_nssp_daily.jl ...", flush=True)
    run(
        [
            *julia,
            "-t1",
            f"--project={RUNNER_DIR}",
            str(RUNNER_DIR / "fit_mech_nssp_daily.jl"),
            f"--json-input={work_dir / 'input.json'}",
            f"--output-dir={work_dir / 'runner'}",
            f"--n-particles={N_PARTICLES}",
            f"--n-forecast-draws={N_DRAWS}",
            f"--seed={SEED}",
            f"--dow-window-days={DOW_WINDOW_DAYS}",
            f"--dow-min-exclude-days={DOW_EXCLUDE_DAYS}",
        ],
        work_dir / "runner.log",
    )

    upstream_samples = pl.read_parquet(work_dir / "forecast" / "samples.parquet").sort(
        [".draw", "date"]
    )
    runner_samples = pl.read_parquet(work_dir / "runner" / "samples.parquet").sort(
        [".draw", "date"]
    )
    same_layout = (
        upstream_samples["date"].to_list() == runner_samples["date"].to_list()
        and upstream_samples[".draw"].to_list() == runner_samples[".draw"].to_list()
    )
    identical = (
        upstream_samples[".value"].to_list() == runner_samples[".value"].to_list()
    )
    print(f"rows: upstream {upstream_samples.height}, runner {runner_samples.height}")
    print(f"same date/draw layout: {same_layout}")
    print(f"bit-identical .value: {identical}")
    if not (same_layout and identical):
        print("PARITY FAILED", file=sys.stderr)
        return 1
    print("PARITY OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
