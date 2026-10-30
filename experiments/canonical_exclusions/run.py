"""Reproducible missing-data experiments for GitHub issue #1343.

Run with ``uv run --frozen python experiments/canonical_exclusions/run.py``.
Python/R fits run in separate processes; Julia compilation is reused across fits.
Identical effective inputs share results.
The combined excluded-20 case is independently refitted to check reproducibility.
"""

from __future__ import annotations

import argparse
import atexit
import datetime as dt
import hashlib
import html
import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import polars as pl

from cfa.stf.routine._paths import FABLE_DIR, NOWCASTAUTOGP_DIR, PRODUCTION_PRIORS
from cfa.stf.routine.data.data_access import (
    DataFreshness,
    NHSNData,
    NSSPData,
    SurveillanceInputs,
)
from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.nowcastautogp.config import NowcastAutoGPConfig
from cfa.stf.routine.nowcastautogp.prep_nowcastautogp_data import (
    convert_to_nowcastautogp_json,
)
from cfa.stf.routine.nowcastautogp.reporting_delay_nowcast import ReportingDelayNowcast
from cfa.stf.routine.pyrenew_hew.model_inputs import (
    PyRenewModelInputs,
    serialize_pyrenew_model_params,
)
from cfa.stf.routine.utils.data_utils import aggregate_nssp_to_epiweekly

ROOT = Path(__file__).resolve().parents[2]
REPORT_DATE = dt.date(2026, 7, 5)  # Sunday; forecast-through is exactly 28 target days.
N_DAYS = 168
SEED = 1343
CDF = np.array([0.6, 0.8, 0.9, 0.99, 1.0])
PMF = np.diff(np.r_[0, CDF]).tolist()
MODELS = [
    "fable_daily",
    "fable_weekly",
    "pyrenew_h",
    "pyrenew_e",
    "pyrenew_he",
    "gp_daily",
    "gp_other",
    "gp_weekly",
    "gp_hubverse",
]
SCENARIOS = ["beginning", "middle", "tail_short", "tail_long", "isolated", "combined"]
CONDITIONS = ["missing", "contaminated", "excluded20", "excluded100"]
PROFILES = {
    "smoke": dict(chains=1, warmup=20, samples=20, particles=2, mcmc=1, hmc=1),
    "full": dict(chains=4, warmup=500, samples=500, particles=12, mcmc=25, hmc=10),
}


class GPWorker:
    """Amortize Julia compilation; every request still fits a fresh model."""

    def __init__(self, output):
        self.log = (output / "julia-startup.log").open("w")
        self.process = subprocess.Popen(
            [
                "julia",
                f"--project={NOWCASTAUTOGP_DIR}",
                "--threads=1",
                "--startup-file=no",
                str(Path(__file__).with_name("gp_worker.jl")),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            text=True,
        )
        atexit.register(self.close)

    def fit(self, run, model, settings, log):
        arguments = [
            f"--json-input={run.model_dir / (run.model_name + '_input.json')}",
            f"--output-dir={run.model_dir}",
            f"--n-particles={settings['particles']}",
            f"--n-mcmc={settings['mcmc']}",
            f"--n-hmc={settings['hmc']}",
            f"--n-forecast-draws={settings['samples']}",
            f"--transformation={'percentage' if model == 'gp_weekly' else 'boxcox'}",
            "--smc-data-proportion=0.5",
        ]
        self.process.stdin.write(
            json.dumps(dict(arguments=arguments, seed=SEED, log=str(log))) + "\n"
        )
        self.process.stdin.flush()
        response = self.process.stdout.readline()
        if not response:
            raise RuntimeError("Julia worker exited; inspect julia-startup.log")
        result = json.loads(response)
        if not result["ok"]:
            raise RuntimeError(result["error"])

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait()
        self.log.close()


def simulate(report_date=REPORT_DATE, n_days=N_DAYS):
    """Daily disease/other ED counts and weekly admissions with known truth."""
    rng = np.random.default_rng(SEED)
    start = report_date - dt.timedelta(days=n_days)
    rows, hosp = [], []
    for i in range(n_days + 28):
        date = start + dt.timedelta(days=i)
        wave = 1 + 0.55 * np.sin(2 * np.pi * (i - 35) / 140)
        weekday = [1.07, 1.10, 1.08, 1.04, 1.02, 0.85, 0.84][date.weekday()]
        fraction = CDF[min(max((report_date - date).days - 1, 0), len(CDF) - 1)]
        for variable, mean in [
            ("observed_ed_visits", 150 * wave * weekday),
            ("other_ed_visits", 4200 * weekday * (1 + 0.04 * np.sin(i / 30))),
        ]:
            truth = int(rng.negative_binomial(200, 200 / (200 + mean)))
            reported = (
                int(rng.binomial(truth, fraction)) if date < report_date else truth
            )
            rows.append(
                dict(
                    date=date,
                    state_abb="CA",
                    **{
                        ".variable": variable,
                        ".value": float(reported),
                        "truth": float(truth),
                        "resolution": "daily",
                        "data_type": "train" if date < report_date else "eval",
                    },
                )
            )
        if date.weekday() == 5:
            truth = int(rng.negative_binomial(300, 300 / (300 + 450 * wave)))
            hosp.append(
                dict(
                    date=date,
                    state_abb="CA",
                    value=float(truth),
                    truth=float(truth),
                    resolution="epiweekly",
                    data_type="train" if date < report_date else "eval",
                )
            )
    return pl.DataFrame(rows), pl.DataFrame(hosp)


def mask_dates(dates, scenario, *, weekly=False, hospital=False):
    """Define masks by native calendar cadence, independently for H and E."""
    n = len(dates)
    width = 2 if weekly else 14
    middle = n // 2 + (1 if hospital else 0)
    beginning = set(dates[:width])
    central = set(dates[middle : middle + width])
    short = set(dates[-1:] if weekly else dates[-2:])
    long = set(dates[-2:] if weekly else dates[-7:])
    isolated = {dates[i] for i in range(3, n - 2, 5 if weekly else 11)}
    return {
        "clean": set(),
        "beginning": beginning,
        "middle": central,
        "tail_short": short,
        "tail_long": long,
        "isolated": isolated,
        "combined": beginning | central | long | isolated,
    }[scenario]


def apply_condition(frame, value_col, excluded, condition, *, variable=None):
    """Keep contaminated raw values for auditing, then mask before modelling."""
    is_excluded = pl.col("date").is_in(sorted(excluded))
    multiplier = 100 if condition == "excluded100" else 20
    contaminate = is_excluded
    if variable is not None:
        contaminate &= pl.col(".variable") == variable
    raw = frame.with_columns(
        pl.when(
            contaminate
            & pl.lit(condition in {"contaminated", "excluded20", "excluded100"})
        )
        .then(pl.col(value_col) * multiplier)
        .otherwise(pl.col(value_col))
        .alias(value_col),
        is_excluded.alias("excluded"),
    )
    effective = raw.with_columns(
        pl.when(pl.col("excluded") & pl.lit(condition != "contaminated"))
        .then(None)
        .otherwise(pl.col(value_col))
        .alias(value_col)
    )
    return raw, effective.drop("excluded", "truth")


def prepare(output, model, scenario, condition):
    daily, hospital = simulate()
    # Honour the existing three-day omission for models without adjustment.
    omit = 3 if model in {"fable_daily", "fable_weekly", "gp_other", "gp_weekly"} else 0
    window = ForecastWindow(REPORT_DATE, N_DAYS, omit)
    daily = daily.with_columns(
        pl.when(pl.col("date") <= window.max_allowed_training_date)
        .then(pl.lit("train"))
        .otherwise(pl.lit("eval"))
        .alias("data_type")
    )
    weekly = model in {"fable_weekly", "gp_weekly"}
    if weekly:
        truth = aggregate_nssp_to_epiweekly(
            daily.drop(".value").rename({"truth": ".value"})
        ).rename({".value": "truth"})
        daily = aggregate_nssp_to_epiweekly(daily.drop("truth")).join(
            truth.select("date", ".variable", "truth"), on=["date", ".variable"]
        )
    ed_dates = (
        daily.filter(pl.col("data_type") == "train")["date"].unique().sort().to_list()
    )
    h_dates = hospital.filter(pl.col("data_type") == "train")["date"].to_list()
    ed_mask = mask_dates(ed_dates, scenario, weekly=weekly)
    h_mask = mask_dates(h_dates, scenario, weekly=True, hospital=True)
    raw_ed, ed = apply_condition(
        daily,
        ".value",
        ed_mask,
        condition,
        variable="observed_ed_visits" if model == "gp_weekly" else None,
    )
    raw_h, h = apply_condition(hospital, "value", h_mask, condition)
    need_ed = model not in {"pyrenew_h", "gp_hubverse"}
    need_h = model in {"pyrenew_h", "pyrenew_he", "gp_hubverse"}

    def freshness(source):
        return DataFreshness(source, REPORT_DATE, REPORT_DATE, False, "Simulated")

    surveillance = SurveillanceInputs(
        loc_pop=39_000_000,
        nssp=NSSPData(ed, freshness("nssp"), "epiweekly" if weekly else "daily")
        if need_ed
        else None,
        nhsn=NHSNData(h, freshness("nhsn"), prelim=False) if need_h else None,
    )
    names = {
        "fable_daily": "daily_fable_e_other",
        "fable_weekly": "epiweekly_fable_e_other",
        "gp_daily": "nowcastautogp_nssp_daily",
        "gp_other": "nowcastautogp_nssp_daily_other",
        "gp_weekly": "nowcastautogp_nssp_epiweekly_pct",
        "gp_hubverse": "nowcastautogp_nhsn_epiweekly",
    }
    run = ForecastRun(
        "covid", "CA", window, window, names.get(model, model), output, surveillance
    )
    serialize_data(run)
    raw_ed.write_parquet(run.data_dir / "audit_nssp.parquet")
    raw_h.write_parquet(run.data_dir / "audit_nhsn.parquet")
    if model.startswith("pyrenew"):
        gi = np.array([64, 23, 9, 3, 1], dtype=float)
        gi /= gi.sum()
        delay = np.array([0, 2, 17, 24, 20, 14, 9, 6, 4, 2, 1, 1], dtype=float)
        delay /= delay.sum()
        serialize_pyrenew_model_params(
            run=run,
            model_inputs=PyRenewModelInputs(tuple(gi), tuple(delay), tuple(PMF)),
            save_dir=run.data_dir,
        )
        (run.model_dir / "priors.py").write_bytes(PRODUCTION_PRIORS.read_bytes())
    elif model.startswith("gp"):
        config = NowcastAutoGPConfig(
            target="nhsn" if need_h else "nssp",
            frequency="epiweekly" if weekly or need_h else "daily",
            ed_visit_type="pct"
            if weekly
            else "other"
            if model == "gp_other"
            else "observed",
        )
        nowcast = None
        if model == "gp_daily":
            nowcast = ReportingDelayNowcast(PMF, REPORT_DATE - dt.timedelta(days=1))
        elif model == "gp_hubverse":
            # External scenarios depend only on uncorrupted truth. Excluded dates
            # remain in the artifact to test the production exclusion filter.
            directory = output / "nowcasts" / "model-output" / "CFA-nowcastNHSN"
            directory.mkdir(parents=True, exist_ok=True)
            recent = hospital.filter(pl.col("data_type") == "train").tail(4)
            rows = [
                dict(
                    origin_date=REPORT_DATE,
                    target_end_date=row["date"],
                    target="wk inc covid hosp",
                    location="ca",
                    output_type="sample",
                    output_type_id=str(draw),
                    value=row["truth"] * factor,
                )
                for draw, factor in enumerate([0.95, 1.0, 1.05, 1.1])
                for row in recent.iter_rows(named=True)
            ]
            pl.DataFrame(rows).write_parquet(
                directory / f"{REPORT_DATE}-CFA-nowcastNHSN.parquet"
            )
            nowcast = HubverseNowcast(output / "nowcasts", run, config)
        convert_to_nowcastautogp_json(
            forecast_run=run, config=config, nowcast_source=nowcast
        )
    return run


def fingerprint(run, model, profile):
    paths = [run.data_dir / "data_for_model_fit.json"]
    if model.startswith("gp"):
        paths = [run.model_dir / f"{run.model_name}_input.json"]
    elif model.startswith("pyrenew"):
        paths += [run.data_dir / "model_params.json", run.model_dir / "priors.py"]
    else:
        paths = [run.data_dir / "combined_data.tsv"]
    digest = hashlib.sha256((model + profile + str(SEED)).encode())
    for path in paths:
        digest.update(path.read_bytes())
    return digest.hexdigest()


def implementation_hash():
    digest = hashlib.sha256(Path(__file__).read_bytes())
    digest.update(Path(__file__).with_name("gp_worker.jl").read_bytes())
    for path in sorted((ROOT / "src/cfa/stf/routine").rglob("*")):
        if path.suffix in {".py", ".jl", ".R"}:
            digest.update(path.read_bytes())
    digest.update((ROOT / "uv.lock").read_bytes())
    return digest.hexdigest()


def worker(args):
    run = prepare(args.output, args.worker, args.scenario, args.condition)
    settings = PROFILES[args.profile]
    if args.worker.startswith("pyrenew"):
        from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import PyRenewPipeline

        letters = args.worker.split("_")[1]
        pipeline = PyRenewPipeline(
            disease="covid",
            loc="CA",
            output_dir=args.output,
            run_date=REPORT_DATE,
            priors_path=PRODUCTION_PRIORS,
            n_chains=settings["chains"],
            n_warmup=settings["warmup"],
            n_samples=settings["samples"],
            fit_ed_visits="e" in letters,
            fit_hospital_admissions="h" in letters,
            forecast_ed_visits="e" in letters,
            forecast_hospital_admissions="h" in letters,
            rng_key=SEED,
        )
        if args.predict_only:
            pipeline.generate_predictions(run)
        else:
            pipeline.run_model(run)
        import pickle

        from numpyro.diagnostics import summary

        with (run.model_dir / "posterior_samples.pickle").open("rb") as f:
            mcmc = pickle.load(f)
        stats = summary(mcmc.get_samples(group_by_chain=True))
        diagnostics = dict(
            divergences=int(np.asarray(mcmc.get_extra_fields()["diverging"]).sum()),
            max_rhat=float(max(np.nanmax(s["r_hat"]) for s in stats.values())),
            min_ess=float(min(np.nanmin(s["n_eff"]) for s in stats.values())),
        )
        (run.model_dir / "diagnostics.json").write_text(
            json.dumps(diagnostics, indent=2)
        )
    elif args.worker.startswith("fable"):
        code = f"set.seed({SEED}); source({json.dumps(str(FABLE_DIR / 'fit_fable.R'))}); run_cli()"
        subprocess.run(
            [
                "Rscript",
                "-e",
                code,
                "--model-dir",
                str(run.model_dir),
                "--n-forecast-days",
                str(run.n_forecast_days),
                "--n-samples",
                str(settings["samples"]),
            ],
            check=True,
        )
    else:
        code = f"using Random; Random.seed!({SEED}); include({json.dumps(str(NOWCASTAUTOGP_DIR / 'fit_nowcastautogp.jl'))}); main()"
        subprocess.run(
            [
                "julia",
                f"--project={NOWCASTAUTOGP_DIR}",
                "--threads=1",
                "--startup-file=no",
                "-e",
                code,
                "--",
                f"--json-input={run.model_dir / (run.model_name + '_input.json')}",
                f"--output-dir={run.model_dir}",
                f"--n-particles={settings['particles']}",
                f"--n-mcmc={settings['mcmc']}",
                f"--n-hmc={settings['hmc']}",
                f"--n-forecast-draws={settings['samples']}",
                f"--transformation={'percentage' if args.worker == 'gp_weekly' else 'boxcox'}",
                "--smc-data-proportion=0.5",
            ],
            check=True,
        )


def summarise(path):
    samples = pl.read_parquet(path)
    if (
        samples.is_empty()
        or samples.select(
            (pl.col(".value").is_null() | ~pl.col(".value").is_finite()).any()
        ).item()
    ):
        raise ValueError("Forecast samples are empty or nonfinite")
    summary = (
        samples.group_by("date", ".variable")
        .agg(
            pl.col(".value").median().alias("median"),
            pl.col(".value").quantile(0.05).alias("lower"),
            pl.col(".value").quantile(0.95).alias("upper"),
        )
        .sort(".variable", "date")
    )
    fitted_path = path.parent / "fitted.tsv"
    if fitted_path.exists():
        fitted = pl.read_csv(
            fitted_path, separator="\t", try_parse_dates=True, null_values="NA"
        )
        fitted = fitted.select(
            "date",
            pl.lit("other_ed_visits").alias(".variable"),
            pl.col(".fitted").alias("median"),
            pl.lit(None, dtype=pl.Float64).alias("lower"),
            pl.lit(None, dtype=pl.Float64).alias("upper"),
        )
        summary = pl.concat([fitted, summary]).sort(".variable", "date")
    return summary


def write_report(output, records):
    # Also enrich records produced by earlier stages or separate model batches.
    for record in records:
        if record.get("samples"):
            directory = Path(record["samples"]).parent
            diagnostics = directory / "diagnostics.json"
            components = directory / "components.txt"
            if diagnostics.exists():
                record["diagnostics"] = json.loads(diagnostics.read_text())
            elif components.exists():
                record["diagnostics"] = components.read_text().strip()
    (output / "results.json").write_text(json.dumps(records, indent=2))
    cancelled = sum(r["status"] == "cancelled" for r in records)
    cancellation_note = (
        f"<p>{cancelled} planned cases were cancelled at the user's request after PyRenew HE finished. "
        "They were not evaluated and are not counted as passing checks.</p>"
        if cancelled
        else ""
    )
    overview = []
    for model in sorted({r["model"] for r in records}):
        cases = [r for r in records if r["model"] == model]
        passed = sum(r["status"] == "passed" for r in cases)
        exclusions = [r for r in cases if r["condition"].startswith("excluded")]
        invariant = sum(r["status"] == "passed" for r in exclusions)
        replay = next(
            (
                r.get("max_difference", r["status"])
                for r in exclusions
                if r["scenario"] == "combined" and r["condition"] == "excluded20"
            ),
            "pending",
        )
        warning = ""
        if model.startswith("pyrenew"):
            fits = [
                r.get("diagnostics", {})
                for r in cases
                if r["condition"] in {"clean", "missing"}
            ]
            fits = [d for d in fits if isinstance(d, dict) and "max_rhat" in d]
            if fits:
                warning = f"Clean/missing fits: max R-hat {max(d['max_rhat'] for d in fits):.3f}; min ESS {min(d['min_ess'] for d in fits):.1f}; max divergent transitions {max(d['divergences'] for d in fits)}. Inspect diagnostics before interpreting intervals."
        elif model.startswith("gp"):
            warning = "Particle ESS is retained; it does not establish convergence."
            fits = [
                r.get("diagnostics", {})
                for r in cases
                if r["condition"] in {"clean", "missing"}
            ]
            fits = [d for d in fits if "effective_sample_size" in d]
            if fits:
                warning = f"Clean/missing fits: min particle ESS {min(d['effective_sample_size'] for d in fits):.2f} of {fits[0]['particles']}. Particle ESS does not establish convergence."
        elif model.startswith("fable"):
            warning = "ETS + ARIMA after ARIMA Kalman point imputation; imputation uncertainty is not propagated."
        overview.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(value))}</td>"
                for value in [
                    model,
                    f"{passed}/{len(cases)}",
                    f"{invariant}/{len(exclusions)}",
                    replay,
                    warning,
                ]
            )
            + "</tr>"
        )
    rows = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(str(r.get(k, '')))}</td>"
            for k in [
                "model",
                "scenario",
                "condition",
                "status",
                "seconds",
                "reused_from",
                "max_difference",
                "diagnostics",
                "forecast_change_from_clean",
                "error",
            ]
        )
        + "</tr>"
        for r in records
    )
    plots = "".join(
        f'<h2>{p.stem}</h2><img width="1100" src="{p.name}">'
        for p in sorted(output.glob("*.png"))
    )
    (output / "report.html").write_text(
        """<!doctype html><meta charset="utf-8"><title>Canonical exclusions POC</title>
    <style>body{font:16px sans-serif;margin:2em;max-width:1200px}td,th{padding:.4em;border:1px solid #ddd;max-width:24em;overflow-wrap:anywhere;vertical-align:top}table{border-collapse:collapse;font-size:.85rem}img{max-width:100%}summary{cursor:pointer;margin:1em 0}</style>
    <h1>Missing-data proof of concept (#1343)</h1>
    <p>Fixed synthetic CA/covid series; report date 2026-07-05. Missing versus excluded contamination must agree;
    complete clean data need not agree with missing data. Identical effective inputs reuse fitted results,
    except combined/excluded20, which is independently rerun. Reuse is input-invariance evidence, not an independent fit.</p>
    <p>Unmasked contamination is a sensitivity control and may cause inference failure. The smoke profile is only
    a wiring check. Full-profile inference diagnostics are stored beside model samples; poor diagnostics limit
    scientific interpretation even when exclusion invariance holds. Figures show predictive medians and 90% intervals.</p>
    <p>Fable retains ETS + ARIMA after ARIMA Kalman point imputation. Forecasts condition on the imputed values; imputation uncertainty is not propagated.
    Fable fitted curves are conditional point estimates. GP particle ESS measures weight concentration, not convergence.
    Forecast change is mean absolute change in forecast medians relative to complete clean data, by variable.</p>
    """
        + cancellation_note
        + "<table><tr><th>Model</th><th>Cases passed / planned</th><th>Exclusion comparisons passed / planned</th><th>Independent replay: max draw difference</th><th>Interpretation</th></tr>"
        + "".join(overview)
        + """</table><details><summary>Detailed case results, diagnostics, and artifact paths</summary>
    <table><tr><th>Model</th><th>Scenario</th><th>Condition</th><th>Status</th><th>Seconds</th><th>Reused from</th><th>Max draw difference</th><th>Diagnostics / components</th><th>Forecast change from clean</th><th>Error</th></tr>"""
        + rows
        + "</table></details>"
        + plots
    )


def plot_results(output, records):
    from plotnine import (
        aes,
        facet_wrap,
        geom_line,
        geom_point,
        geom_ribbon,
        geom_vline,
        ggplot,
        labs,
        theme_bw,
    )

    for model in sorted({r["model"] for r in records}):
        panels = []
        raw_panels = []
        for r in records:
            if (
                r["model"] != model
                or r["status"] != "passed"
                or r["condition"] == "excluded100"
            ):
                continue
            panels.append(
                summarise(Path(r["samples"])).with_columns(
                    pl.lit(r["scenario"]).alias("scenario"),
                    pl.lit(r["condition"]).alias("condition"),
                )
            )
            if r["condition"] == "excluded20":
                data_dir = Path(r["model_dir"]) / "data"
                name = (
                    "audit_nhsn.parquet"
                    if model in {"pyrenew_h", "gp_hubverse"}
                    else "audit_nssp.parquet"
                )
                raw = pl.read_parquet(data_dir / name)
                if "value" in raw.columns:
                    raw = raw.rename({"value": ".value"}).with_columns(
                        pl.lit("observed_hospital_admissions").alias(".variable")
                    )
                if model == "pyrenew_he":
                    admissions = (
                        pl.read_parquet(data_dir / "audit_nhsn.parquet")
                        .rename({"value": ".value"})
                        .with_columns(
                            pl.lit("observed_hospital_admissions").alias(".variable")
                        )
                    )
                    raw = pl.concat([raw, admissions], how="diagonal_relaxed")
                raw_panels.append(
                    raw.with_columns(pl.lit(r["scenario"]).alias("scenario"))
                )
        if not panels:
            continue
        df = pl.concat(panels)
        # Add the same complete-data reference to each masked scenario panel.
        baseline = df.filter(pl.col("scenario") == "clean")
        scenarios = [s for s in df["scenario"].unique().to_list() if s != "clean"]
        if scenarios:
            df = pl.concat(
                [
                    df.filter(pl.col("scenario") != "clean"),
                    *[
                        baseline.with_columns(pl.lit(s).alias("scenario"))
                        for s in scenarios
                    ],
                ]
            )
        # A syntactic name is needed when layers omit the faceting column.
        df = df.rename({".variable": "variable"})
        p = (
            ggplot(df, aes("date", "median", color="condition", fill="condition"))
            + geom_ribbon(aes(ymin="lower", ymax="upper"), alpha=0.08, color=None)
            + geom_line(size=0.5)
            + geom_vline(xintercept=REPORT_DATE, linetype="dashed", alpha=0.6)
            + facet_wrap(["scenario", "variable"], scales="free_y", ncol=2)
            + theme_bw()
            + labs(
                title=model,
                y="Count / proportion",
                x="Date",
                caption="Dashed line: report date. Black: simulated final observations.",
            )
        )
        if raw_panels:
            raw = pl.concat(raw_panels)
            if model == "gp_weekly":
                raw = raw.pivot(
                    on=".variable", index=["date", "scenario"], values="truth"
                ).with_columns(
                    (
                        pl.col("observed_ed_visits")
                        / (pl.col("observed_ed_visits") + pl.col("other_ed_visits"))
                    ).alias("truth"),
                    pl.lit("prop_disease_ed_visits").alias(".variable"),
                )
            raw = raw.filter(
                pl.col(".variable").is_in(df["variable"].unique().to_list())
            ).rename({".variable": "variable"})
            p += geom_line(
                aes("date", "truth"),
                data=raw,
                inherit_aes=False,
                color="black",
                alpha=0.5,
            )
        p.save(
            output / f"{model}.png",
            width=14,
            height=max(5, 2.5 * len(scenarios)),
            dpi=100,
            verbose=False,
        )
        # Keep the unmasked outliers from compressing the exclusion comparisons.
        invariance = p + labs(title=f"{model}: exclusion invariance")
        invariance.data = df.filter(pl.col("condition") != "contaminated")
        invariance.save(
            output / f"{model}_invariance.png",
            width=14,
            height=max(5, 2.5 * len(scenarios)),
            dpi=100,
            verbose=False,
        )
        if model.startswith("pyrenew"):
            # Uncertain initial states can otherwise hide the forecast window.
            future = (
                ggplot(
                    df.filter(
                        (pl.col("condition") != "contaminated")
                        & (pl.col("date") >= REPORT_DATE)
                    ),
                    aes("date", "median", color="condition", fill="condition"),
                )
                + geom_ribbon(aes(ymin="lower", ymax="upper"), alpha=0.08, color=None)
                + geom_line(size=0.5)
                + facet_wrap(["scenario", "variable"], scales="free_y", ncol=2)
                + theme_bw()
                + labs(title=f"{model}: forecast window", y="Count", x="Date")
            )
            if raw_panels:
                future += geom_line(
                    aes("date", "truth"),
                    data=raw.filter(pl.col("date") >= REPORT_DATE),
                    inherit_aes=False,
                    color="black",
                    alpha=0.5,
                )
            future.save(
                output / f"{model}_forecast.png",
                width=14,
                height=max(5, 2.5 * len(scenarios)),
                dpi=100,
                verbose=False,
            )
        if raw_panels:
            raw = pl.concat(raw_panels).rename({".variable": "variable"})
            q = (
                ggplot(raw, aes("date", ".value", color="excluded"))
                + geom_point(size=0.6)
                + geom_line(aes(y="truth"), color="black", alpha=0.5)
                + facet_wrap(["scenario", "variable"], scales="free_y", ncol=2)
                + theme_bw()
                + labs(
                    title=f"{model}: raw outliers (black = uncorrupted truth)",
                    y="Count",
                )
            )
            q.save(
                output / f"{model}_outliers.png",
                width=14,
                height=max(5, 2.5 * len(scenarios)),
                dpi=100,
                verbose=False,
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "test_output/canonical-exclusions"
    )
    parser.add_argument("--profile", choices=PROFILES, default="full")
    parser.add_argument("--models", nargs="+", choices=MODELS, default=MODELS)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=SCENARIOS)
    parser.add_argument("--worker", choices=MODELS)
    parser.add_argument(
        "--predict-only",
        action="store_true",
        help="For a PyRenew worker, reuse its saved posterior and regenerate predictions",
    )
    parser.add_argument("--scenario", default="combined")
    parser.add_argument("--condition", default="missing")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Rebuild plots and HTML from retained results.json without fitting",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse completed fits with matching inputs and implementation",
    )
    args = parser.parse_args()
    if args.predict_only and (
        args.worker is None or not args.worker.startswith("pyrenew")
    ):
        parser.error("--predict-only requires a PyRenew --worker")
    args.output = args.output.resolve()
    if args.report_only:
        records = json.loads((args.output / "results.json").read_text())
        plot_results(args.output, records)
        write_report(args.output, records)
        return
    if args.worker:
        worker(args)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    code_hash = implementation_hash()
    (args.output / "manifest.json").write_text(
        json.dumps(
            dict(
                profile=args.profile,
                settings=PROFILES[args.profile],
                seed=SEED,
                report_date=str(REPORT_DATE),
                history_days=N_DAYS,
                reporting_cdf=CDF.tolist(),
                models=args.models,
                scenarios=args.scenarios,
                implementation_sha256=code_hash,
                git_commit=subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
                ).strip(),
            ),
            indent=2,
        )
    )
    records, cache = [], {}
    gp_worker = None
    for model in args.models:
        cases = [("clean", "clean")] + [
            (s, c) for s in args.scenarios for c in CONDITIONS
        ]
        for scenario, condition in cases:
            case_dir = args.output / model / scenario / condition
            record = dict(
                model=model, scenario=scenario, condition=condition, status="failed"
            )
            start = time.monotonic()
            print(f"{model} {scenario} {condition}", flush=True)
            try:
                run = prepare(case_dir, model, scenario, condition)
                key = fingerprint(run, model, args.profile)
                record.update(model_dir=str(run.model_dir), input_sha256=key)
                if condition.startswith("excluded"):
                    reference = next(
                        r
                        for r in records
                        if r["model"] == model
                        and r["scenario"] == scenario
                        and r["condition"] == "missing"
                    )
                    if reference.get("input_sha256") != key:
                        raise AssertionError(
                            "Excluded values changed effective model inputs"
                        )
                output_path = run.model_dir / "samples.parquet"
                completion = run.model_dir / "completed.json"
                completed_key = dict(inputs=key, implementation=code_hash)
                replay = scenario == "combined" and condition == "excluded20"
                if key in cache and not replay:
                    record.update(reused_from=cache[key], samples=cache[key])
                elif not args.prepare_only:
                    cmd = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--worker",
                        model,
                        "--scenario",
                        scenario,
                        "--condition",
                        condition,
                        "--profile",
                        args.profile,
                        "--output",
                        str(case_dir),
                    ]
                    if not (
                        args.resume
                        and output_path.exists()
                        and completion.exists()
                        and json.loads(completion.read_text()) == completed_key
                    ):
                        if model.startswith("gp"):
                            if gp_worker is None:
                                gp_worker = GPWorker(args.output)
                            gp_worker.fit(
                                run, model, PROFILES[args.profile], case_dir / "run.log"
                            )
                        else:
                            with (case_dir / "run.log").open("w") as log:
                                subprocess.run(
                                    cmd,
                                    cwd=ROOT,
                                    stdout=log,
                                    stderr=subprocess.STDOUT,
                                    check=True,
                                )
                        summarise(output_path)
                        completion.write_text(json.dumps(completed_key))
                    record["samples"] = str(output_path)
                if not args.prepare_only:
                    summary = summarise(Path(record["samples"]))
                    samples_dir = Path(record["samples"]).parent
                    if (samples_dir / "diagnostics.json").exists():
                        record["diagnostics"] = json.loads(
                            (samples_dir / "diagnostics.json").read_text()
                        )
                    elif (samples_dir / "components.txt").exists():
                        record["diagnostics"] = (
                            (samples_dir / "components.txt").read_text().strip()
                        )
                    if scenario != "clean":
                        baseline = next(
                            r
                            for r in records
                            if r["model"] == model and r["scenario"] == "clean"
                        )
                        if baseline["status"] == "passed":
                            change = summary.join(
                                summarise(Path(baseline["samples"])),
                                on=["date", ".variable"],
                                suffix="_clean",
                            ).filter(pl.col("date") >= REPORT_DATE)
                            record["forecast_change_from_clean"] = dict(
                                change.group_by(".variable")
                                .agg(
                                    (pl.col("median") - pl.col("median_clean"))
                                    .abs()
                                    .mean()
                                    .alias("difference")
                                )
                                .iter_rows()
                            )
                    if condition.startswith("excluded"):
                        reference = next(
                            r
                            for r in records
                            if r["model"] == model
                            and r["scenario"] == scenario
                            and r["condition"] == "missing"
                        )
                        if reference["input_sha256"] != key:
                            raise AssertionError(
                                "Excluded values changed effective model inputs"
                            )
                        a = pl.read_parquet(reference["samples"]).sort(
                            ".variable", "date", ".draw"
                        )
                        b = pl.read_parquet(record["samples"]).sort(
                            ".variable", "date", ".draw"
                        )
                        if not a.select(".variable", "date", ".draw").equals(
                            b.select(".variable", "date", ".draw")
                        ):
                            raise AssertionError("Prediction coordinates differ")
                        difference = float(
                            np.max(
                                np.abs(a[".value"].to_numpy() - b[".value"].to_numpy())
                            )
                        )
                        record["max_difference"] = difference
                        np.testing.assert_allclose(
                            a[".value"].to_numpy(),
                            b[".value"].to_numpy(),
                            rtol=1e-7,
                            atol=1e-8,
                        )
                    cache[key] = record["samples"]
                record["status"] = "prepared" if args.prepare_only else "passed"
            except Exception as error:
                record["error"] = str(error)
                traceback.print_exc()
            record["seconds"] = round(time.monotonic() - start, 1)
            records.append(record)
            write_report(args.output, records)
    if gp_worker is not None:
        gp_worker.close()
    if not args.prepare_only:
        plot_results(args.output, records)
    write_report(args.output, records)
    if any(r["status"] == "failed" for r in records):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
