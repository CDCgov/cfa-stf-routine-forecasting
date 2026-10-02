# mech_nssp_daily

Production runner for the mechanistic daily NSSP trend model (`basic_seir_daily_nssp_trend`): an SEIRS compartmental model with indoor-activity seasonal forcing, a trending ascertainment latent carried as a second order random-walk, estimated weekday effects, and negative-binomial daily ED-visit observations, fitted by a particle filter with Liu-West parameter learning.
The model was selected by the backtest in `cfa-mech-experiment/reports/nssp-daily-pf-trend-2026-09-30`.

The integration mirrors EpiAutoGP: [`forecast_mech_nssp_daily.py`](forecast_mech_nssp_daily.py) is the `ForecastPipeline` subclass, [`prep_mech_nssp_daily_data.py`](prep_mech_nssp_daily_data.py) writes the JSON input, and [`fit_mech_nssp_daily.jl`](fit_mech_nssp_daily.jl) runs in its own Julia project (`Project.toml` / `Manifest.toml` here) and writes `samples.parquet`.

## What one run does

1. **Data.** The base pipeline loads the latest `comprehensive` NSSP vintage for the location (`sources = {"nssp"}`, daily).
   Use `n_lookback_days = None`: the model's start date is the first available day, and its initial state anchors on the whole history.
2. **Nowcast.** Python corrects every training day by its true reporting lag with the latest right-truncation PMF (`get_nnh_right_truncation_pmf`, `cfa.stf.routine.data.reporting_delay`).
   The rounded nowcasts are what the model fits; raw reports and reporting fractions travel with them.
   Reporting gaps become `null` slots the filter predicts through.
3. **Weekday effects.** Julia estimates the plug-in weekday multipliers from the latest vintage's settled days: the trailing days the PMF still marks as incomplete are excluded (floor `--dow-min-exclude-days`, default 14), over a window of `--dow-window-days` (default 365, a full year so that at least one complete rise-peak-fall cycle is always inside it, which is what debiases the estimator).
   A fallback to no effect is an error.
4. **Fit and forecast.** One stateless origin: the whole history is replayed through the particle filter with a report-date-derived seed, then draws are generated from the last training day through `forecast_through`.
5. **Outputs** in `run.model_dir`: `samples.parquet` (routine contract), plus diagnostics `mech_nssp_daily_fitted.csv`, `mech_nssp_daily_hyperparameters.csv`, `mech_nssp_daily_quantiles.csv` and `mech_nssp_daily_run_metadata.json` (seed, weekday estimates, effective inference settings, package provenance, fit time).

## Provenance

- **Packages.** `AlgebraicEpiMech` and `ConfigurableEpi` come from the public [CDCgov/AlgebraicEpiModels](https://github.com/CDCgov/AlgebraicEpiModels) monorepo, pinned in `Project.toml` `[sources]`.
  Development in `cfa-mech-experiment` is synced to that repo before a release; the runner pins a release tag.
- **Seasonality climatology.** `data/indoor_activity_climatology.csv2` holds the 52 spline knots per location that define the indoor-activity forcing (the repeating curve, not the mobility data it was derived from); see [`data/indoor_activity_climatology.README.md`](data/indoor_activity_climatology.README.md).
  The packages ship no data by design but the data used can be found from the Susswein et al paper described in the climatology README.
- **Inference configuration.** `fit_mech_nssp_daily.jl` directly constructs the selected PF + Liu–West inference engine with the backtested settings (`step_days = 1`, `supersample = 2`, Liu–West discount `0.95`, and the model's default priors and learned set).
  Particle count, draw count, seed and weekday window remain runtime controls; generic backtesting axes such as input scale, origin mode, filter selection and hyperparameter-method selection are not part of the production runner.

## Julia environment

```bash
julia --project=src/cfa/stf/routine/mech_nssp_daily -e 'using Pkg; Pkg.instantiate(); Pkg.precompile()'
```

`Manifest.toml` is committed and was seeded from the backtest's manifest so the numerics-relevant package versions (LowLevelParticleFilters, Distributions, StaticArrays, SeeToDee, DataInterpolations, DuckDB) are the backtested ones.
Change the environment only with `Pkg.add(...; preserve = Pkg.PRESERVE_ALL)`; a bare `Pkg.resolve()` can silently move dozens of packages.

Seeded particle-filter runs reproduce only at a fixed thread count, so the pipeline passes an explicit `--threads` (default 4) rather than `auto`.

## Running

For a local end-to-end run with generated mock data:

```bash
just test-mech-nssp-daily mock CA covid
```

### Live-data pipeline

> [!IMPORTANT]
> This is not a self-contained local example. It loads the private `comprehensive` NSSP dataset
> through `cfa-dataops`, so Azure/EXT data access must already be configured. The repository's
> Julia environments and current local `stfroutineforecasting` R package must also be installed.

```bash
uv run python -c "
import datetime as dt
from cfa.stf.routine.mech_nssp_daily.forecast_mech_nssp_daily import main
main(disease='covid', loc='CA', output_dir='test-output', n_lookback_days=None, run_date=dt.date.today())
"
```

### Direct Julia runner

The Python pipeline prepares `<model_dir>/mech_nssp_daily_input.json` before making this call:

```bash
julia --project=src/cfa/stf/routine/mech_nssp_daily --threads=4 \
  src/cfa/stf/routine/mech_nssp_daily/fit_mech_nssp_daily.jl \
  --json-input=<model_dir>/mech_nssp_daily_input.json --output-dir=<model_dir> \
  --n-particles=3000 --n-forecast-draws=2000 --seed=2026
```

## Numerical parity with the backtest (developer check)

> [!IMPORTANT]
> This check cannot run from `cfa-stf-routine-forecasting` alone. You must have access to the
> private `cdcent/cfa-mech-experiment` repository, clone it locally, and instantiate its Julia
> environment. The command below assumes that private checkout is a sibling directory named
> `cfa-mech-experiment`.

```bash
uv run src/cfa/stf/routine/mech_nssp_daily/parity_check.py --mech-experiment ../cfa-mech-experiment --work-dir /tmp/mech-parity
```

[`parity_check.py`](parity_check.py) generates one synthetic series, runs both sides and compares the draws; it needs the private repository checked out with its Julia environment instantiated, so it is not part of the test suite.

The upstream `run_model.jl`, run with `origin_mode = "independent"` on a single report vintage, must give machine-identical `.value` draws to this runner when both see the same series, population, climatology, weekday settings (`window 182 / exclude 14` on both sides, `exclude_last_n_days = 0`), seed, `[filter.pf] threads` flag and Julia thread count.
Use `cfa-mech-experiment/models/ConfigurableEpi/test/test_run_model.jl` (`_write_daily_nssp_smoke`, `_daily_smoke_config` with `submodel = "basic_seir_daily_nssp_trend"`) to produce the upstream side.
This proves that the production extraction, stateless single origin and injected weekday table changed nothing numerically.
