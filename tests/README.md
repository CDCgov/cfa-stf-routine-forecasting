# Pipeline Tests

Tests are organized to match the `cfa.stf.routine` package layout:

- `data/`: tests for `cfa.stf.routine.data`
- `nowcastautogp/`: tests for `cfa.stf.routine.nowcastautogp`
- `fable/`: tests for `cfa.stf.routine.fable`
- `pyrenew_hew/`: tests for `cfa.stf.routine.pyrenew_hew`
- `utils/`: tests for `cfa.stf.routine.utils`
- `integration/`: independent model tests and the cross-pipeline end-to-end test

The pipeline tests use real DataOps data when `cfa.cloudops.util.check_ext_env()` detects the external CFA environment and fall back to deterministic mocked data otherwise.
The data source can also be selected explicitly with `--e2e-data-mode real|mock`.

Each model has a standalone `just` recipe that runs one disease/location pair.
The PyRenew recipe runs the H, E, and HE versions once each; the NowcastAutoGP recipe runs weekly NHSN counts, weekly NSSP percentages, daily NSSP disease counts, and daily NSSP other counts.

```bash
just test-fable mock CA covid
just test-pyrenew mock CA covid
just test-nowcastautogp mock CA covid
```

Replace `mock` with `real` to use CFA DataOps data.
All arguments are optional; the defaults are `auto`, `CA`, and `covid`.

The combined test remains available through `just e2e auto`, `just e2e real`, or `just e2e mock`.

## Canonical-exclusion proof of concept (#1343)

`just exclusion-poc` runs the synthetic-data experiment for Fable daily/weekly, PyRenew H/E/HE, and four NowcastAutoGP configurations.
The full profile uses four PyRenew chains with 500 warmup and 500 sampling iterations, and 12 GP particles with 25 structure and 10 HMC steps.
`just exclusion-poc smoke` checks execution with small inference settings; it does not establish inference quality.

The experiment fixes CA/covid, the report date (2026-07-05), 168 days of history, and the random seed.
It tests beginning, middle, short-tail, long-tail, isolated, and combined exclusions.
Each mask compares missing clean data with excluded 20× and 100× outliers.
Complete clean data and unmasked 20× outliers are controls.
For proportions, only the disease numerator is contaminated, so contamination does not cancel between numerator and denominator.

Equivalent effective inputs share fitted results.
The combined 20× excluded case is independently refitted to check seeded draw-level reproducibility.
Reports explicitly distinguish reused fits.
Exclusion invariance does not imply that removing observations leaves a complete-data fit unchanged, or that a fit has converged.
PyRenew divergences, R-hat and ESS, and GP particle ESS are retained.

Fable uses the production implementation from #1368: fill missing calendar rows, then impute missing values with an ARIMA Kalman smoother before fitting ETS + ARIMA.
It records the ensemble in `components.txt`.
Forecasts condition on point imputations and do not propagate imputation uncertainty.
Exclusion invariance is therefore distinct from forecast calibration.
Explicit boundary rows are optional: the requirement is coverage of the requested forecast period.
When trailing rows are dropped, compute the horizon from the retained training endpoint to the fixed forecast-through date, as `ForecastRun.n_forecast_days` does.
Dropping boundary rows need not yield the same fitted model as imputing them; exclusion invariance compares inputs with the same retained dates and exclusion policy.

Outputs include `report.html`, `results.json`, `manifest.json`, plots, raw-data audits, effective inputs, logs, samples, and inference diagnostics beneath `test_output/canonical-exclusions/{full,smoke}`.
Existing omission policies are held fixed; canonical data-service integration is a later step.

For selected configurations, preparation checks, or resumption:

```bash
uv run --frozen python experiments/canonical_exclusions/run.py \
  --models pyrenew_e gp_daily --scenarios tail_short tail_long combined \
  --output test_output/canonical-exclusions/selected --resume
uv run --frozen python experiments/canonical_exclusions/run.py --prepare-only
```

Resumption requires matching effective inputs and implementation hashes.
The fast suite includes input-invariance checks across the full configuration matrix, a calendar-based oracle for PyRenew's reporting CDF, and R tests of missing-data ensemble forecasts and calendar boundaries.
