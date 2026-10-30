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

## Canonical exclusions

`data/test_canonical_exclusions.py` exercises the production NSSP loader, derived counts and proportions, complete-week aggregation, and both shared and NowcastAutoGP serialization.
Excluded values are multiplied by 20 and 1,000,000; the model-input files must remain byte-for-byte identical while the separate audit file changes.
The tests also check PyRenew's reporting CDF against each observation's calendar age.

Run the production model adapters and postprocessing with simultaneous leading, internal, and trailing exclusions:

```bash
uv run --frozen pytest tests/integration/test_canonical_exclusion_forecasts.py
```

These six synthetic-data cases fit Fable daily/weekly, NowcastAutoGP daily/weekly, and PyRenew E/HE, then check that every draw covers the requested forecast dates.
They use small inference settings to test execution and output contracts, not convergence or forecast calibration.
