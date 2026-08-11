# Pipeline Tests

Tests are organized to match the `cfa.stf.routine` package layout:

- `data/`: tests for `cfa.stf.routine.data`
- `epiautogp/`: tests for `cfa.stf.routine.epiautogp`
- `fable/`: tests for `cfa.stf.routine.fable`
- `pyrenew_hew/`: tests for `cfa.stf.routine.pyrenew_hew`
- `utils/`: tests for `cfa.stf.routine.utils`
- `integration/`: independent model tests and the cross-pipeline end-to-end test

The pipeline tests use real DataOps data when `cfa.cloudops.util.check_ext_env()` detects the external CFA environment and fall back to deterministic mocked data otherwise.
The data source can also be selected explicitly with `--e2e-data-mode real|mock`.

Each model has a standalone `just` recipe that runs one disease/location pair.
The PyRenew recipe runs the H, E, and HE versions once each; the EpiAutoGP recipe runs weekly NHSN counts, weekly NSSP percentages, daily NSSP disease counts, and daily NSSP other counts.

```bash
just test-fable mock CA COVID-19
just test-pyrenew mock CA COVID-19
just test-epiautogp mock CA COVID-19
```

Replace `mock` with `real` to use CFA DataOps data.
All arguments are optional; the defaults are `auto`, `CA`, and `COVID-19`.

The combined test remains available through `just e2e auto`, `just e2e real`, or `just e2e mock`.
