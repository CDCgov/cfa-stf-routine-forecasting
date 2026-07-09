# Pipeline Tests

Tests are organized to match the `pipelines` package layout:

- `data/`: tests for `pipelines/data`
- `epiautogp/`: tests and smoke-test scripts for `pipelines/epiautogp`
- `fable/`: smoke-test scripts for `pipelines/fable`
- `pyrenew_hew/`: smoke-test scripts for `pipelines/pyrenew_hew`
- `utils/`: tests for `pipelines/utils`
- `integration/`: cross-pipeline end-to-end test drivers

The reduced pipeline e2e test uses real DataOps data when
`cfa.cloudops.util.check_ext_env()` detects the external CFA environment and
falls back to mocked synthetic data otherwise. Use `just e2e-auto`,
`just e2e-real`, or `just e2e-mock` to choose the data mode explicitly. You can
also pass `--e2e-data-mode auto|real|mock` directly to pytest.
