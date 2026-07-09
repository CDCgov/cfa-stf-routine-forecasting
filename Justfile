set shell := ["bash", "-uc"]

e2e_output_dir := "pipelines/tests/end_to_end_test_output"

default:
    @just --list

# Start Dagster from the project definitions entrypoint.
dagster:
    uv run dagster_defs.py

# Run the reduced pipeline end-to-end test in auto data mode.
e2e-auto:
    @just e2e auto

# Run the reduced pipeline end-to-end test with real DataOps data.
e2e-real:
    @just e2e real

# Run the reduced pipeline end-to-end test with mocked DataOps data.
e2e-mock:
    @just e2e mock

# Run the reduced pipeline end-to-end test and retain its output in the repo.
e2e data_mode="auto":
    #!/usr/bin/env bash
    set -euo pipefail

    uv run pytest -s \
      -m pipeline_e2e \
      pipelines/tests/integration/test_pipeline_end_to_end.py \
      --e2e-output-dir "{{e2e_output_dir}}" \
      --e2e-force \
      --e2e-data-mode "{{data_mode}}"
