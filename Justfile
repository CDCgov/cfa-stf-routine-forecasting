set shell := ["bash", "-uc"]

e2e_output_dir := "tests/end_to_end_test_output"

default:
    @just --list

# Start Dagster from the project definitions entrypoint.
dagster:
    uv run dagster_defs.py

# Run the fast Python test suite.
test:
    uv run pytest -m "not pipeline_e2e and not model_integration"

# Run the reduced pipeline end-to-end test and retain its output in the repo.
e2e data_mode="auto":
    #!/usr/bin/env bash
    set -euo pipefail

    uv run pytest -s \
      -m pipeline_e2e \
      tests/integration/test_pipeline_end_to_end.py \
      --e2e-output-dir "{{e2e_output_dir}}" \
      --e2e-force \
      --e2e-data-mode "{{data_mode}}"

# Test Fable for one disease and location with mock or real DataOps data.
test-fable data_mode="auto" location="CA" disease="COVID-19":
    uv run pytest -s \
      tests/integration/test_fable_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}"

# Test PyRenew for one disease and location with mock or real DataOps data.
test-pyrenew data_mode="auto" location="CA" disease="COVID-19":
    uv run pytest -s \
      tests/integration/test_pyrenew_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}"

# Test EpiAutoGP for one disease and location with mock or real DataOps data.
test-epiautogp data_mode="auto" location="CA" disease="COVID-19":
    uv run pytest -s \
      tests/integration/test_epiautogp_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}"
