set shell := ["bash", "-uc"]

e2e_output_dir := "test_output"

default:
    @just --list

# Start Dagster from the project definitions entrypoint.
dagster:
    uv run src/cfa/stf/routine/dagster/defs.py

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

    just _print-output-location "{{e2e_output_dir}}"

# Test Fable for one disease and location and retain its output.
test-fable data_mode="auto" location="CA" disease="covid":
    uv run pytest -s \
      tests/integration/test_fable_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}" \
      --e2e-output-dir "{{e2e_output_dir}}/fable" \
      --e2e-force
    @just _print-output-location "{{e2e_output_dir}}/fable"

# Test PyRenew for one disease and location and retain its output.
test-pyrenew data_mode="auto" location="CA" disease="covid":
    uv run pytest -s \
      tests/integration/test_pyrenew_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}" \
      --e2e-output-dir "{{e2e_output_dir}}/pyrenew" \
      --e2e-force
    @just _print-output-location "{{e2e_output_dir}}/pyrenew"

# Test EpiAutoGP for one disease and location and retain its output.
test-epiautogp data_mode="auto" location="CA" disease="covid":
    uv run pytest -s \
      tests/integration/test_epiautogp_forecast.py \
      --e2e-data-mode "{{data_mode}}" \
      --model-test-location "{{location}}" \
      --model-test-disease "{{disease}}" \
      --e2e-output-dir "{{e2e_output_dir}}/epiautogp" \
      --e2e-force
    @just _print-output-location "{{e2e_output_dir}}/epiautogp"

# Print a clickable absolute path to retained test output.
[private]
_print-output-location output_dir:
    @output_path="{{absolute_path(output_dir)}}"; printf '\nTest output: \033]8;;file://%s\033\\%s\033]8;;\033\\\n' "$output_path" "$output_path"

# Remove all retained end-to-end and single-model test outputs.
clean-outputs:
    rm -rf -- "{{e2e_output_dir}}"
