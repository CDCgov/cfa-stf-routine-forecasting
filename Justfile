set shell := ["bash", "-uc"]

e2e_output_dir := "pipelines/tests/end_to_end_test_output"

default:
    @just --list

# Run the reduced pipeline end-to-end test and retain its output in the repo.
e2e:
    #!/usr/bin/env bash
    set -euo pipefail

    uv run pytest -s \
      -m pipeline_e2e \
      pipelines/tests/integration/test_pipeline_end_to_end.py \
      --e2e-output-dir "{{e2e_output_dir}}" \
      --e2e-force
