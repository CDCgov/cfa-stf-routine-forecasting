#!/bin/bash

# Run a single EpiAutoGP forecast
# Called by test_epiautogp_end_to_end.sh
#
# Usage:
#   bash test_epiautogp_fit.sh <base_dir> <disease> <location> <target> <frequency> <use_percentage> [ed_visit_type]

if [[ $# -lt 6 || $# -gt 7 ]]; then
	echo "Usage: $0 <base_dir> <disease> <location> <target> <frequency> <use_percentage> [ed_visit_type]"
	echo "  target: nhsn or nssp"
	echo "  frequency: daily or epiweekly"
	echo "  use_percentage: true or false"
	echo "  ed_visit_type: observed or other (optional, default: observed)"
	exit 1
fi

BASE_DIR="$1"
disease="$2"
location="$3"
target="$4"
frequency="$5"
use_percentage="$6"
ed_visit_type="${7:-observed}" # Default to "observed" if not provided

# Build command arguments
cmd_args=(
	--disease "$disease"
	--loc "$location"
	--facility-level-nssp-data-dir "$BASE_DIR/private_data/nssp_etl_gold"
	--output-dir "$BASE_DIR/2024-12-21_forecasts"
	--n-training-days 90
	--target "$target"
	--frequency "$frequency"
	--nhsn-data-path "$BASE_DIR/private_data/nhsn_test_data/${disease}_${location}.parquet"
	--n-forecast-days 28
	--n-particles 2
	--n-mcmc 2
	--n-hmc 2
	--n-forecast-draws 100
	--smc-data-proportion 0.1
)

# Add percentage flag if needed
if [ "$use_percentage" = "true" ]; then
	cmd_args+=(--use-percentage)
fi

# Add ed-visit-type if not default
if [ "$ed_visit_type" != "observed" ]; then
	cmd_args+=(--ed-visit-type "$ed_visit_type")
fi

uv run python pipelines/epiautogp/forecast_epiautogp.py "${cmd_args[@]}"

if [ "$?" -ne 0 ]; then
	echo "TEST-MODE FAIL: EpiAutoGP forecast pipeline failed"
	exit 1
fi
