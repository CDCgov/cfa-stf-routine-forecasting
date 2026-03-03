#!/bin/bash

# End-to-end test for EpiAutoGP forecasting pipeline
# Tests multiple locations and targets (weekly NHSN and weekly percentage NSSP)
#
# Usage:
#   bash pipelines/tests/test_epiautogp_end_to_end.sh [--force]
#
# Options:
#   --force    Delete existing test output directory

set -e # Exit on any error

BASE_DIR=pipelines/tests/epiautogp_end_to_end_test_output
LOCATIONS=(US CA MT DC)
DISEASES=(COVID-19) # Start with COVID-19 for testing
FORECAST_DATE=2024-12-21

echo "========================================"
echo "EpiAutoGP End-to-End Test"
echo "========================================"
echo "Testing configurations:"
echo "  Locations: ${LOCATIONS[*]}"
echo "  Diseases: ${DISEASES[*]}"
echo "  Targets: NHSN (weekly), NSSP (weekly percentage), NSSP (daily counts), NSSP (daily other ED visits)"
echo "  Forecast date: $FORECAST_DATE"
echo ""

# Step 0: Clean up previous run
if [ -d "$BASE_DIR" ]; then
	if [[ "$*" == *"--force"* ]]; then
		echo "Cleaning previous test output..."
		rm -rf "$BASE_DIR"
	else
		echo "ERROR: Test output directory exists: $BASE_DIR"
		echo "Delete it or run with --force flag"
		exit 1
	fi
fi

# Step 1: Generate test data
echo "========================================="
echo "Step 1: Generating test data"
echo "========================================="
uv run python pipelines/data/generate_test_data.py "$BASE_DIR"

if [ "$?" -ne 0 ]; then
	echo "TEST-MODE FAIL: Generating test data failed"
	exit 1
else
	echo "✓ Test data generated"
	echo ""
fi

# Step 2: Run EpiAutoGP forecasting pipeline for all locations and targets
echo "========================================="
echo "Step 2: Running EpiAutoGP forecasts"
echo "========================================="

for location in "${LOCATIONS[@]}"; do
	for disease in "${DISEASES[@]}"; do
		# Test weekly NHSN (hospital admissions)
		echo "TEST-MODE: Running EpiAutoGP weekly NHSN forecast for $disease, $location"
		bash pipelines/tests/test_epiautogp_fit.sh \
			"$BASE_DIR" \
			"$disease" \
			"$location" \
			"nhsn" \
			"epiweekly" \
			"observed"

		if [ "$?" -ne 0 ]; then
			echo "TEST-MODE FAIL: EpiAutoGP NHSN forecast failed for $disease, $location"
			exit 1
		else
			echo "TEST-MODE: Finished EpiAutoGP weekly NHSN forecast for $disease, $location."
		fi

		# Test weekly NSSP percentage (ED visits as percentage)
		echo "TEST-MODE: Running EpiAutoGP weekly NSSP percentage forecast for $disease, $location"
		bash pipelines/tests/test_epiautogp_fit.sh \
			"$BASE_DIR" \
			"$disease" \
			"$location" \
			"nssp" \
			"epiweekly" \
			"pct"

		if [ "$?" -ne 0 ]; then
			echo "TEST-MODE FAIL: EpiAutoGP NSSP percentage forecast failed for $disease, $location"
			exit 1
		else
			echo "TEST-MODE: Finished EpiAutoGP weekly NSSP percentage forecast for $disease, $location."
		fi

		# Test daily NSSP counts (ED visit counts, not percentages)
		echo "TEST-MODE: Running EpiAutoGP daily NSSP count forecast for $disease, $location"
		bash pipelines/tests/test_epiautogp_fit.sh \
			"$BASE_DIR" \
			"$disease" \
			"$location" \
			"nssp" \
			"daily" \
			"observed"

		if [ "$?" -ne 0 ]; then
			echo "TEST-MODE FAIL: EpiAutoGP daily NSSP count forecast failed for $disease, $location"
			exit 1
		else
			echo "TEST-MODE: Finished EpiAutoGP daily NSSP count forecast for $disease, $location."
		fi

		# Test daily NSSP other ED visits (non-target background)
		echo "TEST-MODE: Running EpiAutoGP daily NSSP other ED visits forecast for $disease, $location"
		bash pipelines/tests/test_epiautogp_fit.sh \
			"$BASE_DIR" \
			"$disease" \
			"$location" \
			"nssp" \
			"daily" \
			"other"

		if [ "$?" -ne 0 ]; then
			echo "TEST-MODE FAIL: EpiAutoGP daily NSSP other ED visits forecast failed for $disease, $location"
			exit 1
		else
			echo "TEST-MODE: Finished EpiAutoGP daily NSSP other ED visits forecast for $disease, $location."
		fi

		echo "✓ All forecasts complete for $disease, $location"
	done
done

echo ""
echo "========================================="
echo "Step 3: Verifying outputs"
echo "========================================="

# Count expected outputs (4 targets × number of locations × number of diseases)
EXPECTED_MODELS=$((4 * ${#LOCATIONS[@]} * ${#DISEASES[@]}))
ACTUAL_MODELS=$(find "$BASE_DIR/${FORECAST_DATE}_forecasts" -type d -name "epiautogp_*" | wc -l)

echo "Expected models: $EXPECTED_MODELS"
echo "Actual models: $ACTUAL_MODELS"

if [ "$ACTUAL_MODELS" -eq "$EXPECTED_MODELS" ]; then
	echo "✓ Output verification passed"
else
	echo "ERROR: Output count mismatch"
	echo "Directory structure:"
	find "$BASE_DIR/${FORECAST_DATE}_forecasts" -type d -name "epiautogp_*"
	exit 1
fi

echo ""
echo "========================================="
echo "All Tests Passed!"
echo "========================================="
echo "Test output saved to: $BASE_DIR"
echo ""
