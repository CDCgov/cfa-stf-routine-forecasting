#!/bin/bash

# Check if the base directory is provided as an argument
if [[ $# -ne 3 ]]; then
	echo "Usage: $0 <base_dir> <disease> <location>"
	echo "Received $# arguments: $@"
	exit 1
fi

BASE_DIR="$1"
disease="$2"
location="$3"

for epiweekly_flag in "" "--epiweekly"; do
	python pipelines/fable/forecast_timeseries.py \
		--disease "$disease" \
		--loc "$location" \
		--facility-level-nssp-data-dir "$BASE_DIR/private_data/nssp_etl_gold" \
		--output-dir "$BASE_DIR/2024-12-21_forecasts" \
		--n-training-days 90 \
		--n-samples 500 \
		$epiweekly_flag \
		--nhsn-data-path "$BASE_DIR/private_data/nhsn_test_data/${disease}_${location}.parquet"
done

if [ "$?" -ne 0 ]; then
	echo "TEST-MODE FAIL: Forecasting/postprocessing pipeline failed"
	exit 1
fi
