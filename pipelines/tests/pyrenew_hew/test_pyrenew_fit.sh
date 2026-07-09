#!/bin/bash

# Check if the base directory is provided as an argument
if [[ $# -ne 4 ]]; then
	echo "Usage: $0 <base_dir> <disease> <location> <model_letters>"
	exit 1
fi

BASE_DIR="$1"
disease="$2"
location="$3"
model_letters="$4"

python pipelines/pyrenew_hew/forecast_pyrenew.py \
	--disease "$disease" \
	--loc "$location" \
	--priors-path pipelines/pyrenew_hew/priors/prod_priors.py \
	--output-dir "$BASE_DIR/2024-12-21_forecasts" \
	--n-training-days 90 \
	--n-chains 2 \
	--n-samples 250 \
	--n-warmup 250 \
	--rng-key 12345 \
	--model-letters "$model_letters" \
	--additional-forecast-letters "$model_letters" \
	--nhsn-data-path "$BASE_DIR/private_data/nhsn_test_data/${disease}_${location}.parquet"
if [ "$?" -ne 0 ]; then
	echo "TEST-MODE FAIL: Forecasting/postprocessing pipeline failed"
	exit 1
fi
