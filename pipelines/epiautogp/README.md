# EpiAutoGP Integration Module

This module provides a forecasting pipeline interface for the `EpiAutoGP` model.

## Overview

The EpiAutoGP pipeline supports forecasting of:
- **NSSP ED visits**: Emergency department visits from the National Syndromic Surveillance Program
- **NHSN hospital admissions**: Hospital admission counts from the National Healthcare Safety Network

It operates on both **daily** and **epiweekly** temporal frequencies, with optional percentage transformations for ED visit data.

## Pipeline Architecture

The forecasting pipeline consists of five main steps:

1. **Setup**: Load data, validate dates, create directory structure
2. **Data Preparation**: Process location data, evaluation data, and generate epiweekly datasets
3. **Data Conversion**: Transform data into EpiAutoGP's JSON input format
4. **Model Execution**: Run the Julia-based EpiAutoGP model
5. **Post-processing**: Process outputs, create hubverse tables, and generate plots

## Module Components

### `forecast_epiautogp.py`

Main entry point for the forecasting pipeline.

**Key Functions:**
- **`main()`**: Orchestrates the complete pipeline from setup to post-processing
- **`run_epiautogp_forecast()`**: Executes the Julia EpiAutoGP model with specified parameters

**EpiAutoGP-Specific Parameters:**
- `--target`: Data type (`nssp` or `nhsn`)
- `--frequency`: Temporal frequency (`daily` or `epiweekly`)
- `--n-particles`: Number of particles for Sequential Monte Carlo (default: 24)
- `--n-mcmc`: MCMC steps for GP kernel structure (default: 100)
- `--n-hmc`: HMC steps for GP kernel hyperparameters (default: 50)
- `--n-forecast-draws`: Number of forecast draws (default: 2000)
- `--smc-data-proportion`: Data proportion per SMC step (default: 0.1)

### `epiautogp_forecast_utils.py`

Shared utilities for the forecast pipeline, containing modular functions for each pipeline stage.

**Data Classes:**
- **`ForecastPipelineContext`**: Container for shared pipeline state (disease, location, dates, data sources, logger)
- **`ModelPaths`**: Container for output directory structure and file paths

### `prep_epiautogp_data.py`

Data conversion utilities for EpiAutoGP JSON format.

**Key Function:**
- **`convert_to_epiautogp_json()`**: Converts surveillance data to EpiAutoGP JSON format
  - Supports both NSSP (ED visits) and NHSN (hospital admission counts)
  - Handles daily and epiweekly data frequencies
  - Optional percentage transformation for ED visits
  - Validates input parameters and data availability

**Input Data Sources:**
1. **Legacy JSON Format**: `data_for_model_fit.json` with `nssp_training_data` and `nhsn_training_data`
2. **TSV Files (Recommended)**:
   - Daily: `combined_data.tsv`
   - Epiweekly: `epiweekly_combined_data.tsv`
   - Contains: `observed_ed_visits`, `other_ed_visits`, `observed_hospital_admissions`

**Output Format:**
```json
{
  "dates": ["2024-09-22", "2024-09-23", ...],
  "reports": [45.5, 52.3, ...],
  "pathogen": "COVID-19",
  "location": "DC",
  "target": "nssp",
  "forecast_date": "2024-12-20",
  "nowcast_dates": [],
  "nowcast_reports": []
}
```

### `process_epiautogp_forecast.py`

Post-processing utilities for EpiAutoGP outputs.

**Key Function:**
- **`calculate_credible_intervals()`**: Computes median and credible intervals from posterior samples
  - Default intervals: 50%, 80%, 95%
- **`process_epiautogp_forecast()`**: Converts Julia outputs to R plotting format
  - Reads raw EpiAutoGP parquet files
  - Calculates credible intervals
  - Saves processed `samples.parquet` and `ci.parquet` files

### `plot_epiautogp_forecast.R`

R script for generating forecast visualizations specific to EpiAutoGP outputs.

## Output Structure

```
output_dir/
└── {disease}_r_{report_date}_f_{first_train}_t_{last_train}/
    └── model_runs/
        └── {loc}/
            └── epiautogp_{target}_{frequency}[_pct]/
                ├── data/
                │   ├── combined_data.tsv
                │   ├── epiweekly_combined_data.tsv
                │   └── eval_data.tsv
                ├── input.json
                ├── samples.parquet
                ├── ci.parquet
                ├── forecast.parquet (raw EpiAutoGP output)
                ├── hubverse_table.csv
                └── plots/
```

## Integration with cfa-stf-routine-forecasting

This module follows the same design patterns as other forecasting models in the cfa-stf-routine-forecasting pipeline:
- Shared pipeline utilities (`setup_forecast_pipeline`, `prepare_model_data`)
- Common data formats (TSV training data, hubverse tables)
- Consistent directory structure
- Modular, reusable functions exported through `__init__.py`
