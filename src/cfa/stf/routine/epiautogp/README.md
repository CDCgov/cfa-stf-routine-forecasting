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
3. **Data nowcasting**: Use no nowcast, reporting-delay correction, or probabilistic NHSN trajectories from a materialized Hubverse asset
4. **Data Conversion**: Transform data into EpiAutoGP's JSON input format
5. **Model Execution**: Run the Julia-based EpiAutoGP model
6. **Post-processing**: Process outputs, create hubverse tables, and generate plots

## Module Components

### `forecast_epiautogp.py`

Main entry point for the forecasting pipeline.

**Key Functions:**

- **`main()`**: Orchestrates the complete pipeline from setup to post-processing
- **`run_epiautogp_forecast()`**: Executes the Julia EpiAutoGP model with specified parameters

**EpiAutoGP-Specific `main()` Parameters:**

- `target`: Data type (`nssp` or `nhsn`)
- `frequency`: Temporal frequency (`daily` or `epiweekly`)
- `n_particles`: Number of particles for Sequential Monte Carlo (default: 24)
- `n_mcmc`: MCMC steps for GP kernel structure (default: 100)
- `n_hmc`: HMC steps for GP kernel hyperparameters (default: 50)
- `n_forecast_draws`: Number of forecast draws (default: 2000)
- `smc_data_proportion`: Data proportion per SMC step (default: 0.1)

### `config.py`

Defines the frozen `EpiAutoGPConfig` value object for model-specific options: `target`, `frequency`, `ed_visit_type`, and excluded date ranges.
Disease, location, report date, training dates, loaded inputs, and output paths come from the shared `ForecastRun`.

### Shared forecast lifecycle

`EpiAutoGPPipeline` extends the repository-wide `ForecastPipeline` lifecycle used by Fable and PyRenew.
Shared setup constructs one canonical `ForecastRun`; EpiAutoGP then resolves its model-specific `EpiAutoGPModelInputs`, performs optional epiweekly aggregation, converts the run to JSON, executes Julia, and uses the common post-processing stage.

**Key Types:**

- **`SurveillanceInputs`**: Loaded surveillance frames, freshness, and location population.
- **`ForecastRun`**: Canonical run identity, training window, forecast horizon, model name, surveillance inputs, and derived metadata and output paths.
- **`EpiAutoGPModelInputs`**: The nowcast source resolved for a particular forecast run.
- **`EpiAutoGPPipeline`**: EpiAutoGP-specific implementation of the shared pipeline lifecycle.

**Key Functions:**

- **`_resolve_nowcast_source()`**: Dispatches on `nowcast_source_name` to construct a `NowcastSource`.
  Applicability uses `EpiAutoGPConfig`, while source loading uses the canonical `ForecastRun` identity.

### `nowcast.py`, `reporting_delay_nowcast.py`, and `../data/hubverse_nowcast.py`

Pluggable nowcasting sources for nowcasting recent observations.

- **`NowcastData`** (`../data/nowcast.py`): Stores nowcast dates and report trajectories.
- **`NowcastSource`** (`../data/nowcast.py`): Protocol declaring `ensure_applicable(*, config) -> None` and `get_nowcast_data(*, dates, reports) -> NowcastData`.
- **`FixedNowcast`**: Trivial source wrapping a precomputed `NowcastData`.
- **`ReportingDelayNowcast`**: Inflates the most-recent observations by the inverse of a reporting-delay PMF.
  Applies to count series (rejects `ed_visit_type="pct"`); warns when used on a non-daily series since the PMF support is daily by convention.
- **`HubverseNowcast`**: Reads a pathogen-partitioned directory materialized by the Dagster ADLS filesystem IO manager.
  The directory must contain exactly one Parquet under `model-output/CFA-nowcastNHSN/`.
  It applies to epiweekly NHSN observed counts.

For Hubverse sample output, `target_end_date` supplies the sorted `nowcast_dates`.
Rows sharing an `output_type_id` form one complete trajectory, with `value` ordered by `target_end_date`, so the JSON contains one inner `nowcast_reports` vector per sample ID.
`origin_date` identifies the artifact vintage and must equal the EpiAutoGP run's report date.

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
  "pathogen": "covid",
  "location": "DC",
  "target": "nssp",
  "frequency": "daily",
  "ed_visit_type": "observed",
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

- Shared `ForecastPipeline.execute()` lifecycle and canonical `ForecastRun` state
- Common data formats (TSV training data, hubverse tables)
- Consistent directory structure
- Modular, reusable functions exported through `__init__.py`
