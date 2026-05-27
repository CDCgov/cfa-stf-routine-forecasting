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
3. **Data nowcasting**: Either simple right-truncation correction, production NHSN Hubverse nowcasts, or no nowcasting
4. **Data Conversion**: Transform data into EpiAutoGP's JSON input format
5. **Model Execution**: Run the Julia-based EpiAutoGP model
6. **Post-processing**: Process outputs, create hubverse tables, and generate plots

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
- `--nowcast-source`: Nowcast source (`none`, `reporting-delay`, or `hubverse`)
- `--hubverse-nowcast-pointer-uri`: Handoff pointer when using `hubverse`

### `forecast_spec.py`

Defines the `ForecastSpec` value object — a frozen dataclass bundling the six fields that identify a single forecast run: `disease`, `loc`, `report_date`, `target`, `frequency`, `ed_visit_type`. These fields travel together through the pipeline and have inter-field validity constraints (see `_validate_epiautogp_parameters`), so they're treated as one cohesive unit. Lives in its own module to avoid an import cycle between `nowcast.py` and `epiautogp_forecast_utils.py`.

### `epiautogp_forecast_utils.py`

Shared utilities for the forecast pipeline, containing modular functions for each pipeline stage.

**Data Classes:**
- **`ForecastPipelineContext`**: Container for shared pipeline state. Embeds a `ForecastSpec` plus workflow-only fields (training dates, output directories, data sources, logger, nowcast source).
- **`ModelPaths`**: Container for output directory structure and file paths

**Key Functions:**
- **`setup_forecast_pipeline()`**: Builds the `ForecastSpec`, resolves the nowcast source, and assembles a `ForecastPipelineContext` for downstream stages.
- **`_resolve_nowcast_source()`**: Dispatches on `nowcast_source_name` to construct a `NowcastSource`. Universal args (`forecast_spec`, `nowcast_source_name`) are explicit; source-specific options are read by the chosen builder, which validates them.

### `nowcast.py`, `reporting_delay_nowcast.py`, & `hubverse_nowcast.py`

Pluggable nowcasting sources for nowcasting recent observations.

- **`NowcastSource`** (Protocol): Declares `applies_to(*, forecast_spec) -> bool` (the predicate the resolver queries before constructing) and `get_nowcast_data(*, dates, reports) -> NowcastData` (the action).
- **`FixedNowcast`**: Trivial source wrapping a precomputed `NowcastData`.
- **`ReportingDelayNowcast`**: Inflates the most-recent observations by the inverse of a reporting-delay PMF. Applies to count series (rejects `ed_visit_type="pct"`); warns when used on a non-daily series since the PMF support is daily by convention.
- **`HubversePointerNowcast`**: Generic reader for production handoff pointers whose `hubverse.model_output_uri` is a Hubverse sample-format parquet. It applies to any forecast spec; the pointer disease target and row target are inferred from `ForecastSpec`.

#### Hubverse Nowcasts

Use `--nowcast-source hubverse` to read negative-horizon samples from another model's Hubverse output:

```bash
uv run python pipelines/epiautogp/forecast_epiautogp.py \
  --target nhsn \
  --frequency epiweekly \
  --nowcast-source hubverse \
  --hubverse-nowcast-pointer-uri nowcastnhsn-prod/latest/covid.json \
  ...
```

The pointer must be a production `handoff_pointer` JSON whose `target` and `report_date` match the EpiAutoGP run and whose `hubverse.validation_status` is `passed`. The source then reads `hubverse.model_output_uri`.

The Hubverse row target is inferred from `ForecastSpec`: disease maps to `covid`/`flu`/`rsv`, `target` and `ed_visit_type` choose the producer variable, and `frequency` adds the epiweekly `wk ` prefix. The inference mirrors the producer convention in `hewr/R/to_hubverse_tbl.R`.

The pointer URI is a plain file path (absolute or relative) to the mounted blob container. In Docker/Azure Batch execution, the `nowcastnhsn-prod` container is bind-mounted into the working directory. For local testing, ensure the container is mounted via `make mount` or provide a path to local test data.

Only strictly negative Hubverse horizons are used as nowcast dates; horizon `0` is ignored. The source fails fast for missing rows, duplicate sample/date rows, incomplete sample grids, unsupported diseases, mismatched pointer metadata, or missing/non-finite/negative values.

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
- Shared pipeline utilities (`setup_forecast_pipeline`, `prepare_model_data`)
- Common data formats (TSV training data, hubverse tables)
- Consistent directory structure
- Modular, reusable functions exported through `__init__.py`
