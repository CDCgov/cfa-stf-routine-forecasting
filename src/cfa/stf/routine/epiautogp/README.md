# EpiAutoGP integration

This directory contains the model-specific adapter between the repository's shared forecast pipeline and [`NowcastAutoGP.jl`](https://github.com/CDCgov/NowcastAutoGP).
It is not a standalone pipeline implementation.

## Shared and model-specific responsibilities

`EpiAutoGPPipeline` extends the same `ForecastPipeline` used by Fable and PyRenew.
The shared pipeline owns:

- training-window calculation and surveillance-data loading;
- `ForecastRun`, output directories, and common serialized artifacts;
- standard plots, credible intervals, and Hubverse output.

The EpiAutoGP adapter only owns:

- validation of EpiAutoGP target combinations;
- optional daily-to-epiweekly conversion of common plotting artifacts;
- selection of an EpiAutoGP training series from `ForecastRun`;
- optional reporting-delay or Hubverse nowcast trajectories;
- EpiAutoGP JSON serialization and Julia invocation.

The model-input serializer reads the loaded NSSP or NHSN frame directly from `ForecastRun`.
`combined_data.tsv` remains a shared output and plotting artifact; it is not reread to reconstruct model input.

## Supported configurations

  | Target | Frequency          | `ed_visit_type`               |
  | ------ | ------------------ | ----------------------------- |
  | NSSP   | daily or epiweekly | `observed`, `other`, or `pct` |
  | NHSN   | epiweekly          | `observed`                    |

For epiweekly NSSP forecasts, only complete Sunday-through-Saturday training weeks are included.
Percentage inputs are calculated after weekly aggregation.

Nowcast modes are:

- `none`: fit from the observed series only;
- `reporting-delay`: inflate the recent tail of a count series using a reporting-delay PMF;
- `hubverse`: read probabilistic NHSN trajectories from one materialized `model-output/CFA-nowcastNHSN/*.parquet` artifact.

## Files

- `forecast_epiautogp.py`: pipeline subclass, nowcast-source resolution, and Julia process invocation.
- `prep_epiautogp_data.py`: extraction and JSON serialization of model input.
- `reporting_delay_nowcast.py`: reporting-delay nowcast implementation.
- `fit_epiautogp.jl`: thin Julia adapter that validates JSON, calls `NowcastAutoGP`, and writes standardized `samples.parquet` output.
- `Project.toml` and `Manifest.toml`: pinned Julia environment.

The JSON input is written as `{model_dir}/{model_name}_input.json`.
The Julia adapter writes `{model_dir}/samples.parquet`; the shared publication stage adds the remaining standard forecast outputs.
