# Production Hubverse nowcast fixture

This fixture is the `ca` location slice from the COVID-19 production nowcast
artifact with origin date 2026-07-18:

`dagster-files/usi1/hubverse_model_output/covid/model-output/CFA-nowcastNHSN/2026-07-18-CFA-nowcastNHSN.parquet`

The source table was filtered only to `location == "ca"`. It retains all 2,000
sample trajectories and all four target end dates, giving 8,000 rows in the
same Parquet schema and directory layout materialized by the Dagster ADLS
filesystem IO manager.
