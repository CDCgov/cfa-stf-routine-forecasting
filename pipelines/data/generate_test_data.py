"""
Script to generate synthetic test data for disease modelling.

This script creates a bootstrap data structure and synthetic data for multiple states
and diseases, saving results in the specified output directory. The aim is to produce
fairly realistic test data that mimics real-world surveillance data for use in
development and testing of disease modelling pipelines.

Steps:
1. Creates parameter estimates (generation interval, right truncation, delay PMFs)
2. Generates bootstrap data structure for a reference location/disease (populated with zeros for observations)
3. Uses the bootstrap data structure to build a PyRenew model
4. Runs prior predictive sampling to generate synthetic observations
    4a. Creates facility-level and state-level NSSP ED visit data (useful for NSSP-ETL and NSSP state-level gold)
    4b. Creates NWSS wastewater surveillance data with site-level concentrations
    4c. Creates NHSN hospital admission data (state and US-level)
5. Saves all data as parquet files in private_data directory structure
6. Optionally removes bootstrap data directory after simulation, otherwise updates bootstrap data files with prior predictive values

Arguments:
    base_dir: Base directory where output will be saved. Creates two subdirectories:
        - bootstrap_private_data/: Temporary bootstrap data (removed if --clean)
        - private_data/: Final synthetic test data in production format
    --clean: Optional flag to remove bootstrap_private_data directory after generation. Default is `False`.
"""

import argparse
import datetime as dt
from pathlib import Path

import jax.random as jr
import numpy as np
import polars as pl
import polars.selectors as cs

from cfa.stf.forecasttools import get_us_loc_pop_tbl
from pipelines.data.generate_test_data_lib import (
    FACILITY_LEVEL_NSSP_DATA_COLS,
    LOC_LEVEL_NSSP_DATA_COLS,
    LOC_LEVEL_NWSS_DATA_COLUMNS,
    NHSN_COLS,
    PREDICTIVE_VAR_NAMES,
    create_default_param_estimates,
    dirichlet_integer_split,
    simulate_data_from_bootstrap,
)


def main(base_dir: Path, clean: bool):
    # Configuration
    max_train_date_str = "2024-12-21"
    max_train_date = dt.datetime.strptime(max_train_date_str, "%Y-%m-%d").date()
    # Verify this is a Saturday
    assert max_train_date.weekday() == 5

    states_to_simulate = ["MT", "CA", "DC"]
    diseases_to_simulate = ["Influenza", "COVID-19", "RSV"]

    # Create parameter estimates
    param_estimates = create_default_param_estimates(
        states_to_simulate=states_to_simulate,
        diseases_to_simulate=diseases_to_simulate,
        max_train_date_str=max_train_date_str,
        max_train_date=max_train_date,
    )

    # Setup directories
    bootstrap_dir_name = "bootstrap_private_data"
    private_data_dir_name = "private_data"
    bootstrap_private_data_dir = Path(base_dir, bootstrap_dir_name)
    bootstrap_private_data_dir.mkdir(parents=True, exist_ok=True)

    private_data_dir = Path(base_dir, private_data_dir_name)
    private_data_dir.mkdir(parents=True, exist_ok=True)

    # Simulation parameters
    n_training_weeks = 16
    n_training_days = n_training_weeks * 7
    n_forecast_weeks = 4
    n_forecast_days = 7 * n_forecast_weeks
    n_nssp_sites = 5
    n_ww_sites = 5
    ww_flag_prob = 0.1

    # Save parameter estimates
    param_estimates_dir = Path(private_data_dir, "prod_param_estimates")
    param_estimates_dir.mkdir(parents=True, exist_ok=True)
    param_estimates.write_parquet(Path(param_estimates_dir, "prod.parquet"))

    # Simulate data for states with reference subpopulation
    # This bootstrap data is not cleaned up to allow next step to use
    dfs_ref_subpop = simulate_data_from_bootstrap(
        n_training_days=n_training_days,
        max_train_date=max_train_date,
        n_nssp_sites=n_nssp_sites,
        n_training_weeks=n_training_weeks,
        bootstrap_private_data_dir=bootstrap_private_data_dir,
        param_estimates=param_estimates.lazy(),
        n_forecast_days=n_forecast_days,
        n_ww_sites=n_ww_sites,
        states_to_simulate=[s for s in states_to_simulate if s != "DC"],
        diseases_to_simulate=diseases_to_simulate,
        clean=False,
        rng_key=jr.key(461),
        priors_source_path=Path("pipelines/priors/test_data_generation_priors.py"),
    )

    # Simulate data for states without reference subpopulation
    # This bootstrap data is cleaned up after simulation
    # depending on user input
    dfs_no_ref_subpop = simulate_data_from_bootstrap(
        n_training_days=n_training_days,
        max_train_date=max_train_date,
        n_nssp_sites=n_nssp_sites,
        n_training_weeks=n_training_weeks,
        bootstrap_private_data_dir=bootstrap_private_data_dir,
        param_estimates=param_estimates.lazy(),
        n_forecast_days=n_forecast_days,
        n_ww_sites=1,
        states_to_simulate=["DC"],
        diseases_to_simulate=diseases_to_simulate,
        clean=clean,
        rng_key=jr.key(461),
        priors_source_path=Path("pipelines/priors/test_data_generation_priors.py"),
    )

    # Concatenate dataframes by variable names
    dfs = {}
    for var in PREDICTIVE_VAR_NAMES:
        dfs[var] = pl.concat([dfs_ref_subpop[var], dfs_no_ref_subpop[var]])

    # Generate NSSP ETL gold data (facility-level ED visits)
    nssp_etl_gold_no_total = (
        dfs["observed_ed_visits"]
        .with_columns(
            (
                pl.lit(max_train_date)
                + pl.duration(
                    days=(pl.col("time") - pl.col("time").max() + n_forecast_days)
                )
            ).alias("reference_date"),
            pl.lit(max_train_date).alias("report_date"),
            pl.lit("state").alias("geo_type"),
            pl.lit("count_ed_visits").alias("metric"),
            pl.col("disease").replace({"COVID-19": "COVID-19/Omicron"}),
            pl.lit(True).alias("any_update_this_day"),
            pl.lit(np.arange(1, n_nssp_sites + 1).tolist()).alias("facility"),
            pl.lit(max_train_date).alias("asof"),
            pl.lit(0).alias("run_id"),
            pl.col("observed_ed_visits").map_elements(
                lambda x: dirichlet_integer_split(x, k=n_nssp_sites).tolist(),
                pl.List(pl.Int64),
            ),
        )
        .rename({"state": "geo_value", "observed_ed_visits": "value"})
        .explode(["value", "facility"])
        .select(cs.by_name(FACILITY_LEVEL_NSSP_DATA_COLS))
    )

    nssp_etl_gold_total = (
        nssp_etl_gold_no_total.group_by(cs.exclude("disease", "value"))
        .agg(pl.col("value").sum())
        .with_columns(
            (
                pl.col("value") + 5000 + np.sin(pl.row_index() * 2 * np.pi / 50) * 1000
            ).cast(pl.Int64),
            pl.lit("Total").alias("disease"),
        )
        .select(nssp_etl_gold_no_total.columns)
        .sort(["reference_date", "geo_value", "facility", "disease"])
    )

    nssp_etl_gold = pl.concat([nssp_etl_gold_no_total, nssp_etl_gold_total]).sort(
        ["reference_date", "geo_value", "facility", "disease"]
    )

    nssp_etl_gold_dir = Path(private_data_dir, "nssp_etl_gold")
    nssp_etl_gold_dir.mkdir(parents=True, exist_ok=True)
    nssp_etl_gold.filter(pl.col("reference_date") <= max_train_date).write_parquet(
        Path(nssp_etl_gold_dir, f"{max_train_date}.parquet")
    )

    # Generate NSSP state-level gold data
    nssp_state_level_gold = (
        nssp_etl_gold.group_by(cs.exclude("facility", "value"))
        .agg(pl.col("value").sum())
        .with_columns(pl.lit(True).alias("any_update_this_day"))
        .sort(["reference_date", "geo_value", "disease"])
        .select(cs.by_name(LOC_LEVEL_NSSP_DATA_COLS))
    )

    nssp_state_level_gold_dir = Path(private_data_dir, "nssp_state_level_gold")
    nssp_state_level_gold_dir.mkdir(parents=True, exist_ok=True)
    nssp_state_level_gold.filter(
        pl.col("reference_date") <= max_train_date
    ).write_parquet(Path(nssp_state_level_gold_dir, f"{max_train_date}.parquet"))

    # Generate NSSP-ETL latest comprehensive data
    nssp_etl_dir = Path(private_data_dir, "nssp-etl")
    nssp_etl_dir.mkdir(parents=True, exist_ok=True)
    nssp_state_level_gold.select(cs.exclude("any_update_this_day")).write_parquet(
        Path(nssp_etl_dir, "latest_comprehensive.parquet")
    )

    # Generate NWSS data
    nwss_etl_base = (
        dfs["site_level_log_ww_conc"]
        .filter(pl.col("disease") == "COVID-19")
        .with_row_index()
        .with_columns(
            (
                pl.lit(max_train_date)
                + pl.duration(
                    days=(pl.col("time") - pl.col("time").max() + n_forecast_days)
                )
            ).alias("sample_collect_date"),
            pl.first("index").over("state", "site").rank("dense").alias("site"),
            pl.lit("wwtp").alias("sample_location"),
            pl.lit("raw wastewater").alias("sample_matrix"),
            pl.lit("copies/l wastewater").alias("pcr_target_units"),
            pl.lit("sars-cov-2").alias("pcr_target"),
            pl.col("site_level_log_ww_conc").exp().alias("pcr_target_avg_conc"),
        )
        .with_columns(
            pl.col("site").alias("lab_id"),
            pl.col("site").alias("wwtp_id"),
        )
        .with_columns(
            pl.quantile("pcr_target_avg_conc", 0.05)
            .over("state", "site")
            .alias("lod_sewage")
        )
        .rename({"state": "wwtp_jurisdiction"})
        .pipe(
            lambda df: df.with_columns(
                quality_flag=np.random.choice(
                    ["n", "y"], size=df.height, p=[1 - ww_flag_prob, ww_flag_prob]
                )
            )
        )
        .select(cs.by_name(LOC_LEVEL_NWSS_DATA_COLUMNS, require_all=False))
        .pipe(
            lambda df: pl.concat(
                [
                    df,
                    df.sample(n=5).with_columns(
                        (pl.col("pcr_target_avg_conc") + np.random.rand(5))
                        .cast(pl.Float32)
                        .alias("pcr_target_avg_conc"),
                    ),
                ]
            )
        )
    )

    nwss_site_pop = (
        nwss_etl_base.select(["wwtp_jurisdiction", "wwtp_id"])
        .unique(["wwtp_jurisdiction", "wwtp_id"])
        .group_by("wwtp_jurisdiction")
        .agg("wwtp_id")
        .join(
            get_us_loc_pop_tbl()
            .rename({"abbr": "wwtp_jurisdiction"})
            .select("wwtp_jurisdiction", "population"),
            on="wwtp_jurisdiction",
        )
        .with_columns(
            pl.when(pl.col("wwtp_jurisdiction") == "DC")
            .then(pl.concat_list([pl.col("population") * 2]))
            # Simulates nwss data in DC where pop served
            # by ww surveillance > state population
            .otherwise(
                pl.struct(["population", "wwtp_id"]).map_elements(
                    lambda x: dirichlet_integer_split(
                        x["population"], len(x["wwtp_id"]) + 1
                    )[1:],
                    pl.List(pl.Int64),
                )
            )
            .alias("population_served")
        )
        .explode("wwtp_id", "population_served")
    )

    nwss_etl = nwss_etl_base.join(nwss_site_pop, on="wwtp_id").select(
        cs.by_name(LOC_LEVEL_NWSS_DATA_COLUMNS)
    )

    nwss_etl_dir = Path(
        private_data_dir, "nwss_vintages", f"NWSS-ETL-covid-{max_train_date}"
    )
    nwss_etl_dir.mkdir(parents=True, exist_ok=True)
    nwss_etl.filter(pl.col("sample_collect_date") <= max_train_date).write_parquet(
        Path(nwss_etl_dir, "bronze.parquet")
    )

    # Generate NHSN data
    nhsn_data_states = (
        dfs["observed_hospital_admissions"]
        .with_columns(
            (
                pl.lit(max_train_date)
                + pl.duration(
                    weeks=(pl.col("time") - pl.col("time").max() + n_forecast_weeks)
                )
            ).alias("weekendingdate")
        )
        .rename(
            {
                "state": "jurisdiction",
                "observed_hospital_admissions": "hospital_admissions",
            }
        )
        .select("disease", cs.by_name(NHSN_COLS))
    )

    # Create US data by summing across jurisdictions
    nhsn_data_us = (
        nhsn_data_states.group_by(["disease", "weekendingdate"])
        .agg(pl.col("hospital_admissions").sum())
        .with_columns(pl.lit("US").alias("jurisdiction"))
        .select("disease", cs.by_name(NHSN_COLS))
    )

    # Combine with state data
    nhsn_data_combined = pl.concat([nhsn_data_states, nhsn_data_us]).sort(
        "disease", "jurisdiction", "weekendingdate"
    )

    # Create directory and save NHSN data
    nhsn_dir = Path(private_data_dir, "nhsn_test_data")
    nhsn_dir.mkdir(parents=True, exist_ok=True)

    for name, data in nhsn_data_combined.group_by("disease", "jurisdiction"):
        data.select(cs.by_name(NHSN_COLS)).write_parquet(
            Path(nhsn_dir, f"{name[0]}_{name[1]}.parquet")
        )

    print(f"Successfully generated test data in {private_data_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create fit data for disease modeling."
    )
    parser.add_argument(
        "base_dir",
        type=Path,
        help="Base directory for output data.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        default=False,
        help="Remove bootstrap_private_data_dir after simulation",
    )
    np.random.seed(3)
    args = parser.parse_args()
    base_dir = args.base_dir
    clean = args.clean
    main(base_dir, clean)
