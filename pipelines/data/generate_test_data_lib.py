"""
Library functions for generating test data for disease modeling from `pyrenew`.

This module contains functions and constants for creating
synthetic test data, including facility-level and location-level NSSP data,
NWSS data, and NHSN data.
"""

import datetime as dt
import itertools
import json
import shutil
from pathlib import Path

import arviz as az
import jax.random as jr
import numpy as np
import polars as pl
import polars.selectors as cs
from jax._src.typing import Array
from pyrenew_multisignal.hew import PyrenewHEWData
from scipy.stats import expon, norm

from cfa.stf.forecasttools import get_us_loc_pop_tbl
from pipelines.data.prep_data import (
    process_and_save_loc_data,
    process_and_save_loc_param,
)
from pipelines.data.prep_ww_data import clean_nwss_data, preprocess_ww_data
from pipelines.utils.common_utils import build_pyrenew_hew_model_from_dir

FACILITY_LEVEL_NSSP_DATA_COLS = [
    "reference_date",
    "report_date",
    "geo_type",
    "geo_value",
    "asof",
    "metric",
    "run_id",
    "facility",
    "disease",
    "value",
]

LOC_LEVEL_NSSP_DATA_COLS = [
    "reference_date",
    "report_date",
    "geo_type",
    "geo_value",
    "metric",
    "disease",
    "value",
    "any_update_this_day",
]

LOC_LEVEL_NWSS_DATA_COLUMNS = [
    "sample_collect_date",
    "lab_id",
    "wwtp_id",
    "pcr_target_avg_conc",
    "sample_location",
    "sample_matrix",
    "pcr_target_units",
    "pcr_target",
    "wwtp_jurisdiction",
    "population_served",
    "quality_flag",
    "lod_sewage",
]

PARAM_ESTIMATES_COLS = [
    "id",
    "start_date",
    "end_date",
    "reference_date",
    "disease",
    "format",
    "parameter",
    "geo_value",
    "value",
]

NHSN_COLS = ["jurisdiction", "weekendingdate", "hospital_admissions"]

PREDICTIVE_VAR_NAMES = [
    "observed_ed_visits",
    "observed_hospital_admissions",
    "site_level_log_ww_conc",
]


def dirichlet_integer_split(n: int, k: int, alpha: float = 1.0) -> np.ndarray:
    """
    Split an integer n into k parts using Dirichlet distribution.

    Args:
        n: Integer to split
        k: Number of parts
        alpha: Dirichlet concentration parameter

    Returns:
        Array of k integers that sum to n
    """
    proportions = np.random.dirichlet(np.full(k, alpha))
    scaled = proportions * n
    counts = np.floor(scaled).astype(int)

    remainder = n - counts.sum()
    if remainder > 0:
        frac_parts = scaled - counts
        indices = np.argpartition(-frac_parts, remainder)[:remainder]
        counts[indices] += 1

    return counts


def create_var_df(
    idata: az.InferenceData, var: str, state_disease_key: pl.DataFrame
) -> pl.DataFrame:
    """Create a DataFrame from InferenceData for a specific variable.

    Args:
        idata: ArviZ InferenceData object
        var: Variable name to extract
        state_disease_key: DataFrame mapping draws to state/disease combinations

    Returns:
        Polars DataFrame with variable data
    """
    df = (
        pl.from_pandas(
            idata.prior[var].to_dataframe(),
            include_index=True,
        )
        .join(state_disease_key, on="draw")
        .select(cs.exclude("draw", "chain"))
    )

    dim_0_col = f"{var}_dim_0"
    dim_1_col = f"{var}_dim_1"

    rename_dict = {}

    if dim_0_col in df.columns:
        rename_dict[dim_0_col] = "time"
    if dim_1_col in df.columns:
        rename_dict[dim_1_col] = "site"

    renamed_df = df.select(
        "state",
        "disease",
        var,
        cs.by_name([dim_0_col, dim_1_col], require_all=False),
    ).rename(rename_dict)
    return renamed_df


def create_param_estimates(
    gi_pmf: np.ndarray,
    rt_truncation_pmf: np.ndarray,
    delay_pmf: np.ndarray,
    states_to_simulate: list[str],
    diseases_to_simulate: list[str],
    max_train_date_str: str,
    max_train_date: dt.date,
) -> pl.DataFrame:
    """
    Create parameter estimates DataFrame.

    Args:
        gi_pmf: Generation interval PMF
        rt_truncation_pmf: Right truncation PMF
        delay_pmf: Delay PMF
        states_to_simulate: List of state abbreviations
        diseases_to_simulate: List of disease names
        max_train_date_str: Maximum training date as string
        max_train_date: Maximum training date as date object

    Returns:
        Polars DataFrame with parameter estimates
    """
    return (
        (
            pl.DataFrame(
                {
                    "parameter": [
                        "generation_interval",
                        "right_truncation",
                        "delay",
                    ],
                    "value": [gi_pmf, rt_truncation_pmf, delay_pmf],
                }
            )
            .join(
                pl.DataFrame(
                    {
                        "geo_value": states_to_simulate + ["US"],
                        "parameter": "right_truncation",
                    }
                ),
                on="parameter",
                how="left",
            )
            .join(pl.DataFrame({"disease": diseases_to_simulate}), how="cross")
        )
        .with_columns(
            pl.lit("PMF").alias("format"),
            pl.lit(max_train_date_str).alias("reference_date"),
            pl.lit(None).cast(pl.Date).alias("end_date"),
            pl.lit(max_train_date).alias("start_date") - pl.duration(days=180),
        )
        .with_row_index("id")
        .select(cs.by_name(PARAM_ESTIMATES_COLS))
    )


def simulate_data_from_bootstrap(
    n_training_days: int,
    max_train_date: dt.date,
    n_nssp_sites: int,
    n_training_weeks: int,
    bootstrap_private_data_dir: Path,
    param_estimates: pl.DataFrame,
    n_forecast_days: int,
    n_ww_sites: int,
    states_to_simulate: list[str],
    diseases_to_simulate: list[str],
    clean: bool = True,
    rng_key: Array = jr.key(123),
    priors_source_path: Path = Path("pipelines/priors/prod_priors.py"),
) -> dict[str, pl.DataFrame]:
    """
    Simulate data from bootstrap model.

    This function creates bootstrap data structures, builds a PyRenew model,
    runs prior predictive sampling, and returns the simulated data as parquet DataFrames.
    Optionally, it populates the bootstrap directory with the prior predictive values also, but
    default behavior is to clean up the directory after simulation.

    Args:
        n_training_days: Number of training days
        max_train_date: Maximum training date
        n_nssp_sites: Number of NSSP sites
        n_training_weeks: Number of training weeks
        bootstrap_private_data_dir: Directory for bootstrap data
        param_estimates: Parameter estimates DataFrame
        n_forecast_days: Number of forecast days
        n_ww_sites: Number of wastewater sites
        states_to_simulate: List of state abbreviations
        diseases_to_simulate: List of disease names
        clean: If True, remove bootstrap_private_data_dir after simulation,
                otherwise update JSON and TSV files with prior predictive values

    Returns:
        Dictionary mapping variable names to DataFrames with simulated data
    """
    bootstrap_loc = states_to_simulate[0]
    bootstrap_disease = diseases_to_simulate[0]

    # facility_level_nssp_data
    bootstrap_facility_level_nssp_data = (
        pl.DataFrame(
            itertools.product(
                np.arange(-n_training_days, 0 + 1),
                np.arange(1, n_nssp_sites + 1),
                [bootstrap_disease] + ["Total"],
            ),
            schema=["time", "facility", "disease"],
        )
        .with_columns(
            (pl.lit(max_train_date) + pl.duration(days=pl.col("time"))).alias(
                "reference_date"
            ),
            pl.lit(max_train_date).alias("report_date"),
            pl.lit("state").alias("geo_type"),
            pl.lit(bootstrap_loc).alias("geo_value"),
            pl.lit(max_train_date).alias("asof"),
            pl.lit("count_ed_visits").alias("metric"),
            pl.lit(0).alias("run_id"),
            pl.lit(0).alias("value"),
        )
        .select(cs.by_name(FACILITY_LEVEL_NSSP_DATA_COLS))
    )

    # loc_level_nssp_data
    bootstrap_loc_level_nssp_data = (
        bootstrap_facility_level_nssp_data.with_columns(
            pl.lit(True).alias("any_update_this_day")
        )
        .select(cs.by_name(LOC_LEVEL_NSSP_DATA_COLS))
        .unique()
    )

    first_training_date = bootstrap_loc_level_nssp_data.get_column(
        "reference_date"
    ).min()

    # loc_level_nwss_data
    bootstrap_nwss_etl_base = (
        pl.DataFrame(
            itertools.product(
                np.arange(-n_training_days, 0 + 1), np.arange(n_ww_sites)
            ),
            schema=["time", "site"],
        )
        .with_columns(
            (
                pl.lit(max_train_date)
                + pl.duration(days=(pl.col("time") - pl.col("time").max()))
            ).alias("sample_collect_date"),
            pl.lit(bootstrap_loc).alias("state"),
            pl.lit("wwtp").alias("sample_location"),
            pl.lit("raw wastewater").alias("sample_matrix"),
            pl.lit("copies/l wastewater").alias("pcr_target_units"),
            pl.lit("sars-cov-2").alias("pcr_target"),
            pl.lit(0).alias("site_level_log_ww_conc"),
            pl.lit("n").alias("quality_flag"),
        )
        .with_columns(
            pl.col("site_level_log_ww_conc").exp().alias("pcr_target_avg_conc"),
            pl.col("site").alias("lab_id"),
            pl.col("site").alias("wwtp_id"),
        )
        .with_columns(
            pl.quantile("pcr_target_avg_conc", 0.05)
            .over("state", "site")
            .alias("lod_sewage")
        )
        .rename({"state": "wwtp_jurisdiction"})
        .select(cs.by_name(LOC_LEVEL_NWSS_DATA_COLUMNS, require_all=False))
    )

    bootstrap_nwss_site_pop = (
        bootstrap_nwss_etl_base.select(["wwtp_jurisdiction", "wwtp_id"])
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
            pl.struct(["population", "wwtp_id"])
            .map_elements(
                lambda x: dirichlet_integer_split(
                    x["population"], len(x["wwtp_id"]) + 1
                )[1:],
                pl.List(pl.Int64),
            )
            .alias("population_served")
        )
        .explode("wwtp_id", "population_served")
    )

    bootstrap_loc_level_nwss_data = bootstrap_nwss_etl_base.join(
        bootstrap_nwss_site_pop, on="wwtp_id"
    ).select(cs.by_name(LOC_LEVEL_NWSS_DATA_COLUMNS))

    bootstrap_loc_level_nwss_data = preprocess_ww_data(
        clean_nwss_data(bootstrap_loc_level_nwss_data).filter(
            (pl.col("location") == bootstrap_loc)
            & (pl.col("date") >= first_training_date)
        )
    )

    # nhsn_data_path
    bootstrap_nhsn_data = (
        pl.DataFrame(
            {
                "jurisdiction": bootstrap_loc,
                "time": np.arange(-n_training_weeks, 0 + 1),
                "hospital_admissions": 0,
            }
        )
        .with_columns(
            (pl.lit(max_train_date) + pl.duration(weeks=pl.col("time"))).alias(
                "weekendingdate"
            )
        )
        .select(cs.by_name(NHSN_COLS))
    )

    bootstrap_nhsn_data_path = Path(bootstrap_private_data_dir, "nhsn_data.parquet")
    bootstrap_nhsn_data.write_parquet(bootstrap_nhsn_data_path)

    model_run_dir = Path(bootstrap_private_data_dir, bootstrap_loc)
    model_run_dir.mkdir(parents=True, exist_ok=True)

    process_and_save_loc_data(
        loc_abb=bootstrap_loc,
        disease=bootstrap_disease,
        facility_level_nssp_data=bootstrap_facility_level_nssp_data.lazy(),
        loc_level_nwss_data=bootstrap_loc_level_nwss_data,
        report_date=max_train_date,
        first_training_date=first_training_date,
        last_training_date=max_train_date,
        save_dir=model_run_dir / "data",
        nhsn_data_path=bootstrap_nhsn_data_path,
    )

    shutil.copy(
        Path(priors_source_path),
        Path(model_run_dir, "priors.py"),
    )

    process_and_save_loc_param(
        loc_abb=bootstrap_loc,
        disease=bootstrap_disease,
        loc_level_nwss_data=bootstrap_loc_level_nwss_data,
        param_estimates=param_estimates,
        fit_ed_visits=True,
        save_dir=model_run_dir / "data",
    )

    my_data = PyrenewHEWData.from_json(
        json_file_path=Path(model_run_dir) / "data" / "data_for_model_fit.json",
        fit_ed_visits=True,
        fit_hospital_admissions=True,
        fit_wastewater=True,
    )

    my_model = build_pyrenew_hew_model_from_dir(
        model_run_dir,
        fit_ed_visits=True,
        fit_hospital_admissions=True,
        fit_wastewater=True,
    )

    state_disease_key = pl.DataFrame(
        itertools.product(states_to_simulate, diseases_to_simulate),
        schema=["state", "disease"],
    ).with_row_index("draw")

    max_draw = state_disease_key.height

    prior_predictive_samples = my_model.prior_predictive(
        rng_key=rng_key,
        numpyro_predictive_args={"num_samples": max_draw},
        data=my_data.to_forecast_data(n_forecast_points=n_forecast_days),
        sample_ed_visits=True,
        sample_hospital_admissions=True,
        sample_wastewater=True,
    )

    idata = az.from_numpyro(
        prior=prior_predictive_samples,
    ).sel(draw=slice(0, max_draw - 1))

    result = {
        var: create_var_df(idata, var, state_disease_key)
        for var in PREDICTIVE_VAR_NAMES
    }

    # Clean up bootstrap directory if requested
    if clean and bootstrap_private_data_dir.exists():
        shutil.rmtree(bootstrap_private_data_dir)
    else:
        # Update the JSON file with realistic prior predictive values
        json_file_path = Path(model_run_dir) / "data" / "data_for_model_fit.json"
        update_json_with_prior_predictive(
            json_file_path=json_file_path,
            idata=idata,
            state_disease_key=state_disease_key,
            bootstrap_loc=bootstrap_loc,
            bootstrap_disease=bootstrap_disease,
        )
        # Update the TSV file with realistic prior predictive values
        tsv_file_path = Path(model_run_dir) / "data" / "combined_data.tsv"
        update_tsv_with_prior_predictive(
            tsv_file_path=tsv_file_path,
            idata=idata,
            state_disease_key=state_disease_key,
            bootstrap_loc=bootstrap_loc,
            bootstrap_disease=bootstrap_disease,
        )

    return result


def create_default_param_estimates(
    states_to_simulate: list[str],
    diseases_to_simulate: list[str],
    max_train_date_str: str,
    max_train_date: dt.date,
) -> pl.DataFrame:
    """Create parameter estimates with default PMF values.

    Args:
        states_to_simulate: List of state abbreviations
        diseases_to_simulate: List of disease names
        max_train_date_str: Maximum training date as string
        max_train_date: Maximum training date as date object

    Returns:
        Polars DataFrame with parameter estimates
    """
    # GI PMF: Exponential on discrete times from 0.5 to 6.5
    gi_support = np.arange(0.5, 7.0)
    gi_pmf = expon.pdf(gi_support)
    gi_pmf = gi_pmf / gi_pmf.sum()

    # Delay PMF: Normal on log-transformed support, normalized and prepended with 0
    delay_support = np.log(np.arange(1, 12))
    delay_pmf = norm.pdf(delay_support, loc=np.log(3), scale=0.5)
    delay_pmf = delay_pmf / delay_pmf.sum()
    delay_pmf = np.insert(delay_pmf, 0, 0)

    # RT Truncation PMF
    rt_truncation_pmf = np.array([1.0, 0, 0, 0])

    return create_param_estimates(
        gi_pmf,
        rt_truncation_pmf,
        delay_pmf,
        states_to_simulate,
        diseases_to_simulate,
        max_train_date_str,
        max_train_date,
    )


def update_json_with_prior_predictive(
    json_file_path: Path,
    idata: az.InferenceData,
    state_disease_key: pl.DataFrame,
    bootstrap_loc: str,
    bootstrap_disease: str,
) -> None:
    """
    Update JSON file with values from prior predictive sampling.

    Args:
        json_file_path: Path to the JSON file to update
        idata: ArviZ InferenceData containing prior predictive samples
        state_disease_key: DataFrame mapping draws to state/disease combinations
        bootstrap_loc: State abbreviation used for bootstrap
        bootstrap_disease: Disease name used for bootstrap

    This function reads the JSON file, extracts realistic values from the prior
    predictive samples for the bootstrap location/disease, and writes them back
    to the JSON file.
    """
    # Read the existing JSON
    with open(json_file_path) as f:
        data = json.load(f)

    # Find the draw index for the bootstrap location and disease
    bootstrap_draw = state_disease_key.filter(
        (pl.col("state") == bootstrap_loc) & (pl.col("disease") == bootstrap_disease)
    )["draw"].to_list()[0]

    # Update hospital admissions if present
    if (
        "nhsn_training_data" in data
        and "hospital_admissions" in data["nhsn_training_data"]
    ):
        hosp_samples = idata.prior["observed_hospital_admissions"].values[
            0, bootstrap_draw, :
        ]
        data["nhsn_training_data"]["hospital_admissions"] = [
            int(x.item()) for x in hosp_samples
        ]

    # Update ED visits if present (note: key is 'observed_ed_visits', not 'total')
    if (
        "nssp_training_data" in data
        and "observed_ed_visits" in data["nssp_training_data"]
    ):
        ed_samples = idata.prior["observed_ed_visits"].values[0, bootstrap_draw, :]
        data["nssp_training_data"]["observed_ed_visits"] = [
            int(x.item()) for x in ed_samples
        ]

    # Update wastewater if present (note: key is 'nwss_training_data', not 'ww_training_data')
    if (
        "nwss_training_data" in data
        and "log_genome_copies_per_ml" in data["nwss_training_data"]
    ):
        # The wastewater data is stored as a flat list, not nested
        # We need to figure out the shape from the data
        n_ww_values = len(data["nwss_training_data"]["log_genome_copies_per_ml"])

        # Get wastewater samples from prior predictive
        # Shape is (chain, draw, time, site)
        ww_samples = idata.prior["site_level_log_ww_conc"].values[
            0, bootstrap_draw, :, :
        ]
        n_times, n_sites = ww_samples.shape

        # Flatten in the same order as the original data (site-major order based on the site list)
        # The JSON appears to be in site-major order based on the repeating pattern
        ww_flat = []
        for s in range(n_sites):
            for t in range(n_times):
                ww_flat.append(float(ww_samples[t, s]))

        # Only use as many values as exist in the original data
        data["nwss_training_data"]["log_genome_copies_per_ml"] = ww_flat[:n_ww_values]

    # Write the updated JSON back
    with open(json_file_path, "w") as f:
        json.dump(data, f, indent=2)


def update_tsv_with_prior_predictive(
    tsv_file_path: Path,
    idata: az.InferenceData,
    state_disease_key: pl.DataFrame,
    bootstrap_loc: str,
    bootstrap_disease: str,
) -> None:
    """Update TSV file with realistic values from prior predictive sampling.

    Args:
        tsv_file_path: Path to the combined_data.tsv file to update
        idata: ArviZ InferenceData containing prior predictive samples
        state_disease_key: DataFrame mapping draws to state/disease combinations
        bootstrap_loc: State abbreviation used for bootstrap
        bootstrap_disease: Disease name used for bootstrap

    This function reads the TSV file, extracts realistic values from the prior
    predictive samples for the bootstrap location/disease, and writes them back
    to the TSV file.
    """
    # Read the existing TSV
    df = pl.read_csv(tsv_file_path, separator="\t")

    # Find the draw index for the bootstrap location and disease
    bootstrap_draw = state_disease_key.filter(
        (pl.col("state") == bootstrap_loc) & (pl.col("disease") == bootstrap_disease)
    )["draw"].to_list()[0]

    # Get the prior predictive samples
    hosp_samples = idata.prior["observed_hospital_admissions"].values[
        0, bootstrap_draw, :
    ]
    ed_samples = idata.prior["observed_ed_visits"].values[0, bootstrap_draw, :]
    ww_samples = idata.prior["site_level_log_ww_conc"].values[0, bootstrap_draw, :, :]

    # Get date information from the TSV
    dates_df = (
        df.filter(pl.col(".variable") == "observed_ed_visits")
        .select("date")
        .unique()
        .sort("date")
    )
    dates = dates_df.get_column("date").to_list()

    # Update ED visits (daily data)
    for i, date in enumerate(dates):
        if i < len(ed_samples):
            df = df.with_columns(
                pl.when(
                    (pl.col("date") == date)
                    & (pl.col(".variable") == "observed_ed_visits")
                )
                .then(float(ed_samples[i].item()))
                .otherwise(pl.col(".value"))
                .alias(".value")
            )

    # Generate other_ed_visits from Poisson with mean = 10 * max(observed_ed_visits)
    max_ed = float(ed_samples.max())
    poisson_mean = 10 * max_ed
    other_ed_samples = np.random.poisson(lam=poisson_mean, size=len(dates))

    # Update other_ed_visits (daily data)
    for i, date in enumerate(dates):
        if i < len(other_ed_samples):
            df = df.with_columns(
                pl.when(
                    (pl.col("date") == date)
                    & (pl.col(".variable") == "other_ed_visits")
                )
                .then(float(other_ed_samples[i]))
                .otherwise(pl.col(".value"))
                .alias(".value")
            )

    # Update hospital admissions (weekly data - only on Saturdays)
    # Need to map weekly samples to the correct dates
    hosp_dates = (
        df.filter(pl.col(".variable") == "observed_hospital_admissions")
        .select("date")
        .unique()
        .sort("date")
    )
    hosp_dates_list = hosp_dates.get_column("date").to_list()
    for i, date in enumerate(hosp_dates_list):
        if i < len(hosp_samples):
            df = df.with_columns(
                pl.when(
                    (pl.col("date") == date)
                    & (pl.col(".variable") == "observed_hospital_admissions")
                )
                .then(float(hosp_samples[i]))
                .otherwise(pl.col(".value"))
                .alias(".value")
            )

    # Update wastewater (daily data, multiple sites)
    n_times, n_sites = ww_samples.shape

    # Create a mapping of (date, lab_site_index) to value
    for site_idx in range(n_sites):
        for time_idx, date in enumerate(dates):
            if time_idx < n_times:
                df = df.with_columns(
                    pl.when(
                        (pl.col("date") == date)
                        & (pl.col(".variable") == "site_level_log_ww_conc")
                        & (pl.col("lab_site_index") == site_idx)
                    )
                    .then(float(ww_samples[time_idx, site_idx]))
                    .otherwise(pl.col(".value"))
                    .alias(".value")
                )

    # Write the updated TSV back
    df.write_csv(tsv_file_path, separator="\t")
