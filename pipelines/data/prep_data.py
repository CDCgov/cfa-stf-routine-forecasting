import datetime as dt
import json
import logging
from pathlib import Path

import jax.numpy as jnp
import polars as pl
import polars.selectors as cs
from cfa.stf.data import (
    get_nnh_delay_pmf,
    get_nnh_generation_interval_pmf,
    get_nnh_right_truncation_pmf,
)
from cfa.stf.forecasttools import get_us_loc_pop_tbl
from pyrenew_multisignal.hew import approx_lognorm

from pipelines.data.data_access import ForecastData

_disease_map = {
    "COVID-19": "COVID-19/Omicron",
}

_inverse_disease_map = {v: k for k, v in _disease_map.items()}


def combine_surveillance_data(
    *,
    disease: str,
    nssp_data: pl.DataFrame,
    nhsn_data: pl.DataFrame,
) -> pl.DataFrame:
    nssp_data_long = nssp_data.unpivot(
        on=["observed_ed_visits", "other_ed_visits"],
        variable_name=".variable",
        index=cs.exclude(["observed_ed_visits", "other_ed_visits"]),
        value_name=".value",
    ).with_columns(pl.lit(None).alias("lab_site_index"))

    nhsn_data_long = (
        nhsn_data.rename(
            {
                "weekendingdate": "date",
                "jurisdiction": "geo_value",
                "hospital_admissions": "observed_hospital_admissions",
            }
        )
        .unpivot(
            on="observed_hospital_admissions",
            index=cs.exclude("observed_hospital_admissions"),
            variable_name=".variable",
            value_name=".value",
        )
        .with_columns(pl.lit(None).alias("lab_site_index"))
    )

    return (
        pl.concat(
            [nssp_data_long, nhsn_data_long],
            how="diagonal_relaxed",
        )
        .with_columns(pl.lit(disease).alias("disease"))
        .sort(["date", "geo_value", ".variable"])
        .select(
            [
                "date",
                "geo_value",
                "disease",
                ".variable",
                ".value",
                "lab_site_index",
                "resolution",
                "data_type",
            ]
        )
    )


def aggregate_nssp_to_national(
    data: pl.LazyFrame,
    geo_values_to_include: pl.Series | list[str],
    first_date_to_include: dt.date,
    national_geo_value="US",
):
    assert national_geo_value not in geo_values_to_include
    return (
        data.filter(
            pl.col("geo_value").is_in(geo_values_to_include),
            pl.col("reference_date") >= first_date_to_include,
        )
        .group_by(["disease", "metric", "geo_type", "reference_date"])
        .agg(geo_value=pl.lit(national_geo_value), value=pl.col("value").sum())
    )


# not currently used, but could be used for processing latest_comprehensive
def process_loc_level_nssp_data(
    loc_level_nssp_data: pl.LazyFrame,
    loc_abb: str,
    disease: str,
    first_training_date: dt.date,
    loc_pop_df: pl.DataFrame,
) -> pl.DataFrame:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if loc_level_nssp_data is None:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "geo_value": pl.Utf8,
                "disease": pl.Utf8,
                "ed_visits": pl.Float64,
            }
        )

    disease_key = _disease_map.get(disease, disease)

    if loc_abb == "US":
        locations_to_aggregate = (
            loc_pop_df.filter(pl.col("abbr") != "US")
            .get_column("abbr")
            .unique()
            .to_list()
        )
        logger.info("Aggregating state-level data to national")
        loc_level_nssp_data = aggregate_nssp_to_national(
            loc_level_nssp_data,
            locations_to_aggregate,
            first_training_date,
            national_geo_value="US",
        )

    return (
        loc_level_nssp_data.filter(
            pl.col("disease").is_in([disease_key, "Total"]),
            pl.col("metric") == "count_ed_visits",
            pl.col("geo_value") == loc_abb,
            pl.col("geo_type") == "state",
            pl.col("reference_date") >= first_training_date,
        )
        .select(
            [
                pl.col("reference_date").alias("date"),
                pl.col("geo_value").cast(pl.Utf8),
                pl.col("disease").cast(pl.Utf8),
                pl.col("value").alias("ed_visits"),
            ]
        )
        .with_columns(
            disease=pl.col("disease").cast(pl.Utf8).replace(_inverse_disease_map),
        )
        .sort(["date", "disease"])
        .collect()
    )


def aggregate_facility_level_nssp_to_loc(
    facility_level_nssp_data: pl.LazyFrame,
    loc_abb: str,
    disease: str,
    first_training_date: dt.date,
    loc_pop_df: pl.DataFrame,
) -> pl.DataFrame:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if facility_level_nssp_data is None:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "geo_value": pl.Utf8,
                "disease": pl.Utf8,
                "ed_visits": pl.Float64,
            }
        )

    disease_key = _disease_map.get(disease, disease)

    if loc_abb == "US":
        logger.info("Aggregating facility-level data to national")
        locations_to_aggregate = (
            loc_pop_df.filter(pl.col("abbr") != "US").get_column("abbr").unique()
        )
        facility_level_nssp_data = aggregate_nssp_to_national(
            facility_level_nssp_data,
            locations_to_aggregate,
            first_training_date,
            national_geo_value="US",
        )

    return (
        facility_level_nssp_data.filter(
            pl.col("disease").is_in([disease_key, "Total"]),
            pl.col("metric") == "count_ed_visits",
            pl.col("geo_value") == loc_abb,
            pl.col("reference_date") >= first_training_date,
        )
        .group_by(["reference_date", "disease"])
        .agg(pl.col("value").sum().alias("ed_visits"))
        .with_columns(
            disease=pl.col("disease").cast(pl.Utf8).replace(_inverse_disease_map),
            geo_value=pl.lit(loc_abb).cast(pl.Utf8),
        )
        .rename({"reference_date": "date"})
        .sort(["date", "disease"])
        .select(["date", "geo_value", "disease", "ed_visits"])
        .collect()
    )


def process_and_save_loc_data(
    forecast_data: ForecastData,
    save_dir: Path,
    logger: logging.Logger | None = None,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logger or logging.getLogger(__name__)

    Path(save_dir).mkdir(parents=True, exist_ok=True)

    nssp_training_data = forecast_data.nssp.data.filter(pl.col("data_type") == "train")
    nhsn_training_data = forecast_data.nhsn.data.filter(pl.col("data_type") == "train")

    data_for_model_fit = {
        "loc_pop": forecast_data.loc_pop,
        "right_truncation_offset": forecast_data.right_truncation_offset,
        "nwss_training_data": None,
        "nssp_training_data": nssp_training_data.drop("resolution").to_dict(
            as_series=False
        ),
        "nhsn_training_data": nhsn_training_data.drop("resolution").to_dict(
            as_series=False
        ),
        "nhsn_step_size": 7,
        "nssp_step_size": 1,
        "nwss_step_size": 1,
    }

    with open(Path(save_dir, "data_for_model_fit.json"), "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    combined_data = combine_surveillance_data(
        disease=forecast_data.disease,
        nssp_data=forecast_data.nssp.data,
        nhsn_data=forecast_data.nhsn.data,
    )

    logger.info(f"Saving {forecast_data.loc_abb} to {save_dir}")

    combined_data.write_csv(Path(save_dir, "combined_data.tsv"), separator="\t")
    return None


def process_and_save_loc_param(
    loc_abb,
    disease,
    fit_ed_visits,
    save_dir,
    as_of: dt.date | None = None,
) -> None:
    loc_pop_df = get_us_loc_pop_tbl()
    loc_pop = loc_pop_df.filter(pl.col("abbr") == loc_abb).item(0, "population")
    pop_fraction = jnp.array([1])

    generation_interval_pmf = get_nnh_generation_interval_pmf(
        disease=disease,
        as_of=as_of,
    )
    delay_pmf = get_nnh_delay_pmf(disease=disease, as_of=as_of)
    # We do not model a zero infection-to-recorded-admission delay.
    delay_pmf[0] = 0.0
    delay_pmf = jnp.array(delay_pmf)
    delay_pmf = (delay_pmf / delay_pmf.sum()).tolist()

    try:
        right_truncation_pmf = get_nnh_right_truncation_pmf(
            loc_abb=loc_abb,
            disease=disease,
            as_of=as_of,
            reference_date=as_of,
        )
    except ValueError:
        if fit_ed_visits:
            raise
        right_truncation_pmf = [1]

    inf_to_hosp_admit_lognormal_loc, inf_to_hosp_admit_lognormal_scale = approx_lognorm(
        jnp.array(delay_pmf)[1:],  # only fit the non-zero delays
        loc_guess=0,
        scale_guess=0.5,
    )

    model_params = {
        "population_size": loc_pop,
        "pop_fraction": pop_fraction.tolist(),
        "generation_interval_pmf": generation_interval_pmf,
        "right_truncation_pmf": right_truncation_pmf,
        "inf_to_hosp_admit_lognormal_loc": inf_to_hosp_admit_lognormal_loc,
        "inf_to_hosp_admit_lognormal_scale": inf_to_hosp_admit_lognormal_scale,
        "inf_to_hosp_admit_pmf": delay_pmf,
    }
    with open(Path(save_dir, "model_params.json"), "w") as json_file:
        json.dump(model_params, json_file, default=str)

    return None
