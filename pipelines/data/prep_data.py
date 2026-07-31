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


def combine_surveillance_data(
    *,
    disease: str,
    nssp_data: pl.DataFrame | None,
    nhsn_data: pl.DataFrame | None,
) -> pl.DataFrame:
    source_frames = []
    if nssp_data is not None:
        source_frames.append(
            nssp_data.unpivot(
                on=["observed_ed_visits", "other_ed_visits"],
                variable_name=".variable",
                index=cs.exclude(["observed_ed_visits", "other_ed_visits"]),
                value_name=".value",
            ).with_columns(pl.lit(None).alias("lab_site_index"))
        )

    if nhsn_data is not None:
        source_frames.append(
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

    if not source_frames:
        raise ValueError("At least one surveillance data source is required")

    return (
        pl.concat(
            source_frames,
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


def process_and_save_loc_data(
    forecast_data: ForecastData,
    save_dir: Path,
    logger: logging.Logger | None = None,
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logger or logging.getLogger(__name__)

    Path(save_dir).mkdir(parents=True, exist_ok=True)

    nssp_training_data = (
        forecast_data.nssp.data.filter(pl.col("data_type") == "train")
        if forecast_data.nssp is not None
        else None
    )
    nhsn_training_data = (
        forecast_data.nhsn.data.filter(pl.col("data_type") == "train")
        if forecast_data.nhsn is not None
        else None
    )

    data_for_model_fit = {
        "loc_pop": forecast_data.loc_pop,
        "right_truncation_offset": forecast_data.right_truncation_offset,
        "nwss_training_data": None,
        "nssp_training_data": (
            nssp_training_data.drop("resolution").to_dict(as_series=False)
            if nssp_training_data is not None
            else None
        ),
        "nhsn_training_data": (
            nhsn_training_data.drop("resolution").to_dict(as_series=False)
            if nhsn_training_data is not None
            else None
        ),
        "nhsn_step_size": 7,
        "nssp_step_size": 1,
        "nwss_step_size": 1,
    }

    with open(Path(save_dir, "data_for_model_fit.json"), "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    combined_data = combine_surveillance_data(
        disease=forecast_data.disease,
        nssp_data=forecast_data.nssp.data if forecast_data.nssp is not None else None,
        nhsn_data=forecast_data.nhsn.data if forecast_data.nhsn is not None else None,
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
