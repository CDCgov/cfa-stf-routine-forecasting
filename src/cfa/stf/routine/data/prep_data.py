import json
import logging
from typing import TYPE_CHECKING

import polars as pl
import polars.selectors as cs

from cfa.stf.routine.utils.prop_utils import append_prop_ed_data

if TYPE_CHECKING:
    from cfa.stf.routine.forecast_run import ForecastRun


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
            )
        )

    if nhsn_data is not None:
        source_frames.append(
            nhsn_data.select(
                cs.exclude("value"),
                pl.lit("observed_hospital_admissions").alias(".variable"),
                pl.col("value").alias(".value"),
            )
        )

    if not source_frames:
        raise ValueError("At least one surveillance data source is required")

    return (
        pl.concat(
            source_frames,
            how="diagonal_relaxed",
        )
        .rename({"state_abb": "geo_value"})
        .with_columns(
            pl.lit(disease).alias("disease"),
            pl.lit(None).alias("lab_site_index"),
        )
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


def serialize_data(
    forecast_run: "ForecastRun",
    logger: logging.Logger | None = None,
) -> None:
    logger = logger or logging.getLogger(__name__)
    save_dir = forecast_run.data_dir

    save_dir.mkdir(parents=True, exist_ok=True)

    nssp = forecast_run.nssp
    nssp_data = nssp.data if nssp is not None else None
    nhsn = forecast_run.nhsn
    nhsn_data = nhsn.data if nhsn is not None else None

    nssp_training_data = (
        nssp_data.filter(pl.col("data_type") == "train")
        .drop("resolution", "data_type")
        .rename({"state_abb": "geo_value"})
        if nssp_data is not None
        else None
    )
    nhsn_training_data = (
        nhsn_data.filter(pl.col("data_type") == "train")
        .drop("resolution", "data_type")
        .rename(
            {
                "date": "weekendingdate",
                "state_abb": "jurisdiction",
                "value": "hospital_admissions",
            }
        )
        if nhsn_data is not None
        else None
    )

    data_for_model_fit = {
        "loc_pop": forecast_run.loc_pop,
        "right_truncation_offset": forecast_run.right_truncation_offset,
        "nwss_training_data": None,
        "nssp_training_data": (
            nssp_training_data.to_dict(as_series=False)
            if nssp_training_data is not None
            else None
        ),
        "nhsn_training_data": (
            nhsn_training_data.to_dict(as_series=False)
            if nhsn_training_data is not None
            else None
        ),
        "nhsn_step_size": nhsn.step_size if nhsn is not None else 7,
        "nssp_step_size": nssp.step_size if nssp is not None else 1,
        "nwss_step_size": 1,
    }

    with open(save_dir / "data_for_model_fit.json", "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    combined_data = combine_surveillance_data(
        disease=forecast_run.disease,
        nssp_data=nssp_data,
        nhsn_data=nhsn_data,
    )
    if nssp_data is not None:
        combined_data = append_prop_ed_data(combined_data)

    logger.info("Saving %s to %s", forecast_run.loc, save_dir)

    combined_data.write_csv(save_dir / "combined_data.tsv", separator="\t")
