import json
import logging
from typing import TYPE_CHECKING

import polars as pl
from cfa.stf.forecasttools import write_tabular

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
        source_frames.append(nssp_data)

    if nhsn_data is not None:
        source_frames.append(
            nhsn_data.rename({"value": ".value"}).with_columns(
                pl.lit("observed_hospital_admissions").alias(".variable"),
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
        .with_columns(pl.lit(disease).alias("disease"))
        .sort(["date", "geo_value", ".variable"])
        .select(
            [
                "date",
                "geo_value",
                "disease",
                ".variable",
                ".value",
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

    nssp_training_data = (
        forecast_run.nssp.data.filter(pl.col("data_type") == "train")
        .pivot(
            on=".variable",
            index=["date", "state_abb", "data_type", "resolution"],
            values=".value",
        )
        .drop("resolution", "data_type")
        .rename({"state_abb": "geo_value"})
        if forecast_run.nssp is not None
        else None
    )
    nhsn_training_data = (
        forecast_run.nhsn.data.filter(pl.col("data_type") == "train")
        .drop("resolution", "data_type")
        .rename(
            {
                "date": "weekendingdate",
                "state_abb": "jurisdiction",
                "value": "hospital_admissions",
            }
        )
        if forecast_run.nhsn is not None
        else None
    )

    data_for_model_fit = {
        "loc_pop": forecast_run.loc_pop,
        "right_truncation_offset": forecast_run.right_truncation_offset,
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
        "nhsn_step_size": (
            forecast_run.nhsn.step_size if forecast_run.nhsn is not None else None
        ),
        "nssp_step_size": (
            forecast_run.nssp.step_size if forecast_run.nssp is not None else None
        ),
    }

    with open(save_dir / "data_for_model_fit.json", "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    combined_data = combine_surveillance_data(
        disease=forecast_run.disease,
        nssp_data=(forecast_run.nssp.data if forecast_run.nssp is not None else None),
        nhsn_data=(forecast_run.nhsn.data if forecast_run.nhsn is not None else None),
    )
    if forecast_run.nssp is not None:
        combined_data = append_prop_ed_data(combined_data)

    logger.info(f"Saving {forecast_run.loc} to {save_dir}")

    write_tabular(combined_data, save_dir / "combined_data.tsv")
