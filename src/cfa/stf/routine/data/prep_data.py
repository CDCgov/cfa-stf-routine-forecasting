import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
import polars.selectors as cs

from cfa.stf.routine.utils.data_utils import aggregate_ed_visits_to_epiweekly
from cfa.stf.routine.utils.prop_utils import append_prop_data_to_combined_data

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
            nssp_data.rename({"state_abb": "geo_value"})
            .unpivot(
                on=["observed_ed_visits", "other_ed_visits"],
                variable_name=".variable",
                index=cs.exclude(["observed_ed_visits", "other_ed_visits"]),
                value_name=".value",
            )
            .with_columns(pl.lit(None).alias("lab_site_index"))
        )

    if nhsn_data is not None:
        source_frames.append(
            nhsn_data.rename(
                {
                    "state_abb": "geo_value",
                    "value": "observed_hospital_admissions",
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


def serialize_data(
    forecast_run: "ForecastRun",
    save_dir: Path,
    logger: logging.Logger | None = None,
    ed_visit_input_resolution: Literal["daily", "epiweekly"] = "daily",
) -> None:
    logger = logger or logging.getLogger(__name__)

    Path(save_dir).mkdir(parents=True, exist_ok=True)

    combined_data = combine_surveillance_data(
        disease=forecast_run.disease,
        nssp_data=forecast_run.nssp.data if forecast_run.nssp is not None else None,
        nhsn_data=forecast_run.nhsn.data if forecast_run.nhsn is not None else None,
    )
    if ed_visit_input_resolution == "epiweekly":
        logger.info("Aggregating ED visits to epiweekly resolution...")
        combined_data = aggregate_ed_visits_to_epiweekly(combined_data)

    training_data = combined_data.filter(pl.col("data_type") == "train")

    nssp_training_data = None
    if forecast_run.nssp is not None:
        nssp_training_data = (
            training_data.filter(
                pl.col(".variable").is_in(["observed_ed_visits", "other_ed_visits"])
            )
            .pivot(on=".variable", index=["date", "geo_value"], values=".value")
            .select(
                "date",
                "geo_value",
                "observed_ed_visits",
                "other_ed_visits",
            )
            .sort("date", "geo_value")
        )

    nhsn_training_data = None
    if forecast_run.nhsn is not None:
        nhsn_training_data = training_data.filter(
            pl.col(".variable") == "observed_hospital_admissions"
        ).select(
            pl.col("date").alias("weekendingdate"),
            pl.col("geo_value").alias("jurisdiction"),
            pl.col(".value").alias("hospital_admissions"),
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
        "nhsn_step_size": 7,
        "nssp_step_size": 7 if ed_visit_input_resolution == "epiweekly" else 1,
        "nwss_step_size": 1,
    }

    with open(Path(save_dir, "data_for_model_fit.json"), "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    combined_data = append_prop_data_to_combined_data(combined_data)

    logger.info(f"Saving {forecast_run.loc} to {save_dir}")

    combined_data.write_csv(Path(save_dir, "combined_data.tsv"), separator="\t")
    return None
