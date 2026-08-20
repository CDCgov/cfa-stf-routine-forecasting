import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
import polars.selectors as cs

from cfa.stf.routine.data.aggregation import aggregate_long_to_epiweekly

if TYPE_CHECKING:
    from cfa.stf.routine.forecast_run import ForecastRun


NSSPFrequency = Literal["daily", "epiweekly"]


def aggregate_epiweekly_nssp(data: pl.DataFrame) -> pl.DataFrame:
    """Aggregate NSSP data using the shared forecasttools implementation."""
    value_columns = ["observed_ed_visits", "other_ed_visits"]
    required_columns = {"date", "data_type", "resolution", *value_columns}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Cannot aggregate NSSP data; missing column(s): {missing}")

    value_selector = cs.by_name(value_columns)
    grouping_selector = cs.exclude(
        value_selector,
        "date",
        "data_type",
        "resolution",
    )
    grouping_columns = list(cs.expand_selector(data, grouping_selector))
    long_data = data.unpivot(
        on=value_selector,
        index=cs.exclude(value_selector, "resolution"),
        variable_name=".variable",
        value_name=".value",
    )
    weekly_data = aggregate_long_to_epiweekly(long_data)
    if weekly_data.is_empty():
        return data.clear()

    aggregated = (
        weekly_data.pivot(
            on=".variable",
            index=["date", *grouping_columns, "data_type"],
            values=".value",
        )
        .with_columns(pl.lit("epiweekly").alias("resolution"))
        .select(data.columns)
    )
    return aggregated.sort([*grouping_columns, "date"])


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
    *,
    nssp_frequency: NSSPFrequency = "daily",
) -> None:
    logger = logger or logging.getLogger(__name__)

    Path(save_dir).mkdir(parents=True, exist_ok=True)

    nssp_training_data = (
        forecast_run.nssp.data.filter(pl.col("data_type") == "train")
        if forecast_run.nssp is not None
        else None
    )
    nhsn_training_data = (
        forecast_run.nhsn.data.filter(pl.col("data_type") == "train")
        if forecast_run.nhsn is not None
        else None
    )

    data_for_model_fit = {
        "loc_pop": forecast_run.loc_pop,
        "right_truncation_offset": forecast_run.right_truncation_offset,
        "nwss_training_data": None,
        "nssp_training_data": (
            nssp_training_data.drop("resolution", "data_type")
            .rename({"state_abb": "geo_value"})
            .to_dict(as_series=False)
            if nssp_training_data is not None
            else None
        ),
        "nhsn_training_data": (
            nhsn_training_data.drop("resolution", "data_type")
            .rename(
                {
                    "date": "weekendingdate",
                    "state_abb": "jurisdiction",
                    "value": "hospital_admissions",
                }
            )
            .to_dict(as_series=False)
            if nhsn_training_data is not None
            else None
        ),
        "nhsn_step_size": 7,
        "nssp_step_size": 1,
        "nwss_step_size": 1,
    }

    with open(Path(save_dir, "data_for_model_fit.json"), "w") as json_file:
        json.dump(data_for_model_fit, json_file, default=str)

    nssp_data = forecast_run.nssp.data if forecast_run.nssp is not None else None
    if nssp_data is not None and nssp_frequency == "epiweekly":
        nssp_data = aggregate_epiweekly_nssp(nssp_data)

    combined_data = combine_surveillance_data(
        disease=forecast_run.disease,
        nssp_data=nssp_data,
        nhsn_data=forecast_run.nhsn.data if forecast_run.nhsn is not None else None,
    )

    logger.info(f"Saving {forecast_run.loc} to {save_dir}")

    combined_data.write_csv(Path(save_dir, "combined_data.tsv"), separator="\t")
    return None
