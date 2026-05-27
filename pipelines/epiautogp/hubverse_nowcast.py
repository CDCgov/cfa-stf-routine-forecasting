"""
Hubverse sample-format nowcast ingestion for EpiAutoGP.

This module knows how to read a handoff pointer, load its Hubverse sample
parquet, and reshape strictly negative-horizon sample rows into the
vector-of-vectors interface expected by NowcastAutoGP.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pipelines.epiautogp.forecast_spec import ForecastSpec
from pipelines.epiautogp.hubverse_pointer import load_hubverse_model_output_path
from pipelines.epiautogp.nowcast import NowcastData

# hewr/R/to_hubverse_tbl.R selects forecasttools::cdc_hub_std_colnames,
# plus horizon_timescale, resolution, and disease. This reader only needs
# the standard sample columns used below. HEWR writes reference_date; the
# NHSN nowcast handoff currently writes origin_date, so accept either.
HUBVERSE_SAMPLE_SCHEMA = {
    "target_end_date": pl.Date,
    "horizon": pl.Int64,
    "target": pl.Utf8,
    "location": pl.Utf8,
    "output_type": pl.Utf8,
    "output_type_id": pl.Utf8,
    "value": pl.Float64,
}
ORIGIN_DATE_COLUMNS = ("origin_date", "reference_date")
HUBVERSE_CAST_SCHEMA = {"origin_date": pl.Date, **HUBVERSE_SAMPLE_SCHEMA}
REQUIRED_CANDIDATE_FIELDS = ("target_end_date", "horizon", "output_type_id")

# Found in hewr/R/to_hubverse_tbl.R::var_to_target()
_DISEASE_TO_HUBVERSE_ABBR = {
    "COVID-19": "covid",
    "Influenza": "flu",
    "RSV": "rsv",
}


def _disease_abbr(disease: str) -> str:
    try:
        return _DISEASE_TO_HUBVERSE_ABBR[disease]
    except KeyError as exc:
        valid = ", ".join(sorted(_DISEASE_TO_HUBVERSE_ABBR))
        raise ValueError(
            f"Unsupported Hubverse nowcast disease {disease!r}; "
            f"expected one of {valid}."
        ) from exc


def _target_prefix(frequency: str) -> str:
    if frequency == "epiweekly":
        return "wk "
    if frequency == "daily":
        return ""
    raise ValueError(
        f"Unsupported Hubverse nowcast frequency {frequency!r}; "
        "expected 'daily' or 'epiweekly'."
    )


def _target_core(forecast_spec: ForecastSpec, disease_abbr: str) -> str:
    match forecast_spec.target:
        case "nhsn":
            if forecast_spec.ed_visit_type != "observed":
                raise ValueError(
                    "Hubverse nowcasts for target='nhsn' require "
                    "ed_visit_type='observed'."
                )
            return f"inc {disease_abbr} hosp"
        case "nssp":
            match forecast_spec.ed_visit_type:
                case "observed":
                    return f"inc {disease_abbr} ed visits"
                case "other":
                    return "inc other ed visits"
                case "pct":
                    return f"inc {disease_abbr} prop ed visits"
                case _:
                    raise ValueError(
                        "Hubverse nowcasts for target='nssp' require "
                        "ed_visit_type to be 'observed', 'other', or 'pct'."
                    )
        case _:
            raise ValueError(
                f"Unsupported Hubverse nowcast target {forecast_spec.target!r}; "
                "expected 'nssp' or 'nhsn'."
            )


def hubverse_nowcast_config_from_forecast_spec(
    forecast_spec: ForecastSpec,
) -> tuple[str, str]:
    """
    Infer handoff pointer disease target and Hubverse target label from a spec.

    The target naming mirrors `hewr/R/to_hubverse_tbl.R::var_to_target()`
    plus the cadence prefix added by `raw_samples_to_prelim()`.
    """
    disease_abbr = _disease_abbr(forecast_spec.disease)
    hubverse_target = _target_prefix(forecast_spec.frequency) + _target_core(
        forecast_spec, disease_abbr
    )
    return disease_abbr, hubverse_target


def _normalize_hubverse_table(df: pl.DataFrame, *, source_label: str) -> pl.DataFrame:
    origin_date_column = next(
        (column for column in ORIGIN_DATE_COLUMNS if column in df.columns),
        None,
    )
    missing = sorted(set(HUBVERSE_SAMPLE_SCHEMA) - set(df.columns))
    if origin_date_column is None:
        missing.insert(0, "origin_date/reference_date")
    if missing:
        raise ValueError(
            f"{source_label} output missing required columns: " + ", ".join(missing)
        )

    return df.select(
        pl.col(origin_date_column).alias("origin_date"),
        *HUBVERSE_SAMPLE_SCHEMA,
    ).cast(HUBVERSE_CAST_SCHEMA, strict=False)


def _validate_required_candidate_fields(
    df: pl.DataFrame,
    *,
    source_label: str,
) -> None:
    bad_fields = [
        field
        for field in REQUIRED_CANDIDATE_FIELDS
        if df.get_column(field).is_null().any()
    ]
    if bad_fields:
        raise ValueError(
            f"{source_label} output has missing or malformed values in: "
            + ", ".join(bad_fields)
        )


def _validate_nowcast_values(df: pl.DataFrame, *, source_label: str) -> None:
    if df.select(
        (
            pl.col("value").is_null()
            | pl.col("value").is_nan()
            | ~pl.col("value").is_finite()
            | (pl.col("value") < 0)
        ).any()
    ).item():
        raise ValueError(
            f"{source_label} output contains missing, non-finite, "
            "or negative predicted values."
        )


def _validate_no_duplicate_sample_dates(
    df: pl.DataFrame,
    *,
    source_label: str,
) -> None:
    if df.select(["output_type_id", "target_end_date"]).is_duplicated().any():
        raise ValueError(
            f"{source_label} output contains duplicate rows for the same sample "
            "and target_end_date."
        )


def _date_columns(dates: list[dt.date]) -> list[str]:
    return [date.isoformat() for date in dates]


def hubverse_samples_to_nowcast_data(
    df: pl.DataFrame,
    *,
    forecast_spec: ForecastSpec,
    hubverse_target: str,
    source_label: str = "Hubverse nowcast",
) -> NowcastData:
    """Convert matching Hubverse sample rows into EpiAutoGP nowcast data."""
    normalized = _normalize_hubverse_table(df, source_label=source_label)
    loc = forecast_spec.loc.lower()

    candidate_rows = normalized.filter(
        (pl.col("target") == hubverse_target)
        & (pl.col("location").str.to_lowercase() == loc)
        & (pl.col("origin_date") == forecast_spec.report_date)
        & (pl.col("output_type") == "sample")
    )
    if candidate_rows.height == 0:
        raise ValueError(
            f"No matching {source_label} sample rows for "
            f"target={hubverse_target!r}, location={forecast_spec.loc!r}, "
            f"origin_date={forecast_spec.report_date.isoformat()!r}."
        )

    _validate_required_candidate_fields(candidate_rows, source_label=source_label)

    nowcast_rows = candidate_rows.filter(pl.col("horizon") < 0)
    if nowcast_rows.height == 0:
        raise ValueError(
            f"No strictly negative-horizon {source_label} sample rows matched "
            "this EpiAutoGP run."
        )

    _validate_nowcast_values(nowcast_rows, source_label=source_label)
    _validate_no_duplicate_sample_dates(nowcast_rows, source_label=source_label)

    nowcast_dates = (
        nowcast_rows.select("target_end_date")
        .unique()
        .sort("target_end_date")
        .get_column("target_end_date")
        .to_list()
    )
    date_columns = _date_columns(nowcast_dates)
    sample_matrix = (
        nowcast_rows.select(["output_type_id", "target_end_date", "value"])
        .pivot(index="output_type_id", on="target_end_date", values="value")
        .sort("output_type_id")
        .select(["output_type_id", *date_columns])
    )
    if sample_matrix.select(
        pl.any_horizontal(pl.exclude("output_type_id").is_null()).any()
    ).item():
        raise ValueError(
            f"{source_label} output must contain the same target_end_date "
            "values for every sample."
        )

    reports = [
        [float(value) for value in row]
        for row in sample_matrix.select(date_columns).iter_rows()
    ]

    return NowcastData(dates=nowcast_dates, reports=reports)


@dataclass(frozen=True)
class HubversePointerNowcast:
    """Read Hubverse sample nowcasts through a production handoff pointer."""

    pointer_uri: str | Path
    forecast_spec: ForecastSpec
    source_label: str = "Hubverse nowcast"

    @classmethod
    def from_pointer_uri(
        cls,
        *,
        pointer_uri: str | Path,
        forecast_spec: ForecastSpec,
    ) -> HubversePointerNowcast:
        """Build a Hubverse nowcast source from a required pointer URI."""
        return cls(pointer_uri=pointer_uri, forecast_spec=forecast_spec)

    @staticmethod
    def applies_to(
        *,
        forecast_spec: ForecastSpec,
    ) -> bool:
        try:
            hubverse_nowcast_config_from_forecast_spec(forecast_spec)
        except ValueError:
            return False
        return True

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        if len(dates) != len(reports):
            raise ValueError("dates and reports must have the same length")

        pointer_disease, hubverse_target = hubverse_nowcast_config_from_forecast_spec(
            self.forecast_spec
        )
        model_output_path = load_hubverse_model_output_path(
            self.pointer_uri,
            expected_pointer_disease=pointer_disease,
            forecast_spec=self.forecast_spec,
            source_label=self.source_label,
        )
        model_output = pl.read_parquet(model_output_path)
        return hubverse_samples_to_nowcast_data(
            model_output,
            forecast_spec=self.forecast_spec,
            hubverse_target=hubverse_target,
            source_label=self.source_label,
        )
