"""Hubverse sample nowcast data source."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from pipelines.data.nowcast import NowcastData

if TYPE_CHECKING:
    from pipelines.epiautogp.forecast_spec import ForecastSpec

HUBVERSE_MODEL_OUTPUT_SUBDIR = Path("model-output", "CFA-nowcastNHSN")
HUBVERSE_TARGETS = {
    "COVID-19": "wk inc covid hosp",
    "Influenza": "wk inc flu hosp",
    "RSV": "wk inc rsv hosp",
}
REQUIRED_HUBVERSE_COLUMNS = {
    "origin_date",
    "target_end_date",
    "target",
    "location",
    "output_type",
    "output_type_id",
    "value",
}


@dataclass(frozen=True)
class HubverseNowcast:
    """Read one materialized Hubverse sample artifact as nowcast trajectories."""

    containing_dir: Path
    forecast_spec: ForecastSpec

    def __post_init__(self) -> None:
        object.__setattr__(self, "containing_dir", Path(self.containing_dir))

    @staticmethod
    def ensure_applicable(*, forecast_spec: ForecastSpec) -> None:
        """Whether Hubverse NHSN sample output matches the requested model."""
        if (
            forecast_spec.target != "nhsn"
            or forecast_spec.frequency != "epiweekly"
            or forecast_spec.ed_visit_type != "observed"
        ):
            raise ValueError(
                "Hubverse nowcasting is only applicable when target='nhsn', "
                "frequency='epiweekly', and ed_visit_type='observed'; got "
                f"target={forecast_spec.target!r}, "
                f"frequency={forecast_spec.frequency!r}, and "
                f"ed_visit_type={forecast_spec.ed_visit_type!r}."
            )

    def _artifact_path(self) -> Path:
        model_output_dir = self.containing_dir / HUBVERSE_MODEL_OUTPUT_SUBDIR
        parquet_paths = (
            sorted(model_output_dir.glob("*.parquet"))
            if model_output_dir.is_dir()
            else []
        )
        if len(parquet_paths) != 1:
            raise ValueError(
                "Expected exactly one Hubverse Parquet in "
                f"{model_output_dir}, found {len(parquet_paths)}."
            )
        return parquet_paths[0]

    def _expected_target(self) -> str:
        try:
            return HUBVERSE_TARGETS[self.forecast_spec.disease]
        except KeyError as exc:
            raise ValueError(
                "No Hubverse NHSN target mapping for disease "
                f"{self.forecast_spec.disease!r}."
            ) from exc

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        """Convert Hubverse long sample rows to EpiAutoGP trajectories."""
        if len(dates) != len(reports):
            raise ValueError("dates and reports must have the same length")

        artifact_path = self._artifact_path()
        hubverse = pl.read_parquet(artifact_path)
        missing_columns = sorted(REQUIRED_HUBVERSE_COLUMNS - set(hubverse.columns))
        if missing_columns:
            raise ValueError(
                f"Hubverse Parquet is missing required columns: {missing_columns}."
            )

        hubverse = hubverse.with_columns(
            pl.col("origin_date").cast(pl.Date),
            pl.col("target_end_date").cast(pl.Date),
            pl.col("target").cast(pl.String),
            pl.col("location").cast(pl.String),
            pl.col("output_type").cast(pl.String),
            pl.col("output_type_id").cast(pl.String),
            pl.col("value").cast(pl.Float64),
        )

        if hubverse.get_column("origin_date").null_count():
            raise ValueError("Hubverse Parquet contains missing origin_date values.")
        origins = hubverse.get_column("origin_date").unique().to_list()
        expected_origin = self.forecast_spec.report_date
        if len(origins) != 1 or origins[0] != expected_origin:
            raise ValueError(
                "Hubverse Parquet must contain exactly the EpiAutoGP run vintage "
                f"{expected_origin}; found {sorted(origins)}."
            )

        expected_target = self._expected_target()
        target_rows = hubverse.filter(pl.col("target") == expected_target)
        if target_rows.is_empty():
            raise ValueError(
                f"Hubverse Parquet contains no rows for target {expected_target!r}."
            )

        expected_location = self.forecast_spec.loc.lower()
        location_rows = target_rows.filter(
            pl.col("location").str.to_lowercase() == expected_location
        )
        if location_rows.is_empty():
            raise ValueError(
                "Hubverse Parquet contains no rows for location "
                f"{self.forecast_spec.loc!r} and target {expected_target!r}."
            )

        samples = location_rows.filter(pl.col("output_type") == "sample")
        if samples.is_empty():
            raise ValueError(
                "Hubverse Parquet contains no sample rows for location "
                f"{self.forecast_spec.loc!r} and target {expected_target!r}."
            )

        nullable_columns = [
            "target_end_date",
            "output_type_id",
            "value",
        ]
        if samples.select(
            pl.any_horizontal(
                [pl.col(column).is_null() for column in nullable_columns]
            ).any()
        ).item():
            raise ValueError("Hubverse sample rows contain missing values.")

        duplicate_rows = samples.select(
            pl.struct(["output_type_id", "target_end_date"]).is_duplicated().any()
        ).item()
        if duplicate_rows:
            raise ValueError(
                "Hubverse sample rows contain duplicate output_type_id/"
                "target_end_date pairs."
            )

        invalid_values = samples.select(
            ((~pl.col("value").is_finite()) | (pl.col("value") < 0)).any()
        ).item()
        if invalid_values:
            raise ValueError("Hubverse sample values must be finite and non-negative.")

        nowcast_dates = samples.get_column("target_end_date").unique().sort().to_list()
        missing_observation_dates = sorted(set(nowcast_dates) - set(dates))
        if missing_observation_dates:
            raise ValueError(
                "Hubverse nowcast dates are absent from the EpiAutoGP "
                f"observation dates: {missing_observation_dates}."
            )

        expected_trajectory_length = len(nowcast_dates)
        trajectory_lengths = samples.group_by("output_type_id").len()
        if trajectory_lengths.filter(
            pl.col("len") != expected_trajectory_length
        ).height:
            raise ValueError(
                "Hubverse sample trajectories are incomplete; every "
                "output_type_id must have one value for every target_end_date."
            )

        ordered_samples = samples.sort(["output_type_id", "target_end_date"])
        nowcast_reports = [
            trajectory.get_column("value").to_list()
            for trajectory in ordered_samples.partition_by(
                "output_type_id", maintain_order=True
            )
        ]

        return NowcastData(dates=nowcast_dates, reports=nowcast_reports)
