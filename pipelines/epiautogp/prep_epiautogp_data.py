"""
Data preparation and conversion functions for EpiAutoGP.

This module provides functions to convert surveillance data from the
cfa-stf-routine-forecasting pipeline format to the JSON format expected by EpiAutoGP.
"""

import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from pipelines.epiautogp.epiautogp_forecast_utils import (
    ForecastPipelineContext,
    ModelPaths,
)


def _validate_epiautogp_parameters(
    target: str,
    frequency: str,
    ed_visit_type: str,
) -> None:
    """
    Validate EpiAutoGP conversion parameters.

    The inadmissible parameter combinations are:
    - `target` not in ['nssp', 'nhsn']
    - `frequency` not in ['daily', 'epiweekly']
    - `ed_visit_type` not in ['observed', 'other', 'pct']
    - `frequency` is 'daily' when target is 'nhsn' (NHSN data are only epiweekly)
    - `ed_visit_type` is not 'pct' when target is 'nhsn'
    """
    # Validate individual parameters
    if target not in ["nssp", "nhsn"]:
        raise ValueError(f"target must be 'nssp' or 'nhsn', got '{target}'")

    if frequency not in ["daily", "epiweekly"]:
        raise ValueError(f"frequency must be 'daily' or 'epiweekly', got '{frequency}'")

    if ed_visit_type not in ["observed", "other", "pct"]:
        raise ValueError(
            f"ed_visit_type must be 'observed', 'other', or 'pct', got '{ed_visit_type}'"
        )

    if target == "nhsn" and frequency == "daily":
        raise ValueError("NHSN data is only available in epiweekly frequency.")

    if target == "nhsn" and ed_visit_type != "observed":
        raise ValueError(
            "ed_visit_type is only applicable when target='nssp'. "
            "For NHSN, ed_visit_type must be 'observed' (the default)."
        )


def convert_to_epiautogp_json(
    context: ForecastPipelineContext,
    paths: ModelPaths,
    nowcast_dates: list[dt.date] | None = None,
    nowcast_reports: list[list[float]] | None = None,
) -> Path:
    """
    Convert surveillance data to EpiAutoGP JSON format.

    This function reads surveillance data from the cfa-stf-routine-forecasting pipeline
    and converts it to the JSON format required by EpiAutoGP. It supports
    both daily and epiweekly data, and can output either counts or percentages.

    Parameters
    ----------
    context : ForecastPipelineContext
        Forecast pipeline context containing disease, location, report_date,
        target, frequency, ed_visit_type, and logger
    paths : ModelPaths
        Model paths containing daily and epiweekly training data paths,
        and model_output_dir where the JSON file will be saved
    nowcast_dates : list[dt.date] | `None`, default=`None`
        Dates requiring nowcasting (typically recent dates with
        incomplete data). If `None`, defaults to empty list. Not currently used.
    nowcast_reports : list[list[float]] | `None`, default=`None`
        Samples for nowcast dates to represent nowcast uncertainty. If `None`,
        defaults to empty list. Not currently used.

    Returns
    -------
    Path
        Path to the created EpiAutoGP JSON file

    Raises
    ------
    ValueError
        If target is not "nssp" or "nhsn", if frequency is not "daily" or
        "epiweekly", if frequency is "daily" when target is "nhsn",
        if ed_visit_type is not "pct" when target is "nhsn",
        or if the required data is not present in the TSV files
    FileNotFoundError
        If data files don't exist

    Notes
    -----
    The output JSON file is saved to:
    `paths.model_output_dir / f"{context.model_name}_input.json"`

    The output JSON for EpiAutoGP has the following structure:
    {
        "dates": ["2024-01-01", "2024-01-02", ...],
        "reports": [45.0, 52.0, ...],
        "pathogen": "COVID-19",
        "location": "CA",
        "target": "nssp",
        "frequency": "epiweekly",
        "ed_visit_type": "pct",
        "forecast_date": "2024-01-02",
        "nowcast_dates": [],
        "nowcast_reports": []
    }
    """
    logger = context.logger

    # Validate parameters
    _validate_epiautogp_parameters(
        context.target, context.frequency, context.ed_visit_type
    )

    # Set defaults for nowcasting
    if nowcast_dates is None:
        nowcast_dates = []
    if nowcast_reports is None:
        nowcast_reports = []

    # Define input data JSON path
    input_json_path = paths.model_output_dir / f"{context.model_name}_input.json"
    # Determine which data path to use based on frequency
    data_path = paths.training_data

    # Read data from TSV
    logger.info(f"Reading {context.frequency} data from {data_path}")
    dates, reports = _read_tsv_data(
        data_path,
        context.disease,
        context.loc,
        context.target,
        context.frequency,
        context.ed_visit_type,
        context.exclude_date_ranges,
        logger,
    )

    # Create EpiAutoGP input structure
    epiautogp_input = {
        "dates": [d.isoformat() for d in dates],
        "reports": reports,
        "pathogen": context.disease,
        "location": context.loc,
        "target": context.target,
        "frequency": context.frequency,
        "ed_visit_type": context.ed_visit_type,
        "forecast_date": context.report_date.isoformat(),
        "nowcast_dates": [d.isoformat() for d in nowcast_dates],
        "nowcast_reports": nowcast_reports,
    }

    # Write JSON file
    input_json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(input_json_path, "w") as f:
        json.dump(epiautogp_input, f, indent=2)

    logger.info(
        f"Saved EpiAutoGP input JSON for {context.disease} {context.loc} "
        f"(target={context.target}) to {input_json_path}"
    )

    return input_json_path


def _apply_date_exclusions(
    df: pl.DataFrame,
    exclude_date_ranges: list[tuple[dt.date, dt.date]],
    logger: logging.Logger,
) -> pl.DataFrame:
    """
    Filter out rows with dates that fall within excluded date ranges.
    """
    original_count = df.height

    # Build exclusion condition: exclude if date falls within ANY range
    # Start with a condition that's always False
    exclude_condition = pl.lit(False)
    for start_date, end_date in exclude_date_ranges:
        exclude_condition = exclude_condition | (
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )

    # Keep rows that DON'T match the exclusion condition
    df_filtered = df.filter(~exclude_condition)

    if df_filtered.height == 0:
        raise ValueError(
            "All dates were excluded by the provided date ranges. "
            f"Original observation count: {original_count}"
        )

    excluded_count = original_count - df_filtered.height
    logger.info(
        f"Excluded {excluded_count} observations from {len(exclude_date_ranges)} date range(s). "
        f"Remaining: {df_filtered.height} observations"
    )

    return df_filtered


def _read_tsv_data(
    tsv_path: Path,
    disease: str,
    location: str,
    target: str,
    frequency: str,
    ed_visit_type: str,
    exclude_date_ranges: list[tuple[dt.date, dt.date]] | None,
    logger: logging.Logger,
) -> tuple[list[dt.date], list[float]]:
    """
    Read surveillance data from TSV files and extract target variable.

    Reads a TSV file containing surveillance data, filters for the specified
    disease and location, pivots the data, and extracts the appropriate
    target variable (NSSP ED visits or NHSN hospital admissions).

    Parameters
    ----------
    tsv_path : Path
        Path to the TSV file containing surveillance data
    disease : str
        Disease name (case-insensitive)
    location : str
        Geographic location code (e.g., "CA", "US")
    target : str
        Target data type: "nssp" or "nhsn"
    frequency : str
        Data frequency: "daily" or "epiweekly"
    ed_visit_type : str
        Type of ED visits: "observed" or "other" (only for NSSP)
    exclude_date_ranges : list[tuple[dt.date, dt.date]] | None
        List of date ranges to exclude from the data (inclusive).
        Each tuple contains (start_date, end_date). If None, no dates are excluded.
    logger : logging.Logger
        Logger for progress messages

    Returns
    -------
    tuple[list[dt.date], list[float]]
        Tuple of (dates, reports) where dates is a list of dates and
        reports is a list of corresponding values

    Raises
    ------
    FileNotFoundError
        If the TSV file doesn't exist
    ValueError
        If no data is found for the specified disease and location,
        or if required columns are missing
    """
    if not tsv_path.exists():
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    logger.info(f"Reading {frequency} data from {tsv_path}")

    # Read TSV file
    df = pl.read_csv(tsv_path, separator="\t")

    # Filter for the specified location and disease (case-insensitive for disease)
    df = df.filter(
        (pl.col("geo_value") == location)
        & (pl.col("disease").str.to_uppercase() == disease.upper())
    )

    if df.height == 0:
        raise ValueError(f"No data found for {disease} {location} in {tsv_path}")

    # Pivot the data to get columns for each variable
    df_pivot = df.pivot(
        index=["date", "geo_value", "disease", "data_type"],
        on=".variable",
        values=".value",
    )

    # Ensure date column is properly typed
    df_pivot = df_pivot.with_columns(pl.col("date").cast(pl.Date))
    df_pivot = df_pivot.sort("date")

    # Apply date exclusions if provided (before extracting to lists)
    if exclude_date_ranges:
        df_pivot = _apply_date_exclusions(df_pivot, exclude_date_ranges, logger)

    # Extract data based on target
    if target == "nssp":
        dates, reports = _extract_nssp_from_pivot(
            df_pivot, ed_visit_type, tsv_path, logger
        )
    else:  # target == "nhsn"
        dates, reports = _extract_nhsn_from_pivot(df_pivot, tsv_path, logger)

    logger.info(
        f"Extracted {len(dates)} {frequency} {target} observations "
        f"from {dates[0]} to {dates[-1]}"
    )

    return dates, reports


def _extract_nssp_from_pivot(
    df_pivot: pl.DataFrame,
    ed_visit_type: str,
    tsv_path: Path,
    logger: logging.Logger,
) -> tuple[list[dt.date], list[float]]:
    """
    Extract NSSP ED visit data from pivoted DataFrame.

    Extracts emergency department visit data from NSSP (National Syndromic
    Surveillance Program) and optionally converts to percentage format.

    Parameters
    ----------
    df_pivot : pl.DataFrame
        Pivoted DataFrame with columns for dates and ED visit types
    ed_visit_type : str
        Type of ED visits to extract: "observed" or "other"
    tsv_path : Path
        Path to source TSV file (for error messages)
    logger : logging.Logger
        Logger for progress messages

    Returns
    -------
    tuple[list[dt.date], list[float]]
        Tuple of (dates, reports) with ED visit counts or percentages

    Raises
    ------
    ValueError
        If required columns are missing from the DataFrame
    """
    # Determine which ED visit column to use
    ed_visit_type_map = {
        "observed": "observed_ed_visits",
        "other": "other_ed_visits",
        "pct": "prop_disease_ed_visits",
    }

    ed_column = ed_visit_type_map.get(ed_visit_type)
    if not ed_column:
        raise ValueError(
            f"Invalid ed_visit_type: {ed_visit_type}. Must be 'observed', 'other', or 'pct'."
        )

    # Check required columns
    if ed_column not in df_pivot.columns:
        raise ValueError(f"Column '{ed_column}' not found in {tsv_path}")

    df_pivot = df_pivot.with_columns(pl.col(ed_column).cast(pl.Float64).alias("value"))

    if ed_visit_type == "pct":
        df_pivot = df_pivot.with_columns(pl.col("value") * 100)

    # Filter out any rows with null values
    df_pivot = df_pivot.filter(pl.col("value").is_not_null())

    dates = df_pivot["date"].to_list()
    reports = df_pivot["value"].to_list()

    return dates, reports


def _extract_nhsn_from_pivot(
    df_pivot: pl.DataFrame,
    tsv_path: Path,
    logger: logging.Logger,
) -> tuple[list[dt.date], list[float]]:
    """
    Extract NHSN hospital admission data from pivoted DataFrame.

    Extracts hospital admission counts from NHSN (National Healthcare Safety
    Network) data. NHSN data is always reported as counts, not percentages.

    Parameters
    ----------
    df_pivot : pl.DataFrame
        Pivoted DataFrame with columns for dates and hospital admissions
    tsv_path : Path
        Path to source TSV file (for error messages)
    logger : logging.Logger
        Logger for progress messages

    Returns
    -------
    tuple[list[dt.date], list[float]]
        Tuple of (dates, reports) with hospital admission counts

    Raises
    ------
    ValueError
        If the 'observed_hospital_admissions' column is missing
    """
    if "observed_hospital_admissions" not in df_pivot.columns:
        raise ValueError(
            f"Column 'observed_hospital_admissions' not found in {tsv_path}"
        )

    df_pivot = df_pivot.with_columns(
        pl.col("observed_hospital_admissions").cast(pl.Float64).alias("value")
    )
    logger.info("Using hospital admission counts")

    # Filter out any rows with null values
    df_pivot = df_pivot.filter(pl.col("value").is_not_null())

    dates = df_pivot["date"].to_list()
    reports = df_pivot["value"].to_list()

    return dates, reports
