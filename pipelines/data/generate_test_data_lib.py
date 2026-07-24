"""
Library functions for generating test data for disease modeling from `pyrenew`.

This module contains functions and constants for creating
synthetic test data, including facility-level and location-level NSSP data,
NHSN data, and parameter estimates.
"""

import datetime as dt

import numpy as np
import polars as pl
import polars.selectors as cs
from scipy.stats import expon, norm

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
