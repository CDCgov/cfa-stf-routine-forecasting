import datetime as dt
from functools import partial
from typing import Literal

import numpy as np
import polars as pl
from jax.typing import ArrayLike

from cfa.stf.data import get_nnh_right_truncation_pmf, get_nssp
from cfa.stf.data.get_data import NSSPDataset
from cfa.stf.forecasttools import ensure_list


def identify_outlier_tail(
    x: ArrayLike,
    low_outliers_only: bool = True,
    spread_estimator: Literal["median", "mean"] = "mean",
    outlier_multiplier: float = 8.0,
    max_tail_length: int = 7,
) -> list[bool]:
    """
    Identify outliers in the tail of a time series based on log differences.

    Parameters:
    -----------
    x : array-like
        Input data
    spread_estimator : Literal["median", "mean"]
        Method to estimate the spread of the log differences (default: "mean")
    outlier_multiplier : float
        Number of standard deviations to use as threshold (default: 8.0)
    max_tail_length : int
        Length of tail to examine (default: 7)

    Returns:
    --------
    list[bool]
        Boolean list indicating which elements are outliers in the tail
    """
    x = np.asarray(x)
    n = len(x)

    # Handle edge case: array too small
    if n < 2:
        return np.zeros(n, dtype=bool).tolist()

    # Compute log differences, removing NaNs
    log_diff = np.diff(np.log1p(x))
    log_diff = log_diff[~np.isnan(log_diff)]

    # Handle edge case: insufficient data after removing NaNs
    if len(log_diff) <= max_tail_length:
        return np.zeros(n, dtype=bool).tolist()

    # Split into head and tail
    head_log_diff = log_diff[:-max_tail_length]
    tail_log_diff = log_diff[-max_tail_length:]

    if spread_estimator == "median":
        log_diff_spread = np.median(np.abs(head_log_diff))
    elif spread_estimator == "mean":
        log_diff_spread = np.mean(np.abs(head_log_diff))
    else:
        raise ValueError(f"Invalid spread_estimator: {spread_estimator}")

    # Identify first outlier in tail
    threshold = log_diff_spread * outlier_multiplier
    if low_outliers_only:
        outlier_indices = np.where(tail_log_diff < -threshold)[0]
    else:
        outlier_indices = np.where(np.abs(tail_log_diff) > threshold)[0]

    # Build exclusion boolean array
    exclude = np.zeros(n, dtype=bool)
    if len(outlier_indices) > 0:
        first_outlier_pos = outlier_indices[0]
        exclude[n - max_tail_length + first_outlier_pos :] = True

    return exclude.tolist()


def exclude_tail_auto(
    loc_abb: str,
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    exclusion_calc_disease: str | list[str] = ["Influenza", "RSV", "COVID-19"],
    spread_estimator: Literal["median", "mean"] = "mean",
    outlier_multiplier: float = 8.0,
    max_tail_length: int = 7,
) -> pl.DataFrame:
    disease_pmf = (
        exclusion_calc_disease
        if exclusion_calc_disease != "Total"
        else ["Influenza", "RSV", "COVID-19"]
    )

    nssp_dat = (
        get_nssp(
            as_of=as_of,
            loc_abb=loc_abb,
            start_date=start_date,
            end_date=end_date,
            disease=exclusion_calc_disease,
            lazy=False,
        ).with_columns(
            offset_days=(
                pl.col("reference_date").max() - pl.col("reference_date")
            ).dt.total_days()
            + 1
        )
    ).sort("reference_date")

    pmf_df = (
        pl.DataFrame(
            {
                "disease": disease_pmf,
                "nnh_right_truncation_pmf": [
                    get_nnh_right_truncation_pmf(
                        as_of=as_of, loc_abb=loc_abb, disease=disease
                    )
                    for disease in ensure_list(disease_pmf)
                ],
            }
        )
        .explode("nnh_right_truncation_pmf")
        .with_columns(
            right_truncation_cdf=pl.col("nnh_right_truncation_pmf")
            .cum_sum()
            .over("disease"),
            offset_days=pl.row_index().over("disease") + 1,
        )
    )
    # If exclusion_calc_disease is "Total", disease_pmf is all diseases,
    # so we average the pmf across all of them
    if exclusion_calc_disease == "Total":
        pmf_df = (
            pmf_df.group_by("offset_days")
            .agg(pl.col("right_truncation_cdf").mean())
            .sort("offset_days")
            .with_columns(pl.lit(exclusion_calc_disease).alias("disease"))
        )

    joined_dat = (
        (
            nssp_dat.join(pmf_df, on=["disease", "offset_days"], how="left")
            .with_columns(
                right_truncation_cdf=pl.col("right_truncation_cdf").fill_null(1)
            )
            .with_columns(
                adjusted_value=pl.col("value") / pl.col("right_truncation_cdf")
            )
            .group_by("reference_date")
            .agg(pl.col("adjusted_value").sum(), pl.col("value").sum())
            .sort("reference_date")
        )
        .pipe(
            lambda df: df.with_columns(
                pl.Series(
                    identify_outlier_tail(
                        df["adjusted_value"],
                        spread_estimator=spread_estimator,
                        outlier_multiplier=outlier_multiplier,
                        max_tail_length=max_tail_length,
                    )
                ).alias("exclude")
            )
        )
        .select("reference_date", "exclude")
    )
    return joined_dat


def exclude_tail_n(reference_dates: list[dt.date], n: int) -> pl.DataFrame:
    exclusion_df = (
        pl.DataFrame({"reference_date": reference_dates})
        .unique("reference_date")
        .sort("reference_date")
        .with_columns(
            exclude=pl.col("reference_date")
            >= pl.col("reference_date").max() - dt.timedelta(days=n - 1)
        )
    )
    return exclusion_df


def get_nssp_with_exclusion(
    disease: str,
    loc_abb: str,
    exclusion_strategy: Literal[
        "tail_by_target_disease", "tail_by_all_disease", "tail_by_total", "tail_by_n"
    ],
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    **exclusion_strategy_args,
) -> pl.DataFrame:
    if len(ensure_list(disease)) != 1:
        raise ValueError(f"Only one disease can be processed at a time. Got {disease}.")

    if len(ensure_list(loc_abb)) != 1:
        raise ValueError(
            f"Only one location can be processed at a time. Got {loc_abb}."
        )

    exclusion_strategies = {
        "tail_by_target_disease": partial(
            exclude_tail_auto,
            loc_abb=loc_abb,
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            exclusion_calc_disease=disease,
        ),
        "tail_by_all_disease": partial(
            exclude_tail_auto,
            loc_abb=loc_abb,
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            exclusion_calc_disease=["Influenza", "RSV", "COVID-19"],
        ),
        "tail_by_total": partial(
            exclude_tail_auto,
            loc_abb=loc_abb,
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            exclusion_calc_disease="Total",
        ),
        "tail_by_n": lambda **kwargs: exclude_tail_n(
            reference_dates=nssp_dat["reference_date"].to_list(), **kwargs
        ),
    }

    if exclusion_strategy not in exclusion_strategies:
        raise ValueError(
            f"exclusion_strategy must be one of {set(exclusion_strategies)}, "
            f"got {exclusion_strategy!r}"
        )

    nssp_dat = get_nssp(
        as_of=as_of,
        loc_abb=loc_abb,
        disease=disease,
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        lazy=False,
    )

    exclusion_dat = exclusion_strategies[exclusion_strategy](**exclusion_strategy_args)

    nssp_with_exclusion = nssp_dat.join(exclusion_dat, on="reference_date", how="left")

    return nssp_with_exclusion
