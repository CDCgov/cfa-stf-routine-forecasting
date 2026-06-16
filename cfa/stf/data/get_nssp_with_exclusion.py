import datetime as dt
from collections.abc import Iterable
from functools import partial
from typing import Literal

import numpy as np
import polars as pl
from jax.typing import ArrayLike

from cfa.stf.data import get_nnh_right_truncation_pmf, get_nssp
from cfa.stf.data.get_data import NSSPDataset
from cfa.stf.forecasttools import ensure_list

DEFAULT_EXCLUSION_CALC_DISEASES = ("Influenza", "RSV", "COVID-19")


def identify_outlier_tail(
    x: ArrayLike,
    low_outliers_only: bool = True,
    loc_estimator: Literal["median", "mean"] = "median",
    outlier_multiplier: float = 11.0,
    max_tail_length: int = 7,
) -> list[bool]:
    """
    Identify outliers in the tail of a time series based on log differences.

    Parameters:
    -----------
    x : array-like
        Input data
    loc_estimator : Literal["median", "mean"]
        Method to estimate the typical absolute log-difference size in the non-tail portion (default: "median")
    outlier_multiplier : float
        Multiplier applied to the typical absolute log-difference size to set the threshold (default: 11.0)
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

    if loc_estimator == "median":
        log_diff_spread = np.median(np.abs(head_log_diff))
    elif loc_estimator == "mean":
        log_diff_spread = np.mean(np.abs(head_log_diff))
    else:
        raise ValueError(f"Invalid loc_estimator: {loc_estimator}")

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
    exclusion_calc_disease: str | Iterable[str] | None = None,
    loc_estimator: Literal["median", "mean"] = "median",
    outlier_multiplier: float = 11.0,
    max_tail_length: int = 7,
    nowcast_adjustment: bool = False,
) -> pl.DataFrame:
    """
    Identify dates to exclude from the tail of an NSSP time series.

    This function retrieves NSSP data for one location and one or more
    diseases, optionally adjusts recent observations for right truncation,
    aggregates across the selected diseases by date, and flags the tail after
    the first detected outlier. Outliers are detected from log differences by
    `identify_outlier_tail`.

    Parameters
    ----------
    loc_abb
        Location abbreviation to filter for.
    as_of
        Reference date for data availability. Only data available as of this
        date will be used. If None, all available data will be used.
    start_date
        Start date for filtering data, inclusive. If None, no lower bound is
        applied.
    end_date
        End date for filtering data, inclusive. If None, no upper bound is
        applied.
    exclusion_calc_disease
        Disease or diseases used to calculate exclusions. If "Total" and
        `nowcast_adjustment` is True, right-truncation adjustments are averaged
        across Influenza, RSV, and COVID-19.
    loc_estimator
        Summary statistic used by `identify_outlier_tail` to estimate the
        typical absolute log difference in the non-tail portion of the series.
        Must be "median" or "mean".
    outlier_multiplier
        Multiplier applied to the estimated typical log-difference size to set
        the outlier threshold.
    max_tail_length
        Number of final log differences to inspect for outliers.
    nowcast_adjustment
        Whether to adjust recent values by the NNH right-truncation cumulative
        distribution before detecting outliers. If False, all adjustment factors
        are treated as 1.

    Returns
    -------
    pl.DataFrame
        Data frame with one row per `reference_date` and columns:
        `reference_date` and `exclude`.
    """
    exclusion_calc_diseases = ensure_list(
        DEFAULT_EXCLUSION_CALC_DISEASES
        if exclusion_calc_disease is None
        else exclusion_calc_disease
    )
    use_total_pmf = exclusion_calc_diseases == ["Total"]

    if nowcast_adjustment:
        # If adjusting for right truncation, we need to work with the right truncation PMFs
        # There is no right truncation pmf for Total, so we use the average of the PMFs for the individual diseases as an approximation
        disease_pmf = (
            DEFAULT_EXCLUSION_CALC_DISEASES
            if use_total_pmf
            else exclusion_calc_diseases
        )
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
        if use_total_pmf:
            pmf_df = (
                pmf_df.group_by("offset_days")
                .agg(pl.col("right_truncation_cdf").mean())
                .sort("offset_days")
                .with_columns(pl.lit("Total").alias("disease"))
            )
    else:  # not adjusting for right truncation, so we fill the pmf_df with 1s
        pmf_df = pl.DataFrame(
            {
                "disease": exclusion_calc_diseases,
                "right_truncation_cdf": 1.0,
                "offset_days": 1,
            }
        )

    nssp_dat = (
        get_nssp(
            as_of=as_of,
            loc_abb=loc_abb,
            start_date=start_date,
            end_date=end_date,
            disease=exclusion_calc_diseases,
            lazy=False,
        ).with_columns(
            offset_days=(
                pl.col("reference_date").max() - pl.col("reference_date")
            ).dt.total_days()
            + 1
        )
    ).sort("reference_date")

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
                        loc_estimator=loc_estimator,
                        outlier_multiplier=outlier_multiplier,
                        max_tail_length=max_tail_length,
                    )
                ).alias("exclude")
            )
        )
        .select("reference_date", "exclude")
    )
    return joined_dat


def exclude_tail_n(reference_dates: Iterable[dt.date], n: int) -> pl.DataFrame:
    """
    Mark the final `n` dates in a set of reference dates for exclusion.

    Parameters
    ----------
    reference_dates
        Reference dates from the NSSP data. Duplicate dates are removed before
        calculating exclusions.
    n
        Number of trailing dates to mark as excluded. A value of 1 excludes
        only the latest date.

    Returns
    -------
    pl.DataFrame
        Data frame with one row per unique `reference_date`, sorted by date,
        and columns: `reference_date` and `exclude`.
    """
    if n < 0:
        raise ValueError(f"n must be non-negative; got {n}.")
    reference_dates = list(reference_dates)
    exclusion_cutoff = max(len(set(reference_dates)) - n, 0)
    exclusion_df = (
        pl.DataFrame({"reference_date": reference_dates})
        .unique("reference_date")
        .sort("reference_date")
        .with_row_index("_reference_date_order")
        .with_columns(exclude=pl.col("_reference_date_order") >= exclusion_cutoff)
        .drop("_reference_date_order")
    )
    return exclusion_df


def get_nssp_with_exclusion(
    disease: str,
    loc_abb: str,
    exclusion_strategy: Literal[
        "tail_by_target_disease", "tail_by_all_disease", "tail_by_total", "tail_by_n"
    ] = "tail_by_total",
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    **exclusion_strategy_args,
) -> pl.DataFrame:
    """
    Retrieve NSSP data and attach an exclusion flag for tail observations.

    The exclusion flag is calculated for a single disease and location using
    one of the supported strategies, then joined back to the requested NSSP
    data by `reference_date`.

    Parameters
    ----------
    disease
        Disease to retrieve. Exactly one disease must be supplied.
    loc_abb
        Location abbreviation to retrieve. Exactly one location must be
        supplied.
    exclusion_strategy
        Strategy used to calculate the exclusion flag:

        - "tail_by_target_disease": detect tail outliers using the requested
          disease only.
        - "tail_by_all_disease": detect tail outliers using Influenza, RSV,
          and COVID-19 summed by date.
        - "tail_by_total": detect tail outliers using the NSSP "Total"
          disease series.
        - "tail_by_n": exclude the final `n` reference dates.
    dataset
        NSSP dataset to retrieve from datacat: "gold" or "comprehensive".
    as_of
        Reference date for data availability. Only data available as of this
        date will be used. If None, all available data will be used.
    start_date
        Start date for filtering NSSP data, inclusive. If None, no lower bound
        is applied.
    end_date
        End date for filtering NSSP data, inclusive. If None, no upper bound is
        applied.
    **exclusion_strategy_args
        Additional keyword arguments passed to the selected exclusion strategy.
        For automatic tail strategies, these are passed to `exclude_tail_auto`;
        for "tail_by_n", pass `n`.

    Returns
    -------
    pl.DataFrame
        NSSP data for the requested disease and location with an added
        `exclude` column indicating whether each `reference_date` should be
        excluded.

    Raises
    ------
    ValueError
        If multiple diseases or locations are supplied, or if
        `exclusion_strategy` is not supported.
    """
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
