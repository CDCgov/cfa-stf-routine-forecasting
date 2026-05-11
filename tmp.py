import datetime as dt

import numpy as np
import polars as pl
from jax.typing import ArrayLike

from cfa.stf.data import get_nnh_right_truncation_pmf, get_nssp


def identify_outlier_tail(
    x: ArrayLike, outlier_sd_multiplier: float = 2.5, max_tail_length: int = 7
):
    """
    Identify outliers in the tail of a time series based on log differences.

    Parameters:
    -----------
    x : array-like
        Input data
    outlier_sd_multiplier : float
        Number of standard deviations to use as threshold (default: 2.5)
    max_tail_length : int
        Length of tail to examine (default: 7)

    Returns:
    --------
    ndarray
        Boolean array indicating which elements are outliers in the tail
    """
    x = np.asarray(x)
    n = len(x)

    # Handle edge case: array too small
    if n < 2:
        return np.zeros(n, dtype=bool)

    # Compute log differences, removing NaNs
    log_diff = np.diff(np.log1p(x))
    log_diff = log_diff[~np.isnan(log_diff)]

    # Handle edge case: insufficient data after removing NaNs
    if len(log_diff) <= max_tail_length:
        return np.zeros(n, dtype=bool)

    # Split into head and tail
    head_log_diff = log_diff[:-max_tail_length]
    tail_log_diff = log_diff[-max_tail_length:]

    # Compute standard deviation of head
    log_diff_sd = np.std(head_log_diff, ddof=1)

    # Identify first outlier in tail
    threshold = log_diff_sd * outlier_sd_multiplier
    outlier_indices = np.where(np.abs(tail_log_diff) > threshold)[0]

    # Build exclusion boolean array
    excluded = np.zeros(n, dtype=bool)
    if len(outlier_indices) > 0:
        first_outlier_pos = outlier_indices[0]
        excluded[n - max_tail_length + first_outlier_pos :] = True

    return excluded


loc_abb = "LA"
non_total_disease = ["Influenza", "RSV", "COVID-19"]
as_of = dt.date(2026, 5, 6)


nssp_dat = get_nssp(
    as_of=as_of, loc_abb=loc_abb, disease=non_total_disease, lazy=False
).with_columns(offset_days=(pl.lit(as_of) - pl.col("reference_date")).dt.total_days())

pmf_df = (
    pl.DataFrame(
        {
            "disease": non_total_disease,
            "nnh_right_truncation_pmf": [
                get_nnh_right_truncation_pmf(
                    as_of=as_of, loc_abb=loc_abb, disease=disease
                )
                for disease in non_total_disease
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
    .select(
        "disease",
        "offset_days",
        "right_truncation_cdf",
    )
)

joined_dat = (
    nssp_dat.join(pmf_df, on=["disease", "offset_days"], how="left")
    .with_columns(right_truncation_cdf=pl.col("right_truncation_cdf").fill_null(1))
    .with_columns(adjusted_value=pl.col("value") / pl.col("right_truncation_cdf"))
    .group_by("reference_date")
    .agg(pl.col("adjusted_value").sum(), pl.col("value").sum())
    .sort("reference_date")
)

joined_dat = joined_dat.with_columns(
    excluded=pl.lit(identify_outlier_tail(joined_dat["adjusted_value"]))
)

joined_dat
# For LA dt.date(2026, 5, 6), the algorithm suggests excluding last day because the adjusted value is too high, but the data quality report suggests excluding it because it is too low...
joined_dat.write_csv("adjusted_nssp.csv")


# might still be better to base off of unadjusted total, since it is so much more stable?
# Possibly we can right-trun adjust it by the mean of the diseases-specif right trunc estimates

nssp_dat_total = (
    get_nssp(as_of=as_of, loc_abb=loc_abb, disease="Total", lazy=False)
    .with_columns(
        offset_days=(pl.lit(as_of) - pl.col("reference_date")).dt.total_days()
    )
    .sort("reference_date")
)

pmf_df_total = (
    pl.DataFrame(
        {
            "disease": non_total_disease,
            "nnh_right_truncation_pmf": [
                get_nnh_right_truncation_pmf(
                    as_of=as_of, loc_abb=loc_abb, disease=disease
                )
                for disease in non_total_disease
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
    .group_by("offset_days")
    .agg(pl.col("right_truncation_cdf").mean())
    .sort("offset_days")
    .select(
        "offset_days",
        "right_truncation_cdf",
    )
)

joined_dat_total = (
    nssp_dat_total.join(pmf_df_total, on="offset_days", how="left")
    .with_columns(right_truncation_cdf=pl.col("right_truncation_cdf").fill_null(1))
    .with_columns(adjusted_value=pl.col("value") / pl.col("right_truncation_cdf"))
    .group_by("reference_date")
    .agg(pl.col("adjusted_value").sum(), pl.col("value").sum())
    .sort("reference_date")
)

# Seems to work pretty well
joined_dat_total = joined_dat_total.with_columns(
    excluded=pl.lit(identify_outlier_tail(joined_dat_total["adjusted_value"]))
)

# I don't know if I love it because the variations are pretty different.
pmf_df.filter(pl.col("offset_days") == 1)

# not right-trunc adjusting also seems to work well
nssp_dat_total.with_columns(
    excluded=pl.lit(identify_outlier_tail(nssp_dat_total["value"]))
)
