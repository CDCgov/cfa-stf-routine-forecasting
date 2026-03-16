import datetime as dt

import jax.numpy as jnp
import polars as pl
from cfa.dataops import datacat


def _validate_and_extract(
    df_lazy: pl.LazyFrame,
    parameter_name: str,
) -> list:
    df = df_lazy.filter(pl.col("parameter") == parameter_name).collect()
    if df.height != 1:
        error_msg = (
            f"Expected exactly one {parameter_name} parameter row, "
            f"but found {df.height}. "
            f"Rows={df.to_dicts() if df.height > 0 else []}"
        )
        raise ValueError(error_msg)
    return df.item(0, "value").to_list()


def filter_pmfs(
    loc_abb: str,
    disease: str,
    as_of: dt.date | None = None,
    reference_date: dt.date | None = None,
    right_truncation_required: bool = True,
) -> dict[str, list[float]]:
    """
    Filter and extract probability mass function (PMF) parameters based on
    disease, location, and date filters.

    This function retrieves a DataFrame containing epidemiological parameters
    from datacat.public.stf catalog and returns three types of PMF parameters:
    delay, generation interval, and right truncation.

    Parameters
    ----------
    disease
        The disease name to filter for.
    loc_abbr
        Location abbreviation (geo_value) to filter right_truncation parameters.
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of <= end_date. Defaults to latest estimates.
    reference_date
        The reference date for right_truncation parameter filtering. Defaults to as_of value.
        The reference date for right_truncation parameter filtering. Defaults to as_of value.
        For right_truncation, selects the most recent parameter with reference_date <= this value.

    Returns
    -------
    dict[str, list[float]]
        A dictionary containing three PMF lists:
        - 'delay_pmf': List representing the delay distribution.
        - 'generation_interval_pmf': List representing the generation interval distribution.
        - 'right_truncation_pmf': List representing the right truncation distribution.

    Raises
    ------
    ValueError
        If exactly one row is not found for any of the required parameters.

    Notes
    -----
    The function applies specific filtering logic for each parameter type:
    - For delay and generation_interval: filters by disease, parameter name, and validity date range.
    - For right_truncation: additionally filters by location.
    """
    min_as_of = dt.date(1000, 1, 1)
    max_as_of = dt.date(3000, 1, 1)
    as_of = as_of or max_as_of
    reference_date = reference_date or as_of

    dat = datacat.public.stf.param_estimates.load.get_dataframe(output="pl")
    dat_filtered = (
        dat.with_columns(
            pl.col("start_date").fill_null(min_as_of),
            pl.col("end_date").fill_null(max_as_of),
        )
        .filter(pl.col("disease") == disease)
        .filter(
            pl.col("start_date") <= as_of,
            pl.col("end_date") >= as_of,
        )
    )

    generation_interval_pmf = _validate_and_extract(dat_filtered, "generation_interval")

    delay_pmf = _validate_and_extract(dat_filtered, "delay")

    # ensure 0 first entry; we do not model the possibility
    # of a zero infection-to-recorded-admission delay in Pyrenew-HEW
    delay_pmf[0] = 0.0
    delay_pmf = jnp.array(delay_pmf)
    delay_pmf = delay_pmf / delay_pmf.sum()
    delay_pmf = delay_pmf.tolist()

    right_truncation_df = (
        dat_filtered.filter(pl.col("geo_value") == loc_abb)
        .filter(pl.col("reference_date") <= reference_date)
        .filter(pl.col("reference_date") == pl.col("reference_date").max())
    )

    if right_truncation_df.collect().height == 0 and not right_truncation_required:
        right_truncation_pmf = [1]
    else:
        right_truncation_pmf = _validate_and_extract(
            right_truncation_df, "right_truncation"
        )

    result = {
        "delay_pmf": delay_pmf,
        "generation_interval_pmf": generation_interval_pmf,
        "right_truncation_pmf": right_truncation_pmf,
    }
    return result
