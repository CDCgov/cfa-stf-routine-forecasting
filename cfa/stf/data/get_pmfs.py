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
            f"Expected exactly one {parameter_name!r} parameter row, "
            f"but found {df.height}. "
            f"Rows={df.to_dicts()}"
        )
        raise ValueError(error_msg)
    return df.item(0, "value").to_list()


def _filter_param_estimates(
    disease: str,
    as_of: dt.date | None = None,
) -> pl.LazyFrame:
    min_as_of = dt.date(1000, 1, 1)
    max_as_of = dt.date(3000, 1, 1)
    as_of = as_of or max_as_of

    dat = datacat.public.stf.param_estimates.load.get_dataframe(output="pl")
    return (
        dat.with_columns(
            pl.col("start_date").fill_null(min_as_of),
            pl.col("end_date").fill_null(max_as_of),
        )
        .filter(pl.col("disease") == disease)
        .filter(
            pl.col("start_date") <= as_of,
            as_of < pl.col("end_date"),
        )
    )


def get_nnh_generation_interval_pmf(
    disease: str,
    as_of: dt.date | None = None,
) -> list[float]:
    """
    Filter and extract the generation interval probability mass function (PMF)
    based on disease and date filters.

    This function retrieves epidemiological parameters from
    datacat.public.stf catalog and returns the generation interval PMF.

    Parameters
    ----------
    disease
        The disease name to filter for.
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.

    Returns
    -------
    list[float]
        A list representing the generation interval distribution.

    Raises
    ------
    ValueError
        If exactly one generation_interval row is not found.
    """
    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of)
    return _validate_and_extract(dat_filtered, "generation_interval")


def get_nnh_delay_pmf(
    disease: str,
    as_of: dt.date | None = None,
) -> list[float]:
    """
    Filter and extract the delay probability mass function (PMF)
    based on disease and date filters.

    This function retrieves epidemiological parameters from
    datacat.public.stf catalog and returns the delay PMF.

    Parameters
    ----------
    disease
        The disease name to filter for.
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.

    Returns
    -------
    list[float]
        A list representing the delay distribution.

    Raises
    ------
    ValueError
        If exactly one delay row is not found.

    Notes
    -----
    The first entry is forced to 0 and the distribution is renormalized.
    """
    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of)
    delay_pmf = _validate_and_extract(dat_filtered, "delay")

    # ensure 0 first entry; we do not model the possibility
    # of a zero infection-to-recorded-admission delay in Pyrenew-HEW
    delay_pmf[0] = 0.0
    delay_pmf = jnp.array(delay_pmf)
    delay_pmf = delay_pmf / delay_pmf.sum()
    return delay_pmf.tolist()


def get_nnh_right_truncation_pmf(
    loc_abb: str,
    disease: str,
    as_of: dt.date | None = None,
    reference_date: dt.date | None = None,
) -> list[float]:
    """
    Filter and extract the right truncation probability mass function (PMF)
    based on disease, location, and date filters.

    This function retrieves epidemiological parameters from
    datacat.public.stf catalog and returns the right truncation PMF.

    Parameters
    ----------
    loc_abb
        Location abbreviation (geo_value) used to filter right_truncation
        parameters.
    disease
        The disease name to filter for.
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.
    reference_date
        The reference date for filtering. Defaults to as_of value.
        Selects the most recent parameter with
        reference_date <= this value.

    Returns
    -------
    list[float]
        A list representing the right truncation distribution.

    Raises
    ------
    ValueError
        If exactly one right_truncation row is not found when required.
    """
    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of)
    reference_date = reference_date or as_of or dt.date(3000, 1, 1)

    right_truncation_df = (
        dat_filtered.filter(pl.col("geo_value") == loc_abb)
        .filter(pl.col("reference_date") <= reference_date)
        .filter(pl.col("reference_date") == pl.col("reference_date").max())
    )
    right_truncation_pmf = _validate_and_extract(
        right_truncation_df, "right_truncation"
    )

    return right_truncation_pmf.tolist()
