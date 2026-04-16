import datetime as dt

import polars as pl
from cfa.dataops import datacat


def _extract_pmf(
    df: pl.DataFrame,
    parameter_name: str,
) -> list:
    df = df.filter(pl.col("parameter") == parameter_name)
    if df.height != 1:
        raise ValueError(
            f"Expected exactly one {parameter_name!r} parameter row, "
            f"but found {df.height}. "
            f"Rows={df.to_dicts()}"
        )
    return df.item(0, "value").to_list()


def _filter_param_estimates(
    disease: str,
    as_of: dt.date | None = None,
    lazy: bool = True,
) -> pl.DataFrame:
    as_of = as_of or dt.date.max - dt.timedelta(days=1)

    output = "pl_lazy" if lazy else "pl"
    dat = datacat.public.stf.param_estimates.load.get_dataframe(output=output)
    result = (
        dat.with_columns(
            pl.col("start_date").fill_null(dt.date.min),
            pl.col("end_date").fill_null(dt.date.max),
        )
        .filter(pl.col("disease") == disease)
        .filter(
            pl.col("start_date") <= as_of,
            as_of < pl.col("end_date"),
        )
    )
    return result.collect() if lazy else result


def get_nnh_generation_interval_pmf(
    disease: str,
    as_of: dt.date | None = None,
    lazy: bool = True,
) -> list[float]:
    """
    Filter and extract the generation interval probability mass function (PMF)
    based on disease and date filters.

    This function retrieves epidemiological parameters from
    datacat.public.stf catalog and returns the generation interval PMF.

    Parameters
    ----------
    disease
        The disease name to filter for ("COVID-19", "Influenza", or "RSV").
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.
    lazy
        Whether to load data lazily (defaults to True).

    Returns
    -------
    list[float]
        A list representing the generation interval distribution.

    Raises
    ------
    ValueError
        If exactly one generation_interval row is not found.
    """
    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of, lazy=lazy)
    return _extract_pmf(dat_filtered, "generation_interval")


def get_nnh_delay_pmf(
    disease: str,
    as_of: dt.date | None = None,
    lazy: bool = True,
) -> list[float]:
    """
    Filter and extract the delay probability mass function (PMF)
    based on disease and date filters.

    This function retrieves epidemiological parameters from
    datacat.public.stf catalog and returns the delay PMF.

    Parameters
    ----------
    disease
        The disease name to filter for ("COVID-19", "Influenza", or "RSV").
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.
    lazy
        Whether to load data lazily (defaults to True).

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
    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of, lazy=lazy)
    delay_pmf = _extract_pmf(dat_filtered, "delay")

    return delay_pmf


def get_nnh_right_truncation_pmf(
    loc_abb: str,
    disease: str,
    as_of: dt.date | None = None,
    reference_date: dt.date | None = None,
    lazy: bool = True,
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
        The disease name to filter for ("COVID-19", "Influenza", or "RSV").
    as_of
        The date for which parameters should be valid. Parameters must have
        start_date <= as_of < end_date. Defaults to latest estimates.
    reference_date
        The reference date for filtering. Defaults to as_of value.
        Selects the most recent parameter with
        reference_date <= this value.
    lazy
        Whether to load data lazily (defaults to True).

    Returns
    -------
    list[float]
        A list representing the right truncation distribution.

    Raises
    ------
    ValueError
        If exactly one right_truncation row is not found when required.
    """
    if loc_abb == "GA":
        if as_of is None or as_of > dt.date(2025, 10, 14):
            as_of = dt.date(2025, 10, 14)

    dat_filtered = _filter_param_estimates(disease=disease, as_of=as_of, lazy=lazy)
    reference_date = reference_date or as_of or dt.date.max

    right_truncation_df = (
        dat_filtered.filter(pl.col("geo_value") == loc_abb)
        .filter(pl.col("reference_date") <= reference_date)
        .filter(pl.col("reference_date") == pl.col("reference_date").max())
    )
    right_truncation_pmf = _extract_pmf(right_truncation_df, "right_truncation")

    return right_truncation_pmf
