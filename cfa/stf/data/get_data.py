import datetime as dt
import warnings
from collections.abc import Iterable
from typing import Literal, overload

import polars as pl
from cfa.dataops import datacat

from cfa.stf.forecasttools import ensure_list

NSSPDataset = Literal["gold", "comprehensive"]


@overload
def get_nhsn_hrd(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    prelim: bool = ...,
    as_of: dt.date | None = ...,
    start_date: dt.date | None = ...,
    end_date: dt.date | None = ...,
    lazy: Literal[True] = ...,
) -> pl.LazyFrame: ...


@overload
def get_nhsn_hrd(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    prelim: bool = ...,
    as_of: dt.date | None = ...,
    start_date: dt.date | None = ...,
    end_date: dt.date | None = ...,
    lazy: Literal[False] = ...,
) -> pl.DataFrame: ...


def get_nhsn_hrd(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    prelim: bool = True,
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    lazy: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Retrieve and filter NHSN hospital respiratory data based on specified criteria.

    This function retrieves vintages of NHSN hrd data specified by
    the `as_of` date from the datacat.public.stf catalog, applies filters
    for a specific disease, location, and dates if provided.

    Parameters
    ----------
    disease
        The disease to filter for ("COVID-19", "Influenza", or "RSV"). If None, all diseases are included.
    loc_abb
        The location abbreviation to filter for. If None, all locations are included.
    prelim
        Whether to retrieve "nhsn_hrd_prelim" data as opposed to "nhsn_hrd" data (defaults to True).
    as_of
        The reference date for filtering. If None, the most recent 'as_of' date is used.
    start_date
        The start date for the time period to include. If None, no lower bound is applied.
    end_date
        The end date for the time period to include. If None, no upper bound is applied.
    lazy
        Whether to return a lazy frame (defaults to True). If True, returns a
        `pl.LazyFrame`; if False, returns a `pl.DataFrame`.

    Returns
    -------
    pl.DataFrame | pl.LazyFrame
        Filtered data in long format with columns:
        `weekendingdate`, `jurisdiction`, `disease`, and `hospital_admissions`.
    """
    if not as_of:
        as_of = dt.date.max

    disease = ensure_list(disease)
    get_all_diseases = not disease

    loc_abb = ensure_list(loc_abb)
    get_all_locs = not loc_abb

    nhsn_disease_map = {
        "COVID-19": "totalconfc19newadm",
        "Influenza": "totalconfflunewadm",
        "RSV": "totalconfrsvnewadm",
    }

    disease_valid = (
        list(nhsn_disease_map.keys())
        if get_all_diseases
        else [x for x in disease if x in nhsn_disease_map.keys()]
    )

    raw_disease_col = [nhsn_disease_map.get(x) for x in disease_valid]

    inv_nhsn_disease_map = {nhsn_disease_map.get(x): x for x in disease_valid}

    filters = []
    if not get_all_locs:
        filters.append(pl.col("jurisdiction").is_in(loc_abb))
    if start_date:
        filters.append(pl.col("weekendingdate") >= start_date)
    if end_date:
        filters.append(pl.col("weekendingdate") <= end_date)

    datacat_dataset = (
        datacat.public.stf.nhsn_hrd_prelim if prelim else datacat.public.stf.nhsn_hrd
    )

    dat = (
        datacat_dataset.load.get_dataframe(
            output="pl_lazy", version=f"<={as_of.strftime('%Y-%m-%d')}"
        )
        .select(raw_disease_col + ["weekendingdate", "jurisdiction"])
        .with_columns(
            pl.col("jurisdiction").replace_strict(
                {"USA": "US"}, default=pl.col("jurisdiction")
            )
        )
        .filter(*filters)
        .rename(inv_nhsn_disease_map)
        .unpivot(
            on=disease_valid,
            index=["weekendingdate", "jurisdiction"],
            variable_name="disease",
            value_name="hospital_admissions",
        )
        .sort(
            ["jurisdiction", "disease", "weekendingdate"],
            descending=[False, False, True],
        )
    )

    if not get_all_diseases:
        result_disease = dat.unique("disease").collect().get_column("disease").to_list()
        if missing_diseases := set(disease) - set(result_disease):
            warnings.warn(
                f"Requested diseases {missing_diseases} not found in results."
            )
    if not get_all_locs:
        result_loc_abbr = (
            dat.unique("jurisdiction").collect().get_column("jurisdiction").to_list()
        )
        if missing_locs := set(loc_abb) - set(result_loc_abbr):
            warnings.warn(f"Requested locations {missing_locs} not found in results.")

    if not lazy:
        dat = dat.collect()
    return dat


@overload
def get_nssp(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    lazy: Literal[True] = ...,
) -> pl.LazyFrame: ...


@overload
def get_nssp(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    lazy: Literal[False] = ...,
) -> pl.DataFrame: ...


def get_nssp(
    disease: str | Iterable[str] | None = None,
    loc_abb: str | Iterable[str] | None = None,
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    lazy: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Retrieve and filter NSSP emergency department data.

    This function retrieves vintages of NSSP emergency department
    visits data specified by the `as_of` date from the
    datacat.public.stf catalog. It filters data for a specific disease
    and location, within optional date boundaries, as available up to
    a specified reference date.

    Parameters
    ----------
    disease
        The disease to filter for ("COVID-19", "Influenza", "RSV", or "Total"). If None, all diseases are included.
    loc_abb
        Location abbreviation to filter for. If None, all locations are included.
    dataset
        One of the two datasets to retrieve from datacat: "gold" or
        "comprehensive" (defaults to "gold").
    as_of
        Reference date for data availability. Only data available as of this date will be used.
        If None, all available data will be used (defaults to None).
    start_date
        Start date for filtering data (inclusive). If None, no lower bound is applied (defaults to None).
    end_date
        End date for filtering data (inclusive). If None, no upper bound is applied (defaults to None).
    lazy
        Whether to return a lazy frame (defaults to True). If True, returns a
        `pl.LazyFrame`; if False, returns a `pl.DataFrame`.

    Returns
    -------
    pl.DataFrame | pl.LazyFrame
        Aggregated ED counts with columns:
        `reference_date`, `disease`, and `value`.

    Raises
    ------
    ValueError
        If the specified dataset is invalid or the specified location
        abbreviation is not found in the data.

    Notes
    -----
    - The function handles special naming for COVID-19/Omicron, converting it to "COVID-19".
    - The function only includes data from parquet files with dates up to and including the as_of date.
    """
    if as_of is None:
        as_of = dt.date.max

    loc_abb = ensure_list(loc_abb)
    get_all_locs = not loc_abb

    disease = ensure_list(disease)
    get_all_diseases = not disease

    dataset_map = {
        "gold": datacat.public.stf.nssp_gold_v1,
        "comprehensive": datacat.public.stf.comprehensive_nssp_gold,
    }

    if not (datacat_dataset := dataset_map.get(dataset)):
        raise ValueError(
            f"Invalid dataset: {dataset!r}. Expected one of: {set(dataset_map)!r}."
        )

    national_required = get_all_locs or "US" in loc_abb

    filters = [
        pl.col("metric") == "count_ed_visits",
    ]

    if not get_all_diseases:
        filters.append(pl.col("disease").is_in(disease))
    if start_date:
        filters.append(pl.col("reference_date") >= start_date)
    if end_date:
        filters.append(pl.col("reference_date") <= end_date)

    dat = (
        datacat_dataset.load.get_dataframe(
            output="pl_lazy", version=f"<={as_of.strftime('%Y-%m-%d')}"
        )
        .with_columns(
            pl.col("disease").cast(pl.String).replace("COVID-19/Omicron", "COVID-19")
        )
        .filter(*filters)
    )

    state_locs = [loc for loc in loc_abb if loc != "US"]
    state_dat = (
        dat if get_all_locs else dat.filter(pl.col("geo_value").is_in(state_locs))
    )

    combined_dat = (
        pl.concat(
            [
                state_dat,
                dat.with_columns(pl.lit("US").alias("geo_value").cast(pl.Categorical)),
            ]
        )
        if national_required
        else state_dat
    )

    result = (
        combined_dat.group_by("reference_date", "disease", "geo_value")
        .agg(pl.col("value").sum())
        .sort(
            ["geo_value", "disease", "reference_date"], descending=[False, False, True]
        )
    )

    if not get_all_diseases:
        result_disease = (
            result.unique("disease").collect().get_column("disease").to_list()
        )
        if missing_diseases := set(disease) - set(result_disease):
            warnings.warn(
                f"Requested diseases {missing_diseases} not found in results."
            )

    if not get_all_locs:
        result_loc_abbr = (
            result.unique("geo_value").collect().get_column("geo_value").to_list()
        )
        if missing_locs := set(loc_abb) - set(result_loc_abbr):
            warnings.warn(f"Requested locations {missing_locs} not found in results.")

    if not lazy:
        result = result.collect()
    return result
