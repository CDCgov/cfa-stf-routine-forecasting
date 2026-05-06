import datetime as dt
from collections.abc import Iterable
from typing import Literal, overload

import polars as pl
from cfa.dataops import datacat

from cfa.stf.forecasttools import ensure_list

nhsn_disease_map = {
    "COVID-19": "totalconfc19newadm",
    "Influenza": "totalconfflunewadm",
    "RSV": "totalconfrsvnewadm",
}

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
        Filtered data with columns:
        `weekendingdate`, `jurisdiction`, and `hospital_admissions`.
    """
    if as_of is None:
        as_of = dt.date.max

    disease = ensure_list(disease) if disease else list(nhsn_disease_map.keys())

    disease_col = [nhsn_disease_map[x] for x in disease]
    loc_abb = ensure_list(loc_abb) if loc_abb else None

    filters = []
    if loc_abb is not None:
        filters.append(pl.col("jurisdiction").is_in(loc_abb))
    if start_date is not None:
        filters.append(pl.col("weekendingdate") >= start_date)
    if end_date is not None:
        filters.append(pl.col("weekendingdate") <= end_date)

    datacat_dataset = (
        datacat.public.stf.nhsn_hrd_prelim if prelim else datacat.public.stf.nhsn_hrd
    )

    output = "pl_lazy" if lazy else "pl"
    dat = datacat_dataset.load.get_dataframe(
        output=output, version=f"<={as_of.strftime('%Y-%m-%d')}"
    )

    filtered_dat = (
        dat.with_columns(pl.col("jurisdiction").cast(pl.String).replace("USA", "US"))
        .filter(*filters)
        .select(disease_col + ["weekendingdate", "jurisdiction"])
    )
    # need to pivot and add a new column for disease name

    # .alias("hospital_admissions")
    return filtered_dat


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
        The disease to filter for ("COVID-19", "Influenza", or "RSV"). If None, all diseases are included.
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

    output = "pl_lazy" if lazy else "pl"

    dataset_map = {
        "gold": datacat.public.stf.nssp_gold_v1,
        "comprehensive": datacat.public.stf.comprehensive_nssp_gold,
    }

    if dataset not in dataset_map:
        raise ValueError(
            f"Invalid dataset: {dataset!r}. Expected one of: {set(dataset_map)}."
        )
    datacat_dataset = dataset_map[dataset]

    dat = datacat_dataset.load.get_dataframe(
        output=output, version=f"<={as_of.strftime('%Y-%m-%d')}"
    ).with_columns(
        pl.col("disease").cast(pl.String).replace("COVID-19/Omicron", "COVID-19")
    )

    valid_locs = (
        dat.select("geo_value").unique().collect()
        if lazy
        else dat.select("geo_value").unique()
    ).get_column("geo_value").sort().to_list() + ["US"]

    loc_abb = ensure_list(loc_abb) if loc_abb else valid_locs

    US_required = "US" in loc_abb
    non_US_locs = [loc for loc in loc_abb if loc != "US"]

    invalid_locs = set(non_US_locs) - set(valid_locs)
    if invalid_locs:
        raise ValueError(f"Invalid location abbreviation(s): {invalid_locs}")

    valid_diseases = (
        (
            dat.select("disease").unique().collect()
            if lazy
            else dat.select("disease").unique()
        )
        .get_column("disease")
        .to_list()
    )
    disease = ensure_list(disease) if disease else valid_diseases

    if "Total" not in disease:
        disease.append("Total")

    invalid_diseases = set(disease) - set(valid_diseases)
    if invalid_diseases:
        raise ValueError(f"Invalid disease(s): {invalid_diseases}")

    filters = [
        pl.col("disease").is_in(disease),
        pl.col("metric") == "count_ed_visits",
    ]

    if start_date is not None:
        filters.append(pl.col("reference_date") >= start_date)
    if end_date is not None:
        filters.append(pl.col("reference_date") <= end_date)

    dat = dat.filter(*filters)

    non_US_dat = dat.filter(pl.col("geo_value").is_in(non_US_locs))

    combined_dat = (
        pl.concat(
            [
                non_US_dat,
                dat.with_columns(pl.lit("US").alias("geo_value").cast(pl.Categorical)),
            ]
        )
        if US_required
        else non_US_dat
    )

    result = (
        combined_dat.group_by("reference_date", "disease", "geo_value")
        .agg(pl.col("value").sum())
        .sort(
            ["geo_value", "disease", "reference_date"], descending=[False, False, True]
        )
    )

    return result
