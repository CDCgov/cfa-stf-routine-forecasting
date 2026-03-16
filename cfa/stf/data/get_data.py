import datetime as dt
from typing import Literal

import polars as pl
from cfa.dataops import datacat

nhsn_disease_map = {
    "COVID-19": "totalconfc19newadm",
    "Influenza": "totalconfflunewadm",
    "RSV": "totalconfrsvnewadm",
}

NSSPDataset = Literal["gold", "latest_comprehensive"]


def get_nhsn_hrd(
    disease: str,
    loc_abbr: str,
    prelim: bool = True,
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pl.DataFrame:
    """
    Retrieve and filter NHSN hospital respiratory data based on specified criteria.

    This function retrieves vintages of NHSN hrd data specified by
    the `as_of` date from the datacat.public.stf catalog, applies filters
    for a specific disease, location, and dates if provided.

    Parameters
    ----------
    disease
        The disease to filter for ("COVID-19", "Influenza", or "RSV").
    loc_abbr
        The location abbreviation to filter for.
    prelim
        Whether to retrieve "nhsn_hrd_prelim" data as opposed to "nhsn_hrd" data (defaults to True).
    as_of
        The reference date for filtering. If None, the most recent 'as_of' date is used.
    start_date
        The start date for the time period to include. If None, no lower bound is applied.
    end_date
        The end date for the time period to include. If None, no upper bound is applied.

    Returns
    -------
    pl.DataFrame
        Filtered data with columns:
        `weekendingdate`, `jurisdiction`, and `hospital_admissions`.
    """

    disease_col = nhsn_disease_map[disease]

    filters = [
        pl.col("jurisdiction") == loc_abbr,
    ]
    if start_date is not None:
        filters.append(pl.col("weekendingdate") >= start_date)
    if end_date is not None:
        filters.append(pl.col("weekendingdate") <= end_date)

    if as_of is None:
        as_of = dt.date(3000, 1, 1)

    datacat_dataset = (
        datacat.public.stf.nhsn_hrd_prelim if prelim else datacat.public.stf.nhsn_hrd
    )

    dat = datacat_dataset.load.get_dataframe(
        output="pl", version=f"<={as_of.strftime('%Y-%m-%d')}"
    )

    filtered_dat = (
        dat.with_columns(pl.col("jurisdiction").cast(pl.String).replace("USA", "US"))
        .filter(*filters)
        .select(
            "weekendingdate",
            "jurisdiction",
            pl.col(disease_col).alias("hospital_admissions"),
        )
    )
    return filtered_dat


def get_nssp(
    disease: str,
    loc_abb: str,
    dataset: NSSPDataset = "gold",
    as_of: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pl.DataFrame:
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
        The disease to filter for ("COVID-19", "Influenza", or "RSV").
    loc_abb
        Location abbreviation to filter for.
    dataset
        One of the two datasets to retrieve from datacat: "gold" or
        "latest_comprehensive" (defaults to "gold").
    as_of
        Reference date for data availability. Only data available as of this date will be used.
        If None, all available data will be used (defaults to None).
    start_date
        Start date for filtering data (inclusive). If None, no lower bound is applied (defaults to None).
    end_date
        End date for filtering data (inclusive). If None, no upper bound is applied (defaults to None).

    Returns
    -------
    pl.DataFrame
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
        as_of = dt.date(3000, 1, 1)

    dataset_map = {
        "gold": datacat.public.stf.nssp_gold,
        "latest_comprehensive": datacat.public.stf.nssp_latest_comprehensive,
    }
    if dataset not in dataset_map:
        raise ValueError(
            f"Invalid dataset: {dataset!r}. Expected one of: {set(dataset_map)}."
        )
    datacat_dataset = dataset_map[dataset]

    filters = [
        pl.col("disease").is_in([disease, "Total"]),
        pl.col("metric") == "count_ed_visits",
    ]

    if start_date is not None:
        filters.append(pl.col("reference_date") >= start_date)
    if end_date is not None:
        filters.append(pl.col("reference_date") <= end_date)
    if loc_abb != "US":
        filters.append(pl.col("geo_value") == loc_abb)

    dat = datacat_dataset.load.get_dataframe(
        output="pl", version=f"<={as_of.strftime('%Y-%m-%d')}"
    )

    valid_locs = dat.unique("geo_value").get_column("geo_value").to_list() + ["US"]

    if loc_abb not in valid_locs:
        raise ValueError(f"Invalid location abbreviation: {loc_abb}")

    dat_filtered = dat.with_columns(
        pl.col("disease").cast(pl.String).replace("COVID-19/Omicron", "COVID-19")
    ).filter(*filters)

    result = (
        dat_filtered.group_by("reference_date", "disease")
        .agg(pl.col("value").sum())
        .sort("reference_date")
    )

    return result
