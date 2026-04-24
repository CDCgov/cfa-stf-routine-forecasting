import datetime as dt
from copy import deepcopy
from typing import Literal

import arviz as az
import numpy as np
import polars as pl
import xarray as xr


def get_all_dims(idata: xr.DataTree) -> set[str]:
    """
    Get all unique dimension names from a xr.DataTree object.
    This function iterates through all groups in the DataTree object and
    collects all unique dimension names across all groups.
    Parameters
    ----------
    idata : xr.DataTree
        The xr.DataTree object to extract dimensions from.
    Returns
    -------
    set[str]
        A set containing all unique dimension names found across all groups
        in the DataTree object.
    """

    dims = set(str(dim) for group in idata.values() for dim in group.dims)
    return dims


def replace_all_dim_suffix(
    idata: xr.DataTree,
    new_suffixes: list[str],
    dim_prefix: str = "dim_",
    inplace: bool = False,
) -> xr.DataTree | None:
    """
    Replace dimension suffixes in an DataTree object.

    This function renames dimensions by replacing their suffixes with new ones.
    Dimensions are expected to end with patterns like 'dim_0', 'dim_1', etc.,
    which will be replaced with the corresponding new suffixes.

    Parameters
    ----------
    idata : arviz.DataTree
        The DataTree object containing dimensions to rename.
    new_suffixes : list of str
        List of new suffixes to replace the existing 'dim_i' suffixes.
        The order corresponds to dim_0, dim_1, dim_2, etc.
    dim_prefix : str, default "dim_"
        The prefix used for dimension names. This is used to identify
        the dimensions to rename.
    inplace : bool, default False
        If ``True``, modify the DataTree object inplace, otherwise, return the modified copy.

    Returns
    -------
    DataTree
        A new DataTree object by default.
        When `inplace==True` perform renaming in-place and return `None`
    """
    suffix_dict = {f"{dim_prefix}{i}": new for i, new in enumerate(new_suffixes)}
    out = idata if inplace else idata.copy()

    def rename_ds_from_suffix_dict(ds, suffix_dict):
        original_dim_names = get_all_dims(ds)
        name_dict = {
            original_dim_name: original_dim_name.replace(suffix, new)
            for original_dim_name in original_dim_names
            for suffix, new in suffix_dict.items()
            if original_dim_name.endswith(suffix)
        }
        return ds.rename(name_dict)

    out.update(
        out.map_over_datasets(lambda ds: rename_ds_from_suffix_dict(ds, suffix_dict))
    )
    if inplace:
        return None
    else:
        return out


def assign_coords_from_start_step(
    idata: xr.DataTree,
    dim_name: str,
    start_date: dt.date,
    interval: dt.timedelta = dt.timedelta(days=1),
    inplace: bool = False,
) -> xr.DataTree | None:
    """
    Assign coordinates to a dimension based on a start date and step interval.

    This function updates the coordinates of a specified dimension across all groups
    in an ArviZ DataTree object by creating a sequence of dates starting from
    a given start date and incrementing by a specified time interval.

    Parameters
    ----------
    idata : xr.DataTree
        The ArviZ DataTree object to update.
    dim_name : str
        The name of the dimension to assign new coordinates to.
    start_date : dt.date
        The starting date for the coordinate sequence.
    interval : dt.timedelta, optional
        The time interval between consecutive coordinate values. Default is 1 day.
    inplace : bool, optional
        If True, modify the DataTree object in place and return None.
        If False, return a deep copy with updated coordinates. Default is False.

    Returns
    -------
    xr.DataTree or None
        If inplace is False, returns a new DataTree object with updated
        coordinates. If inplace is True, returns None and modifies the input
        object directly.

    Notes
    -----
    The function iterates through all groups in the DataTree object and
    updates the specified dimension coordinates only in groups where the
    dimension exists. The new coordinates are generated as a sequence of
    dates starting from `start_date` and incrementing by `interval` for
    each position in the dimension.
    """
    out = idata if inplace else deepcopy(idata)
    for group in list(out.groups()):
        ds = getattr(out, group)
        if dim_name in ds.dims:
            n = ds.sizes[dim_name]
            coords = np.arange(start_date, start_date + interval * n, interval).astype(
                "datetime64[D]"
            )
            new_ds = ds.assign_coords({dim_name: coords})
            setattr(out, group, new_ds)
    if inplace:
        return None
    else:
        return out


def prune_chains_by_rel_diff(
    idata: xr.DataTree, rel_diff_thresh: float, inplace=False
) -> xr.DataTree | None:
    """
    Prune MCMC chains based on relative difference in log probability/likelihood.

    This function removes chains whose average log probability or log likelihood
    is significantly lower than the best-performing chain, based on a relative
    difference threshold.

    Parameters
    ----------
    idata : xr.DataTree
        ArviZ DataTree object containing MCMC samples and statistics.
    rel_diff_thresh : float
        Relative difference threshold for keeping chains. Chains with relative
        performance above this threshold compared to the best chain are kept.
        Value should be between 0 and 1, where 1 means only keep chains identical
        to the best, and 0 means keep all chains.
    inplace : bool, default False
        If True, modify the input DataTree object in place. If False,
        return a new DataTree object with pruned chains.

    Returns
    -------
    xr.DataTree or None
        If inplace=False, returns a new DataTree object with only the
        chains that meet the threshold criteria. If inplace=True, returns None
        and modifies the input object directly.

    Raises
    ------
    ValueError
        If neither log probability ('lp' in sample_stats) nor log_likelihood
        data is found in the DataTree object.

    Notes
    -----
    The function first attempts to use log probability data from sample_stats['lp'],
    falling back to log_likelihood data if available. The relative difference is
    calculated as: 1 - (best_chain_value - chain_value) / |best_chain_value|
    """
    if "sample_stats" in idata.groups() and "lp" in idata["sample_stats"]:
        l_data = idata["sample_stats"]["lp"]
    elif "log_likelihood" in idata.groups():
        l_data = idata["log_likelihood"]
    else:
        raise ValueError(
            "Neither log_prob ('lp') nor log_likelihood data found in DataTree"
        )
    l_by_chain = (
        pl.from_pandas(
            l_data.mean(dim=set(l_data.dims) - {"chain"}).to_dataframe(),
            include_index=True,
        )
        .unpivot(index=["chain"])
        .group_by("chain")
        .agg(pl.col("value").sum())
        .sort("chain")
    )
    best_chain_val = l_by_chain.get_column("value").max()

    chains_to_keep = (
        l_by_chain.filter(
            1 - (best_chain_val - pl.col("value")) / abs(best_chain_val)
            > rel_diff_thresh
        )
        .get_column("chain")
        .to_list()
    )
    return idata.sel(chain=chains_to_keep, inplace=inplace)
