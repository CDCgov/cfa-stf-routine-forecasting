from collections.abc import Iterable
from typing import overload

import polars as pl
import polars.selectors as cs


@overload
def ensure_list[T](x: None) -> None: ...


@overload
def ensure_list[T](x: T | Iterable[T]) -> list[T]: ...


def ensure_list[T](x: T | Iterable[T] | None) -> list[T] | None:
    if x is None:
        return []
    elif isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
        return list(x)
    else:
        return [x]


def coalesce_common_columns(
    df: pl.DataFrame, suffix: str, new_colname: str | None = None
) -> pl.DataFrame:
    """
    Coalesce multiple columns with a common suffix into a single column.
    This function finds all columns in the DataFrame that end with the specified
    suffix, coalesces them (takes the first non-null value across the columns),
    and creates a new column with the coalesced values. The original columns
    with the suffix are then removed from the DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        The input Polars DataFrame to process.
    suffix : str
        The suffix to match column names for coalescing.
    new_colname : str | None, optional
        The name for the new coalesced column.
        If None, defaults to the suffix with leading underscores stripped.

    Returns:
        pl.DataFrame: A new DataFrame with the coalesced column and original suffix columns removed.
    """
    if new_colname is None:
        new_colname = suffix.lstrip("_")
    coalesced_df = df.with_columns(
        pl.coalesce(cs.ends_with(suffix)).alias(new_colname)
    ).select(cs.exclude(cs.ends_with(suffix)))

    return coalesced_df
