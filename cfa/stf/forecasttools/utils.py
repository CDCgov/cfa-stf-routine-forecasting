from collections.abc import MutableSequence

import polars as pl
import polars.selectors as cs


def ensure_listlike(x):
    """
    Ensure that an object either behaves like a
    :class:`MutableSequence` and if not return a
    one-item :class:`list` containing the object.
    Useful for handling list-of-strings inputs
    alongside single strings.
    Based on this _`StackOverflow approach
    <https://stackoverflow.com/a/66485952>`.

    Parameters
    ----------
    x
        The item to ensure is :class:`list`-like.
    Returns
    -------
    MutableSequence
        ``x`` if ``x`` is a :class:`MutableSequence`
        otherwise ``[x]`` (i.e. a one-item list containing
        ``x``.
    """
    return x if isinstance(x, MutableSequence) else [x]


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
