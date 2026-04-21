"""Forecasttools helpers exposed through the cfa.stf.forecasttools namespace."""

from collections.abc import MutableSequence

from . import arviz_helpers as arviz
from .location_table import LOCATION_LIST, get_us_loc_pop_tbl


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


__all__ = ["ensure_listlike", "get_us_loc_pop_tbl", "LOCATION_LIST", "arviz"]
