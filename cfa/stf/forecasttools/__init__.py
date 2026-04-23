"""Forecasttools helpers exposed through the cfa.stf.forecasttools namespace."""

from importlib import import_module

from .location_constants import LOCATION_LIST
from .utils import coalesce_common_columns, ensure_listlike


def __getattr__(name):
    if name == "arviz":
        return import_module(".arviz_helpers", __name__)
    if name == "get_us_loc_pop_tbl":
        return import_module(".location_table", __name__).get_us_loc_pop_tbl
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "coalesce_common_columns",
    "ensure_listlike",
    "get_us_loc_pop_tbl",
    "LOCATION_LIST",
    "arviz",
]
