import polars as pl

from pipelines.utils.common_utils import run_r_code

__all__ = ["get_us_loc_pop_tbl", "LOCATION_LIST"]


def get_us_loc_pop_tbl():
    r_code = """
    dplyr::left_join(
    forecasttools::us_location_table,
    forecasttools::us_location_pop,
    by = "name"
) |>
    readr::write_csv(file = stdout(), na = "")
    """
    tbl_csv = run_r_code(r_code).stdout
    return pl.read_csv(tbl_csv)


# get_us_loc_pop_tbl().get_column("abbr").to_list()
LOCATION_LIST = [
    "US",
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "AS",
    "GU",
    "MP",
    "PR",
    "UM",
    "VI",
]
