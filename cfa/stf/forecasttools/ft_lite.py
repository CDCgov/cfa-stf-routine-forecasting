import polars as pl

from pipelines.utils.common_utils import run_r_code


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
