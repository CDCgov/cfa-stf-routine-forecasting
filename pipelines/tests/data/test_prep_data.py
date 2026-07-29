import datetime as dt
from unittest.mock import patch

import polars as pl

from pipelines.data.prep_data import process_and_save_loc_param


@patch("pipelines.data.prep_data.approx_lognorm", return_value=(1.2, 0.3))
@patch("pipelines.data.prep_data.get_us_loc_pop_tbl")
@patch("pipelines.data.prep_data.get_nnh_right_truncation_pmf")
@patch("pipelines.data.prep_data.get_nnh_delay_pmf")
@patch("pipelines.data.prep_data.get_nnh_generation_interval_pmf")
def test_process_and_save_loc_param_loads_pmfs_from_cfa_stf_data(
    mock_generation_interval,
    mock_delay,
    mock_right_truncation,
    mock_loc_pop,
    _mock_approx_lognorm,
    tmp_path,
):
    as_of = dt.date(2026, 7, 28)
    mock_generation_interval.return_value = [0.4, 0.6]
    mock_delay.return_value = [0.2, 0.3, 0.5]
    mock_right_truncation.return_value = [0.25, 0.75]
    mock_loc_pop.return_value = pl.DataFrame(
        {"abbr": ["CA"], "population": [39_000_000]}
    )

    process_and_save_loc_param(
        loc_abb="CA",
        disease="COVID-19",
        fit_ed_visits=True,
        save_dir=tmp_path,
        as_of=as_of,
    )

    mock_generation_interval.assert_called_once_with(
        disease="COVID-19",
        as_of=as_of,
    )
    mock_delay.assert_called_once_with(disease="COVID-19", as_of=as_of)
    mock_right_truncation.assert_called_once_with(
        loc_abb="CA",
        disease="COVID-19",
        as_of=as_of,
        reference_date=as_of,
    )
