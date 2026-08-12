import datetime as dt
import json
from unittest.mock import patch

import polars as pl
import pytest
from tests.factories import make_test_forecast_data

from cfa.stf.routine.data.prep_data import (
    process_and_save_loc_data,
    process_and_save_loc_param,
)
from cfa.stf.routine.utils.common_utils import append_prop_data_to_combined_data


@pytest.mark.parametrize(
    ("sources", "expected_variables"),
    [
        pytest.param(
            ("nssp",),
            {
                "observed_ed_visits",
                "other_ed_visits",
                "prop_disease_ed_visits",
            },
            id="nssp-only",
        ),
        pytest.param(
            ("nhsn",),
            {"observed_hospital_admissions"},
            id="nhsn-only",
        ),
        pytest.param(
            ("nssp", "nhsn"),
            {
                "observed_ed_visits",
                "other_ed_visits",
                "observed_hospital_admissions",
                "prop_disease_ed_visits",
            },
            id="both",
        ),
    ],
)
def test_process_and_save_loc_data_handles_present_sources(
    tmp_path,
    sources,
    expected_variables,
):
    forecast_data = make_test_forecast_data(sources=sources)

    process_and_save_loc_data(
        forecast_data=forecast_data,
        save_dir=tmp_path,
    )
    append_prop_data_to_combined_data(tmp_path / "combined_data.tsv")

    with open(tmp_path / "data_for_model_fit.json") as file:
        model_data = json.load(file)
    assert (model_data["nssp_training_data"] is not None) == ("nssp" in sources)
    assert (model_data["nhsn_training_data"] is not None) == ("nhsn" in sources)
    if "nssp" in sources:
        assert set(model_data["nssp_training_data"]) == {
            "date",
            "geo_value",
            "observed_ed_visits",
            "other_ed_visits",
        }
    if "nhsn" in sources:
        assert set(model_data["nhsn_training_data"]) == {
            "weekendingdate",
            "jurisdiction",
            "hospital_admissions",
        }

    combined_data = pl.read_csv(tmp_path / "combined_data.tsv", separator="\t")
    assert set(combined_data.get_column(".variable")) == expected_variables


@patch("cfa.stf.routine.data.prep_data.approx_lognorm", return_value=(1.2, 0.3))
@patch("cfa.stf.routine.data.prep_data.get_us_loc_pop_tbl")
@patch("cfa.stf.routine.data.prep_data.get_nnh_right_truncation_pmf")
@patch("cfa.stf.routine.data.prep_data.get_nnh_delay_pmf")
@patch("cfa.stf.routine.data.prep_data.get_nnh_generation_interval_pmf")
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
        disease="covid",
        fit_ed_visits=True,
        save_dir=tmp_path,
        as_of=as_of,
    )

    mock_generation_interval.assert_called_once_with(
        disease="covid",
        as_of=as_of,
    )
    mock_delay.assert_called_once_with(disease="covid", as_of=as_of)
    mock_right_truncation.assert_called_once_with(
        state_abb="CA",
        disease="covid",
        as_of=as_of,
        reference_date=as_of,
    )
