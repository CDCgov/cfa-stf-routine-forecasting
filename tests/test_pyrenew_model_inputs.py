import datetime as dt
import json
from dataclasses import replace
from unittest.mock import patch

import polars as pl
import pytest

from cfa.stf.routine.pyrenew_hew.model_inputs import (
    PyRenewModelInputs,
    resolve_pyrenew_model_inputs,
    serialize_pyrenew_model_params,
)
from tests.factories import make_test_forecast_run


def _run(tmp_path):
    return make_test_forecast_run(
        output_dir=tmp_path,
        disease="covid",
        loc="CA",
        forecast_date=dt.date(2026, 7, 28),
        first_training_date=dt.date(2026, 4, 29),
        max_allowed_training_date=dt.date(2026, 7, 27),
        model_name="pyrenew_e",
        loc_pop=39_000_000,
    )


@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_right_truncation_pmf",
    return_value=[0.25, 0.75],
)
@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_delay_pmf",
    return_value=[0.2, 0.3, 0.5],
)
@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_generation_interval_pmf",
    return_value=[0.4, 0.6],
)
def test_resolve_pyrenew_model_inputs_loads_run_vintaged_pmfs(
    mock_generation_interval,
    mock_delay,
    mock_right_truncation,
    tmp_path,
):
    run = _run(tmp_path)

    model_inputs = resolve_pyrenew_model_inputs(run=run, fit_ed_visits=True)

    assert model_inputs.generation_interval_pmf == (0.4, 0.6)
    assert model_inputs.infection_to_admission_pmf == pytest.approx([0.0, 0.375, 0.625])
    assert model_inputs.right_truncation_pmf == (0.25, 0.75)
    mock_generation_interval.assert_called_once_with(
        disease="covid",
        as_of=run.forecast_date,
    )
    mock_delay.assert_called_once_with(disease="covid", as_of=run.forecast_date)
    mock_right_truncation.assert_called_once_with(
        state_abb="CA",
        disease="covid",
        as_of=run.forecast_date,
        reference_date=run.forecast_date,
    )


@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_right_truncation_pmf",
    side_effect=ValueError("unavailable"),
)
@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_delay_pmf",
    return_value=[0.0, 1.0],
)
@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.get_nnh_generation_interval_pmf",
    return_value=[1.0],
)
def test_resolve_pyrenew_model_inputs_allows_missing_truncation_without_ed_visits(
    _mock_generation_interval,
    _mock_delay,
    _mock_right_truncation,
    tmp_path,
):
    model_inputs = resolve_pyrenew_model_inputs(
        run=_run(tmp_path),
        fit_ed_visits=False,
    )

    assert model_inputs.right_truncation_pmf == (1.0,)


@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.approx_lognorm",
    return_value=(1.2, 0.3),
)
def test_serialize_pyrenew_model_params_uses_run_population(
    mock_approx_lognorm,
    tmp_path,
):
    run = _run(tmp_path)
    model_inputs = PyRenewModelInputs(
        generation_interval_pmf=(0.4, 0.6),
        infection_to_admission_pmf=(0.0, 0.375, 0.625),
        right_truncation_pmf=(0.25, 0.75),
    )

    serialize_pyrenew_model_params(
        run=run,
        model_inputs=model_inputs,
        save_dir=tmp_path,
    )

    with open(tmp_path / "model_params.json") as file:
        model_params = json.load(file)
    assert model_params == {
        "population_size": 39_000_000,
        "pop_fraction": [1],
        "generation_interval_pmf": [0.4, 0.6],
        "right_truncation_pmf": [0.25, 0.75],
        "inf_to_hosp_admit_lognormal_loc": 1.2,
        "inf_to_hosp_admit_lognormal_scale": 0.3,
        "inf_to_hosp_admit_pmf": [0.0, 0.375, 0.625],
    }
    mock_approx_lognorm.assert_called_once()


@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.approx_lognorm", return_value=(1.2, 0.3)
)
def test_shared_timeline_after_nssp_vintage_preserves_reporting_age(_mock, tmp_path):
    from cfa.stf.routine.data.prep_data import serialize_data

    run = _run(tmp_path)
    nssp = replace(
        run.nssp,
        data=run.nssp.data.filter(pl.col("date") < dt.date(2026, 7, 25)),
        freshness=replace(
            run.nssp.freshness, selected_version_date=dt.date(2026, 7, 25)
        ),
    )
    run = replace(run, surveillance=replace(run.surveillance, nssp=nssp))
    assert run.last_training_date == dt.date(2026, 7, 27)
    assert run.right_truncation_offset == -3
    inputs = PyRenewModelInputs((1.0,), (0.0, 1.0), (0.25, 0.75))
    serialize_pyrenew_model_params(run=run, model_inputs=inputs, save_dir=tmp_path)
    params = json.loads((tmp_path / "model_params.json").read_text())
    assert params["right_truncation_pmf"] == [0.0, 0.0, 0.0, 0.25, 0.75]
    serialize_data(run)
    data = json.loads((run.data_dir / "data_for_model_fit.json").read_text())
    assert data["right_truncation_offset"] == 0
