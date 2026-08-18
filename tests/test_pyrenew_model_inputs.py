import datetime as dt
import json
from unittest.mock import patch

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
        report_date=dt.date(2026, 7, 28),
        first_training_date=dt.date(2026, 4, 29),
        last_training_date=dt.date(2026, 7, 27),
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
        as_of=run.report_date,
    )
    mock_delay.assert_called_once_with(disease="covid", as_of=run.report_date)
    mock_right_truncation.assert_called_once_with(
        state_abb="CA",
        disease="covid",
        as_of=run.report_date,
        reference_date=run.report_date,
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
