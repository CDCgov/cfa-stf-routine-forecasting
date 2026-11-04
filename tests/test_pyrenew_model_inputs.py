import datetime as dt
import json
from dataclasses import replace
from unittest.mock import patch

import jax.numpy as jnp
import polars as pl
import pytest
from numpyro.handlers import seed, trace
from pyrenew_multisignal.hew import PyrenewHEWData

from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import PyRenewPipeline
from cfa.stf.routine.pyrenew_hew.model_inputs import (
    PyRenewModelInputs,
    resolve_pyrenew_model_inputs,
    serialize_pyrenew_model_params,
)
from cfa.stf.routine.pyrenew_hew.utils import build_pyrenew_hew_model_from_dir
from tests.factories import make_test_forecast_run


@pytest.mark.parametrize("fit_hospital_admissions", [False, True])
@pytest.mark.parametrize("omitted_days, expected_fraction", [(0, 0.25), (1, 0.75)])
@patch(
    "cfa.stf.routine.pyrenew_hew.model_inputs.approx_lognorm", return_value=(1.2, 0.3)
)
@patch(
    "cfa.stf.routine.pyrenew_hew.forecast_pyrenew.resolve_pyrenew_model_inputs",
    return_value=PyRenewModelInputs((0.4, 0.6), (0.0, 0.5, 0.5), (0.25, 0.5, 0.25)),
)
def test_pyrenew_reporting_age_with_nssp_vintage_one_day_before_forecast_date(
    _mock_inputs,
    _mock_lognorm,
    tmp_path,
    fit_hospital_admissions,
    omitted_days,
    expected_fraction,
):
    pipeline = PyRenewPipeline(
        disease="covid",
        loc="CA",
        output_dir=tmp_path,
        forecast_date=dt.date(2026, 11, 1),
        priors_path=PRODUCTION_PRIORS,
        n_chains=1,
        n_warmup=1,
        n_samples=1,
        fit_ed_visits=True,
        forecast_ed_visits=True,
        fit_hospital_admissions=fit_hospital_admissions,
        forecast_hospital_admissions=fit_hospital_admissions,
    )
    run = make_test_forecast_run(
        output_dir=tmp_path,
        model_name=pipeline.model_name,
        forecast_date=dt.date(2026, 11, 1),
        first_training_date=dt.date(2026, 10, 1),
        last_training_date=dt.date(2026, 10, 30) - dt.timedelta(days=omitted_days),
        sources=pipeline.sources,
        loc_pop=39_000_000,
    )
    nssp = replace(
        run.nssp,
        freshness=replace(
            run.nssp.freshness, selected_version_date=dt.date(2026, 10, 31)
        ),
    )
    nhsn = run.nhsn
    if fit_hospital_admissions:
        # A later NHSN observation extends the latent timeline past the last
        # NSSP observation, requiring a zero-padded reporting PMF.
        nhsn = replace(
            nhsn, data=nhsn.data.head(1).with_columns(date=dt.date(2026, 10, 31))
        )
    run = replace(run, surveillance=replace(run.surveillance, nssp=nssp, nhsn=nhsn))
    pipeline.prepare_input_artifacts(run)

    data = PyrenewHEWData.from_json(
        run.data_dir / "data_for_model_fit.json",
        fit_ed_visits=True,
        fit_hospital_admissions=fit_hospital_admissions,
    )
    model = build_pyrenew_hew_model_from_dir(
        run.model_dir,
        fit_ed_visits=True,
        fit_hospital_admissions=fit_hospital_admissions,
    )
    assert run.forecast_through == dt.date(2026, 11, 28)
    assert data.right_truncation_offset == (
        0 if fit_hospital_admissions else omitted_days
    )

    n_init = model.latent_infection_process_rv.n_initialization_points

    def observation_mean(offset):
        # Same latent incidence and random seed isolate the effect of reporting
        # truncation in the actual PyRenew ED observation likelihood.
        samples = trace(seed(model.ed_visit_obs_process_rv, rng_seed=0)).get_trace(
            latent_infections=jnp.full(data.n_days_post_init + n_init, 0.001),
            population_size=data.population_size,
            data_observed=data.data_observed_disease_ed_visits,
            model_t_observed=data.model_t_obs_ed_visits,
            model_t_first_latent_infection=-n_init,
            right_truncation_offset=offset,
        )
        return samples["observed_ed_visits"]["fn"].mean

    fraction = observation_mean(data.right_truncation_offset) / observation_mean(None)
    assert fraction.tolist() == pytest.approx([1.0, expected_fraction])


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
