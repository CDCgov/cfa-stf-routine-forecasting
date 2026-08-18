import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pytest

from cfa.stf.routine.fable.forecast_fable import FablePipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import PyRenewPipeline
from cfa.stf.routine.pyrenew_hew.model_inputs import PyRenewModelInputs
from tests.factories import make_test_surveillance_inputs


def _run(tmp_path, *, model_name="test_model", exclude_last_n_days=2):
    return ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 18),
        n_forecast_days=28,
        exclude_last_n_days=exclude_last_n_days,
        model_name=model_name,
        output_dir=tmp_path,
        surveillance=make_test_surveillance_inputs(),
    )


def _common_kwargs(tmp_path):
    return {
        "disease": "covid",
        "loc": "CA",
        "output_dir": tmp_path,
        "n_training_days": 90,
        "n_forecast_days": 28,
        "run_date": dt.date(2024, 12, 20),
    }


@patch("cfa.stf.routine.fable.forecast_fable.generate_epiweekly_data")
def test_fable_pipeline_aggregates_weekly_inputs(mock_generate, tmp_path):
    pipeline = FablePipeline(
        **_common_kwargs(tmp_path),
        n_samples=10,
        epiweekly=True,
    )
    run = _run(tmp_path, model_name=pipeline.model_name)

    pipeline.after_data_serialization(run, None)

    mock_generate.assert_called_once_with(run.data_dir, overwrite_daily=True)


@patch("cfa.stf.routine.fable.forecast_fable.fable_e_other_forecasts")
def test_fable_pipeline_forecasts_through_excluded_tail(mock_forecast, tmp_path):
    pipeline = FablePipeline(
        **_common_kwargs(tmp_path),
        n_samples=10,
    )
    run = _run(tmp_path, model_name=pipeline.model_name)

    pipeline.fit_and_forecast(run, None)

    mock_forecast.assert_called_once_with(run.model_dir, 30, 10)


def _pyrenew_pipeline(tmp_path, **overrides):
    kwargs = {
        **_common_kwargs(tmp_path),
        "priors_path": Path("priors.py"),
        "n_chains": 2,
        "n_warmup": 3,
        "n_samples": 4,
        "fit_ed_visits": True,
        "forecast_ed_visits": True,
        "rng_key": 123,
    }
    kwargs.update(overrides)
    return PyRenewPipeline(**kwargs)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"fit_wastewater": True},
            "Wastewater data loading is no longer supported",
        ),
        (
            {"forecast_ed_visits": False},
            "fitting to but not forecasting",
        ),
        (
            {"fit_ed_visits": False, "forecast_ed_visits": False},
            "pyrenew_null",
        ),
    ],
)
def test_pyrenew_pipeline_preserves_signal_validation(tmp_path, overrides, message):
    with pytest.raises(ValueError, match=message):
        _pyrenew_pipeline(tmp_path, **overrides).validate_configuration()


@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.serialize_pyrenew_model_params")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.copy_and_record_priors")
def test_pyrenew_pipeline_extends_common_data_preparation(
    mock_copy,
    mock_params,
    tmp_path,
):
    pipeline = _pyrenew_pipeline(tmp_path)
    run = _run(tmp_path, model_name=pipeline.model_name)
    model_inputs = PyRenewModelInputs(
        generation_interval_pmf=(1.0,),
        infection_to_admission_pmf=(0.0, 1.0),
        right_truncation_pmf=(1.0,),
    )

    pipeline.after_data_serialization(run, model_inputs)

    mock_copy.assert_called_once_with(Path("priors.py"), run.model_dir)
    assert mock_params.call_args.kwargs == {
        "run": run,
        "model_inputs": model_inputs,
        "save_dir": run.data_dir,
    }


@patch(
    "cfa.stf.routine.pyrenew_hew.forecast_pyrenew.create_samples_from_pyrenew_fit_dir"
)
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.generate_and_save_predictions")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.fit_and_save_model")
def test_pyrenew_pipeline_fits_predicts_and_converts_samples(
    mock_fit,
    mock_predict,
    mock_create_samples,
    tmp_path,
):
    pipeline = _pyrenew_pipeline(tmp_path)
    run = _run(tmp_path, model_name=pipeline.model_name)

    model_inputs = PyRenewModelInputs((1.0,), (0.0, 1.0), (1.0,))
    pipeline.fit_and_forecast(run, model_inputs)
    pipeline.before_post_process(run)

    assert mock_fit.call_args.args == (run.model_dir,)
    assert mock_fit.call_args.kwargs["n_chains"] == 2
    assert mock_fit.call_args.kwargs["fit_ed_visits"] is True
    assert mock_predict.call_args.args[:3] == (
        run.model_run_dir,
        run.model_name,
        30,
    )
    assert mock_predict.call_args.kwargs["predict_ed_visits"] is True
    mock_create_samples.assert_called_once_with(run.model_dir)
