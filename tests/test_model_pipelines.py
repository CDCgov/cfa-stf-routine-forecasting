import datetime as dt
from pathlib import Path
from unittest.mock import patch, sentinel

import pytest

from cfa.stf.routine.fable.forecast_fable import FablePipeline
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import PyRenewPipeline
from cfa.stf.routine.pyrenew_hew.model_inputs import PyRenewModelInputs
from tests.factories import make_test_forecast_run


def _run(tmp_path, *, model_name="test_model", exclude_last_n_days=2):
    return make_test_forecast_run(
        output_dir=tmp_path,
        exclude_last_n_days=exclude_last_n_days,
        model_name=model_name,
    )


def _common_kwargs(tmp_path):
    return {
        "disease": "covid",
        "loc": "CA",
        "output_dir": tmp_path,
        "n_lookback_days": 90,
        "forecast_date": dt.date(2024, 12, 20),
    }


@pytest.mark.parametrize(
    "resolution",
    ["daily", "epiweekly"],
)
def test_fable_pipeline_declares_ed_visit_input_resolution(tmp_path, resolution):
    pipeline = FablePipeline(
        **_common_kwargs(tmp_path),
        n_samples=10,
        ed_visit_input_resolution=resolution,
    )
    assert pipeline.ed_visit_input_resolution == resolution
    assert pipeline.model_name == f"{resolution}_fable_e_other"
    assert pipeline.minimum_exclude_last_n_days == 3


@patch("cfa.stf.routine.fable.forecast_fable.fable_e_other_forecasts")
def test_fable_pipeline_forecasts_through_excluded_tail(mock_forecast, tmp_path):
    pipeline = FablePipeline(
        **_common_kwargs(tmp_path),
        n_samples=10,
    )
    run = _run(tmp_path, model_name=pipeline.model_name)

    pipeline.run_model(run)

    mock_forecast.assert_called_once_with(run.model_dir, 25, 10)


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
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.copy_priors")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.resolve_pyrenew_model_inputs")
def test_pyrenew_pipeline_extends_common_data_preparation(
    mock_resolve,
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
    mock_resolve.return_value = model_inputs

    pipeline.prepare_model_artifacts(run)

    mock_resolve.assert_called_once_with(run=run, fit_ed_visits=True)
    mock_copy.assert_called_once_with(Path("priors.py"), run.model_dir)
    assert mock_params.call_args.kwargs == {
        "run": run,
        "model_inputs": model_inputs,
        "save_dir": run.data_dir,
    }


@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.write_tabular")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.format_pyrenew_samples")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.read_tabular")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.generate_and_save_predictions")
@patch("cfa.stf.routine.pyrenew_hew.forecast_pyrenew.fit_and_save_model")
def test_pyrenew_pipeline_fits_predicts_and_converts_samples(
    mock_fit,
    mock_predict,
    mock_read,
    mock_format,
    mock_write,
    tmp_path,
):
    pipeline = _pyrenew_pipeline(tmp_path)
    run = _run(tmp_path, model_name=pipeline.model_name)
    mock_read.return_value = sentinel.posterior_predictive
    mock_format.return_value = sentinel.samples

    pipeline.run_model(run)

    assert mock_fit.call_args.args == (run.model_dir,)
    assert mock_fit.call_args.kwargs["n_chains"] == 2
    assert mock_fit.call_args.kwargs["fit_ed_visits"] is True
    assert mock_predict.call_args.args[:3] == (
        run.model_run_dir,
        run.model_name,
        run.forecast_through,
    )
    assert mock_predict.call_args.kwargs["predict_ed_visits"] is True
    mock_read.assert_called_once_with(
        run.model_dir / "mcmc_output" / "tidy_posterior_predictive.parquet"
    )
    mock_format.assert_called_once_with(
        sentinel.posterior_predictive,
        geo_value=run.loc,
        disease=run.disease,
    )
    mock_write.assert_called_once_with(
        sentinel.samples,
        run.model_dir / "samples.parquet",
    )


def test_explicit_invocation_configuration_round_trips(tmp_path):
    from yaml12 import format_yaml, parse_yaml

    from cfa.stf.routine.nowcastautogp.forecast_nowcastautogp import (
        NowcastAutoGPPipeline,
    )

    pipelines = [
        FablePipeline(**_common_kwargs(tmp_path), n_samples=10),
        _pyrenew_pipeline(tmp_path),
        NowcastAutoGPPipeline(
            **_common_kwargs(tmp_path),
            target="nssp",
            frequency="daily",
            exclude_date_ranges=[(dt.date(2024, 1, 1), dt.date(2024, 1, 2))],
        ),
    ]
    for pipeline in pipelines:
        pipeline.unrelated_internal_state = object()
        config = pipeline.invocation_config()
        assert parse_yaml(format_yaml(config)) == config
        assert "unrelated_internal_state" not in config
        assert config["forecast_date"] == "2024-12-20"
        assert config["model"] == pipeline.model_name
    assert pipelines[1].invocation_config()["priors_path"] == "priors.py"
    assert pipelines[2].invocation_config()["exclude_date_ranges"] == [
        ["2024-01-01", "2024-01-02"]
    ]
