"""Tests for the mech_nssp_daily pipeline class and Julia runner call."""

import datetime as dt
import json
import logging
from unittest.mock import patch

import pytest
from tests.factories import make_test_forecast_run

from cfa.stf.routine._paths import MECH_NSSP_DAILY_DIR
from cfa.stf.routine.mech_nssp_daily.forecast_mech_nssp_daily import (
    MechNSSPDailyPipeline,
    run_mech_nssp_daily_forecast,
)

MODULE = "cfa.stf.routine.mech_nssp_daily.forecast_mech_nssp_daily"
REPORT_DATE = dt.date(2024, 12, 20)


def _pipeline(tmp_path, **overrides):
    kwargs = {
        "disease": "covid",
        "loc": "CA",
        "output_dir": tmp_path,
        "n_lookback_days": None,
        "run_date": REPORT_DATE,
        "logger": logging.getLogger("test-mech-nssp-daily-pipeline"),
    }
    kwargs.update(overrides)
    return MechNSSPDailyPipeline(**kwargs)


def _run(tmp_path):
    return make_test_forecast_run(
        output_dir=tmp_path,
        model_name="mech_nssp_daily",
        sources=("nssp",),
        report_date=REPORT_DATE,
        n_lookback_days=None,
        first_training_date=REPORT_DATE - dt.timedelta(days=5),
    )


def test_pipeline_contract(tmp_path):
    pipeline = _pipeline(tmp_path)
    assert pipeline.model_name == "mech_nssp_daily"
    assert pipeline.sources == {"nssp"}
    assert pipeline.ed_visit_input_resolution == "daily"
    assert pipeline.minimum_exclude_last_n_days == 0
    pipeline.validate_configuration()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"disease": "flu"}, "COVID-informed"),
        ({"dow_window_days": 34}, "at least 35"),
        ({"n_threads": "auto"}, "n_threads must be a positive integer"),
        ({"n_particles": 0}, "n_particles must be a positive integer"),
        ({"dow_min_exclude_days": -1}, "non-negative"),
        ({"reporting_delay_pmf": []}, "non-empty"),
        ({"reporting_delay_pmf": [0.5, -0.5]}, "non-negative weights"),
    ],
)
def test_validate_configuration_rejects(tmp_path, overrides, message):
    with pytest.raises(ValueError, match=message):
        _pipeline(tmp_path, **overrides).validate_configuration()


@patch(f"{MODULE}.run_julia_script")
def test_runner_builds_explicit_julia_command(mock_run_julia, tmp_path):
    input_path = tmp_path / "input.json"
    model_dir = tmp_path / "model"

    run_mech_nssp_daily_forecast(
        input_path,
        model_dir,
        n_particles=2,
        n_forecast_draws=3,
        seed=4,
        min_observations=5,
        dow_window_days=36,
        dow_min_exclude_days=7,
        n_threads=8,
    )

    assert model_dir.is_dir()
    script, args = mock_run_julia.call_args.args
    assert script == MECH_NSSP_DAILY_DIR / "fit_mech_nssp_daily.jl"
    assert args == [
        f"--json-input={input_path}",
        f"--output-dir={model_dir}",
        "--n-particles=2",
        "--n-forecast-draws=3",
        "--seed=4",
        "--min-observations=5",
        "--dow-window-days=36",
        "--dow-min-exclude-days=7",
    ]
    assert mock_run_julia.call_args.kwargs["executor_flags"] == [
        f"--project={MECH_NSSP_DAILY_DIR}",
        "--threads=8",
    ]
    assert mock_run_julia.call_args.kwargs["capture_output"] is False


def test_prepare_model_artifacts_fetches_pmf_from_run(tmp_path):
    pipeline = _pipeline(tmp_path)
    run = _run(tmp_path)

    with patch(
        f"{MODULE}.get_nnh_right_truncation_pmf", return_value=[0.25, 0.75]
    ) as mock_get_pmf:
        pipeline.prepare_model_artifacts(run)

    mock_get_pmf.assert_called_once_with(
        state_abb="CA",
        disease="covid",
        as_of=REPORT_DATE,
        reference_date=REPORT_DATE,
    )
    payload = json.loads((run.model_dir / "mech_nssp_daily_input.json").read_text())
    assert payload["reporting_fractions"][-1] == pytest.approx(0.25)


def test_injected_pmf_skips_fetch(tmp_path):
    pipeline = _pipeline(tmp_path, reporting_delay_pmf=[0.4, 0.6])
    run = _run(tmp_path)

    with patch(f"{MODULE}.get_nnh_right_truncation_pmf") as mock_get_pmf:
        pipeline.prepare_model_artifacts(run)

    mock_get_pmf.assert_not_called()
    payload = json.loads((run.model_dir / "mech_nssp_daily_input.json").read_text())
    assert payload["reporting_fractions"][-1] == pytest.approx(0.4)


@patch(f"{MODULE}.run_mech_nssp_daily_forecast")
def test_run_model_forwards_settings(mock_forecast, tmp_path):
    pipeline = _pipeline(
        tmp_path,
        n_particles=11,
        n_forecast_draws=12,
        seed=13,
        n_threads=3,
        min_observations=14,
        dow_window_days=40,
        dow_min_exclude_days=2,
    )
    run = _run(tmp_path)

    pipeline.run_model(run)

    mock_forecast.assert_called_once_with(
        json_input_path=run.model_dir / "mech_nssp_daily_input.json",
        model_dir=run.model_dir,
        n_particles=11,
        n_forecast_draws=12,
        seed=13,
        min_observations=14,
        dow_window_days=40,
        dow_min_exclude_days=2,
        n_threads=3,
    )
