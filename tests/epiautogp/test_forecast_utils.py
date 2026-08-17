import datetime as dt
import logging
from unittest.mock import patch

import pytest
from tests.factories import make_test_forecast_inputs

from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig
from cfa.stf.routine.epiautogp.forecast_epiautogp import (
    EpiAutoGPPipeline,
    _resolve_nowcast_source,
)
from cfa.stf.routine.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast
from cfa.stf.routine.forecast_run import ForecastRun


def _run(tmp_path, *, sources=("nssp", "nhsn")):
    return ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 20),
        n_forecast_days=28,
        exclude_last_n_days=0,
        model_name="epiautogp_nssp_daily",
        output_dir=tmp_path,
        inputs=make_test_forecast_inputs(sources=sources),
    )


def _pipeline(tmp_path, **overrides):
    kwargs = {
        "disease": "covid",
        "loc": "CA",
        "target": "nssp",
        "frequency": "daily",
        "ed_visit_type": "observed",
        "output_dir": tmp_path,
        "n_training_days": 90,
        "n_forecast_days": 28,
        "run_date": dt.date(2024, 12, 20),
        "logger": logging.getLogger("test-epiautogp-pipeline"),
    }
    kwargs.update(overrides)
    return EpiAutoGPPipeline(**kwargs)


@pytest.mark.parametrize(
    ("target", "frequency", "ed_visit_type", "expected"),
    [
        ("nssp", "daily", "observed", "epiautogp_nssp_daily"),
        ("nssp", "epiweekly", "pct", "epiautogp_nssp_epiweekly_pct"),
        ("nssp", "daily", "other", "epiautogp_nssp_daily_other"),
        ("nhsn", "epiweekly", "observed", "epiautogp_nhsn_epiweekly"),
    ],
)
def test_pipeline_declares_model_name_and_source(
    tmp_path, target, frequency, ed_visit_type, expected
):
    pipeline = _pipeline(
        tmp_path,
        target=target,
        frequency=frequency,
        ed_visit_type=ed_visit_type,
    )

    assert pipeline.model_name == expected
    assert pipeline.sources == {target}


def test_pipeline_validates_configuration_before_loading(tmp_path):
    pipeline = _pipeline(tmp_path, target="nhsn", frequency="daily")
    with pytest.raises(ValueError, match="only available in epiweekly"):
        pipeline.validate_configuration()


@patch("cfa.stf.routine.epiautogp.forecast_epiautogp.generate_epiweekly_data")
def test_epiweekly_pipeline_aggregates_common_inputs(mock_generate, tmp_path):
    pipeline = _pipeline(tmp_path, frequency="epiweekly")
    run = _run(tmp_path)

    pipeline.after_data_preparation(run)

    mock_generate.assert_called_once_with(run.data_dir, overwrite_daily=True)


@patch("cfa.stf.routine.epiautogp.forecast_epiautogp.run_epiautogp_forecast")
@patch("cfa.stf.routine.epiautogp.forecast_epiautogp.convert_to_epiautogp_json")
def test_fit_and_forecast_passes_run_and_model_options(
    mock_convert,
    mock_forecast,
    tmp_path,
):
    mock_convert.return_value = tmp_path / "input.json"
    pipeline = _pipeline(
        tmp_path,
        frequency="epiweekly",
        ed_visit_type="pct",
        n_particles=2,
        n_mcmc=3,
        n_hmc=4,
        n_forecast_draws=5,
        smc_data_proportion=0.2,
        n_threads=6,
    )
    run = _run(tmp_path)

    pipeline.fit_and_forecast(run)

    assert mock_convert.call_args.kwargs["forecast_run"] is run
    assert mock_convert.call_args.kwargs["config"] is pipeline.config
    assert mock_forecast.call_args.kwargs["model_dir"] == run.model_dir
    assert mock_forecast.call_args.kwargs["params"] == {
        "n_ahead": 4,
        "n_particles": 2,
        "n_mcmc": 3,
        "n_hmc": 4,
        "n_forecast_draws": 5,
        "transformation": "percentage",
        "smc_data_proportion": 0.2,
    }
    assert mock_forecast.call_args.kwargs["execution_settings"]["threads"] == 6


def test_reporting_delay_fetches_pmf_from_run(monkeypatch, tmp_path):
    get_pmf = patch(
        "cfa.stf.routine.epiautogp.forecast_epiautogp.get_nnh_right_truncation_pmf",
        return_value=[0.25, 0.75],
    )
    with get_pmf as mock_get_pmf:
        source = _resolve_nowcast_source(
            forecast_run=_run(tmp_path),
            config=EpiAutoGPConfig("nssp", "daily", "observed"),
            nowcast_source_name="reporting-delay",
        )

    assert isinstance(source, ReportingDelayNowcast)
    assert source.reporting_delay_pmf == [0.25, 0.75]
    mock_get_pmf.assert_called_once_with(
        state_abb="CA",
        disease="covid",
        as_of=dt.date(2024, 12, 20),
        reference_date=dt.date(2024, 12, 20),
    )


def test_direct_reporting_delay_pmf_skips_fetch(monkeypatch, tmp_path):
    with patch(
        "cfa.stf.routine.epiautogp.forecast_epiautogp.get_nnh_right_truncation_pmf"
    ) as mock_get_pmf:
        source = _resolve_nowcast_source(
            forecast_run=_run(tmp_path),
            config=EpiAutoGPConfig("nssp", "daily", "other"),
            nowcast_source_name="reporting-delay",
            reporting_delay_pmf=[0.4, 0.6],
        )

    assert isinstance(source, ReportingDelayNowcast)
    assert source.reporting_delay_pmf == [0.4, 0.6]
    mock_get_pmf.assert_not_called()


def test_reporting_delay_rejects_percentage_target(tmp_path):
    with pytest.raises(ValueError, match="not applicable"):
        _resolve_nowcast_source(
            forecast_run=_run(tmp_path),
            config=EpiAutoGPConfig("nssp", "daily", "pct"),
            nowcast_source_name="reporting-delay",
            reporting_delay_pmf=[1.0],
        )


def test_reporting_delay_warns_for_non_daily_frequency(caplog, tmp_path):
    with caplog.at_level(logging.WARNING):
        source = _resolve_nowcast_source(
            forecast_run=_run(tmp_path),
            config=EpiAutoGPConfig("nssp", "epiweekly", "observed"),
            nowcast_source_name="reporting-delay",
            reporting_delay_pmf=[1.0],
        )

    assert isinstance(source, ReportingDelayNowcast)
    assert "PMF support matches the model cadence" in caplog.text


def test_hubverse_resolution_uses_run_and_config(tmp_path):
    run = _run(tmp_path)
    config = EpiAutoGPConfig("nhsn", "epiweekly", "observed")
    source = _resolve_nowcast_source(
        forecast_run=run,
        config=config,
        nowcast_source_name="hubverse",
        hubverse_nowcast_dir=tmp_path,
    )

    assert isinstance(source, HubverseNowcast)
    assert source.forecast_run is run
    assert source.config is config


def test_none_and_invalid_nowcast_names(tmp_path):
    run = _run(tmp_path)
    config = EpiAutoGPConfig("nssp", "daily", "observed")
    assert (
        _resolve_nowcast_source(
            forecast_run=run,
            config=config,
            nowcast_source_name="none",
        )
        is None
    )
    with pytest.raises(ValueError, match="must be one of"):
        _resolve_nowcast_source(
            forecast_run=run,
            config=config,
            nowcast_source_name="auto",
        )
