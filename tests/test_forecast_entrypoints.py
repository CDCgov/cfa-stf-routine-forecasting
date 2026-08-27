import datetime as dt
import logging
from pathlib import Path

import pytest

from cfa.stf.routine.epiautogp import forecast_epiautogp
from cfa.stf.routine.fable import forecast_fable
from cfa.stf.routine.pyrenew_hew import forecast_pyrenew


@pytest.mark.parametrize(
    ("module", "pipeline_class_name", "main_kwargs", "expected_pipeline_kwargs"),
    [
        pytest.param(
            forecast_epiautogp,
            "EpiAutoGPPipeline",
            {
                "disease": "covid",
                "run_date": dt.date(2026, 1, 7),
                "loc": "CA",
                "output_dir": Path("epiautogp-output"),
                "n_training_days": 91,
                "n_forecast_days": 29,
                "target": "nssp",
                "frequency": "epiweekly",
                "ed_visit_type": "other",
                "exclude_last_n_days": 2,
                "exclude_date_ranges": "2025-12-01:2025-12-03",
                "n_particles": 11,
                "n_mcmc": 12,
                "n_hmc": 13,
                "n_forecast_draws": 14,
                "smc_data_proportion": 0.25,
                "n_threads": 3,
                "nowcast_source_name": "reporting-delay",
                "reporting_delay_pmf": [0.25, 0.75],
                "fail_on_stale_data": True,
            },
            {
                "disease": "covid",
                "loc": "CA",
                "target": "nssp",
                "frequency": "epiweekly",
                "ed_visit_type": "other",
                "output_dir": Path("epiautogp-output"),
                "n_training_days": 91,
                "n_forecast_days": 29,
                "exclude_last_n_days": 2,
                "exclude_date_ranges": [(dt.date(2025, 12, 1), dt.date(2025, 12, 3))],
                "nowcast_source_name": "reporting-delay",
                "reporting_delay_pmf": [0.25, 0.75],
                "hubverse_nowcast_dir": None,
                "run_date": dt.date(2026, 1, 7),
                "fail_on_stale_data": True,
                "n_particles": 11,
                "n_mcmc": 12,
                "n_hmc": 13,
                "n_forecast_draws": 14,
                "smc_data_proportion": 0.25,
                "n_threads": 3,
            },
            id="epiautogp",
        ),
        pytest.param(
            forecast_fable,
            "FablePipeline",
            {
                "disease": "covid",
                "loc": "CA",
                "output_dir": Path("fable-output"),
                "n_training_days": 92,
                "n_forecast_days": 30,
                "n_samples": 15,
                "run_date": dt.date(2026, 1, 7),
                "exclude_last_n_days": 3,
                "ed_visit_input_resolution": "epiweekly",
                "fail_on_stale_data": True,
            },
            {
                "disease": "covid",
                "loc": "CA",
                "output_dir": Path("fable-output"),
                "n_training_days": 92,
                "n_forecast_days": 30,
                "run_date": dt.date(2026, 1, 7),
                "exclude_last_n_days": 3,
                "fail_on_stale_data": True,
                "n_samples": 15,
                "ed_visit_input_resolution": "epiweekly",
            },
            id="fable",
        ),
        pytest.param(
            forecast_pyrenew,
            "PyRenewPipeline",
            {
                "disease": "covid",
                "loc": "CA",
                "priors_path": Path("priors.py"),
                "output_dir": Path("pyrenew-output"),
                "n_training_days": 93,
                "n_forecast_days": 31,
                "n_chains": 2,
                "n_warmup": 3,
                "n_samples": 4,
                "run_date": dt.date(2026, 1, 7),
                "exclude_last_n_days": 4,
                "fit_ed_visits": True,
                "fit_hospital_admissions": True,
                "forecast_ed_visits": True,
                "forecast_hospital_admissions": True,
                "rng_key": 123,
                "fail_on_stale_data": True,
            },
            {
                "disease": "covid",
                "loc": "CA",
                "priors_path": Path("priors.py"),
                "output_dir": Path("pyrenew-output"),
                "n_training_days": 93,
                "n_forecast_days": 31,
                "n_chains": 2,
                "n_warmup": 3,
                "n_samples": 4,
                "run_date": dt.date(2026, 1, 7),
                "exclude_last_n_days": 4,
                "fit_ed_visits": True,
                "fit_hospital_admissions": True,
                "forecast_ed_visits": True,
                "forecast_hospital_admissions": True,
                "rng_key": 123,
                "fail_on_stale_data": True,
            },
            id="pyrenew",
        ),
    ],
)
def test_entrypoint_forwards_all_pipeline_options(
    monkeypatch,
    module,
    pipeline_class_name,
    main_kwargs,
    expected_pipeline_kwargs,
):
    captured = {}
    logger = logging.getLogger(f"test-{module.__name__}")

    class PipelineSpy:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs

        def execute(self):
            captured["executed"] = True

    def fail_if_called(**kwargs):
        pytest.fail(f"basicConfig called with an injected logger: {kwargs}")

    monkeypatch.setattr(module, pipeline_class_name, PipelineSpy)
    monkeypatch.setattr(module.logging, "basicConfig", fail_if_called)

    module.main(**main_kwargs, logger=logger)

    assert captured["kwargs"] == {
        **expected_pipeline_kwargs,
        "logger": logger,
    }
    assert captured["executed"] is True


@pytest.mark.parametrize(
    ("fit_ed_visits", "fit_hospital_admissions", "expected_sources"),
    [
        pytest.param(True, False, {"nssp"}, id="pyrenew-e"),
        pytest.param(False, True, {"nhsn"}, id="pyrenew-h"),
        pytest.param(True, True, {"nssp", "nhsn"}, id="pyrenew-he"),
    ],
)
def test_pyrenew_pipeline_requests_sources_for_fitted_signals(
    fit_ed_visits,
    fit_hospital_admissions,
    expected_sources,
):
    pipeline = forecast_pyrenew.PyRenewPipeline(
        disease="covid",
        loc="CA",
        priors_path=Path("unused"),
        output_dir=Path("unused"),
        n_training_days=90,
        n_forecast_days=28,
        n_chains=1,
        n_warmup=1,
        n_samples=1,
        run_date=dt.date(2026, 1, 7),
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        forecast_ed_visits=fit_ed_visits,
        forecast_hospital_admissions=fit_hospital_admissions,
    )

    assert pipeline.sources == expected_sources
