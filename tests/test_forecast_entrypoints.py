import datetime as dt
from pathlib import Path

import pytest

from cfa.stf.routine.epiautogp import forecast_epiautogp
from cfa.stf.routine.fable import forecast_fable
from cfa.stf.routine.pyrenew_hew import forecast_pyrenew


class _StopBeforeExecution(Exception):
    pass


@pytest.mark.parametrize(
    ("module", "pipeline_class_name", "main_kwargs", "expected_sources"),
    [
        pytest.param(
            forecast_epiautogp,
            "EpiAutoGPPipeline",
            {
                "disease": "covid",
                "run_date": dt.date(2026, 1, 7),
                "loc": "CA",
                "output_dir": Path("unused"),
                "n_training_days": 90,
                "n_forecast_days": 28,
                "target": "nssp",
                "frequency": "daily",
                "fail_on_stale_data": True,
            },
            {"nssp"},
            id="epiautogp",
        ),
        pytest.param(
            forecast_fable,
            "FablePipeline",
            {
                "disease": "covid",
                "loc": "CA",
                "output_dir": Path("unused"),
                "n_training_days": 90,
                "n_forecast_days": 28,
                "n_samples": 10,
                "run_date": dt.date(2026, 1, 7),
                "fail_on_stale_data": True,
            },
            {"nssp"},
            id="fable",
        ),
        pytest.param(
            forecast_pyrenew,
            "PyRenewPipeline",
            {
                "disease": "covid",
                "loc": "CA",
                "priors_path": Path("unused"),
                "output_dir": Path("unused"),
                "n_training_days": 90,
                "n_forecast_days": 28,
                "n_chains": 1,
                "n_warmup": 1,
                "n_samples": 1,
                "run_date": dt.date(2026, 1, 7),
                "fit_ed_visits": True,
                "forecast_ed_visits": True,
                "fail_on_stale_data": True,
            },
            {"nssp"},
            id="pyrenew",
        ),
    ],
)
def test_entrypoint_constructs_pipeline_with_shared_options(
    monkeypatch,
    module,
    pipeline_class_name,
    main_kwargs,
    expected_sources,
):
    captured = {}
    pipeline_class = getattr(module, pipeline_class_name)

    def stop_before_execution(self):
        captured["pipeline"] = self
        raise _StopBeforeExecution

    monkeypatch.setattr(pipeline_class, "execute", stop_before_execution)

    with pytest.raises(_StopBeforeExecution):
        module.main(**main_kwargs)

    pipeline = captured["pipeline"]
    assert pipeline.fail_on_stale_data is True
    assert pipeline.sources == expected_sources
    assert pipeline.disease == "covid"
    assert pipeline.loc == "CA"
    assert pipeline.run_date == dt.date(2026, 1, 7)


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
