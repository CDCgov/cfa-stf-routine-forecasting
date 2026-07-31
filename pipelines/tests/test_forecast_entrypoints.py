import datetime as dt
from pathlib import Path

import pytest

from pipelines.epiautogp import forecast_epiautogp
from pipelines.fable import forecast_fable
from pipelines.pyrenew_hew import forecast_pyrenew


class _StopAfterDataBoundary(Exception):
    pass


@pytest.mark.parametrize(
    ("module", "dependency_name", "main_kwargs"),
    [
        pytest.param(
            forecast_epiautogp,
            "setup_forecast_pipeline",
            {
                "disease": "COVID-19",
                "run_date": dt.date(2026, 1, 7),
                "loc": "CA",
                "output_dir": Path("unused"),
                "n_training_days": 90,
                "n_forecast_days": 28,
                "target": "nssp",
                "frequency": "daily",
                "fail_on_stale_data": True,
            },
            id="epiautogp",
        ),
        pytest.param(
            forecast_fable,
            "load_forecast_data",
            {
                "disease": "COVID-19",
                "loc": "CA",
                "output_dir": Path("unused"),
                "n_training_days": 90,
                "n_forecast_days": 28,
                "n_samples": 10,
                "run_date": dt.date(2026, 1, 7),
                "fail_on_stale_data": True,
            },
            id="fable",
        ),
        pytest.param(
            forecast_pyrenew,
            "load_forecast_data",
            {
                "disease": "COVID-19",
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
            id="pyrenew",
        ),
    ],
)
def test_entrypoint_forwards_fail_on_stale_data(
    monkeypatch,
    module,
    dependency_name,
    main_kwargs,
):
    dependency_kwargs = {}

    def stop_at_data_boundary(**kwargs):
        dependency_kwargs.update(kwargs)
        raise _StopAfterDataBoundary

    monkeypatch.setattr(module, dependency_name, stop_at_data_boundary)

    with pytest.raises(_StopAfterDataBoundary):
        module.main(**main_kwargs)

    assert dependency_kwargs["fail_on_stale_data"] is True
    if module is forecast_fable:
        assert dependency_kwargs["sources"] == {"nssp"}
    if module is forecast_pyrenew:
        assert dependency_kwargs["sources"] == {"nssp"}


@pytest.mark.parametrize(
    ("fit_ed_visits", "fit_hospital_admissions", "expected_sources"),
    [
        pytest.param(True, False, {"nssp"}, id="pyrenew-e"),
        pytest.param(False, True, {"nhsn"}, id="pyrenew-h"),
        pytest.param(True, True, {"nssp", "nhsn"}, id="pyrenew-he"),
    ],
)
def test_pyrenew_requests_sources_for_fitted_signals(
    monkeypatch,
    fit_ed_visits,
    fit_hospital_admissions,
    expected_sources,
):
    dependency_kwargs = {}

    def stop_at_data_boundary(**kwargs):
        dependency_kwargs.update(kwargs)
        raise _StopAfterDataBoundary

    monkeypatch.setattr(forecast_pyrenew, "load_forecast_data", stop_at_data_boundary)

    with pytest.raises(_StopAfterDataBoundary):
        forecast_pyrenew.main(
            disease="COVID-19",
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

    assert dependency_kwargs["sources"] == expected_sources
