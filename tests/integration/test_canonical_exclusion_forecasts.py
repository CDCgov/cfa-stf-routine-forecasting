"""Run production model adapters on inputs filtered by canonical exclusions."""

import datetime as dt

import polars as pl
import pytest
from tests.data.test_canonical_exclusions import DATES, MASKS, REPORT_DATE, raw_data
from tests.integration.model_test_utils import DELAY_PMF, GENERATION_INTERVAL_PMF

from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.data import data_access
from cfa.stf.routine.fable.forecast_fable import FablePipeline
from cfa.stf.routine.nowcastautogp.forecast_nowcastautogp import NowcastAutoGPPipeline
from cfa.stf.routine.pyrenew_hew import forecast_pyrenew
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import PyRenewPipeline
from cfa.stf.routine.pyrenew_hew.model_inputs import PyRenewModelInputs


@pytest.mark.model_integration
@pytest.mark.parametrize(
    "model",
    ["fable_daily", "fable_weekly", "gp_daily", "gp_weekly", "pyrenew_e", "pyrenew_he"],
)
def test_forecast_period_after_canonical_exclusions(monkeypatch, tmp_path, model):
    # Excluded observations are extreme, with gaps at both boundaries and inside.
    source_data = raw_data(MASKS["combined"], multiplier=1000000)
    monkeypatch.setattr(data_access, "get_nssp", lambda **kwargs: source_data)
    monkeypatch.setattr(data_access, "resolve_nssp_report_date", lambda: REPORT_DATE)
    monkeypatch.setattr(
        data_access,
        "get_us_loc_pop_tbl",
        lambda: pl.DataFrame({"abbr": ["CA"], "population": [1000000]}),
    )
    monkeypatch.setattr(
        data_access, "select_latest_nhsn_release", lambda: (True, REPORT_DATE)
    )
    monkeypatch.setattr(
        data_access,
        "get_nhsn_hrd",
        lambda **kwargs: pl.DataFrame(
            {
                "date": [date for date in DATES if date.weekday() == 5],
                "state_abb": "CA",
                "value": 5.0,
            }
        ),
    )
    monkeypatch.setattr(
        forecast_pyrenew,
        "resolve_pyrenew_model_inputs",
        lambda **kwargs: PyRenewModelInputs(
            generation_interval_pmf=tuple(GENERATION_INTERVAL_PMF),
            infection_to_admission_pmf=tuple(DELAY_PMF),
            right_truncation_pmf=(0.6, 0.2, 0.1, 0.1),
        ),
    )
    common = dict(
        disease="covid",
        loc="CA",
        output_dir=tmp_path,
        n_lookback_days=168,
        run_date=REPORT_DATE,
    )
    if model.startswith("fable"):
        pipeline = FablePipeline(
            **common,
            n_samples=20,
            ed_visit_input_resolution="epiweekly"
            if model.endswith("weekly")
            else "daily",
        )
    elif model.startswith("gp"):
        weekly = model.endswith("weekly")
        pipeline = NowcastAutoGPPipeline(
            **common,
            target="nssp",
            frequency="epiweekly" if weekly else "daily",
            ed_visit_type="pct" if weekly else "observed",
            n_particles=2,
            n_mcmc=1,
            n_hmc=1,
            n_forecast_draws=20,
            n_threads=1,
            smc_data_proportion=0.5,
            nowcast_source_name="none" if weekly else "reporting-delay",
            reporting_delay_pmf=None if weekly else [0.6, 0.2, 0.1, 0.1],
        )
    else:
        pipeline = PyRenewPipeline(
            **common,
            priors_path=PRODUCTION_PRIORS,
            n_chains=1,
            n_warmup=20,
            n_samples=20,
            rng_key=1343,
            fit_ed_visits=True,
            forecast_ed_visits=True,
            fit_hospital_admissions=model.endswith("he"),
            forecast_hospital_admissions=model.endswith("he"),
        )
    run = pipeline.build_forecast_run()
    pipeline.prepare_input_artifacts(run)
    pipeline.run_model(run)
    samples = pl.read_parquet(run.model_dir / "samples.parquet")
    assert samples[".value"].is_finite().all()
    assert (samples[".value"] >= 0).all()
    for (variable, resolution), values in samples.group_by(".variable", "resolution"):
        step = 7 if resolution == "epiweekly" else 1
        first = REPORT_DATE + dt.timedelta(days=6) if step == 7 else REPORT_DATE
        expected = pl.date_range(
            first, run.forecast_through, interval=f"{step}d", eager=True
        )
        forecast = values.filter(pl.col("date") >= first)
        assert forecast["date"].unique().sort().to_list() == expected.to_list(), (
            variable
        )
        for draw in forecast.partition_by(".draw"):
            assert draw["date"].sort().to_list() == expected.to_list(), variable
    # Serialization, plotting, and Hubverse conversion use the same retained data.
    pipeline.publish_outputs(run)
    if run.nssp.resolution == "daily":
        assert not set(MASKS["combined"]) & set(run.nssp.data["date"])
