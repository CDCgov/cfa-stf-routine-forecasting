"""Canonical exclusions are removed before any model input is constructed."""

import datetime as dt
from dataclasses import replace

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pyrenew_multisignal.hew import PyrenewHEWData
from tests.factories import make_test_forecast_run

from cfa.stf.routine.data import data_access
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.nowcastautogp.config import NowcastAutoGPConfig
from cfa.stf.routine.nowcastautogp.prep_nowcastautogp_data import (
    convert_to_nowcastautogp_json,
)
from cfa.stf.routine.nowcastautogp.reporting_delay_nowcast import ReportingDelayNowcast
from cfa.stf.routine.utils.data_utils import aggregate_nssp_to_epiweekly

REPORT_DATE = dt.date(2026, 7, 5)
DATES = pl.date_range(
    REPORT_DATE - dt.timedelta(days=168), REPORT_DATE - dt.timedelta(days=1), eager=True
).to_list()
MASKS = {
    "beginning": DATES[:7],
    "middle": DATES[70:84],
    "tail": DATES[-10:],
    "isolated": [DATES[17], DATES[35], DATES[-2]],
    "combined": DATES[:7] + DATES[70:84] + DATES[-10:],
}


def raw_data(excluded_dates, multiplier=1):
    return pl.DataFrame(
        [
            {
                "date": date,
                "state_abb": "CA",
                "disease": disease,
                "value": float(value * (multiplier if date in excluded_dates else 1)),
                "exclude": date in excluded_dates,
            }
            for i, date in enumerate(DATES)
            for disease, value in [("covid", 20 + i % 7), ("total", 200 + i % 7)]
        ]
    )


def load(monkeypatch, frame):
    def get_nssp(**kwargs):
        assert kwargs["get_exclusions"] is True
        return frame

    monkeypatch.setattr(data_access, "get_nssp", get_nssp)
    monkeypatch.setattr(data_access, "resolve_nssp_report_date", lambda: REPORT_DATE)
    return data_access._load_dataops_nssp(
        loc_abb="CA",
        disease="covid",
        min_allowed_training_date=DATES[0],
        max_allowed_training_date=DATES[-1],
        run_date=REPORT_DATE,
    )


def prepare(tmp_path, source, resolution):
    if resolution == "epiweekly":
        source = replace(
            source, data=aggregate_nssp_to_epiweekly(source.data), resolution=resolution
        )
    run = make_test_forecast_run(
        output_dir=tmp_path,
        report_date=REPORT_DATE,
        n_lookback_days=168,
        sources=("nssp",),
    )
    run = replace(run, surveillance=replace(run.surveillance, nssp=source))
    serialize_data(run)
    for target in ["observed", "other", "pct"]:
        config = NowcastAutoGPConfig("nssp", resolution, target)
        nowcast = (
            ReportingDelayNowcast(
                [0.6, 0.2, 0.1, 0.1], reference_date=REPORT_DATE - dt.timedelta(days=1)
            )
            if resolution == "daily" and target != "pct"
            else None
        )
        model_run = replace(run, model_name=f"gp_{target}")
        convert_to_nowcastautogp_json(
            forecast_run=model_run, config=config, nowcast_source=nowcast
        )
    return run


@pytest.mark.parametrize("scenario", MASKS)
@pytest.mark.parametrize("resolution", ["daily", "epiweekly"])
def test_excluded_values_cannot_reach_any_model(
    monkeypatch, tmp_path, scenario, resolution
):
    excluded_dates = MASKS[scenario]
    baseline = prepare(
        tmp_path / "baseline", load(monkeypatch, raw_data(excluded_dates)), resolution
    )
    for multiplier in [20, 1000000]:
        run = prepare(
            tmp_path / str(multiplier),
            load(monkeypatch, raw_data(excluded_dates, multiplier)),
            resolution,
        )
        assert_frame_equal(run.nssp.data, baseline.nssp.data)
        assert run.forecast_through == baseline.forecast_through
        assert run.n_forecast_days == baseline.n_forecast_days
        for filename in ["combined_data.tsv", "data_for_model_fit.json"]:
            assert (run.data_dir / filename).read_bytes() == (
                baseline.data_dir / filename
            ).read_bytes()
        for target in ["observed", "other", "pct"]:
            path = f"gp_{target}/gp_{target}_input.json"
            assert (run.model_run_dir / path).read_bytes() == (
                baseline.model_run_dir / path
            ).read_bytes()
        # Audit values remain available only in the explicitly separate artifact.
        assert (run.data_dir / "excluded_data.tsv").read_bytes() != (
            baseline.data_dir / "excluded_data.tsv"
        ).read_bytes()
    if resolution == "daily":
        assert not set(excluded_dates) & set(baseline.nssp.data["date"])
        data = PyrenewHEWData.from_json(
            baseline.data_dir / "data_for_model_fit.json", fit_ed_visits=True
        )
        dates = baseline.nssp.data["date"].unique().sort().to_numpy()
        np.testing.assert_array_equal(
            data.model_t_obs_ed_visits,
            (dates - dates[0]).astype("timedelta64[D]").astype(int),
        )
        assert np.isfinite(data.data_observed_disease_ed_visits).all()
    else:
        # No partial weekly totals: every retained week has seven retained days.
        for end in baseline.nssp.data["date"].unique():
            week = {end - dt.timedelta(days=i) for i in range(7)}
            assert not week.intersection(excluded_dates)


def test_excluding_either_component_excludes_the_date(monkeypatch):
    dates = MASKS["isolated"]
    frame = raw_data(dates).with_columns(
        exclude=pl.col("exclude") & (pl.col("disease") == "total")
    )
    source = load(monkeypatch, frame)
    assert not set(dates) & set(source.data["date"])
    assert set(source.excluded_data["date"]) == set(dates)


@pytest.mark.parametrize(
    "flags", [None, [None] * (len(DATES) * 2), [0] * (len(DATES) * 2)]
)
def test_missing_or_ambiguous_exclusion_metadata_fails(monkeypatch, flags):
    frame = raw_data([]).drop("exclude")
    if flags is not None:
        frame = frame.with_columns(pl.Series("exclude", flags))
    with pytest.raises(ValueError, match="non-null Boolean exclude"):
        load(monkeypatch, frame)


def test_all_excluded_fails_before_model_preparation(monkeypatch):
    with pytest.raises(ValueError, match="No retained NSSP observations"):
        load(monkeypatch, raw_data(DATES))


def test_aggregation_ignores_metadata_and_drops_incomplete_weeks(monkeypatch):
    source = load(monkeypatch, raw_data([]))
    annotated = source.data.with_columns(
        review_note=pl.int_range(pl.len()).cast(pl.String)
    )
    assert_frame_equal(
        aggregate_nssp_to_epiweekly(source.data), aggregate_nssp_to_epiweekly(annotated)
    )
    missing = source.data.with_columns(
        pl.when(pl.col("date") == DATES[3])
        .then(None)
        .otherwise(pl.col(".value"))
        .alias(".value")
    )
    assert DATES[6] not in aggregate_nssp_to_epiweekly(missing)["date"].to_list()


@pytest.mark.parametrize("with_hospital", [False, True])
@pytest.mark.parametrize("tail_days", [2, 10])
def test_pyrenew_reporting_adjustment_uses_calendar_age(
    monkeypatch, tmp_path, with_hospital, tail_days
):
    import copy

    from numpyro.handlers import seed, trace

    from cfa.stf.routine._paths import PRODUCTION_PRIORS
    from cfa.stf.routine.pyrenew_hew.model_inputs import (
        PyRenewModelInputs,
        serialize_pyrenew_model_params,
    )
    from cfa.stf.routine.pyrenew_hew.utils import build_pyrenew_hew_model_from_dir

    source = load(monkeypatch, raw_data(DATES[:7] + DATES[70:84] + DATES[-tail_days:]))
    run = prepare(tmp_path, source, "daily")
    hospital = (
        data_access.NHSNData(
            data=pl.DataFrame(
                {
                    "date": [date for date in DATES if date.weekday() == 5],
                    "state_abb": "CA",
                    "value": 5.0,
                    "data_type": "train",
                    "resolution": "epiweekly",
                }
            ),
            freshness=source.freshness,
            prelim=True,
        )
        if with_hospital
        else None
    )
    run = replace(
        run, surveillance=replace(run.surveillance, nhsn=hospital, loc_pop=1000000)
    )
    serialize_data(run)
    pmf = (0.6, 0.2, 0.1, 0.1)
    serialize_pyrenew_model_params(
        run=run,
        model_inputs=PyRenewModelInputs(
            generation_interval_pmf=(0.7, 0.2, 0.1),
            infection_to_admission_pmf=(0.0, 0.1, 0.3, 0.4, 0.2),
            right_truncation_pmf=pmf,
        ),
        save_dir=run.data_dir,
    )
    (run.model_dir / "priors.py").write_bytes(PRODUCTION_PRIORS.read_bytes())
    data = PyrenewHEWData.from_json(
        run.data_dir / "data_for_model_fit.json",
        fit_ed_visits=True,
        fit_hospital_admissions=with_hospital,
    )
    model = build_pyrenew_hew_model_from_dir(
        run.model_dir, fit_ed_visits=True, fit_hospital_admissions=with_hospital
    )
    kwargs = dict(sample_ed_visits=True, sample_hospital_admissions=with_hospital)
    observed = trace(seed(model.sample, rng_seed=1343)).get_trace(data=data, **kwargs)
    untruncated = copy.copy(data)
    untruncated.right_truncation_offset = None
    final = trace(seed(model.sample, rng_seed=1343)).get_trace(
        data=untruncated, **kwargs
    )
    dates = data.nssp_training_data["date"].to_list()
    ages = np.array(
        [(REPORT_DATE - dt.timedelta(days=1) - date).days for date in dates]
    )
    np.testing.assert_allclose(
        np.asarray(
            observed["observed_ed_visits"]["fn"].mean
            / final["observed_ed_visits"]["fn"].mean
        ),
        np.cumsum(pmf)[np.minimum(ages, len(pmf) - 1)],
        rtol=1e-6,
    )
    for variable in ["observed_ed_visits"] + (
        ["observed_hospital_admissions"] if with_hospital else []
    ):
        site = observed[variable]
        assert np.isfinite(np.asarray(site["fn"].log_prob(site["value"]))).all()
