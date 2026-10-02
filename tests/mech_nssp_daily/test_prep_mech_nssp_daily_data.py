"""Tests for the mech_nssp_daily JSON input writer."""

import datetime as dt
import json
import logging
from dataclasses import replace

import polars as pl
import pytest
from tests.factories import make_test_forecast_run

from cfa.stf.routine.mech_nssp_daily.prep_mech_nssp_daily_data import (
    convert_to_mech_nssp_daily_json,
)

REPORT_DATE = dt.date(2024, 12, 20)
LOGGER = logging.getLogger("test-mech-nssp-daily-prep")


def _run(tmp_path, *, exclude_last_n_days=0, loc_pop=1234567):
    return make_test_forecast_run(
        output_dir=tmp_path,
        model_name="mech_nssp_daily",
        sources=("nssp",),
        report_date=REPORT_DATE,
        exclude_last_n_days=exclude_last_n_days,
        n_lookback_days=None,
        first_training_date=REPORT_DATE - dt.timedelta(days=10),
        loc_pop=loc_pop,
    )


def _with_nssp_rows(run, rows: pl.DataFrame):
    return replace(
        run,
        surveillance=replace(run.surveillance, nssp=replace(run.nssp, data=rows)),
    )


def _nssp_rows(dates, values, *, variable="observed_ed_visits", data_type="train"):
    n = len(dates)
    return pl.DataFrame(
        {
            "date": dates,
            "state_abb": ["CA"] * n,
            ".variable": [variable] * n,
            ".value": values,
            "data_type": [data_type] * n,
            "resolution": ["daily"] * n,
        }
    )


def test_writes_daily_grid_with_null_gaps_and_lag_nowcast(tmp_path):
    run = _run(tmp_path)
    last = run.last_training_date
    dates = [last - dt.timedelta(days=i) for i in reversed(range(10))]
    dates_with_gap = dates[:4] + dates[5:]
    values = [float(30 + i) for i in range(10)]
    values_with_gap = values[:4] + values[5:]
    other_rows = _nssp_rows(dates, [100.0] * 10, variable="other_ed_visits")
    eval_rows = _nssp_rows([last + dt.timedelta(days=1)], [1.0], data_type="eval")
    run = _with_nssp_rows(
        run,
        pl.concat([_nssp_rows(dates_with_gap, values_with_gap), other_rows, eval_rows]),
    )

    path = convert_to_mech_nssp_daily_json(
        forecast_run=run, reporting_delay_pmf=[0.5, 0.5], logger=LOGGER
    )

    assert path == run.model_dir / "mech_nssp_daily_input.json"
    payload = json.loads(path.read_text())
    assert payload["dates"] == [date.isoformat() for date in dates]
    assert payload["location"] == "CA"
    assert payload["disease"] == "covid"
    assert payload["population"] == 1234567
    assert payload["report_date"] == REPORT_DATE.isoformat()
    assert payload["forecast_through"] == run.forecast_through.isoformat()
    # The gap is a null slot in every series.
    for key in ("observations", "raw_observations", "reporting_fractions"):
        assert len(payload[key]) == 10
        assert payload[key][4] is None
    # Only the newest report (lag 1) is incomplete with a one-entry CDF.
    assert payload["reporting_fractions"][:4] == [1.0] * 4
    assert payload["reporting_fractions"][5:9] == [1.0] * 4
    assert payload["reporting_fractions"][9] == 0.5
    assert payload["raw_observations"][9] == 39.0
    assert payload["observations"][9] == round((39.0 + 0.5) / 0.5)
    assert payload["observations"][:4] == values[:4]
    assert payload["observations"][5:9] == values[5:9]


def test_newest_report_at_lag_two_uses_second_cdf_entry(tmp_path):
    run = _run(tmp_path, exclude_last_n_days=1)
    last = run.last_training_date
    assert last == REPORT_DATE - dt.timedelta(days=2)
    dates = [last - dt.timedelta(days=i) for i in reversed(range(3))]
    run = _with_nssp_rows(run, _nssp_rows(dates, [10.0, 10.0, 10.0]))

    payload = json.loads(
        convert_to_mech_nssp_daily_json(
            forecast_run=run, reporting_delay_pmf=[0.6, 0.2, 0.2], logger=LOGGER
        ).read_text()
    )

    assert payload["reporting_fractions"] == pytest.approx([1.0, 1.0, 0.8])


def test_rejects_runs_without_nssp(tmp_path):
    run = make_test_forecast_run(
        output_dir=tmp_path,
        model_name="mech_nssp_daily",
        sources=("nhsn",),
        report_date=REPORT_DATE,
        n_lookback_days=None,
    )
    with pytest.raises(ValueError):
        convert_to_mech_nssp_daily_json(
            forecast_run=run, reporting_delay_pmf=[1.0], logger=LOGGER
        )


def test_rejects_non_daily_resolution(tmp_path):
    run = _run(tmp_path)
    run = replace(
        run,
        surveillance=replace(
            run.surveillance, nssp=replace(run.nssp, resolution="epiweekly")
        ),
    )
    with pytest.raises(ValueError, match="not daily"):
        convert_to_mech_nssp_daily_json(
            forecast_run=run, reporting_delay_pmf=[1.0], logger=LOGGER
        )


def test_rejects_missing_training_rows(tmp_path):
    run = _run(tmp_path)
    run = _with_nssp_rows(
        run, _nssp_rows([run.last_training_date], [5.0], variable="other_ed_visits")
    )
    with pytest.raises(ValueError, match="No NSSP"):
        convert_to_mech_nssp_daily_json(
            forecast_run=run, reporting_delay_pmf=[1.0], logger=LOGGER
        )
