import datetime as dt
import logging

import polars as pl
import pytest

from pipelines.data import data_access


def test_nssp_freshness_requires_run_date_match():
    fresh = data_access.nssp_freshness(
        selected_version_date=dt.date(2026, 1, 7),
        latest_observed_date=dt.date(2026, 1, 6),
        run_date=dt.date(2026, 1, 7),
    )
    stale = data_access.nssp_freshness(
        selected_version_date=dt.date(2026, 1, 6),
        latest_observed_date=dt.date(2026, 1, 5),
        run_date=dt.date(2026, 1, 7),
    )

    assert not fresh.is_stale
    assert stale.is_stale
    assert "does not match run date" in stale.reason


def test_nhsn_freshness_is_strict_on_wednesday_and_friday():
    stale = data_access.nhsn_freshness(
        selected_version_date=dt.date(2026, 1, 6),
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=dt.date(2026, 1, 7),
    )
    fresh = data_access.nhsn_freshness(
        selected_version_date=dt.date(2026, 1, 9),
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=dt.date(2026, 1, 9),
    )

    assert stale.is_stale
    assert not fresh.is_stale


def test_nhsn_freshness_allows_less_than_one_week_on_other_days():
    fresh = data_access.nhsn_freshness(
        selected_version_date=dt.date(2026, 1, 5),
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=dt.date(2026, 1, 8),
    )
    stale = data_access.nhsn_freshness(
        selected_version_date=dt.date(2026, 1, 1),
        latest_observed_date=dt.date(2025, 12, 27),
        run_date=dt.date(2026, 1, 8),
    )

    assert not fresh.is_stale
    assert stale.is_stale


def test_apply_freshness_policy_logs_versions_and_warns_or_raises(caplog):
    stale = data_access.nssp_freshness(
        selected_version_date=dt.date(2026, 1, 6),
        latest_observed_date=dt.date(2026, 1, 5),
        run_date=dt.date(2026, 1, 7),
    )
    fresh = data_access.nhsn_freshness(
        selected_version_date=dt.date(2026, 1, 7),
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=dt.date(2026, 1, 7),
    )
    logger = logging.getLogger("test-data-access")

    with caplog.at_level(logging.INFO):
        data_access.apply_freshness_policy(
            (stale, fresh),
            fail_on_stale_data=False,
            logger=logger,
        )
    assert (
        "source=nssp version=2026-01-06 latest_observed_date=2026-01-05 "
        "run_date=2026-01-07 status=stale"
    ) in caplog.text
    assert (
        "source=nhsn version=2026-01-07 latest_observed_date=2026-01-03 "
        "run_date=2026-01-07 status=fresh"
    ) in caplog.text
    assert "Stale input data" in caplog.text

    with pytest.raises(RuntimeError, match="Stale input data"):
        data_access.apply_freshness_policy(
            (stale,),
            fail_on_stale_data=True,
            logger=logger,
        )


def test_select_latest_nhsn_release_uses_newer_preliminary_version(monkeypatch):
    versions = iter([dt.datetime(2026, 1, 8, 8), dt.datetime(2026, 1, 7, 10)])
    calls = []
    monkeypatch.setattr(
        data_access,
        "resolve_nhsn_hrd_version",
        lambda **kwargs: calls.append(kwargs) or next(versions),
    )

    prelim, selected_version = data_access.select_latest_nhsn_release()

    assert prelim
    assert selected_version == dt.date(2026, 1, 8)
    assert calls == [
        {"prelim": True},
        {"prelim": False},
    ]


def test_select_latest_nhsn_release_uses_newer_final_version(monkeypatch):
    versions = iter([dt.datetime(2026, 1, 7, 10), dt.datetime(2026, 1, 8, 8)])
    monkeypatch.setattr(
        data_access,
        "resolve_nhsn_hrd_version",
        lambda **kwargs: next(versions),
    )

    prelim, selected_version = data_access.select_latest_nhsn_release()

    assert not prelim
    assert selected_version == dt.date(2026, 1, 8)


def test_load_dataops_nssp_uses_latest_available_version(monkeypatch):
    calls = {}
    raw_data = pl.DataFrame(
        {
            "reference_date": [dt.date(2026, 1, 7)],
            "geo_value": ["CA"],
            "disease": ["COVID-19"],
            "value": [10],
        }
    )
    monkeypatch.setattr(
        data_access,
        "resolve_nssp_report_date",
        lambda: dt.date(2026, 1, 8),
    )
    monkeypatch.setattr(
        data_access,
        "get_nssp",
        lambda **kwargs: calls.update(kwargs) or raw_data,
    )

    data, selected_version = data_access._load_dataops_nssp(
        loc_abb="CA",
        disease="COVID-19",
        first_training_date=dt.date(2025, 12, 1),
    )

    assert selected_version == dt.date(2026, 1, 8)
    assert data.columns == ["date", "geo_value", "disease", "ed_visits"]
    assert calls == {
        "disease": ["COVID-19", "Total"],
        "loc_abb": "CA",
        "dataset": "gold",
        "start_date": dt.date(2025, 12, 1),
        "lazy": False,
    }


def test_load_forecast_data_uses_dataops_loaders(monkeypatch):
    calls = {}
    nssp_data = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 8), dt.date(2026, 1, 8)],
            "geo_value": ["CA", "CA"],
            "disease": ["COVID-19", "Total"],
            "ed_visits": [10, 100],
        }
    )
    nhsn_data = pl.DataFrame(
        {
            "weekendingdate": [dt.date(2026, 1, 3)],
            "jurisdiction": ["CA"],
            "disease": ["COVID-19"],
            "hospital_admissions": [5],
        }
    )

    def fake_load_nssp(**kwargs):
        calls["nssp"] = kwargs
        return nssp_data, dt.date(2026, 1, 8)

    def fake_load_nhsn(**kwargs):
        calls["nhsn"] = kwargs
        return nhsn_data, True, dt.date(2026, 1, 8)

    monkeypatch.setattr(data_access, "_load_dataops_nssp", fake_load_nssp)
    monkeypatch.setattr(data_access, "_load_dataops_nhsn", fake_load_nhsn)
    monkeypatch.setattr(
        data_access,
        "get_us_loc_pop_tbl",
        lambda: pl.DataFrame({"abbr": ["CA"], "population": [39_000_000]}),
    )

    forecast_data = data_access.load_forecast_data(
        disease="COVID-19",
        loc_abb="CA",
        run_date=dt.date(2026, 1, 8),
        first_training_date=dt.date(2025, 12, 1),
        last_training_date=dt.date(2026, 1, 7),
    )

    assert forecast_data.loc_abb == "CA"
    assert forecast_data.disease == "COVID-19"
    assert forecast_data.report_date == dt.date(2026, 1, 8)
    assert forecast_data.loc_pop == 39_000_000
    assert forecast_data.right_truncation_offset == 0
    assert forecast_data.nssp.data.select(
        "observed_ed_visits", "other_ed_visits", "data_type", "resolution"
    ).rows() == [(10, 90, "eval", "daily")]
    assert forecast_data.nhsn.data.select("data_type", "resolution").rows() == [
        ("train", "epiweekly")
    ]
    assert forecast_data.nhsn.prelim
    assert all(
        isinstance(source, data_access.ForecastSourceData)
        for source in forecast_data.sources
    )
    assert forecast_data.freshness == (
        forecast_data.nssp.freshness,
        forecast_data.nhsn.freshness,
    )
    assert not forecast_data.is_stale
    assert calls["nssp"] == {
        "loc_abb": "CA",
        "disease": "COVID-19",
        "first_training_date": dt.date(2025, 12, 1),
    }
    assert calls["nhsn"] == {
        "disease": "COVID-19",
        "loc_abb": "CA",
        "first_training_date": dt.date(2025, 12, 1),
    }
