import datetime as dt
import logging

import polars as pl
import pytest

from pipelines.data import data_access


class FakeLoad:
    prefix = "public/stf/nssp_gold_v1"

    def __init__(self, versions):
        self.versions = versions

    def get_versions(self):
        return self.versions


class FakeEndpoint:
    def __init__(self, versions):
        self.load = FakeLoad(versions)


def test_latest_version_date_uses_public_dataops_versions():
    endpoint = FakeEndpoint(
        [
            "2026-01-09/data.parquet",
            "2026-01-08T00-00-00/data.parquet",
            "2026-01-07",
        ]
    )

    assert data_access._latest_version_date(endpoint) == dt.date(2026, 1, 9)
    assert data_access._latest_version_date(
        endpoint, as_of=dt.date(2026, 1, 8)
    ) == dt.date(2026, 1, 8)


def test_latest_version_date_raises_when_no_versions_match_as_of():
    endpoint = FakeEndpoint(["2026-01-09/data.parquet"])

    with pytest.raises(ValueError, match="No public/stf/nssp_gold_v1 versions found"):
        data_access._latest_version_date(endpoint, as_of=dt.date(2026, 1, 8))


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


def test_enforce_freshness_warns_or_raises(caplog):
    stale = data_access.nssp_freshness(
        selected_version_date=dt.date(2026, 1, 6),
        latest_observed_date=None,
        run_date=dt.date(2026, 1, 7),
    )
    logger = logging.getLogger("test-data-access")

    with caplog.at_level(logging.WARNING):
        data_access.enforce_freshness(
            (stale,),
            fail_on_stale_data=False,
            logger=logger,
        )
    assert "Stale input data" in caplog.text

    with pytest.raises(RuntimeError, match="Stale input data"):
        data_access.enforce_freshness(
            (stale,),
            fail_on_stale_data=True,
            logger=logger,
        )


def test_choose_nhsn_prelim_uses_newer_version(monkeypatch):
    versions = iter([dt.date(2026, 1, 8), dt.date(2026, 1, 7)])
    monkeypatch.setattr(
        data_access,
        "_latest_version_date",
        lambda endpoint, run_date=None: next(versions),
    )

    prelim, selected_version = data_access.choose_nhsn_prelim(
        run_date=dt.date(2026, 1, 8)
    )

    assert prelim
    assert selected_version == dt.date(2026, 1, 8)


def test_choose_nhsn_final_when_final_is_newer(monkeypatch):
    versions = iter([dt.date(2026, 1, 7), dt.date(2026, 1, 8)])
    monkeypatch.setattr(
        data_access,
        "_latest_version_date",
        lambda endpoint, run_date=None: next(versions),
    )

    prelim, selected_version = data_access.choose_nhsn_prelim(
        run_date=dt.date(2026, 1, 8)
    )

    assert not prelim
    assert selected_version == dt.date(2026, 1, 8)


def test_load_forecast_data_uses_dataops_loaders(monkeypatch):
    calls = {}
    nssp_data = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 8)],
            "geo_value": ["CA"],
            "disease": ["COVID-19"],
            "ed_visits": [10],
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
        return nssp_data

    def fake_load_nhsn(**kwargs):
        calls["nhsn"] = kwargs
        return nhsn_data, True, dt.date(2026, 1, 8)

    monkeypatch.setattr(data_access, "_load_dataops_nssp", fake_load_nssp)
    monkeypatch.setattr(data_access, "_load_dataops_nhsn", fake_load_nhsn)

    forecast_data = data_access.load_forecast_data(
        disease="COVID-19",
        loc_abb="CA",
        report_date=dt.date(2026, 1, 8),
        first_training_date=dt.date(2025, 12, 1),
    )

    assert forecast_data.nssp.data.equals(nssp_data)
    assert forecast_data.nhsn.data.equals(nhsn_data)
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
        "report_date": dt.date(2026, 1, 8),
        "loc_abb": "CA",
        "disease": "COVID-19",
        "first_training_date": dt.date(2025, 12, 1),
    }
    assert calls["nhsn"] == {
        "disease": "COVID-19",
        "loc_abb": "CA",
        "first_training_date": dt.date(2025, 12, 1),
        "run_date": dt.date(2026, 1, 8),
    }
