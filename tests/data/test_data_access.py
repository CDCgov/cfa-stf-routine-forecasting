import calendar
import datetime as dt
import logging

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from cfa.stf.routine.data import data_access


def _freshness(source: str) -> data_access.DataFreshness:
    report_date = dt.date(2026, 1, 7)
    return data_access.DataFreshness(
        source=source,
        selected_version_date=report_date,
        latest_observed_date=report_date,
        run_date=report_date,
        is_stale=False,
        reason="Test data",
    )


def test_load_dataops_nssp_returns_normalized_source(monkeypatch):
    calls = {}
    source_data = pl.DataFrame(
        {
            "reference_date": [
                dt.date(2026, 1, 8),
                dt.date(2026, 1, 8),
                dt.date(2026, 1, 7),
                dt.date(2026, 1, 7),
                dt.date(2026, 1, 7),
            ],
            "geo_value": ["CA"] * 5,
            "disease": [
                "COVID-19",
                "Total",
                "Total",
                "Influenza",
                "COVID-19",
            ],
            "value": [12, 120, 100, 8, 10],
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
        lambda **kwargs: calls.update(kwargs) or source_data,
    )

    result = data_access._load_dataops_nssp(
        loc_abb="CA",
        disease="COVID-19",
        first_training_date=dt.date(2025, 12, 1),
        last_training_date=dt.date(2026, 1, 7),
        run_date=dt.date(2026, 1, 8),
    )

    expected = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 7), dt.date(2026, 1, 8)],
            "observed_ed_visits": [10, 12],
            "other_ed_visits": [90, 108],
            "data_type": ["train", "eval"],
            "resolution": ["daily", "daily"],
        }
    )
    assert_frame_equal(
        result.data.select(
            "date",
            "observed_ed_visits",
            "other_ed_visits",
            "data_type",
            "resolution",
        ),
        expected,
    )
    assert result.freshness.selected_version_date == dt.date(2026, 1, 8)
    assert result.freshness.latest_observed_date == dt.date(2026, 1, 8)
    assert not result.freshness.is_stale
    assert calls == {
        "disease": ["COVID-19", "Total"],
        "loc_abb": "CA",
        "dataset": "gold",
        "start_date": dt.date(2025, 12, 1),
        "lazy": False,
    }


def test_load_dataops_nhsn_returns_normalized_source(monkeypatch):
    calls = {}
    source_data = pl.DataFrame(
        {
            "weekendingdate": [
                dt.date(2025, 12, 27),
                dt.date(2026, 1, 10),
                dt.date(2026, 1, 1),
                dt.date(2026, 1, 7),
            ],
            "jurisdiction": ["CA"] * 4,
            "disease": ["COVID-19"] * 4,
            "hospital_admissions": [3, 6, 4, 5],
        }
    )
    monkeypatch.setattr(
        data_access,
        "select_latest_nhsn_release",
        lambda: (True, dt.date(2026, 1, 8)),
    )
    monkeypatch.setattr(
        data_access,
        "get_nhsn_hrd",
        lambda **kwargs: calls.update(kwargs) or source_data,
    )

    result = data_access._load_dataops_nhsn(
        disease="COVID-19",
        loc_abb="CA",
        first_training_date=dt.date(2026, 1, 1),
        last_training_date=dt.date(2026, 1, 7),
        run_date=dt.date(2026, 1, 8),
    )

    assert result.prelim
    expected = pl.DataFrame(
        {
            "weekendingdate": [
                dt.date(2026, 1, 10),
                dt.date(2026, 1, 1),
                dt.date(2026, 1, 7),
            ],
            "jurisdiction": ["CA"] * 3,
            "hospital_admissions": [6, 4, 5],
            "data_type": ["eval", "train", "train"],
            "resolution": ["epiweekly"] * 3,
        }
    )
    assert_frame_equal(result.data, expected)
    assert result.freshness.selected_version_date == dt.date(2026, 1, 8)
    assert result.freshness.latest_observed_date == dt.date(2026, 1, 10)
    assert not result.freshness.is_stale
    assert calls == {
        "disease": "COVID-19",
        "loc_abb": "CA",
        "prelim": True,
        "start_date": dt.date(2026, 1, 1),
        "lazy": False,
    }


@pytest.mark.parametrize("source_name", ["nssp", "nhsn"])
def test_forecast_data_allows_one_source(source_name):
    source = (
        data_access.NSSPData(
            data=pl.DataFrame(),
            freshness=_freshness("nssp"),
        )
        if source_name == "nssp"
        else data_access.NHSNData(
            data=pl.DataFrame(),
            freshness=_freshness("nhsn"),
            prelim=False,
        )
    )
    forecast_data = data_access.ForecastData(
        loc_abb="CA",
        disease="COVID-19",
        report_date=dt.date(2026, 1, 7),
        loc_pop=39_000_000,
        right_truncation_offset=0,
        nssp=source if source_name == "nssp" else None,
        nhsn=source if source_name == "nhsn" else None,
    )

    assert forecast_data.sources == (source,)
    assert forecast_data.freshness == (source.freshness,)
    assert not forecast_data.is_stale


def test_forecast_data_requires_at_least_one_source():
    with pytest.raises(ValueError, match="at least one data source"):
        data_access.ForecastData(
            loc_abb="CA",
            disease="COVID-19",
            report_date=dt.date(2026, 1, 7),
            loc_pop=39_000_000,
            right_truncation_offset=0,
        )


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


@pytest.mark.parametrize(
    ("run_date", "expected_weekday"),
    [
        pytest.param(
            dt.date(2026, 1, 7),
            calendar.WEDNESDAY,
            id="wednesday",
        ),
        pytest.param(
            dt.date(2026, 1, 9),
            calendar.FRIDAY,
            id="friday",
        ),
    ],
)
def test_nhsn_freshness_is_strict_on_wednesday_and_friday(
    run_date,
    expected_weekday,
):
    assert run_date.weekday() == expected_weekday

    stale = data_access.nhsn_freshness(
        selected_version_date=run_date - dt.timedelta(days=1),
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=run_date,
    )
    fresh = data_access.nhsn_freshness(
        selected_version_date=run_date,
        latest_observed_date=dt.date(2026, 1, 3),
        run_date=run_date,
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


@pytest.mark.parametrize("version", [None, "latest"])
def test_resolved_version_date_requires_datetime(version):
    with pytest.raises(ValueError, match="No dated NHSN version found"):
        data_access._resolved_version_date(version, dataset="NHSN")


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


def test_load_forecast_data_uses_dataops_loaders(monkeypatch):
    calls = {}
    report_date = dt.date(2026, 1, 8)

    def freshness(source, latest_observed_date):
        return data_access.DataFreshness(
            source=source,
            selected_version_date=report_date,
            latest_observed_date=latest_observed_date,
            run_date=report_date,
            is_stale=False,
            reason="Test data",
        )

    nssp = data_access.NSSPData(
        data=pl.DataFrame(
            {
                "date": [dt.date(2026, 1, 8)],
                "geo_value": ["CA"],
                "observed_ed_visits": [10],
                "other_ed_visits": [90],
                "data_type": ["eval"],
                "resolution": ["daily"],
            }
        ),
        freshness=freshness("nssp", dt.date(2026, 1, 8)),
    )
    nhsn = data_access.NHSNData(
        data=pl.DataFrame(
            {
                "weekendingdate": [dt.date(2026, 1, 3)],
                "jurisdiction": ["CA"],
                "hospital_admissions": [5],
                "data_type": ["train"],
                "resolution": ["epiweekly"],
            }
        ),
        freshness=freshness("nhsn", dt.date(2026, 1, 3)),
        prelim=True,
    )

    def fake_load_nssp(**kwargs):
        calls["nssp"] = kwargs
        return nssp

    def fake_load_nhsn(**kwargs):
        calls["nhsn"] = kwargs
        return nhsn

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
        run_date=report_date,
        first_training_date=dt.date(2025, 12, 1),
        last_training_date=dt.date(2026, 1, 7),
        sources={"nssp", "nhsn"},
    )

    assert forecast_data.loc_abb == "CA"
    assert forecast_data.disease == "COVID-19"
    assert forecast_data.report_date == report_date
    assert forecast_data.loc_pop == 39_000_000
    assert forecast_data.right_truncation_offset == 0
    expected_nssp = pl.DataFrame(
        {
            "observed_ed_visits": [10],
            "other_ed_visits": [90],
            "data_type": ["eval"],
            "resolution": ["daily"],
        }
    )
    assert_frame_equal(
        forecast_data.nssp.data.select(
            "observed_ed_visits",
            "other_ed_visits",
            "data_type",
            "resolution",
        ),
        expected_nssp,
    )
    expected_nhsn = pl.DataFrame(
        {
            "data_type": ["train"],
            "resolution": ["epiweekly"],
        }
    )
    assert_frame_equal(
        forecast_data.nhsn.data.select("data_type", "resolution"),
        expected_nhsn,
    )
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
        "last_training_date": dt.date(2026, 1, 7),
        "run_date": report_date,
    }
    assert calls["nhsn"] == {
        "disease": "COVID-19",
        "loc_abb": "CA",
        "first_training_date": dt.date(2025, 12, 1),
        "last_training_date": dt.date(2026, 1, 7),
        "run_date": report_date,
    }


@pytest.mark.parametrize("requested_source", ["nssp", "nhsn"])
def test_load_forecast_data_only_loads_requested_source(
    monkeypatch,
    requested_source,
):
    nssp = data_access.NSSPData(
        data=pl.DataFrame(
            {
                "date": [dt.date(2026, 1, 8)],
                "geo_value": ["CA"],
                "observed_ed_visits": [10],
                "other_ed_visits": [90],
                "data_type": ["eval"],
                "resolution": ["daily"],
            }
        ),
        freshness=_freshness("nssp"),
    )
    nhsn = data_access.NHSNData(
        data=pl.DataFrame(
            {
                "weekendingdate": [dt.date(2026, 1, 3)],
                "jurisdiction": ["CA"],
                "hospital_admissions": [5],
                "data_type": ["train"],
                "resolution": ["epiweekly"],
            }
        ),
        freshness=_freshness("nhsn"),
        prelim=True,
    )

    def fail_if_called(**kwargs):
        raise AssertionError(f"Excluded source loader called with {kwargs}")

    if requested_source == "nssp":
        monkeypatch.setattr(
            data_access,
            "_load_dataops_nssp",
            lambda **kwargs: nssp,
        )
        monkeypatch.setattr(data_access, "_load_dataops_nhsn", fail_if_called)
    else:
        monkeypatch.setattr(data_access, "_load_dataops_nssp", fail_if_called)
        monkeypatch.setattr(
            data_access,
            "_load_dataops_nhsn",
            lambda **kwargs: nhsn,
        )
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
        sources={requested_source},
        fail_on_stale_data=True,
    )

    assert (forecast_data.nssp is not None) == (requested_source == "nssp")
    assert (forecast_data.nhsn is not None) == (requested_source == "nhsn")
    assert tuple(record.source for record in forecast_data.freshness) == (
        requested_source,
    )


@pytest.mark.parametrize(
    ("sources", "message"),
    [
        pytest.param(set(), "At least one", id="empty"),
        pytest.param({"nwss"}, "Unknown forecast data source", id="unknown"),
    ],
)
def test_load_forecast_data_rejects_invalid_sources(sources, message):
    with pytest.raises(ValueError, match=message):
        data_access.load_forecast_data(
            disease="COVID-19",
            loc_abb="CA",
            run_date=dt.date(2026, 1, 8),
            first_training_date=dt.date(2025, 12, 1),
            last_training_date=dt.date(2026, 1, 7),
            sources=sources,
        )
