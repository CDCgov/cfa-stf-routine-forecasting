import datetime as dt
import logging

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
