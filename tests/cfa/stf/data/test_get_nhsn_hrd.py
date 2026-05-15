import datetime as dt

import polars as pl
import pytest

from cfa.stf.data import get_data
from cfa.stf.forecasttools import ensure_list
from tests.cfa.stf.data.data_test_utils import (
    _unique_values,
    catalog_ext_env_test,
)


@pytest.fixture
def nhsn_hrd_data() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "weekendingdate": [
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
            ],
            "jurisdiction": ["USA", "AK", "CA", "SD"],
            "totalconfc19newadm": [10, 20, 30, 40],
            "totalconfflunewadm": [50, 60, 70, 80],
            "totalconfrsvnewadm": [90, 100, 110, 120],
        }
    )


@pytest.fixture(autouse=True)
def mock_nhsn_hrd_data(monkeypatch, nhsn_hrd_data: pl.DataFrame, request) -> None:
    if request.node.get_closest_marker("catalog"):
        return

    def get_dataframe(output: str, version: str):
        if output != "pl_lazy":
            raise ValueError(f"Unexpected output={output!r}")
        if version is None:
            raise ValueError("Expected a version constraint")
        return nhsn_hrd_data.lazy()

    monkeypatch.setattr(
        get_data.datacat.public.stf.nhsn_hrd_prelim.load,
        "get_dataframe",
        get_dataframe,
    )
    monkeypatch.setattr(
        get_data.datacat.public.stf.nhsn_hrd.load,
        "get_dataframe",
        get_dataframe,
    )


@pytest.mark.parametrize(
    "loc_abb",
    [
        "US",
        "AK",
        ["AK", "CA"],
        ["CA", "US"],
    ],
)
def test_get_nhsn_hrd_filters_locations(loc_abb) -> None:
    expected_jurisdictions = set(ensure_list(loc_abb))
    result = set(
        _unique_values(
            get_data.get_nhsn_hrd(loc_abb=loc_abb, lazy=False), "jurisdiction"
        )
    )
    assert result == expected_jurisdictions


@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        ["COVID-19", "Influenza"],
    ],
)
def test_get_nhsn_hrd_filters_diseases(disease) -> None:
    expected_diseases = set(ensure_list(disease))
    result = set(
        _unique_values(get_data.get_nhsn_hrd(disease=disease, lazy=False), "disease")
    )
    assert result == expected_diseases


def test_get_nhsn_hrd_returns_all_locations_and_diseases() -> None:
    result = get_data.get_nhsn_hrd(lazy=False)

    assert {"COVID-19", "Influenza", "RSV"} == _unique_values(result, "disease")
    assert {"US", "CA", "SD"}.issubset(_unique_values(result, "jurisdiction"))


def test_get_nhsn_hrd_warns_about_missing_filters() -> None:
    with pytest.warns(UserWarning) as warnings:
        result = get_data.get_nhsn_hrd(
            loc_abb=["CA", "US", "XY"],
            disease=["COVID-19", "Influenza", "ZZ"],
            lazy=False,
        )

    warning_messages = [str(warning.message) for warning in warnings]
    assert any("Requested diseases {'ZZ'} not found" in msg for msg in warning_messages)
    assert any(
        "Requested locations {'XY'} not found" in msg for msg in warning_messages
    )
    assert _unique_values(result, "jurisdiction") == {"CA", "US"}
    assert _unique_values(result, "disease") == {"COVID-19", "Influenza"}


@catalog_ext_env_test
@pytest.mark.parametrize(
    "loc_abb",
    [
        "US",
        "AK",
        ["AK", "CA"],
        ["CA", "US"],
    ],
)
def test_catalog_get_nhsn_hrd_filters_locations(
    loc_abb,
) -> None:
    expected_jurisdictions = set(ensure_list(loc_abb))
    result = set(
        _unique_values(
            get_data.get_nhsn_hrd(loc_abb=loc_abb, lazy=False), "jurisdiction"
        )
    )
    assert result == expected_jurisdictions


@catalog_ext_env_test
@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        ["COVID-19", "Influenza"],
    ],
)
def test_catalog_get_nhsn_hrd_filters_diseases(
    disease,
) -> None:
    expected_diseases = set(ensure_list(disease))
    result = set(
        _unique_values(get_data.get_nhsn_hrd(disease=disease, lazy=False), "disease")
    )
    assert result == expected_diseases


@catalog_ext_env_test
def test_catalog_get_nhsn_hrd_returns_all_locations_and_diseases() -> None:
    result = get_data.get_nhsn_hrd(lazy=False)

    assert {"COVID-19", "Influenza", "RSV"} == _unique_values(result, "disease")
    assert {"US", "CA", "SD"}.issubset(_unique_values(result, "jurisdiction"))
