import datetime as dt

import polars as pl
import pytest

from cfa.stf.data import get_data
from cfa.stf.forecasttools import ensure_list
from tests.cfa.stf.data.data_test_utils import (
    _unique_values,
    catalog_test,
    skip_without_ext_env,
)


@pytest.fixture
def nssp_data() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "metric": [
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "count_ed_visits",
                "other_metric",
            ],
            "disease": [
                "COVID-19/Omicron",
                "Influenza",
                "RSV",
                "Total",
                "COVID-19",
                "Influenza",
                "COVID-19",
                "Influenza",
                "RSV",
                "Total",
                "COVID-19",
            ],
            "geo_value": [
                "AK",
                "AK",
                "CA",
                "CA",
                "CA",
                "CA",
                "SD",
                "SD",
                "US",
                "US",
                "CA",
            ],
            "reference_date": [
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 6),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
                dt.date(2024, 1, 13),
            ],
            "value": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 999],
        }
    ).with_columns(pl.col("geo_value").cast(pl.Categorical))


@pytest.fixture(autouse=True)
def mock_nssp_data(monkeypatch, nssp_data: pl.DataFrame, request) -> None:
    if request.node.get_closest_marker("catalog"):
        return

    def get_dataframe(output: str, version: str):
        if output != "pl_lazy":
            raise ValueError(f"Unexpected output={output!r}")
        if version is None:
            raise ValueError("Expected a version constraint")
        return nssp_data.lazy()

    monkeypatch.setattr(
        get_data.datacat.public.stf.nssp_gold_v1.load,
        "get_dataframe",
        get_dataframe,
    )
    monkeypatch.setattr(
        get_data.datacat.public.stf.comprehensive_nssp_gold.load,
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
def test_get_nssp_filters_locations(loc_abb) -> None:
    expected_geo_values = set(ensure_list(loc_abb))
    result = set(
        _unique_values(get_data.get_nssp(loc_abb=loc_abb, lazy=False), "geo_value")
    )
    assert result == expected_geo_values


@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        ["COVID-19", "Influenza"],
    ],
)
def test_get_nssp_filters_diseases(disease) -> None:
    expected_diseases = set(ensure_list(disease))
    result = set(
        _unique_values(get_data.get_nssp(disease=disease, lazy=False), "disease")
    )
    assert result == expected_diseases


def test_get_nssp_returns_all_locations_and_diseases() -> None:
    result = get_data.get_nssp(lazy=False)

    assert {"COVID-19", "Influenza", "RSV", "Total"} == _unique_values(
        result, "disease"
    )
    assert {"US", "CA", "SD"}.issubset(_unique_values(result, "geo_value"))


def test_get_nssp_warns_about_missing_filters() -> None:
    with pytest.warns(UserWarning) as warnings:
        result = get_data.get_nssp(
            loc_abb=["CA", "US", "XY"],
            disease=["COVID-19", "Influenza", "ZZ"],
            lazy=False,
        )

    warning_messages = [str(warning.message) for warning in warnings]
    assert any("Requested diseases {'ZZ'} not found" in msg for msg in warning_messages)
    assert any(
        "Requested locations {'XY'} not found" in msg for msg in warning_messages
    )
    assert _unique_values(result, "geo_value") == {"CA", "US"}
    assert _unique_values(result, "disease") == {"COVID-19", "Influenza"}


@skip_without_ext_env
@catalog_test
@pytest.mark.parametrize(
    "loc_abb",
    [
        "US",
        "AK",
        ["AK", "CA"],
        ["CA", "US"],
    ],
)
def test_catalog_get_nssp_filters_locations(loc_abb) -> None:
    expected_geo_values = set(ensure_list(loc_abb))
    result = set(
        _unique_values(get_data.get_nssp(loc_abb=loc_abb, lazy=False), "geo_value")
    )
    assert result == expected_geo_values


@skip_without_ext_env
@catalog_test
@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        ["COVID-19", "Influenza"],
    ],
)
def test_catalog_get_nssp_filters_diseases(disease) -> None:
    expected_diseases = set(ensure_list(disease))
    result = set(
        _unique_values(get_data.get_nssp(disease=disease, lazy=False), "disease")
    )
    assert result == expected_diseases


@skip_without_ext_env
@catalog_test
def test_catalog_get_nssp_returns_all_locations_and_diseases() -> None:
    result = get_data.get_nssp(lazy=False)

    assert {"COVID-19", "Influenza", "RSV", "Total"} == _unique_values(
        result, "disease"
    )
    assert {"US", "CA", "SD"}.issubset(_unique_values(result, "geo_value"))
