import datetime as dt

import polars as pl
import pytest

from cfa.stf.data import get_nnh_pmfs
from tests.cfa.stf.data.data_test_utils import (
    _unique_values,
    requires_ext_catalog,
    uses_catalog,
)


def _assert_pmf(result: list[float]) -> None:
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(value >= 0 for value in result)
    assert sum(result) == pytest.approx(1)


@pytest.fixture
def param_estimates() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "disease": "COVID-19",
                "parameter": "generation_interval",
                "value": [0.25, 0.75],
                "geo_value": None,
                "reference_date": dt.date(2024, 1, 10),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "delay",
                "value": [0.1, 0.3, 0.6],
                "geo_value": None,
                "reference_date": dt.date(2024, 1, 10),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "generation_interval",
                "value": [1.0],
                "geo_value": None,
                "reference_date": dt.date(2023, 1, 10),
                "start_date": dt.date(2023, 1, 1),
                "end_date": dt.date(2024, 1, 1),
            },
            {
                "disease": "Influenza",
                "parameter": "generation_interval",
                "value": [0.4, 0.6],
                "geo_value": None,
                "reference_date": dt.date(2024, 1, 10),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "right_truncation",
                "value": [0.7, 0.3],
                "geo_value": "CA",
                "reference_date": dt.date(2024, 1, 10),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "right_truncation",
                "value": [0.2, 0.8],
                "geo_value": "CA",
                "reference_date": dt.date(2024, 1, 20),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "right_truncation",
                "value": [0.5, 0.5],
                "geo_value": "US",
                "reference_date": dt.date(2024, 1, 20),
                "start_date": dt.date(2024, 1, 1),
                "end_date": dt.date(2025, 1, 1),
            },
            {
                "disease": "COVID-19",
                "parameter": "right_truncation",
                "value": [0.6, 0.4],
                "geo_value": "GA",
                "reference_date": dt.date(2025, 10, 14),
                "start_date": dt.date(2025, 1, 1),
                "end_date": dt.date(2025, 10, 15),
            },
        ]
    )


@pytest.fixture(autouse=True)
def mock_param_estimates(monkeypatch, param_estimates: pl.DataFrame, request) -> None:
    if uses_catalog(request):
        return

    def get_dataframe(output: str):
        if output == "pl_lazy":
            return param_estimates.lazy()
        if output == "pl":
            return param_estimates
        raise ValueError(f"Unexpected output={output!r}")

    monkeypatch.setattr(
        get_nnh_pmfs.datacat.public.stf.param_estimates.load,
        "get_dataframe",
        get_dataframe,
    )


@pytest.mark.parametrize(
    "lazy",
    [
        True,
        False,
    ],
)
def test_filter_param_estimates_filters_disease_and_valid_date(lazy) -> None:
    result = get_nnh_pmfs._filter_param_estimates(
        disease="COVID-19",
        as_of=dt.date(2024, 6, 1),
        lazy=lazy,
    )

    assert _unique_values(result, "disease") == {"COVID-19"}
    assert _unique_values(result, "parameter") == {
        "delay",
        "generation_interval",
        "right_truncation",
    }
    if hasattr(result, "collect"):
        result = result.collect()
    assert [1.0] not in result.select("value").to_series().to_list()


@pytest.mark.parametrize(
    ("get_pmf", "expected"),
    [
        (get_nnh_pmfs.get_nnh_generation_interval_pmf, [0.25, 0.75]),
        (get_nnh_pmfs.get_nnh_delay_pmf, [0.1, 0.3, 0.6]),
    ],
)
def test_get_nnh_pmfs_filter_disease_and_parameter(get_pmf, expected) -> None:
    result = get_pmf(disease="COVID-19", as_of=dt.date(2024, 6, 1))

    assert result == expected


@pytest.mark.parametrize(
    ("loc_abb", "reference_date", "expected"),
    [
        ("CA", dt.date(2024, 1, 15), [0.7, 0.3]),
        ("CA", dt.date(2024, 1, 20), [0.2, 0.8]),
        ("US", dt.date(2024, 1, 20), [0.5, 0.5]),
    ],
)
def test_get_nnh_right_truncation_pmf_filters_location_and_reference_date(
    loc_abb,
    reference_date,
    expected,
) -> None:
    result = get_nnh_pmfs.get_nnh_right_truncation_pmf(
        loc_abb=loc_abb,
        disease="COVID-19",
        as_of=dt.date(2024, 6, 1),
        reference_date=reference_date,
    )

    assert result == expected


def test_get_nnh_right_truncation_pmf_caps_ga_as_of_date() -> None:
    result = get_nnh_pmfs.get_nnh_right_truncation_pmf(
        loc_abb="GA",
        disease="COVID-19",
        as_of=dt.date(2025, 11, 1),
    )

    assert result == [0.6, 0.4]


@pytest.mark.parametrize(
    ("get_pmf", "kwargs"),
    [
        (
            get_nnh_pmfs.get_nnh_generation_interval_pmf,
            {"disease": "RSV", "as_of": dt.date(2024, 6, 1)},
        ),
        (
            get_nnh_pmfs.get_nnh_right_truncation_pmf,
            {
                "loc_abb": "XY",
                "disease": "COVID-19",
                "as_of": dt.date(2024, 6, 1),
            },
        ),
    ],
)
def test_get_nnh_pmfs_error_when_exactly_one_row_is_not_found(get_pmf, kwargs) -> None:
    with pytest.raises(ValueError, match="Expected exactly one"):
        get_pmf(**kwargs)


@requires_ext_catalog
@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        "Influenza",
        "RSV",
    ],
)
def test_catalog_get_nnh_generation_interval_pmf_returns_pmf(
    disease,
) -> None:
    result = get_nnh_pmfs.get_nnh_generation_interval_pmf(disease=disease)

    _assert_pmf(result)


@requires_ext_catalog
@pytest.mark.parametrize(
    "disease",
    [
        "COVID-19",
        "Influenza",
        "RSV",
    ],
)
def test_catalog_get_nnh_delay_pmf_returns_pmf(
    disease,
) -> None:
    result = get_nnh_pmfs.get_nnh_delay_pmf(disease=disease)

    _assert_pmf(result)


@requires_ext_catalog
@pytest.mark.parametrize(
    "loc_abb",
    [
        "US",
        "CA",
    ],
)
def test_catalog_get_nnh_right_truncation_pmf_returns_pmf(
    loc_abb,
) -> None:
    result = get_nnh_pmfs.get_nnh_right_truncation_pmf(
        loc_abb=loc_abb,
        disease="COVID-19",
    )

    _assert_pmf(result)
