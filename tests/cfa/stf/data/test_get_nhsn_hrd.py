import pytest
from cfa.cloudops.util import check_ext_env

from cfa.stf.data import get_nhsn_hrd
from cfa.stf.forecasttools import ensure_list
from tests.cfa.stf.data.data_test_utils import _unique_values

pytestmark = pytest.mark.skipif(
    not check_ext_env(),
    reason="requires external CFA data environment",
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
        _unique_values(get_nhsn_hrd(loc_abb=loc_abb, lazy=False), "jurisdiction")
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
    result = set(_unique_values(get_nhsn_hrd(disease=disease, lazy=False), "disease"))
    assert result == expected_diseases


def test_get_nhsn_hrd_returns_all_locations_and_diseases() -> None:
    result = get_nhsn_hrd(lazy=False)

    assert {"COVID-19", "Influenza", "RSV"} == _unique_values(result, "disease")
    assert {"US", "CA", "SD"}.issubset(_unique_values(result, "jurisdiction"))


def test_get_nhsn_hrd_warns_about_missing_filters() -> None:
    with pytest.warns(UserWarning) as warnings:
        result = get_nhsn_hrd(
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
