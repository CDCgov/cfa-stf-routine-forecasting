import pytest

from cfa.stf.data import get_nssp
from cfa.stf.forecasttools import ensure_list

# pytestmark = pytest.mark.skipif(
#     not check_ext_env(),
#     reason="requires external CFA data environment",
# )


def _unique_values(df, column: str) -> set[str]:
    if hasattr(df, "collect"):
        df = df.collect()
    return set(df.unique(column).get_column(column).to_list())


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
    result = set(_unique_values(get_nssp(loc_abb=loc_abb, lazy=False), "geo_value"))
    assert result == expected_geo_values


# def test_get_nssp_returns_all_locations_and_diseases() -> None:
#     result = get_nssp(lazy=False)

#     assert not result.is_empty()
#     assert set(result.columns) == {"reference_date", "disease", "geo_value", "value"}
#     assert {"COVID-19", "Influenza", "RSV", "Total"}.issubset(
#         _unique_values(result, "disease")
#     )
#     assert "US" in _unique_values(result, "geo_value")


# def test_get_nssp_warns_about_missing_filters() -> None:
#     with pytest.warns(UserWarning) as warnings:
#         result = get_nssp(
#             loc_abb=["CA", "US", "XY"],
#             disease=["COVID-19", "Influenza", "ZZ"],
#             lazy=False,
#         )

#     warning_messages = [str(warning.message) for warning in warnings]
#     assert any("Requested diseases {'ZZ'} not found" in msg for msg in warning_messages)
#     assert any(
#         "Requested locations {'XY'} not found" in msg for msg in warning_messages
#     )
#     assert _unique_values(result, "geo_value") == {"CA", "US"}
#     assert _unique_values(result, "disease") == {"COVID-19", "Influenza"}


# def test_get_nhsn_hrd_warns_about_missing_filters() -> None:
#     with pytest.warns(UserWarning) as warnings:
#         result = get_nhsn_hrd(
#             loc_abb=["CA", "US", "XY"],
#             disease=["COVID-19", "Influenza", "ZZ"],
#             lazy=False,
#         )

#     warning_messages = [str(warning.message) for warning in warnings]
#     assert any("Requested diseases {'ZZ'} not found" in msg for msg in warning_messages)
#     assert any(
#         "Requested locations {'XY'} not found" in msg for msg in warning_messages
#     )
#     assert _unique_values(result, "jurisdiction") == {"CA", "US"}
#     assert _unique_values(result, "disease") == {"COVID-19", "Influenza"}


# def test_get_nhsn_hrd_filters_single_location_and_disease() -> None:
#     result = get_nhsn_hrd(loc_abb="CA", disease="COVID-19", lazy=False)

#     assert not result.is_empty()
#     assert set(result.columns) == {
#         "weekendingdate",
#         "jurisdiction",
#         "disease",
#         "hospital_admissions",
#     }
#     assert _unique_values(result, "jurisdiction") == {"CA"}
#     assert _unique_values(result, "disease") == {"COVID-19"}


# def test_get_nhsn_hrd_returns_all_locations_and_diseases() -> None:
#     result = get_nhsn_hrd(lazy=False)

#     assert not result.is_empty()
#     assert set(result.columns) == {
#         "weekendingdate",
#         "jurisdiction",
#         "disease",
#         "hospital_admissions",
#     }
#     assert {"COVID-19", "Influenza", "RSV"}.issubset(_unique_values(result, "disease"))
#     assert "US" in _unique_values(result, "jurisdiction")
