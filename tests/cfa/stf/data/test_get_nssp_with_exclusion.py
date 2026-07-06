import datetime as dt
import importlib

import polars as pl
import pytest

from cfa.stf.data import get_nssp_with_exclusion
from tests.cfa.stf.data.data_test_utils import requires_ext_catalog

nssp_exclusion = importlib.import_module("cfa.stf.data.get_nssp_with_exclusion")


def make_nssp_series(
    reference_dates: list[dt.date],
    values: list[int],
    disease: str = "RSV",
    geo_value: str = "LA",
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "reference_date": reference_dates,
            "disease": [disease] * len(reference_dates),
            "geo_value": [geo_value] * len(reference_dates),
            "value": values,
        }
    )


@pytest.fixture
def single_series_nssp_data() -> pl.DataFrame:
    return make_nssp_series(
        reference_dates=[
            dt.date(2024, 1, 6),
            dt.date(2024, 1, 13),
            dt.date(2024, 1, 20),
            dt.date(2024, 1, 27),
        ],
        values=[10, 20, 30, 40],
    )


@pytest.fixture
def mock_get_nssp(monkeypatch, single_series_nssp_data: pl.DataFrame) -> list[dict]:
    calls = []

    def _get_nssp(**kwargs) -> pl.DataFrame:
        calls.append(kwargs)
        return single_series_nssp_data

    monkeypatch.setattr(nssp_exclusion, "get_nssp", _get_nssp)
    return calls


def test_tail_by_n_excludes_final_n_reference_dates(mock_get_nssp) -> None:
    result = get_nssp_with_exclusion(
        as_of=dt.date(2024, 2, 1),
        loc_abb="LA",
        disease="RSV",
        exclusion_strategy="tail_by_n",
        n=2,
    )

    assert result.select("reference_date", "exclude").to_dict(as_series=False) == {
        "reference_date": [
            dt.date(2024, 1, 6),
            dt.date(2024, 1, 13),
            dt.date(2024, 1, 20),
            dt.date(2024, 1, 27),
        ],
        "exclude": [False, False, True, True],
    }


def test_tail_by_n_uses_unique_reference_dates(monkeypatch) -> None:
    nssp_data = make_nssp_series(
        reference_dates=[
            dt.date(2024, 1, 6),
            dt.date(2024, 1, 13),
            dt.date(2024, 1, 13),
            dt.date(2024, 1, 20),
        ],
        values=[10, 20, 21, 30],
    )
    monkeypatch.setattr(nssp_exclusion, "get_nssp", lambda **kwargs: nssp_data)

    result = get_nssp_with_exclusion(
        loc_abb="LA",
        disease="RSV",
        exclusion_strategy="tail_by_n",
        n=1,
    )

    assert result.filter(pl.col("reference_date") == dt.date(2024, 1, 13))[
        "exclude"
    ].to_list() == [False, False]
    assert result.filter(pl.col("reference_date") == dt.date(2024, 1, 20))[
        "exclude"
    ].to_list() == [True]


@pytest.mark.parametrize(
    "exclusion_strategy,expected_exclusion_calc_disease",
    [
        ("tail_by_target_disease", "RSV"),
        ("tail_by_all_disease", ["Influenza", "RSV", "COVID-19"]),
        ("tail_by_total", "Total"),
    ],
)
def test_auto_strategies_use_expected_exclusion_series(
    monkeypatch,
    mock_get_nssp,
    exclusion_strategy: str,
    expected_exclusion_calc_disease: str | list[str],
) -> None:
    calls = []

    def _exclude_tail_auto(**kwargs) -> pl.DataFrame:
        calls.append(kwargs)
        return pl.DataFrame(
            {
                "reference_date": [
                    dt.date(2024, 1, 6),
                    dt.date(2024, 1, 13),
                    dt.date(2024, 1, 20),
                    dt.date(2024, 1, 27),
                ],
                "exclude": [False, False, False, True],
            }
        )

    monkeypatch.setattr(nssp_exclusion, "exclude_tail_auto", _exclude_tail_auto)

    result = get_nssp_with_exclusion(
        as_of=dt.date(2024, 2, 1),
        loc_abb="LA",
        disease="RSV",
        start_date=dt.date(2024, 1, 6),
        end_date=dt.date(2024, 1, 27),
        exclusion_strategy=exclusion_strategy,
        max_tail_length=3,
    )

    assert result["exclude"].to_list() == [False, False, False, True]
    assert calls == [
        {
            "loc_abb": "LA",
            "as_of": dt.date(2024, 2, 1),
            "start_date": dt.date(2024, 1, 6),
            "end_date": dt.date(2024, 1, 27),
            "exclusion_calc_disease": expected_exclusion_calc_disease,
            "max_tail_length": 3,
        }
    ]


def test_auto_strategy_forwards_exclusion_strategy_args(
    monkeypatch, mock_get_nssp
) -> None:
    calls = []

    def _exclude_tail_auto(**kwargs) -> pl.DataFrame:
        calls.append(kwargs)
        return pl.DataFrame(
            {
                "reference_date": [
                    dt.date(2024, 1, 6),
                    dt.date(2024, 1, 13),
                    dt.date(2024, 1, 20),
                    dt.date(2024, 1, 27),
                ],
                "exclude": [False, False, False, True],
            }
        )

    monkeypatch.setattr(nssp_exclusion, "exclude_tail_auto", _exclude_tail_auto)

    get_nssp_with_exclusion(
        as_of=dt.date(2024, 2, 1),
        loc_abb="LA",
        disease="RSV",
        exclusion_strategy="tail_by_target_disease",
        loc_estimator="mean",
        outlier_multiplier=4.5,
        max_tail_length=2,
        nowcast_adjustment=True,
    )

    assert mock_get_nssp == [
        {
            "as_of": dt.date(2024, 2, 1),
            "loc_abb": "LA",
            "disease": "RSV",
            "dataset": "gold",
            "start_date": None,
            "end_date": None,
            "lazy": False,
        }
    ]
    assert calls == [
        {
            "loc_abb": "LA",
            "as_of": dt.date(2024, 2, 1),
            "start_date": None,
            "end_date": None,
            "exclusion_calc_disease": "RSV",
            "loc_estimator": "mean",
            "outlier_multiplier": 4.5,
            "max_tail_length": 2,
            "nowcast_adjustment": True,
        }
    ]


def test_exclude_tail_auto_applies_nowcast_adjustment(monkeypatch) -> None:
    nssp_data = make_nssp_series(
        reference_dates=[
            dt.date(2024, 1, 6),
            dt.date(2024, 1, 13),
            dt.date(2024, 1, 20),
            dt.date(2024, 1, 27),
            dt.date(2024, 2, 3),
            dt.date(2024, 2, 10),
        ],
        values=[100, 105, 110, 115, 120, 10],
    )
    pmf_calls = []

    def _get_nnh_right_truncation_pmf(**kwargs) -> list[float]:
        pmf_calls.append(kwargs)
        return [0.05, 0.95]

    monkeypatch.setattr(nssp_exclusion, "get_nssp", lambda **kwargs: nssp_data)
    monkeypatch.setattr(
        nssp_exclusion,
        "get_nnh_right_truncation_pmf",
        _get_nnh_right_truncation_pmf,
    )

    unadjusted = nssp_exclusion.exclude_tail_auto(
        as_of=dt.date(2024, 2, 15),
        loc_abb="LA",
        exclusion_calc_disease="RSV",
        max_tail_length=2,
        outlier_multiplier=4.5,
        nowcast_adjustment=False,
    )
    adjusted = nssp_exclusion.exclude_tail_auto(
        as_of=dt.date(2024, 2, 15),
        loc_abb="LA",
        exclusion_calc_disease="RSV",
        max_tail_length=2,
        outlier_multiplier=4.5,
        nowcast_adjustment=True,
    )

    assert unadjusted["exclude"].to_list() == [False, False, False, False, False, True]
    assert adjusted["exclude"].to_list() == [False] * 6
    assert pmf_calls == [
        {
            "as_of": dt.date(2024, 2, 15),
            "loc_abb": "LA",
            "disease": "RSV",
        }
    ]


def test_forwards_nssp_query_args(mock_get_nssp) -> None:
    get_nssp_with_exclusion(
        as_of=dt.date(2024, 2, 1),
        loc_abb="LA",
        disease="RSV",
        dataset="comprehensive",
        start_date=dt.date(2024, 1, 6),
        end_date=dt.date(2024, 1, 27),
        exclusion_strategy="tail_by_n",
        n=1,
    )

    assert mock_get_nssp == [
        {
            "as_of": dt.date(2024, 2, 1),
            "loc_abb": "LA",
            "disease": "RSV",
            "dataset": "comprehensive",
            "start_date": dt.date(2024, 1, 6),
            "end_date": dt.date(2024, 1, 27),
            "lazy": False,
        }
    ]


@pytest.mark.parametrize(
    "kwargs,expected_message",
    [
        (
            {"disease": ["RSV", "Influenza"], "loc_abb": "LA"},
            "Only one disease can be processed",
        ),
        (
            {"disease": "RSV", "loc_abb": ["LA", "TX"]},
            "Only one location can be processed",
        ),
        (
            {
                "disease": "RSV",
                "loc_abb": "LA",
                "exclusion_strategy": "unknown_strategy",
            },
            "exclusion_strategy must be one of",
        ),
    ],
)
def test_rejects_unsupported_inputs(
    mock_get_nssp, kwargs: dict, expected_message: str
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        get_nssp_with_exclusion(**kwargs)

    assert mock_get_nssp == []


@requires_ext_catalog
@pytest.mark.parametrize(
    "exclusion_strategy,exclusion_strategy_args",
    [
        ("tail_by_target_disease", {}),
        ("tail_by_total", {}),
        ("tail_by_all_disease", {}),
        ("tail_by_n", {"n": 2}),
    ],
)
def test_get_nssp_with_exclusion_placeholder(
    exclusion_strategy: str,
    exclusion_strategy_args: dict,
) -> None:
    get_nssp_with_exclusion(
        as_of=dt.date(2026, 5, 6),
        loc_abb="LA",
        disease="RSV",
        exclusion_strategy=exclusion_strategy,
        **exclusion_strategy_args,
    )
