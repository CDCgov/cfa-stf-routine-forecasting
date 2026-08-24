"""Tests for forecast dataset aggregation utilities."""

import datetime as dt

import polars as pl

from cfa.stf.routine.utils.data_utils import (
    aggregate_long_to_epiweekly,
    generate_epiweekly_data,
)


def test_aggregate_long_to_epiweekly_accepts_date_and_value_columns():
    dates = [dt.date(2025, 1, 5) + dt.timedelta(days=day) for day in range(7)]
    data = pl.DataFrame(
        {
            "observation_date": dates,
            "location": ["CA"] * 7,
            "visits": list(range(1, 8)),
            "resolution": ["daily"] * 7,
        }
    )

    result = aggregate_long_to_epiweekly(
        data,
        date_col="observation_date",
        value_col="visits",
    )

    assert result.select("observation_date", "visits", "resolution").row(0) == (
        dt.date(2025, 1, 11),
        28,
        "epiweekly",
    )


def test_generate_epiweekly_data_aggregates_ed_visits_and_preserves_other_data(
    tmp_path,
):
    dates = [dt.date(2025, 1, 5) + dt.timedelta(days=day) for day in range(14)]
    daily_ed_data = pl.DataFrame(
        {
            "date": dates * 2,
            "geo_value": ["CA"] * 28,
            "disease": ["flu"] * 28,
            "data_type": ["train"] * 28,
            "resolution": ["daily"] * 28,
            ".variable": ["observed_ed_visits"] * 14 + ["other_ed_visits"] * 14,
            ".value": list(range(1, 15)) + [9] * 14,
        }
    )
    hospital_data = pl.DataFrame(
        {
            "date": [dt.date(2025, 1, 11)],
            "geo_value": ["CA"],
            "disease": ["flu"],
            "data_type": ["train"],
            "resolution": ["epiweekly"],
            ".variable": ["observed_hospital_admissions"],
            ".value": [3],
        }
    )
    combined_data_path = tmp_path / "combined_data.tsv"
    pl.concat([daily_ed_data, hospital_data]).write_csv(
        combined_data_path, separator="\t"
    )

    result = generate_epiweekly_data(combined_data_path)
    assert result.select("date", ".variable", ".value").rows() == [
        (dt.date(2025, 1, 11), "observed_ed_visits", 28),
        (dt.date(2025, 1, 11), "observed_hospital_admissions", 3),
        (dt.date(2025, 1, 11), "other_ed_visits", 63),
        (dt.date(2025, 1, 18), "observed_ed_visits", 77),
        (dt.date(2025, 1, 18), "other_ed_visits", 63),
    ]
    assert result.filter(pl.col(".variable").str.ends_with("_ed_visits")).get_column(
        "resolution"
    ).unique().to_list() == ["epiweekly"]
