from contextlib import nullcontext
from datetime import date

import polars as pl
import pytest

from pipelines.data import prep_data

valid_diseases = ["COVID-19", "Influenza", "RSV"]


@pytest.mark.parametrize(
    "pivoted_raw_data",
    [
        pl.DataFrame(
            {
                "COVID-19": [10, 15, 20],
                "Influenza": [12, 16, 22],
                "RSV": [0, 2, 0],
                "Total": [497, 502, 499],
                "date": [date(2024, 12, 29), date(2025, 1, 1), date(2025, 1, 3)],
            }
        )
    ],
)
@pytest.mark.parametrize("disease", valid_diseases + ["Iffluenza", "COVID_19"])
@pytest.mark.parametrize(
    "last_data_date",
    [date(2025, 12, 12), date(2024, 12, 1), date(2025, 1, 2), date(2024, 12, 29)],
)
def test_clean_nssp_data(pivoted_raw_data, disease, last_data_date):
    """
    Confirm that clean_nssp_data works as expected.
    """
    raw_data = pivoted_raw_data.unpivot(
        index="date", variable_name="disease", value_name="ed_visits"
    )
    invalid_disease = disease not in valid_diseases

    expect_empty_df = invalid_disease

    if expect_empty_df:
        context = pytest.raises(pl.exceptions.ColumnNotFoundError, match=disease)
    else:
        context = nullcontext()
    with context:
        result = prep_data.clean_nssp_data(raw_data, disease, last_data_date)
    if not expect_empty_df:
        expected = (
            pivoted_raw_data.select(
                pl.col("date"),
                pl.col(disease).alias("observed_ed_visits"),
                pl.col("Total"),
            )
            .with_columns(
                other_ed_visits=pl.col("Total") - pl.col("observed_ed_visits"),
                data_type=pl.when(pl.col("date") <= last_data_date)
                .then(pl.lit("train"))
                .otherwise(pl.lit("eval")),
            )
            .drop("Total")
            .sort("date")
        )
        assert result.select(
            ["date", "observed_ed_visits", "other_ed_visits", "data_type"]
        ).equals(expected)
