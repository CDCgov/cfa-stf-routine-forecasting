import datetime as dt
from collections.abc import Collection

import polars as pl

from cfa.stf.routine.data.data_access import (
    DataFreshness,
    ForecastData,
    ForecastSourceName,
    NHSNData,
    NSSPData,
)

DEFAULT_REPORT_DATE = dt.date(2024, 12, 20)


def make_test_forecast_data(
    *,
    loc_abb: str = "CA",
    disease: str = "COVID-19",
    report_date: dt.date = DEFAULT_REPORT_DATE,
    first_training_date: dt.date | None = None,
    last_training_date: dt.date | None = None,
    loc_pop: int = 1,
    nhsn_prelim: bool = False,
    sources: Collection[ForecastSourceName] = ("nssp", "nhsn"),
) -> ForecastData:
    last_training_date = last_training_date or report_date
    requested_sources = frozenset(sources)

    nssp_data = pl.DataFrame(
        {
            "date": [last_training_date],
            "state_abb": [loc_abb],
            "observed_ed_visits": [10],
            "other_ed_visits": [90],
            "data_type": ["train"],
            "resolution": ["daily"],
        }
    )
    nhsn_data = pl.DataFrame(
        {
            "date": [last_training_date],
            "state_abb": [loc_abb],
            "value": [5],
            "data_type": ["train"],
            "resolution": ["epiweekly"],
        }
    )

    def freshness(source: str) -> DataFreshness:
        return DataFreshness(
            source=source,
            selected_version_date=report_date,
            latest_observed_date=last_training_date,
            run_date=report_date,
            is_stale=False,
            reason=f"Test {source.upper()} data",
        )

    nssp = (
        NSSPData(
            data=nssp_data,
            freshness=freshness("nssp"),
        )
        if "nssp" in requested_sources
        else None
    )
    nhsn = (
        NHSNData(
            data=nhsn_data,
            freshness=freshness("nhsn"),
            prelim=nhsn_prelim,
        )
        if "nhsn" in requested_sources
        else None
    )
    return ForecastData(
        loc_abb=loc_abb,
        disease=disease,
        report_date=report_date,
        loc_pop=loc_pop,
        right_truncation_offset=(report_date - last_training_date).days - 1,
        nssp=nssp,
        nhsn=nhsn,
    )
