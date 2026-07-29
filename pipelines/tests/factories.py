import datetime as dt

import polars as pl

from pipelines.data.data_access import DataFreshness, ForecastData

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
) -> ForecastData:
    first_training_date = first_training_date or report_date - dt.timedelta(days=89)
    last_training_date = last_training_date or report_date

    nssp_data = pl.DataFrame(
        {
            "date": [last_training_date, last_training_date],
            "geo_value": [loc_abb, loc_abb],
            "disease": [disease, "Total"],
            "ed_visits": [10, 100],
        }
    )
    nhsn_data = pl.DataFrame(
        {
            "weekendingdate": [last_training_date],
            "jurisdiction": [loc_abb],
            "disease": [disease],
            "hospital_admissions": [5],
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

    return ForecastData.from_source_frames(
        loc_abb=loc_abb,
        disease=disease,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        nssp_data=nssp_data,
        nssp_freshness=freshness("nssp"),
        nhsn_data=nhsn_data,
        nhsn_freshness=freshness("nhsn"),
        nhsn_prelim=nhsn_prelim,
        loc_pop=loc_pop,
    )
