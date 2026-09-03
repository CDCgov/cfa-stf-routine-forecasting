import datetime as dt
from collections.abc import Collection
from pathlib import Path

import polars as pl

from cfa.stf.routine.data.data_access import (
    DataFreshness,
    ForecastSourceName,
    NHSNData,
    NSSPData,
    SurveillanceInputs,
)
from cfa.stf.routine.forecast_run import ForecastRun

DEFAULT_REPORT_DATE = dt.date(2024, 12, 20)


def make_test_surveillance_inputs(
    *,
    loc_abb: str = "CA",
    report_date: dt.date = DEFAULT_REPORT_DATE,
    last_training_date: dt.date | None = None,
    loc_pop: int = 1,
    nhsn_prelim: bool = False,
    sources: Collection[ForecastSourceName] = ("nssp", "nhsn"),
) -> SurveillanceInputs:
    last_training_date = last_training_date or report_date
    requested_sources = frozenset(sources)

    nssp_data = pl.DataFrame(
        {
            "date": [last_training_date] * 2,
            "state_abb": [loc_abb] * 2,
            ".variable": ["observed_ed_visits", "other_ed_visits"],
            ".value": [10, 90],
            "data_type": ["train"] * 2,
            "resolution": ["daily"] * 2,
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
            resolution="daily",
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
    return SurveillanceInputs(
        loc_pop=loc_pop,
        nssp=nssp,
        nhsn=nhsn,
    )


def make_test_forecast_run(
    *,
    output_dir: Path | str,
    disease: str = "covid",
    loc: str = "CA",
    report_date: dt.date = DEFAULT_REPORT_DATE,
    n_training_days: int = 90,
    first_training_date: dt.date | None = None,
    last_training_date: dt.date | None = None,
    n_forecast_days: int = 28,
    exclude_last_n_days: int = 0,
    model_name: str = "test_model",
    loc_pop: int = 1,
    nhsn_prelim: bool = False,
    sources: Collection[ForecastSourceName] = ("nssp", "nhsn"),
) -> ForecastRun:
    """Build internally consistent state for one test forecast run."""
    expected_last_training_date = report_date - dt.timedelta(
        days=exclude_last_n_days + 1
    )
    if last_training_date is None:
        last_training_date = expected_last_training_date
    elif last_training_date != expected_last_training_date:
        raise ValueError(
            "last_training_date must agree with report_date and exclude_last_n_days"
        )

    expected_first_training_date = last_training_date - dt.timedelta(
        days=n_training_days - 1
    )
    if first_training_date is None:
        first_training_date = expected_first_training_date
    elif first_training_date != expected_first_training_date:
        raise ValueError(
            "first_training_date must agree with last_training_date and n_training_days"
        )

    surveillance = make_test_surveillance_inputs(
        loc_abb=loc,
        report_date=report_date,
        last_training_date=last_training_date,
        loc_pop=loc_pop,
        nhsn_prelim=nhsn_prelim,
        sources=sources,
    )
    return ForecastRun(
        disease=disease,
        loc=loc,
        report_date=report_date,
        n_training_days=n_training_days,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        n_forecast_days=n_forecast_days,
        exclude_last_n_days=exclude_last_n_days,
        model_name=model_name,
        output_dir=Path(output_dir),
        surveillance=surveillance,
    )
