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
from cfa.stf.routine.forecast_window import ForecastWindow

DEFAULT_REPORT_DATE = dt.date(2024, 12, 20)


def make_test_surveillance_inputs(
    *,
    loc_abb: str = "CA",
    report_date: dt.date = DEFAULT_REPORT_DATE,
    first_training_date: dt.date | None = None,
    last_training_date: dt.date | None = None,
    loc_pop: int = 1,
    nhsn_prelim: bool = False,
    sources: Collection[ForecastSourceName] = ("nssp", "nhsn"),
) -> SurveillanceInputs:
    last_training_date = last_training_date or report_date
    first_training_date = first_training_date or last_training_date
    training_dates = list(dict.fromkeys((first_training_date, last_training_date)))
    requested_sources = frozenset(sources)

    nssp_data = pl.DataFrame(
        {
            "date": [date for date in training_dates for _ in range(2)],
            "state_abb": [loc_abb] * (2 * len(training_dates)),
            ".variable": ["observed_ed_visits", "other_ed_visits"]
            * len(training_dates),
            ".value": [10, 90] * len(training_dates),
            "data_type": ["train"] * (2 * len(training_dates)),
            "resolution": ["daily"] * (2 * len(training_dates)),
        }
    )
    nhsn_data = pl.DataFrame(
        {
            "date": training_dates,
            "state_abb": [loc_abb] * len(training_dates),
            "value": [5] * len(training_dates),
            "data_type": ["train"] * len(training_dates),
            "resolution": ["epiweekly"] * len(training_dates),
        }
    )

    def freshness(source: str) -> DataFreshness:
        return DataFreshness(
            source=source,
            selected_version_date=report_date,
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
    n_lookback_days: int = 90,
    min_allowed_training_date: dt.date | None = None,
    first_training_date: dt.date | None = None,
    max_allowed_training_date: dt.date | None = None,
    last_training_date: dt.date | None = None,
    exclude_last_n_days: int = 0,
    model_name: str = "test_model",
    loc_pop: int = 1,
    nhsn_prelim: bool = False,
    sources: Collection[ForecastSourceName] = ("nssp", "nhsn"),
) -> ForecastRun:
    """Build internally consistent state for one test forecast run."""
    expected_max_allowed_training_date = report_date - dt.timedelta(
        days=exclude_last_n_days + 1
    )
    if max_allowed_training_date is None:
        max_allowed_training_date = expected_max_allowed_training_date
    elif max_allowed_training_date != expected_max_allowed_training_date:
        raise ValueError(
            "max_allowed_training_date must agree with report_date and "
            "exclude_last_n_days"
        )

    expected_min_allowed_training_date = report_date - dt.timedelta(
        days=n_lookback_days
    )
    if min_allowed_training_date is None:
        min_allowed_training_date = expected_min_allowed_training_date
    elif min_allowed_training_date != expected_min_allowed_training_date:
        raise ValueError(
            "min_allowed_training_date must agree with report_date and n_lookback_days"
        )
    first_training_date = first_training_date or min_allowed_training_date
    last_training_date = last_training_date or max_allowed_training_date
    if (
        not min_allowed_training_date
        <= first_training_date
        <= last_training_date
        <= max_allowed_training_date
    ):
        raise ValueError("observed training dates must fall within the allowed window")

    surveillance = make_test_surveillance_inputs(
        loc_abb=loc,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        loc_pop=loc_pop,
        nhsn_prelim=nhsn_prelim,
        sources=sources,
    )
    return ForecastRun(
        disease=disease,
        loc=loc,
        forecast_window=ForecastWindow(
            report_date=report_date,
            n_lookback_days=n_lookback_days,
            exclude_last_n_days=exclude_last_n_days,
        ),
        model_name=model_name,
        output_dir=Path(output_dir),
        surveillance=surveillance,
    )
