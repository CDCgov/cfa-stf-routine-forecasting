import datetime as dt
import logging
from dataclasses import dataclass

import polars as pl
from cfa.dataops import datacat
from cfa.stf.data import get_nhsn_hrd, get_nssp


@dataclass(frozen=True)
class DataFreshness:
    source: str
    selected_version_date: dt.date
    latest_observed_date: dt.date | None
    run_date: dt.date
    is_stale: bool
    reason: str


@dataclass(frozen=True)
class ForecastData:
    report_date: dt.date
    nssp_data: pl.DataFrame
    nhsn_data: pl.DataFrame
    freshness: tuple[DataFreshness, ...]
    nhsn_prelim: bool | None = None


def _parse_version_date(version: str) -> dt.date:
    version_date = version.split("/", maxsplit=1)[0]
    return dt.datetime.strptime(version_date[:10], "%Y-%m-%d").date()


def _latest_version_date(endpoint, as_of: dt.date | None = None) -> dt.date:
    version_dates = sorted(
        {_parse_version_date(version) for version in endpoint.load.get_versions()},
        reverse=True,
    )
    if as_of is not None:
        version_dates = [
            version_date for version_date in version_dates if version_date <= as_of
        ]
    if not version_dates:
        raise ValueError(f"No {endpoint.load.prefix} versions found as of {as_of}")
    return version_dates[0]


def resolve_nssp_report_date(
    run_date: dt.date | None = None,
) -> dt.date:
    return _latest_version_date(datacat.public.stf.nssp_gold_v1, as_of=run_date)


def _load_dataops_nssp(
    *,
    report_date: dt.date,
    loc_abb: str,
    disease: str,
    first_training_date: dt.date,
) -> pl.DataFrame:
    return (
        get_nssp(
            disease=[disease, "Total"],
            loc_abb=loc_abb,
            dataset="gold",
            as_of=report_date,
            start_date=first_training_date,
            lazy=False,
        )
        .rename({"reference_date": "date", "value": "ed_visits"})
        .select(["date", "geo_value", "disease", "ed_visits"])
    )


def choose_nhsn_prelim(run_date: dt.date | None = None) -> tuple[bool, dt.date]:
    prelim_version = _latest_version_date(datacat.public.stf.nhsn_hrd_prelim, run_date)
    final_version = _latest_version_date(datacat.public.stf.nhsn_hrd, run_date)
    if prelim_version >= final_version:
        return True, prelim_version
    return False, final_version


def _load_dataops_nhsn(
    *,
    disease: str,
    loc_abb: str,
    first_training_date: dt.date,
    run_date: dt.date | None = None,
) -> tuple[pl.DataFrame, bool, dt.date]:
    prelim, version_date = choose_nhsn_prelim(run_date)
    data = get_nhsn_hrd(
        disease=disease,
        loc_abb=loc_abb,
        prelim=prelim,
        as_of=version_date,
        start_date=first_training_date,
        lazy=False,
    )
    return data, prelim, version_date


def nssp_freshness(
    *,
    selected_version_date: dt.date,
    latest_observed_date: dt.date | None,
    run_date: dt.date,
) -> DataFreshness:
    is_stale = selected_version_date != run_date
    reason = (
        f"NSSP version {selected_version_date} does not match run date {run_date}"
        if is_stale
        else "NSSP version matches run date"
    )
    return DataFreshness(
        source="nssp",
        selected_version_date=selected_version_date,
        latest_observed_date=latest_observed_date,
        run_date=run_date,
        is_stale=is_stale,
        reason=reason,
    )


def nhsn_freshness(
    *,
    selected_version_date: dt.date,
    latest_observed_date: dt.date | None,
    run_date: dt.date,
) -> DataFreshness:
    is_data_pub_day = run_date.weekday() in {2, 4}

    if is_data_pub_day:
        is_stale = selected_version_date != run_date
        if is_stale:
            reason = (
                f"NHSN version {selected_version_date} does not match run date "
                f"{run_date}"
            )
        else:
            reason = "NHSN version matches run date"
    else:
        age_days = (run_date - selected_version_date).days
        is_stale = age_days >= 7
        if is_stale:
            reason = f"NHSN version {selected_version_date} is not less than a week old"
        else:
            reason = f"NHSN version is {age_days} days old"

    return DataFreshness(
        source="nhsn",
        selected_version_date=selected_version_date,
        latest_observed_date=latest_observed_date,
        run_date=run_date,
        is_stale=is_stale,
        reason=reason,
    )


def enforce_freshness(
    freshness: tuple[DataFreshness, ...],
    *,
    fail_on_stale_data: bool,
    logger: logging.Logger,
) -> None:
    stale = [record for record in freshness if record.is_stale]
    if not stale:
        return

    message = "; ".join(record.reason for record in stale)
    if fail_on_stale_data:
        raise RuntimeError(f"Stale input data: {message}")
    logger.warning("Stale input data: %s", message)


def load_forecast_data(
    *,
    disease: str,
    loc_abb: str,
    report_date: dt.date,
    first_training_date: dt.date,
    run_date: dt.date | None = None,
    fail_on_stale_data: bool = False,
    logger: logging.Logger | None = None,
) -> ForecastData:
    logger = logger or logging.getLogger(__name__)
    run_date = run_date or report_date

    nssp_data = _load_dataops_nssp(
        report_date=report_date,
        loc_abb=loc_abb,
        disease=disease,
        first_training_date=first_training_date,
    )

    nssp_record = nssp_freshness(
        selected_version_date=report_date,
        latest_observed_date=nssp_data.get_column("date").max()
        if not nssp_data.is_empty()
        else None,
        run_date=run_date,
    )

    nhsn_data, nhsn_prelim, nhsn_version_date = _load_dataops_nhsn(
        disease=disease,
        loc_abb=loc_abb,
        first_training_date=first_training_date,
        run_date=run_date,
    )

    nhsn_record = nhsn_freshness(
        selected_version_date=nhsn_version_date,
        latest_observed_date=nhsn_data.get_column("weekendingdate").max()
        if not nhsn_data.is_empty()
        else None,
        run_date=run_date,
    )
    freshness = (nssp_record, nhsn_record)
    enforce_freshness(
        freshness,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    )
    return ForecastData(
        report_date=report_date,
        nssp_data=nssp_data,
        nhsn_data=nhsn_data,
        freshness=freshness,
        nhsn_prelim=nhsn_prelim,
    )
