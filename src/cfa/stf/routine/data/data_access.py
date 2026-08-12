import datetime as dt
import logging
from collections.abc import Collection
from dataclasses import dataclass
from typing import Literal, get_args

import polars as pl
from cfa.stf.data import (
    get_nhsn_hrd,
    get_nssp,
    resolve_nhsn_hrd_version,
    resolve_nssp_version,
)
from cfa.stf.forecasttools import get_us_loc_pop_tbl

ForecastSourceName = Literal["nssp", "nhsn"]
_FORECAST_SOURCE_NAMES = frozenset(get_args(ForecastSourceName))
_DATAOPS_DISEASE_NAMES = {
    "COVID-19": "covid",
    "COVID-19/Omicron": "covid",
    "Influenza": "flu",
    "RSV": "rsv",
    "Total": "total",
}


def dataops_disease_name(disease: str) -> str:
    """Return the canonical disease name used by cfa-stf-data."""
    return _DATAOPS_DISEASE_NAMES.get(disease, disease)


@dataclass(frozen=True)
class DataFreshness:
    source: str
    selected_version_date: dt.date
    latest_observed_date: dt.date | None
    run_date: dt.date
    is_stale: bool
    reason: str


@dataclass(frozen=True)
class ForecastSourceData:
    data: pl.DataFrame
    freshness: DataFreshness


@dataclass(frozen=True)
class NSSPData(ForecastSourceData):
    pass


@dataclass(frozen=True)
class NHSNData(ForecastSourceData):
    prelim: bool


@dataclass(frozen=True)
class ForecastData:
    loc_abb: str
    disease: str
    report_date: dt.date
    loc_pop: int
    right_truncation_offset: int
    nssp: NSSPData | None = None
    nhsn: NHSNData | None = None

    def __post_init__(self) -> None:
        if self.nssp is None and self.nhsn is None:
            raise ValueError("ForecastData requires at least one data source")

    @property
    def sources(self) -> tuple[ForecastSourceData, ...]:
        return tuple(source for source in (self.nssp, self.nhsn) if source is not None)

    @property
    def freshness(self) -> tuple[DataFreshness, ...]:
        return tuple(source.freshness for source in self.sources)

    @property
    def is_stale(self) -> bool:
        return any(record.is_stale for record in self.freshness)


def _resolved_version_date(
    version: dt.datetime | str | None,
    *,
    dataset: str,
) -> dt.date:
    if not isinstance(version, dt.datetime):
        raise ValueError(f"No dated {dataset} version found")
    return version.date()


def resolve_nssp_report_date() -> dt.date:
    version = resolve_nssp_version(dataset="gold")
    return _resolved_version_date(version, dataset="NSSP gold")


def _load_dataops_nssp(
    *,
    loc_abb: str,
    disease: str,
    first_training_date: dt.date,
    last_training_date: dt.date,
    run_date: dt.date,
) -> NSSPData:
    version_date = resolve_nssp_report_date()
    canonical_disease = dataops_disease_name(disease)
    source_data = get_nssp(
        disease=[canonical_disease, "total"],
        state_abb=loc_abb,
        dataset="gold",
        start_date=first_training_date,
        lazy=False,
    )
    freshness = nssp_freshness(
        selected_version_date=version_date,
        latest_observed_date=source_data.get_column("date").max(),
        run_date=run_date,
    )
    data = (
        source_data.filter(pl.col("disease").is_in([canonical_disease, "total"]))
        .pivot(
            on="disease",
            values="value",
        )
        .rename({canonical_disease: "observed_ed_visits"})
        .with_columns(
            other_ed_visits=pl.col("total") - pl.col("observed_ed_visits"),
            data_type=pl.when(pl.col("date") <= last_training_date)
            .then(pl.lit("train"))
            .otherwise(pl.lit("eval")),
            resolution=pl.lit("daily"),
        )
        .drop("total", "target_type")
        .sort("date")
    )
    return NSSPData(data=data, freshness=freshness)


def select_latest_nhsn_release() -> tuple[bool, dt.date]:
    prelim_version = _resolved_version_date(
        resolve_nhsn_hrd_version(prelim=True),
        dataset="NHSN preliminary",
    )
    final_version = _resolved_version_date(
        resolve_nhsn_hrd_version(prelim=False),
        dataset="NHSN final",
    )
    if prelim_version >= final_version:
        return True, prelim_version
    return False, final_version


def _load_dataops_nhsn(
    *,
    disease: str,
    loc_abb: str,
    first_training_date: dt.date,
    last_training_date: dt.date,
    run_date: dt.date,
) -> NHSNData:
    prelim, version_date = select_latest_nhsn_release()
    source_data = get_nhsn_hrd(
        disease=dataops_disease_name(disease),
        state_abb=loc_abb,
        prelim=prelim,
        start_date=first_training_date,
        lazy=False,
    )
    freshness = nhsn_freshness(
        selected_version_date=version_date,
        latest_observed_date=source_data.get_column("date").max(),
        run_date=run_date,
    )
    data = (
        source_data.filter(pl.col("date") >= first_training_date)
        .with_columns(
            data_type=pl.when(pl.col("date") <= last_training_date)
            .then(pl.lit("train"))
            .otherwise(pl.lit("eval")),
            resolution=pl.lit("epiweekly"),
        )
        .select(
            "date",
            "state_abb",
            "value",
            "data_type",
            "resolution",
        )
    )
    return NHSNData(data=data, freshness=freshness, prelim=prelim)


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


def apply_freshness_policy(
    freshness: tuple[DataFreshness, ...],
    *,
    fail_on_stale_data: bool,
    logger: logging.Logger,
) -> None:
    for record in freshness:
        logger.info(
            "Input data freshness: source=%s version=%s latest_observed_date=%s "
            "run_date=%s status=%s (%s)",
            record.source,
            record.selected_version_date,
            record.latest_observed_date,
            record.run_date,
            "stale" if record.is_stale else "fresh",
            record.reason,
        )

    stale_records = [record for record in freshness if record.is_stale]
    if not stale_records:
        return

    reasons = "; ".join(record.reason for record in stale_records)
    message = f"Stale input data: {reasons}"
    if fail_on_stale_data:
        raise RuntimeError(message)
    logger.warning(message)


def load_forecast_data(
    *,
    disease: str,
    loc_abb: str,
    run_date: dt.date,
    first_training_date: dt.date,
    last_training_date: dt.date,
    sources: Collection[ForecastSourceName],
    fail_on_stale_data: bool = False,
    logger: logging.Logger | None = None,
) -> ForecastData:
    logger = logger or logging.getLogger(__name__)
    requested_sources = frozenset(sources)
    if not requested_sources:
        raise ValueError("At least one forecast data source is required")
    if invalid_sources := requested_sources - _FORECAST_SOURCE_NAMES:
        invalid = ", ".join(sorted(invalid_sources))
        raise ValueError(f"Unknown forecast data source(s): {invalid}")

    nssp = None
    nhsn = None
    freshness = []
    if "nssp" in requested_sources:
        nssp = _load_dataops_nssp(
            loc_abb=loc_abb,
            disease=disease,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
            run_date=run_date,
        )
        freshness.append(nssp.freshness)

    if "nhsn" in requested_sources:
        nhsn = _load_dataops_nhsn(
            disease=disease,
            loc_abb=loc_abb,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
            run_date=run_date,
        )
        freshness.append(nhsn.freshness)

    apply_freshness_policy(
        tuple(freshness),
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    )

    loc_pop = (
        get_us_loc_pop_tbl().filter(pl.col("abbr") == loc_abb).item(0, "population")
    )
    # The first entry of a source right-truncation PMF corresponds to reports
    # for reference_date = report_date - 1 as of report_date.
    right_truncation_offset = (run_date - last_training_date).days - 1
    return ForecastData(
        loc_abb=loc_abb,
        disease=disease,
        report_date=run_date,
        loc_pop=loc_pop,
        right_truncation_offset=right_truncation_offset,
        nssp=nssp,
        nhsn=nhsn,
    )
