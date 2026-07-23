import datetime as dt
import logging
from dataclasses import dataclass

import polars as pl

from cfa.stf.data import (
    get_nhsn_hrd,
    get_nssp,
    resolve_nhsn_hrd_version,
    resolve_nssp_version,
)
from cfa.stf.forecasttools import get_us_loc_pop_tbl


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
    @classmethod
    def create(
        cls,
        *,
        data: pl.DataFrame,
        freshness: DataFreshness,
        disease: str,
        last_training_date: dt.date,
    ) -> "NSSPData":
        cleaned_data = (
            data.filter(pl.col("disease").is_in([disease, "Total"]))
            .pivot(
                on="disease",
                values="ed_visits",
            )
            .rename({disease: "observed_ed_visits"})
            .with_columns(
                other_ed_visits=pl.col("Total") - pl.col("observed_ed_visits"),
                data_type=pl.when(pl.col("date") <= last_training_date)
                .then(pl.lit("train"))
                .otherwise(pl.lit("eval")),
                resolution=pl.lit("daily"),
            )
            .drop("Total")
            .sort("date")
        )
        return cls(data=cleaned_data, freshness=freshness)


@dataclass(frozen=True)
class NHSNData(ForecastSourceData):
    prelim: bool

    @classmethod
    def create(
        cls,
        *,
        data: pl.DataFrame,
        freshness: DataFreshness,
        prelim: bool,
        first_training_date: dt.date,
        last_training_date: dt.date,
    ) -> "NHSNData":
        cleaned_data = data.filter(
            pl.col("weekendingdate") >= first_training_date
        ).with_columns(
            data_type=pl.when(pl.col("weekendingdate") <= last_training_date)
            .then(pl.lit("train"))
            .otherwise(pl.lit("eval")),
            resolution=pl.lit("epiweekly"),
        )
        return cls(data=cleaned_data, freshness=freshness, prelim=prelim)


@dataclass(frozen=True)
class ForecastData:
    loc_abb: str
    disease: str
    report_date: dt.date
    loc_pop: int
    right_truncation_offset: int
    nssp: NSSPData
    nhsn: NHSNData

    @classmethod
    def create(
        cls,
        *,
        loc_abb: str,
        disease: str,
        report_date: dt.date,
        first_training_date: dt.date,
        last_training_date: dt.date,
        nssp_data: pl.DataFrame,
        nssp_freshness: DataFreshness,
        nhsn_data: pl.DataFrame,
        nhsn_freshness: DataFreshness,
        nhsn_prelim: bool,
        loc_pop: int | None = None,
    ) -> "ForecastData":
        if loc_pop is None:
            loc_pop = (
                get_us_loc_pop_tbl()
                .filter(pl.col("abbr") == loc_abb)
                .item(0, "population")
            )
        # The first entry of a source right-truncation PMF corresponds to reports
        # for reference_date = report_date - 1 as of report_date.
        right_truncation_offset = (report_date - last_training_date).days - 1
        nssp = NSSPData.create(
            data=nssp_data,
            freshness=nssp_freshness,
            disease=disease,
            last_training_date=last_training_date,
        )
        nhsn = NHSNData.create(
            data=nhsn_data,
            freshness=nhsn_freshness,
            prelim=nhsn_prelim,
            first_training_date=first_training_date,
            last_training_date=last_training_date,
        )

        return cls(
            loc_abb=loc_abb,
            disease=disease,
            report_date=report_date,
            loc_pop=loc_pop,
            right_truncation_offset=right_truncation_offset,
            nssp=nssp,
            nhsn=nhsn,
        )

    @property
    def sources(self) -> tuple[ForecastSourceData, ...]:
        return (self.nssp, self.nhsn)

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


def choose_nhsn_prelim() -> tuple[bool, dt.date]:
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
) -> tuple[pl.DataFrame, bool, dt.date]:
    prelim, version_date = choose_nhsn_prelim()
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
    last_training_date: dt.date,
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
        latest_observed_date=nssp_data.get_column("date").max(),
        run_date=run_date,
    )

    nhsn_data, nhsn_prelim, nhsn_version_date = _load_dataops_nhsn(
        disease=disease,
        loc_abb=loc_abb,
        first_training_date=first_training_date,
    )

    nhsn_record = nhsn_freshness(
        selected_version_date=nhsn_version_date,
        latest_observed_date=nhsn_data.get_column("weekendingdate").max(),
        run_date=run_date,
    )
    freshness = (nssp_record, nhsn_record)
    enforce_freshness(
        freshness,
        fail_on_stale_data=fail_on_stale_data,
        logger=logger,
    )
    return ForecastData.create(
        loc_abb=loc_abb,
        disease=disease,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
        nssp_data=nssp_data,
        nssp_freshness=nssp_record,
        nhsn_data=nhsn_data,
        nhsn_freshness=nhsn_record,
        nhsn_prelim=nhsn_prelim,
    )
