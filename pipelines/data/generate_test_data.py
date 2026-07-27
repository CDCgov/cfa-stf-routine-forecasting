"""Generate deterministic synthetic data for pipeline integration tests."""

import argparse
import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import polars.selectors as cs
from cfa.stf.forecasttools import get_us_loc_pop_tbl

from pipelines.data.generate_test_data_lib import (
    FACILITY_LEVEL_NSSP_DATA_COLS,
    LOC_LEVEL_NSSP_DATA_COLS,
    NHSN_COLS,
    create_default_param_estimates,
)

DEFAULT_LOCATIONS = ["CA", "DC"]
DEFAULT_DISEASES = ["COVID-19", "Influenza"]
REPORT_DATE = dt.date(2024, 12, 21)
LAST_OBS_DATE = REPORT_DATE - dt.timedelta(
    days=1
)  # nssp data typically available through report date - 1
OBS_WINDOW_DAYS = 120
FIRST_OBS_DATE = REPORT_DATE - dt.timedelta(days=OBS_WINDOW_DAYS)

N_FACILITIES = 3
FIRST_FACILITY_ID = 1

ED_BASELINE_PERCENT = 0.0012
ED_DISEASE_INCREMENT_PERCENT = 0.0003
ED_FACILITY_INCREMENT_PERCENT = 0.0001
ED_TREND_INCREMENT_PERCENT = 0.0001
ED_SEASONAL_INCREMENT_PERCENT = 0.0001
ED_TREND_PERIOD_DAYS = 21
ED_SEASONAL_PERIOD_DAYS = 7
TOTAL_ED_BASELINE_OFFSET_PERCENT = 0.025
TOTAL_ED_FACILITY_INCREMENT_PERCENT = 0.001

NHSN_BASELINE_PERCENT = 0.002
NHSN_DISEASE_INCREMENT_PERCENT = 0.0005
NHSN_WEEKLY_TREND_INCREMENT_PERCENT = 0.0001
NHSN_SEASONAL_INCREMENT_PERCENT = 0.0001
NHSN_SEASONAL_PERIOD_WEEKS = 4
WEEK_ENDING_WEEKDAY = 5
DAYS_PER_WEEK = 7

_NSSP_DISEASE_NAMES = {"COVID-19": "COVID-19/Omicron"}


@dataclass(frozen=True)
class LocationData:
    abbr: str
    population: int


def _date_range(start: dt.date, end: dt.date, step_days: int = 1) -> list[dt.date]:
    return [
        start + dt.timedelta(days=i)
        for i in range(0, (end - start).days + 1, step_days)
    ]


def _first_weekday_on_or_after(date: dt.date, weekday: int) -> dt.date:
    return date + dt.timedelta(days=(weekday - date.weekday()) % DAYS_PER_WEEK)


def _location_data(locations: list[str]) -> list[LocationData]:
    population_table = get_us_loc_pop_tbl().filter(pl.col("abbr").is_in(locations))
    population_by_location = dict(
        population_table.select("abbr", "population").iter_rows()
    )
    missing_locations = sorted(set(locations) - set(population_by_location))
    if missing_locations:
        raise ValueError(
            "No population found for location(s): " + ", ".join(missing_locations)
        )
    return [
        LocationData(abbr=location, population=population_by_location[location])
        for location in locations
    ]


def _count_from_population_percent(population: int, percent: float) -> int:
    return max(1, round(population * percent / 100))


def _ed_percent(date: dt.date, disease_index: int, facility: int) -> float:
    day_index = (date - FIRST_OBS_DATE).days
    trend = day_index // ED_TREND_PERIOD_DAYS
    seasonal = (day_index + facility + disease_index) % ED_SEASONAL_PERIOD_DAYS
    return (
        ED_BASELINE_PERCENT
        + ED_DISEASE_INCREMENT_PERCENT * disease_index
        + ED_FACILITY_INCREMENT_PERCENT * facility
        + ED_TREND_INCREMENT_PERCENT * trend
        + ED_SEASONAL_INCREMENT_PERCENT * seasonal
    )


def _total_ed_offset_percent(facility: int) -> float:
    return (
        TOTAL_ED_BASELINE_OFFSET_PERCENT
        + TOTAL_ED_FACILITY_INCREMENT_PERCENT * facility
    )


def _nhsn_percent(week_index: int, disease_index: int) -> float:
    seasonal = week_index % NHSN_SEASONAL_PERIOD_WEEKS
    return (
        NHSN_BASELINE_PERCENT
        + NHSN_DISEASE_INCREMENT_PERCENT * disease_index
        + NHSN_WEEKLY_TREND_INCREMENT_PERCENT * week_index
        + NHSN_SEASONAL_INCREMENT_PERCENT * seasonal
    )


def _nssp_row(
    *,
    location: str,
    date: dt.date,
    facility: int,
    disease: str,
    value: int,
) -> dict:
    return {
        "reference_date": date,
        "report_date": REPORT_DATE,
        "geo_type": "state",
        "geo_value": location,
        "asof": REPORT_DATE,
        "metric": "count_ed_visits",
        "run_id": 0,
        "facility": facility,
        "disease": disease,
        "value": value,
    }


def _weekending_dates() -> list[dt.date]:
    first_week = _first_weekday_on_or_after(FIRST_OBS_DATE, WEEK_ENDING_WEEKDAY)
    return _date_range(first_week, REPORT_DATE, step_days=DAYS_PER_WEEK)


def _write_parquet(data: pl.DataFrame, directory: Path, file_name: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    data.write_parquet(directory / file_name)


def _write_param_estimates(
    private_data_dir: Path,
    locations: list[str],
    diseases: list[str],
) -> None:
    param_estimates = create_default_param_estimates(
        states_to_simulate=locations,
        diseases_to_simulate=diseases,
        max_train_date_str=REPORT_DATE.isoformat(),
        max_train_date=REPORT_DATE,
    )
    _write_parquet(
        param_estimates,
        private_data_dir / "prod_param_estimates",
        "prod.parquet",
    )


def _make_facility_level_nssp(
    *,
    locations: list[LocationData],
    diseases: list[str],
) -> pl.DataFrame:
    rows = []
    observation_dates = _date_range(FIRST_OBS_DATE, LAST_OBS_DATE)
    facility_ids = range(FIRST_FACILITY_ID, N_FACILITIES + 1)
    for location in locations:
        for date in observation_dates:
            for facility in facility_ids:
                disease_total = 0
                for disease_index, disease in enumerate(diseases):
                    value = _count_from_population_percent(
                        location.population,
                        _ed_percent(date, disease_index, facility),
                    )
                    disease_total += value
                    rows.append(
                        _nssp_row(
                            location=location.abbr,
                            date=date,
                            facility=facility,
                            disease=_NSSP_DISEASE_NAMES.get(disease, disease),
                            value=value,
                        )
                    )

                total_value = disease_total + _count_from_population_percent(
                    location.population,
                    _total_ed_offset_percent(facility),
                )
                rows.append(
                    _nssp_row(
                        location=location.abbr,
                        date=date,
                        facility=facility,
                        disease="Total",
                        value=total_value,
                    )
                )

    return pl.DataFrame(rows).select(cs.by_name(FACILITY_LEVEL_NSSP_DATA_COLS))


def _make_state_level_nssp(facility_level_nssp: pl.DataFrame) -> pl.DataFrame:
    return (
        facility_level_nssp.group_by(cs.exclude("facility", "value"))
        .agg(pl.col("value").sum())
        .with_columns(pl.lit(True).alias("any_update_this_day"))
        .sort(["reference_date", "geo_value", "disease"])
        .select(cs.by_name(LOC_LEVEL_NSSP_DATA_COLS))
    )


def _make_nhsn(
    *,
    location: LocationData,
    disease_index: int,
) -> pl.DataFrame:
    rows = []
    for week_index, weekendingdate in enumerate(_weekending_dates()):
        rows.append(
            {
                "jurisdiction": location.abbr,
                "weekendingdate": weekendingdate,
                "hospital_admissions": (
                    _count_from_population_percent(
                        location.population,
                        _nhsn_percent(week_index, disease_index),
                    )
                ),
            }
        )

    return pl.DataFrame(rows).select(cs.by_name(NHSN_COLS))


def _write_nssp_data(private_data_dir: Path, facility_level_nssp: pl.DataFrame) -> None:
    _write_parquet(
        facility_level_nssp,
        private_data_dir / "nssp_etl_gold",
        f"{REPORT_DATE}.parquet",
    )

    state_level_nssp = _make_state_level_nssp(facility_level_nssp)
    _write_parquet(
        state_level_nssp,
        private_data_dir / "nssp_state_level_gold",
        f"{REPORT_DATE}.parquet",
    )
    _write_parquet(
        state_level_nssp.select(cs.exclude("any_update_this_day")),
        private_data_dir / "nssp-etl",
        "latest_comprehensive.parquet",
    )


def _write_nhsn_data(
    private_data_dir: Path,
    locations: list[LocationData],
    diseases: list[str],
) -> None:
    for location in locations:
        for disease_index, disease in enumerate(diseases):
            _write_parquet(
                _make_nhsn(location=location, disease_index=disease_index),
                private_data_dir / "nhsn_test_data",
                f"{disease}_{location.abbr}.parquet",
            )


def main(
    base_dir: Path,
    locations: list[str] | None = None,
    diseases: list[str] | None = None,
) -> None:
    locations = locations or DEFAULT_LOCATIONS
    diseases = diseases or DEFAULT_DISEASES
    location_data = _location_data(locations)

    private_data_dir = base_dir / "private_data"
    private_data_dir.mkdir(parents=True, exist_ok=True)

    _write_param_estimates(private_data_dir, locations, diseases)

    facility_level_nssp = _make_facility_level_nssp(
        locations=location_data,
        diseases=diseases,
    )
    _write_nssp_data(private_data_dir, facility_level_nssp)
    _write_nhsn_data(private_data_dir, location_data, diseases)

    # PyRenew accepts an NWSS directory even when wastewater is not fit.
    (private_data_dir / "nwss_vintages").mkdir(exist_ok=True)
    print(f"Successfully generated test data in {private_data_dir}")


def _split_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create pipeline integration data.")
    parser.add_argument("base_dir", type=Path, help="Base directory for output data.")
    parser.add_argument(
        "--locations",
        type=_split_values,
        default=DEFAULT_LOCATIONS,
        help="Comma-separated location abbreviations to generate.",
    )
    parser.add_argument(
        "--diseases",
        type=_split_values,
        default=DEFAULT_DISEASES,
        help="Comma-separated diseases to generate.",
    )
    args = parser.parse_args()
    main(args.base_dir, locations=args.locations, diseases=args.diseases)
