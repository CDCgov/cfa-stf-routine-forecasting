"""Generate deterministic synthetic data for pipeline integration tests."""

import argparse
import datetime as dt
from pathlib import Path

import polars as pl
import polars.selectors as cs

from pipelines.data.generate_test_data_lib import (
    FACILITY_LEVEL_NSSP_DATA_COLS,
    LOC_LEVEL_NSSP_DATA_COLS,
    NHSN_COLS,
    create_default_param_estimates,
)

DEFAULT_LOCATIONS = ["CA", "DC"]
DEFAULT_DISEASES = ["COVID-19", "Influenza"]
REPORT_DATE = dt.date(2024, 12, 21)
OBS_WINDOW_DAYS = 120
FIRST_OBS_DATE = REPORT_DATE - dt.timedelta(days=OBS_WINDOW_DAYS)

N_FACILITIES = 3
FIRST_FACILITY_ID = 1

ED_BASELINE = 12
ED_LOCATION_INCREMENT = 4
ED_DISEASE_INCREMENT = 3
ED_TREND_PERIOD_DAYS = 21
ED_SEASONAL_PERIOD_DAYS = 7
TOTAL_ED_BASELINE_OFFSET = 250
TOTAL_ED_FACILITY_INCREMENT = 10

NHSN_BASELINE = 20
NHSN_LOCATION_INCREMENT = 4
NHSN_DISEASE_INCREMENT = 5
NHSN_SEASONAL_PERIOD_WEEKS = 4
WEEK_ENDING_WEEKDAY = 5
DAYS_PER_WEEK = 7

_NSSP_DISEASE_NAMES = {"COVID-19": "COVID-19/Omicron"}


def _date_range(start: dt.date, end: dt.date, step_days: int = 1) -> list[dt.date]:
    return [
        start + dt.timedelta(days=i)
        for i in range(0, (end - start).days + 1, step_days)
    ]


def _first_weekday_on_or_after(date: dt.date, weekday: int) -> dt.date:
    return date + dt.timedelta(days=(weekday - date.weekday()) % DAYS_PER_WEEK)


def _nssp_disease_name(disease: str) -> str:
    return _NSSP_DISEASE_NAMES.get(disease, disease)


def _daily_count(
    *,
    date: dt.date,
    location_index: int,
    disease_index: int,
    facility: int,
) -> int:
    day_index = (date - FIRST_OBS_DATE).days
    baseline = (
        ED_BASELINE
        + ED_LOCATION_INCREMENT * location_index
        + ED_DISEASE_INCREMENT * disease_index
        + facility
    )
    trend = day_index // ED_TREND_PERIOD_DAYS
    seasonal = (day_index + facility + disease_index) % ED_SEASONAL_PERIOD_DAYS
    return baseline + trend + seasonal


def _make_facility_level_nssp(
    *,
    locations: list[str],
    diseases: list[str],
) -> pl.DataFrame:
    rows = []
    for location_index, location in enumerate(locations):
        for date in _date_range(FIRST_OBS_DATE, REPORT_DATE):
            for facility in range(FIRST_FACILITY_ID, N_FACILITIES + 1):
                disease_total = 0
                for disease_index, disease in enumerate(diseases):
                    value = _daily_count(
                        date=date,
                        location_index=location_index,
                        disease_index=disease_index,
                        facility=facility,
                    )
                    disease_total += value
                    rows.append(
                        {
                            "reference_date": date,
                            "report_date": REPORT_DATE,
                            "geo_type": "state",
                            "geo_value": location,
                            "asof": REPORT_DATE,
                            "metric": "count_ed_visits",
                            "run_id": 0,
                            "facility": facility,
                            "disease": _nssp_disease_name(disease),
                            "value": value,
                        }
                    )

                rows.append(
                    {
                        "reference_date": date,
                        "report_date": REPORT_DATE,
                        "geo_type": "state",
                        "geo_value": location,
                        "asof": REPORT_DATE,
                        "metric": "count_ed_visits",
                        "run_id": 0,
                        "facility": facility,
                        "disease": "Total",
                        "value": (
                            disease_total
                            + TOTAL_ED_BASELINE_OFFSET
                            + TOTAL_ED_FACILITY_INCREMENT * facility
                        ),
                    }
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
    location: str,
    location_index: int,
    disease: str,
    disease_index: int,
) -> pl.DataFrame:
    rows = []
    first_week = _first_weekday_on_or_after(FIRST_OBS_DATE, WEEK_ENDING_WEEKDAY)
    for week_index, weekendingdate in enumerate(
        _date_range(first_week, REPORT_DATE, step_days=DAYS_PER_WEEK)
    ):
        rows.append(
            {
                "jurisdiction": location,
                "weekendingdate": weekendingdate,
                "hospital_admissions": (
                    NHSN_BASELINE
                    + NHSN_LOCATION_INCREMENT * location_index
                    + NHSN_DISEASE_INCREMENT * disease_index
                    + week_index
                    + (week_index % NHSN_SEASONAL_PERIOD_WEEKS)
                ),
            }
        )

    return pl.DataFrame(rows).select(cs.by_name(NHSN_COLS))


def main(
    base_dir: Path,
    locations: list[str] | None = None,
    diseases: list[str] | None = None,
) -> None:
    locations = DEFAULT_LOCATIONS if locations is None else locations
    diseases = DEFAULT_DISEASES if diseases is None else diseases

    if not locations:
        raise ValueError("locations must include at least one location")
    if not diseases:
        raise ValueError("diseases must include at least one disease")

    private_data_dir = base_dir / "private_data"
    private_data_dir.mkdir(parents=True, exist_ok=True)

    param_estimates_dir = private_data_dir / "prod_param_estimates"
    param_estimates_dir.mkdir(parents=True, exist_ok=True)
    create_default_param_estimates(
        states_to_simulate=locations,
        diseases_to_simulate=diseases,
        max_train_date_str=REPORT_DATE.isoformat(),
        max_train_date=REPORT_DATE,
    ).write_parquet(param_estimates_dir / "prod.parquet")

    facility_level_nssp = _make_facility_level_nssp(
        locations=locations,
        diseases=diseases,
    )
    nssp_etl_gold_dir = private_data_dir / "nssp_etl_gold"
    nssp_etl_gold_dir.mkdir(parents=True, exist_ok=True)
    facility_level_nssp.write_parquet(nssp_etl_gold_dir / f"{REPORT_DATE}.parquet")

    state_level_nssp = _make_state_level_nssp(facility_level_nssp)
    nssp_state_level_gold_dir = private_data_dir / "nssp_state_level_gold"
    nssp_state_level_gold_dir.mkdir(parents=True, exist_ok=True)
    state_level_nssp.write_parquet(nssp_state_level_gold_dir / f"{REPORT_DATE}.parquet")

    nssp_etl_dir = private_data_dir / "nssp-etl"
    nssp_etl_dir.mkdir(parents=True, exist_ok=True)
    state_level_nssp.select(cs.exclude("any_update_this_day")).write_parquet(
        nssp_etl_dir / "latest_comprehensive.parquet"
    )

    nhsn_dir = private_data_dir / "nhsn_test_data"
    nhsn_dir.mkdir(parents=True, exist_ok=True)
    for location_index, location in enumerate(locations):
        for disease_index, disease in enumerate(diseases):
            _make_nhsn(
                location=location,
                location_index=location_index,
                disease=disease,
                disease_index=disease_index,
            ).write_parquet(nhsn_dir / f"{disease}_{location}.parquet")

    (private_data_dir / "nwss_vintages").mkdir(parents=True, exist_ok=True)
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
