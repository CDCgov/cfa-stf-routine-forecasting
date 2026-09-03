"""Generate deterministic synthetic data for pipeline integration tests."""

import datetime as dt
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
import polars.selectors as cs
from cfa.stf.forecasttools import get_us_loc_pop_tbl
from plotnine import (
    aes,
    element_text,
    geom_line,
    geom_point,
    ggplot,
    labs,
    scale_color_manual,
    theme,
    theme_minimal,
)

from cfa.stf.routine.data.data_access import (
    DataFreshness,
    ForecastSourceName,
    NHSNData,
    NSSPData,
    SurveillanceInputs,
    _normalize_nssp_data,
)
from cfa.stf.routine.data.hubverse_nowcast import (
    HUBVERSE_MODEL_OUTPUT_SUBDIR,
    HUBVERSE_TARGETS,
)

DEFAULT_LOCATIONS = ["CA", "US"]
DEFAULT_DISEASES = ["covid", "flu"]
REPORT_DATE = dt.date.today()
LAST_OBS_DATE = REPORT_DATE - dt.timedelta(
    days=1
)  # nssp data typically available through report date - 1
OBS_WINDOW_DAYS = 120
FIRST_OBS_DATE = REPORT_DATE - dt.timedelta(days=OBS_WINDOW_DAYS)

ED_BASELINE_PERCENT = 0.0012
ED_DISEASE_INCREMENT_PERCENT = 0.0003
ED_TREND_INCREMENT_PERCENT = 0.0001
ED_SEASONAL_INCREMENT_PERCENT = 0.0001
ED_TREND_PERIOD_DAYS = 21
ED_SEASONAL_PERIOD_DAYS = 7
TOTAL_ED_OFFSET_PERCENT = 0.025

NHSN_BASELINE_PERCENT = 0.002
NHSN_DISEASE_INCREMENT_PERCENT = 0.0005
NHSN_WEEKLY_TREND_INCREMENT_PERCENT = 0.0001
NHSN_SEASONAL_INCREMENT_PERCENT = 0.0001
NHSN_SEASONAL_PERIOD_WEEKS = 4
WEEK_ENDING_WEEKDAY = 5
DAYS_PER_WEEK = 7

REPORTING_FRACTIONS = np.array([0.6, 0.8, 0.9, 0.99, 1.0])
REPORTING_DELAY_PMF = np.diff(np.insert(REPORTING_FRACTIONS, 0, 0.0))

HUBVERSE_NOWCAST_DIR_NAME = "hubverse_nowcasts"
HUBVERSE_N_SAMPLES = 40
HUBVERSE_LOGNORMAL_SIGMA = 0.01
HUBVERSE_RANDOM_SEED = 12345
HUBVERSE_MIN_STABLE_OBSERVATIONS = 2
HUBVERSE_MAX_NOWCAST_DATES = 4
HUBVERSE_MIN_REVISION = 0.01
HUBVERSE_MAX_REVISION = 0.10

_SOURCE_DATA_COLS = [
    "date",
    "state_abb",
    "disease",
    "target_type",
    "value",
]


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


def _ed_percent(date: dt.date, disease_index: int) -> float:
    day_index = (date - FIRST_OBS_DATE).days
    trend = day_index // ED_TREND_PERIOD_DAYS
    seasonal = (day_index + disease_index) % ED_SEASONAL_PERIOD_DAYS
    return (
        ED_BASELINE_PERCENT
        + ED_DISEASE_INCREMENT_PERCENT * disease_index
        + ED_TREND_INCREMENT_PERCENT * trend
        + ED_SEASONAL_INCREMENT_PERCENT * seasonal
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
    disease: str,
    value: int,
) -> dict:
    return {
        "date": date,
        "state_abb": location,
        "disease": disease,
        "target_type": "inc ed visits",
        "value": value,
    }


def _weekending_dates() -> list[dt.date]:
    first_week = _first_weekday_on_or_after(FIRST_OBS_DATE, WEEK_ENDING_WEEKDAY)
    return _date_range(first_week, REPORT_DATE, step_days=DAYS_PER_WEEK)


def _make_nssp(
    *,
    locations: list[LocationData],
    diseases: list[str],
) -> pl.DataFrame:
    """Make location-level NSSP data in the schema returned by `get_nssp`."""
    rows = []
    observation_dates = _date_range(FIRST_OBS_DATE, LAST_OBS_DATE)
    for location in locations:
        for date in observation_dates:
            disease_total = 0
            for disease_index, disease in enumerate(diseases):
                value = _count_from_population_percent(
                    location.population,
                    _ed_percent(date, disease_index),
                )
                disease_total += value
                rows.append(
                    _nssp_row(
                        location=location.abbr,
                        date=date,
                        disease=disease,
                        value=value,
                    )
                )

            total_value = disease_total + _count_from_population_percent(
                location.population,
                TOTAL_ED_OFFSET_PERCENT,
            )
            rows.append(
                _nssp_row(
                    location=location.abbr,
                    date=date,
                    disease="total",
                    value=total_value,
                )
            )

    return (
        pl.DataFrame(rows)
        .select(cs.by_name(_SOURCE_DATA_COLS))
        .sort("state_abb", "disease", "date")
    )


def _make_nhsn(
    *,
    location: LocationData,
    disease: str,
    disease_index: int,
) -> pl.DataFrame:
    rows = []
    for week_index, date in enumerate(_weekending_dates()):
        rows.append(
            {
                "date": date,
                "state_abb": location.abbr,
                "disease": disease,
                "target_type": "wk inc hosp",
                "value": (
                    _count_from_population_percent(
                        location.population,
                        _nhsn_percent(week_index, disease_index),
                    )
                ),
            }
        )

    return pl.DataFrame(rows).select(cs.by_name(_SOURCE_DATA_COLS))


def make_surveillance_inputs(
    location: str,
    disease: str,
    sources: Collection[ForecastSourceName],
    first_training_date: dt.date = FIRST_OBS_DATE,
    last_training_date: dt.date = REPORT_DATE,
) -> SurveillanceInputs:
    requested_sources = frozenset(sources)
    locations = sorted(set(DEFAULT_LOCATIONS + [location]))
    diseases = sorted(set(DEFAULT_DISEASES + [disease]))
    location_data = _location_data(locations)
    location_by_abbr = {item.abbr: item for item in location_data}
    disease_index = diseases.index(disease)
    nssp_data = _make_nssp(
        locations=location_data,
        diseases=diseases,
    ).filter(
        pl.col("state_abb") == location,
        pl.col("disease").is_in([disease, "total"]),
    )
    nhsn_data = _make_nhsn(
        location=location_by_abbr[location],
        disease=disease,
        disease_index=disease_index,
    )

    nssp_freshness = DataFreshness(
        source="nssp",
        selected_version_date=REPORT_DATE,
        latest_observed_date=nssp_data.get_column("date").max(),
        run_date=REPORT_DATE,
        is_stale=False,
        reason="Synthetic NSSP data",
    )
    nhsn_freshness = DataFreshness(
        source="nhsn",
        selected_version_date=REPORT_DATE,
        latest_observed_date=nhsn_data.get_column("date").max(),
        run_date=REPORT_DATE,
        is_stale=False,
        reason="Synthetic NHSN data",
    )

    nssp = (
        NSSPData(
            data=_normalize_nssp_data(
                nssp_data,
                last_training_date=last_training_date,
            ),
            freshness=nssp_freshness,
            resolution="daily",
        )
        if "nssp" in requested_sources
        else None
    )
    nhsn = (
        NHSNData(
            data=(
                nhsn_data.filter(pl.col("date") >= first_training_date)
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
            ),
            freshness=nhsn_freshness,
            prelim=False,
        )
        if "nhsn" in requested_sources
        else None
    )
    return SurveillanceInputs(
        loc_pop=location_by_abbr[location].population,
        nssp=nssp,
        nhsn=nhsn,
    )


def _make_hubverse_nowcast_rows(
    *,
    location: str,
    disease: str,
    nhsn_observations: pl.DataFrame,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Make noisy Hubverse sample nowcasts for one disease and location."""
    selected_reports = nhsn_observations.select(
        pl.col("date").cast(pl.Date),
        pl.col("value").cast(pl.Float64),
    ).sort("date")
    if selected_reports.select(
        (
            pl.col("value").is_null()
            | (~pl.col("value").is_finite())
            | (pl.col("value") < 0)
        ).any()
    ).item():
        raise ValueError("Selected NHSN reports must be finite and non-negative.")
    n_nowcast_dates = min(
        HUBVERSE_MAX_NOWCAST_DATES,
        selected_reports.height - HUBVERSE_MIN_STABLE_OBSERVATIONS,
    )
    if n_nowcast_dates < 1:
        raise ValueError(
            "Expected at least one NHSN nowcast date and "
            f"{HUBVERSE_MIN_STABLE_OBSERVATIONS} stable observations for "
            f"{disease}, {location}; found {selected_reports.height} "
            "selected reports."
        )
    recent_reports = selected_reports.tail(n_nowcast_dates)

    dates = recent_reports.get_column("date").to_list()
    reports = recent_reports.get_column("value").to_numpy()
    horizons = [
        (target_end_date - REPORT_DATE).days // DAYS_PER_WEEK
        for target_end_date in dates
    ]
    revision_rates = np.linspace(
        HUBVERSE_MIN_REVISION,
        HUBVERSE_MAX_REVISION,
        n_nowcast_dates,
    )
    expected_final_counts = reports * (1 + revision_rates)
    noise = rng.lognormal(
        mean=-(HUBVERSE_LOGNORMAL_SIGMA**2) / 2,
        sigma=HUBVERSE_LOGNORMAL_SIGMA,
        size=(HUBVERSE_N_SAMPLES, n_nowcast_dates),
    )
    samples = expected_final_counts * noise
    location_id = location.lower()

    return pl.DataFrame(
        [
            {
                "origin_date": REPORT_DATE,
                "target_end_date": target_end_date,
                "horizon": horizon,
                "target": HUBVERSE_TARGETS[disease],
                "location": location_id,
                "output_type": "sample",
                "output_type_id": f"{disease}_{location_id}_{sample_index}",
                "value": value,
            }
            for sample_index, trajectory in enumerate(samples, start=1)
            for target_end_date, horizon, value in zip(dates, horizons, trajectory)
        ]
    ).with_columns(
        pl.col("horizon").cast(pl.Int32),
        pl.col("value").cast(pl.Float64),
    )


def _write_hubverse_nowcast_figure(
    *,
    nowcasts: pl.DataFrame,
    nhsn_observations: pl.DataFrame,
    output_path: Path,
    disease: str,
    location: str,
) -> None:
    """Plot simulated nowcast trajectories over the selected NHSN reports."""
    trajectory_label = "Simulated nowcast trajectories"
    mean_label = "Mean simulated nowcast"
    observation_label = "Selected NHSN observations"
    observations = nhsn_observations.select(
        pl.col("date").cast(pl.Date).alias("target_end_date"),
        pl.col("value").cast(pl.Float64),
        pl.lit(observation_label).alias("series"),
    ).sort("target_end_date")
    trajectories = nowcasts.with_columns(pl.lit(trajectory_label).alias("series"))
    mean_nowcast = (
        nowcasts.group_by("target_end_date")
        .agg(pl.col("value").mean())
        .sort("target_end_date")
        .with_columns(pl.lit(mean_label).alias("series"))
    )

    plot = (
        ggplot()
        + geom_line(
            trajectories,
            aes(
                x="target_end_date",
                y="value",
                group="output_type_id",
                color="series",
            ),
            alpha=0.12,
            size=0.5,
        )
        + geom_line(
            mean_nowcast,
            aes(x="target_end_date", y="value", color="series"),
            size=1.2,
        )
        + geom_line(
            observations,
            aes(x="target_end_date", y="value", color="series"),
            size=0.8,
        )
        + geom_point(
            observations,
            aes(x="target_end_date", y="value", color="series"),
            size=2,
        )
        + scale_color_manual(
            name=None,
            breaks=[trajectory_label, mean_label, observation_label],
            values={
                trajectory_label: "tab:blue",
                mean_label: "tab:blue",
                observation_label: "black",
            },
        )
        + labs(
            title=f"Simulated Hubverse nowcasts: {disease}, {location}",
            x="Target end date",
            y="Weekly incident hospital admissions",
        )
        + theme_minimal()
        + theme(
            axis_text_x=element_text(rotation=30, ha="right"),
            figure_size=(9, 5),
            legend_position="bottom",
        )
    )
    plot.save(filename=output_path, dpi=150, verbose=False)


def write_hubverse_nowcast(
    base_dir: Path,
    *,
    disease: str,
    location: str,
    nhsn_observations: pl.DataFrame,
) -> None:
    """
    Write one noisy sample nowcast artifact in the production Hubverse schema.

    The expected nowcast is a 1--10% upward revision of each selected NHSN
    report, with larger revisions for more recent dates. Fixed-sigma lognormal
    noise simulates uncertainty around those expectations.
    """
    if disease not in HUBVERSE_TARGETS:
        raise ValueError(f"No Hubverse target mapping for {disease!r}")
    disease_index = sorted(HUBVERSE_TARGETS).index(disease)
    nowcasts = _make_hubverse_nowcast_rows(
        location=location,
        disease=disease,
        nhsn_observations=nhsn_observations,
        rng=np.random.default_rng(HUBVERSE_RANDOM_SEED + disease_index),
    )
    output_dir = (
        base_dir
        / "private_data"
        / HUBVERSE_NOWCAST_DIR_NAME
        / disease
        / HUBVERSE_MODEL_OUTPUT_SUBDIR
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    nowcasts.write_parquet(output_dir / f"{REPORT_DATE}-CFA-nowcastNHSN.parquet")
    _write_hubverse_nowcast_figure(
        nowcasts=nowcasts,
        nhsn_observations=nhsn_observations,
        output_path=output_dir / f"{REPORT_DATE}-CFA-nowcastNHSN.png",
        disease=disease,
        location=location,
    )
