"""Build the model-specific JSON input consumed by EpiAutoGP."""

import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from cfa.stf.routine.data.nowcast import NowcastData, NowcastSource
from cfa.stf.routine.data.prep_data import aggregate_epiweekly_nssp
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig
from cfa.stf.routine.forecast_run import ForecastRun


def _validate_epiautogp_parameters(
    target: str,
    frequency: str,
    ed_visit_type: str,
) -> None:
    """Validate the model-input combination supported by EpiAutoGP."""
    if target not in {"nssp", "nhsn"}:
        raise ValueError(f"target must be 'nssp' or 'nhsn', got {target!r}")
    if frequency not in {"daily", "epiweekly"}:
        raise ValueError(f"frequency must be 'daily' or 'epiweekly', got {frequency!r}")
    if ed_visit_type not in {"observed", "other", "pct"}:
        raise ValueError(
            "ed_visit_type must be 'observed', 'other', or 'pct', "
            f"got {ed_visit_type!r}"
        )
    if target == "nhsn" and frequency == "daily":
        raise ValueError("NHSN data is only available in epiweekly frequency.")
    if target == "nhsn" and ed_visit_type != "observed":
        raise ValueError(
            "ed_visit_type is only applicable when target='nssp'. "
            "For NHSN, ed_visit_type must be 'observed' (the default)."
        )


def _apply_date_exclusions(
    data: pl.DataFrame,
    exclude_date_ranges: list[tuple[dt.date, dt.date]],
    logger: logging.Logger,
) -> pl.DataFrame:
    """Remove inclusive date ranges from a model-input series."""
    exclude = pl.lit(False)
    for start_date, end_date in exclude_date_ranges:
        exclude |= pl.col("date").is_between(start_date, end_date, closed="both")

    filtered = data.filter(~exclude)
    if filtered.is_empty():
        raise ValueError(
            "All dates were excluded by the provided date ranges. "
            f"Original observation count: {data.height}"
        )

    logger.info(
        "Excluded %s observations from %s date range(s). Remaining: %s observations",
        data.height - filtered.height,
        len(exclude_date_ranges),
        filtered.height,
    )
    return filtered


def _extract_model_series(
    *,
    forecast_run: ForecastRun,
    config: EpiAutoGPConfig,
    logger: logging.Logger,
) -> tuple[list[dt.date], list[float]]:
    """Select the requested training series directly from shared run state."""
    if config.target == "nssp":
        if forecast_run.nssp is None:
            raise ValueError("The forecast run does not contain NSSP data")
        data = forecast_run.nssp.data
        if config.frequency == "epiweekly":
            data = aggregate_epiweekly_nssp(data)
        data = data.filter(pl.col("data_type") == "train")

        value = {
            "observed": pl.col("observed_ed_visits"),
            "other": pl.col("other_ed_visits"),
            "pct": (
                pl.col("observed_ed_visits")
                / (pl.col("observed_ed_visits") + pl.col("other_ed_visits"))
                * 100
            ),
        }[config.ed_visit_type]
        data = data.select("date", value.cast(pl.Float64).alias("value"))
    else:
        if forecast_run.nhsn is None:
            raise ValueError("The forecast run does not contain NHSN data")
        data = forecast_run.nhsn.data.filter(pl.col("data_type") == "train").select(
            "date",
            pl.col("value").cast(pl.Float64),
        )

    data = data.filter(
        pl.col("value").is_not_null() & pl.col("value").is_finite()
    ).sort("date")
    if config.exclude_date_ranges:
        data = _apply_date_exclusions(data, config.exclude_date_ranges, logger)
    if data.is_empty():
        raise ValueError(
            f"No training observations are available for target={config.target!r}, "
            f"frequency={config.frequency!r}, and "
            f"ed_visit_type={config.ed_visit_type!r}."
        )

    dates = data.get_column("date").to_list()
    reports = data.get_column("value").to_list()
    logger.info(
        "Extracted %s %s %s observations from %s to %s",
        len(dates),
        config.frequency,
        config.target,
        dates[0],
        dates[-1],
    )
    return dates, reports


def convert_to_epiautogp_json(
    *,
    forecast_run: ForecastRun,
    config: EpiAutoGPConfig,
    nowcast_source: NowcastSource | None = None,
    logger: logging.Logger | None = None,
) -> Path:
    """Serialize one shared forecast run in EpiAutoGP's JSON input format."""
    logger = logger or logging.getLogger(__name__)
    _validate_epiautogp_parameters(
        config.target,
        config.frequency,
        config.ed_visit_type,
    )
    dates, reports = _extract_model_series(
        forecast_run=forecast_run,
        config=config,
        logger=logger,
    )

    nowcast_data = (
        NowcastData()
        if nowcast_source is None
        else nowcast_source.get_nowcast_data(dates=dates, reports=reports)
    )
    model_input = {
        "dates": [date.isoformat() for date in dates],
        "reports": reports,
        "pathogen": forecast_run.disease,
        "location": forecast_run.loc,
        "target": config.target,
        "frequency": config.frequency,
        "ed_visit_type": config.ed_visit_type,
        "forecast_date": forecast_run.report_date.isoformat(),
        "nowcast_dates": [date.isoformat() for date in nowcast_data.dates],
        "nowcast_reports": nowcast_data.reports,
    }

    input_path = forecast_run.model_dir / f"{forecast_run.model_name}_input.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    with input_path.open("w") as file:
        json.dump(model_input, file, indent=2)
    logger.info("Saved EpiAutoGP input JSON to %s", input_path)
    return input_path
