"""Build the model-specific JSON input consumed by EpiAutoGP."""

import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from cfa.stf.routine.data.nowcast import NowcastData, NowcastSource
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.prop_utils import append_prop_ed_data


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
    """Remove observations whose dates fall in any inclusive exclusion range."""
    if not exclude_date_ranges:
        return data
    excluded = pl.any_horizontal(
        pl.col("date").is_between(start, end) for start, end in exclude_date_ranges
    )
    filtered = data.filter(~excluded)
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
        source = forecast_run.nssp
        if source is None:
            raise ValueError("The forecast run does not contain NSSP data")
        if source.resolution != config.frequency:
            raise ValueError(
                f"NSSP data resolution {source.resolution!r} does not match "
                f"EpiAutoGP frequency {config.frequency!r}"
            )
        variable = {
            "observed": "observed_ed_visits",
            "other": "other_ed_visits",
            "pct": "prop_disease_ed_visits",
        }[config.ed_visit_type]
        source_data = (
            append_prop_ed_data(source.data).with_columns(pl.col(".value") * 100)
            if config.ed_visit_type == "pct"
            else source.data
        )
        data = source_data.filter(pl.col(".variable") == variable).select(
            "date",
            "data_type",
            pl.col(".value").cast(pl.Float64).alias("value"),
        )
    else:
        source = forecast_run.nhsn
        if source is None:
            raise ValueError("The forecast run does not contain NHSN data")
        data = source.data.select(
            "date",
            "data_type",
            pl.col("value").cast(pl.Float64),
        )

    data = (
        data.filter(
            (pl.col("data_type") == "train")
            & pl.col("value").is_not_null()
            & pl.col("value").is_finite()
        )
        .drop("data_type")
        .sort("date")
    )
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
