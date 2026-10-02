"""Build the model-specific JSON input consumed by mech_nssp_daily."""

import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from cfa.stf.routine.data.reporting_delay import correct_reports_by_lag
from cfa.stf.routine.forecast_run import ForecastRun

MODEL_VARIABLE = "observed_ed_visits"


def _extract_model_series(
    *,
    forecast_run: ForecastRun,
    logger: logging.Logger,
) -> tuple[list[dt.date], list[float | None]]:
    """Select the daily observed-ED-visit training series from shared run state.

    The returned dates cover every calendar day from the first through the last
    observation. A reporting gap has a ``None`` report so the Julia runner can
    predict through that day without correcting the filter.
    """
    source = forecast_run.nssp
    if source is None:
        raise ValueError("The forecast run does not contain NSSP data")
    if source.resolution != "daily":
        raise ValueError(
            f"NSSP data resolution {source.resolution!r} is not daily; "
            "mech_nssp_daily fits daily counts"
        )
    observed = (
        source.data.filter(
            (pl.col(".variable") == MODEL_VARIABLE) & (pl.col("data_type") == "train")
        )
        .select("date", pl.col(".value").cast(pl.Float64).alias("value"))
        .filter(pl.col("value").is_not_null() & pl.col("value").is_finite())
        .sort("date")
    )
    if observed.is_empty():
        raise ValueError(f"No NSSP {MODEL_VARIABLE!r} training observations available")
    if observed.get_column("date").n_unique() != observed.height:
        raise ValueError("NSSP training observations contain duplicate dates")
    first, last = (
        observed.get_column("date").min(),
        observed.get_column("date").max(),
    )
    grid = pl.DataFrame({"date": pl.date_range(first, last, interval="1d", eager=True)})
    data = grid.join(observed, on="date", how="left").sort("date")
    dates = data.get_column("date").to_list()
    reports = data.get_column("value").to_list()
    logger.info(
        "Extracted %s daily NSSP observations from %s to %s (%s reporting gaps)",
        observed.height,
        dates[0],
        dates[-1],
        data.height - observed.height,
    )
    return dates, reports


def convert_to_mech_nssp_daily_json(
    *,
    forecast_run: ForecastRun,
    reporting_delay_pmf: list[float],
    logger: logging.Logger | None = None,
) -> Path:
    """Serialize one shared forecast run in mech_nssp_daily's JSON input format.

    Reports still subject to backfill are nowcast by their true lag with the
    reporting-delay PMF; the fitted ``observations`` are the rounded nowcasts,
    and the raw reports and reporting fractions travel alongside so the runner
    can tell settled days from provisional ones.
    """
    logger = logger or logging.getLogger(__name__)
    dates, reports = _extract_model_series(
        forecast_run=forecast_run,
        logger=logger,
    )
    present = [index for index, report in enumerate(reports) if report is not None]

    corrected, fractions = correct_reports_by_lag(
        dates=[dates[index] for index in present],
        reports=[reports[index] for index in present],
        pmf=reporting_delay_pmf,
        report_date=forecast_run.report_date,
    )
    observations: list[float | None] = [None] * len(dates)
    reporting_fractions: list[float | None] = [None] * len(dates)
    for position, index in enumerate(present):
        observations[index] = float(round(corrected[position]))
        reporting_fractions[index] = fractions[position]

    n_gaps = len(dates) - len(present)
    n_provisional = sum(fraction < 1.0 for fraction in fractions)
    logger.info(
        "Prepared %s daily NSSP grid slots from %s to %s (%s observed, %s gaps, "
        "%s provisional reports nowcast)",
        len(dates),
        dates[0],
        dates[-1],
        len(present),
        n_gaps,
        n_provisional,
    )

    model_input = {
        "dates": [date.isoformat() for date in dates],
        "observations": observations,
        "raw_observations": reports,
        "reporting_fractions": reporting_fractions,
        "location": forecast_run.loc,
        "disease": forecast_run.disease,
        "population": forecast_run.loc_pop,
        "report_date": forecast_run.report_date.isoformat(),
        "forecast_through": forecast_run.forecast_through.isoformat(),
    }
    input_path = forecast_run.model_dir / f"{forecast_run.model_name}_input.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    with input_path.open("w") as file:
        json.dump(model_input, file, indent=2)
    logger.info("Saved mech_nssp_daily input JSON to %s", input_path)
    return input_path
