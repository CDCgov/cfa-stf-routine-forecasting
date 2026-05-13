"""Run EpiAutoGP forecasts by calling NowcastAutoGP through juliacall."""

import datetime as dt
import json
import math
import os
import sys
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import polars as pl

VALID_TARGETS = frozenset({"nhsn", "nssp"})
VALID_FREQUENCIES = frozenset({"daily", "epiweekly"})
VALID_ED_VISIT_TYPES = frozenset({"observed", "other", "pct"})

NSSP_VARIABLE_BY_ED_VISIT_TYPE = {
    "observed": "observed_ed_visits",
    "other": "other_ed_visits",
    "pct": "prop_disease_ed_visits",
}

JULIA_HELPER_CODE = """
using Dates
using NowcastAutoGP

function run_direct_nowcastautogp(
        date_strings,
        reports,
        frequency,
        forecast_date_string,
        nowcast_date_strings,
        nowcast_reports,
        n_ahead,
        n_forecasts,
        transformation_name,
        n_particles,
        smc_data_proportion,
        n_mcmc,
        n_hmc
    )
    dates = Date.(collect(date_strings))
    report_values = Float64.(collect(reports))
    nowcast_dates = Date.(collect(nowcast_date_strings))
    nowcast_report_values = [
        Float64.(collect(nowcast_report)) for nowcast_report in collect(nowcast_reports)
    ]

    transformation, inv_transformation = get_transformations(
        String(transformation_name),
        report_values
    )

    stable_data_indices = findall(date -> !(date in nowcast_dates), dates)
    stable_data_dates = dates[stable_data_indices]
    stable_data_values = report_values[stable_data_indices]

    nowcast_data = isempty(nowcast_dates) ? nothing : create_nowcast_data(
        nowcast_report_values,
        nowcast_dates;
        transformation = transformation
    )

    n_ahead_int = Int(n_ahead)
    n_forecasts_int = Int(n_forecasts)
    forecast_date = Date(String(forecast_date_string))
    time_step = String(frequency) == "epiweekly" ? Week(1) : Day(1)
    forecast_dates = [forecast_date + i * time_step for i in 0:n_ahead_int]

    n_forecasts_per_nowcast = isnothing(nowcast_data) ?
        n_forecasts_int :
        max(1, n_forecasts_int ÷ length(nowcast_data))

    transformed_data = create_transformed_data(
        stable_data_dates,
        stable_data_values;
        transformation = transformation
    )
    base_model = make_and_fit_model(
        transformed_data;
        n_particles = Int(n_particles),
        smc_data_proportion = Float64(smc_data_proportion),
        n_mcmc = Int(n_mcmc),
        n_hmc = Int(n_hmc)
    )

    forecasts = if isnothing(nowcast_data)
        forecast(
            base_model,
            forecast_dates,
            n_forecasts_per_nowcast;
            inv_transformation = inv_transformation
        )
    else
        forecast_with_nowcasts(
            base_model,
            nowcast_data,
            forecast_dates,
            n_forecasts_per_nowcast;
            inv_transformation = inv_transformation
        )
    end

    return (
        forecast_dates = string.(forecast_dates),
        forecasts_by_draw = [Float64.(forecasts[:, draw]) for draw in axes(forecasts, 2)]
    )
end
"""


@dataclass(frozen=True)
class EpiAutoGPInput:
    dates: list[dt.date]
    reports: list[float]
    pathogen: str
    location: str
    target: str
    frequency: str
    ed_visit_type: str
    forecast_date: dt.date
    nowcast_dates: list[dt.date]
    nowcast_reports: list[list[float]]

    @property
    def date_strings(self) -> list[str]:
        return [date.isoformat() for date in self.dates]

    @property
    def forecast_date_string(self) -> str:
        return self.forecast_date.isoformat()

    @property
    def nowcast_date_strings(self) -> list[str]:
        return [date.isoformat() for date in self.nowcast_dates]


def run_nowcastautogp_forecast(
    json_input_path: Path,
    model_dir: Path,
    params: dict[str, Any],
    execution_settings: dict[str, Any],
) -> None:
    """Run NowcastAutoGP through juliacall and write pipeline samples output."""
    input_data = read_epiautogp_input(json_input_path)
    model_dir.mkdir(parents=True, exist_ok=True)

    _configure_juliacall(execution_settings)
    julia_module = _get_julia_module()

    try:
        result = julia_module.run_direct_nowcastautogp(
            input_data.date_strings,
            input_data.reports,
            input_data.frequency,
            input_data.forecast_date_string,
            input_data.nowcast_date_strings,
            input_data.nowcast_reports,
            params["n_ahead"],
            params["n_forecast_draws"],
            params["transformation"],
            params["n_particles"],
            params["smc_data_proportion"],
            params["n_mcmc"],
            params["n_hmc"],
        )
    except Exception as exc:
        raise RuntimeError("NowcastAutoGP Julia forecast failed") from exc

    forecast_dates = _parse_forecast_dates(result.forecast_dates)
    forecasts_by_draw = _convert_forecasts_by_draw(result.forecasts_by_draw)
    _write_pipeline_samples(input_data, forecast_dates, forecasts_by_draw, model_dir)


def read_epiautogp_input(json_input_path: Path) -> EpiAutoGPInput:
    """Read and validate an EpiAutoGP input JSON file."""
    if not json_input_path.exists():
        raise FileNotFoundError(f"EpiAutoGP input JSON not found: {json_input_path}")

    raw_data = json.loads(json_input_path.read_text(encoding="utf-8"))
    if not isinstance(raw_data, dict):
        raise ValueError("EpiAutoGP input JSON must contain an object.")

    required_fields = [
        "dates",
        "reports",
        "pathogen",
        "location",
        "target",
        "frequency",
        "ed_visit_type",
        "forecast_date",
        "nowcast_dates",
        "nowcast_reports",
    ]
    missing_fields = [field for field in required_fields if field not in raw_data]
    if missing_fields:
        raise ValueError(
            "EpiAutoGP input JSON is missing required field(s): "
            f"{', '.join(missing_fields)}"
        )

    input_data = EpiAutoGPInput(
        dates=_parse_date_list(raw_data["dates"], "dates"),
        reports=_parse_number_list(raw_data["reports"], "reports"),
        pathogen=_parse_non_empty_string(raw_data["pathogen"], "pathogen"),
        location=_parse_non_empty_string(raw_data["location"], "location"),
        target=_parse_non_empty_string(raw_data["target"], "target"),
        frequency=_parse_non_empty_string(raw_data["frequency"], "frequency"),
        ed_visit_type=_parse_non_empty_string(
            raw_data["ed_visit_type"], "ed_visit_type"
        ),
        forecast_date=_parse_date(raw_data["forecast_date"], "forecast_date"),
        nowcast_dates=_parse_date_list(raw_data["nowcast_dates"], "nowcast_dates"),
        nowcast_reports=_parse_number_matrix(
            raw_data["nowcast_reports"], "nowcast_reports"
        ),
    )
    validate_epiautogp_input(input_data)
    return input_data


def validate_epiautogp_input(input_data: EpiAutoGPInput) -> None:
    """Validate an EpiAutoGP input object before passing it to Julia."""
    if input_data.target not in VALID_TARGETS:
        raise ValueError(
            f"target must be one of {sorted(VALID_TARGETS)}, got {input_data.target!r}"
        )

    if input_data.frequency not in VALID_FREQUENCIES:
        raise ValueError(
            "frequency must be one of "
            f"{sorted(VALID_FREQUENCIES)}, got {input_data.frequency!r}"
        )

    if input_data.ed_visit_type not in VALID_ED_VISIT_TYPES:
        raise ValueError(
            "ed_visit_type must be one of "
            f"{sorted(VALID_ED_VISIT_TYPES)}, got {input_data.ed_visit_type!r}"
        )

    if input_data.target == "nhsn" and input_data.frequency == "daily":
        raise ValueError("NHSN data is only available in epiweekly frequency.")

    if input_data.target == "nhsn" and input_data.ed_visit_type != "observed":
        raise ValueError("For NHSN, ed_visit_type must be 'observed'.")

    if len(input_data.dates) != len(input_data.reports):
        raise ValueError(
            "Length mismatch: dates "
            f"({len(input_data.dates)}) and reports ({len(input_data.reports)}) "
            "must have the same length."
        )

    if not input_data.dates:
        raise ValueError("Empty data: dates and reports cannot be empty.")

    if input_data.dates != sorted(input_data.dates):
        raise ValueError("Date ordering: dates must be sorted chronologically.")

    if input_data.nowcast_dates != sorted(input_data.nowcast_dates):
        raise ValueError(
            "Nowcast date ordering: nowcast_dates must be sorted chronologically."
        )

    if not input_data.nowcast_dates and input_data.nowcast_reports:
        raise ValueError(
            "Nowcast consistency error: no nowcast_dates provided but "
            "nowcast_reports is not empty."
        )

    if input_data.nowcast_dates and not input_data.nowcast_reports:
        raise ValueError(
            "Nowcast consistency error: nowcast_dates provided but "
            "nowcast_reports is empty."
        )

    for index, report_vector in enumerate(input_data.nowcast_reports, start=1):
        if len(report_vector) != len(input_data.nowcast_dates):
            raise ValueError(
                "Nowcast vector length mismatch at index "
                f"{index}: got {len(report_vector)} value(s), expected "
                f"{len(input_data.nowcast_dates)}."
            )

    _validate_forecast_date_range(input_data)


def _configure_juliacall(execution_settings: dict[str, Any]) -> None:
    if "juliacall" in sys.modules:
        return

    threads = execution_settings.get("threads", "auto")
    os.environ.setdefault("PYTHON_JULIACALL_THREADS", str(threads))
    os.environ.setdefault("PYTHON_JULIACALL_STARTUP_FILE", "no")


@cache
def _get_julia_module():
    try:
        import juliacall
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "juliacall is required to run EpiAutoGP through NowcastAutoGP. "
            "Install the project dependencies before running this pipeline."
        ) from exc

    julia_module = juliacall.newmodule("PyRenewHEWNowcastAutoGP")
    julia_module.seval(JULIA_HELPER_CODE)
    return julia_module


def _write_pipeline_samples(
    input_data: EpiAutoGPInput,
    forecast_dates: list[dt.date],
    forecasts_by_draw: list[list[float]],
    model_dir: Path,
) -> None:
    variable_name = _variable_name(input_data)
    dates: list[dt.date] = []
    values: list[float] = []
    draws: list[int] = []

    for draw_index, sampled_values in enumerate(forecasts_by_draw, start=1):
        if len(sampled_values) != len(forecast_dates):
            raise ValueError(
                "Forecast shape mismatch: draw "
                f"{draw_index} has {len(sampled_values)} value(s), expected "
                f"{len(forecast_dates)}."
            )

        for forecast_date, sampled_value in zip(forecast_dates, sampled_values):
            dates.append(forecast_date)
            values.append(_output_value(input_data, sampled_value))
            draws.append(draw_index)

    forecast_df = pl.DataFrame(
        {
            "date": dates,
            ".value": values,
            ".draw": draws,
            ".variable": [variable_name] * len(dates),
            "resolution": [input_data.frequency] * len(dates),
            "geo_value": [input_data.location] * len(dates),
            "disease": [input_data.pathogen] * len(dates),
        },
        schema={
            "date": pl.Date,
            ".value": pl.Float64,
            ".draw": pl.Int32,
            ".variable": pl.String,
            "resolution": pl.String,
            "geo_value": pl.String,
            "disease": pl.String,
        },
    )

    samples_path = model_dir / "samples.parquet"
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    forecast_df.write_parquet(samples_path)


def _variable_name(input_data: EpiAutoGPInput) -> str:
    if input_data.target == "nhsn":
        return "observed_hospital_admissions"

    return NSSP_VARIABLE_BY_ED_VISIT_TYPE[input_data.ed_visit_type]


def _output_value(input_data: EpiAutoGPInput, value: float) -> float:
    if input_data.target == "nssp" and input_data.ed_visit_type == "pct":
        return value / 100.0
    return value


def _parse_forecast_dates(date_values: Any) -> list[dt.date]:
    return [
        _parse_date(str(date_value), "forecast_dates") for date_value in date_values
    ]


def _convert_forecasts_by_draw(forecasts_by_draw: Any) -> list[list[float]]:
    return [
        [float(value) for value in draw_values] for draw_values in forecasts_by_draw
    ]


def _parse_date_list(value: Any, field_name: str) -> list[dt.date]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")
    return [_parse_date(item, field_name) for item in value]


def _parse_number_matrix(value: Any, field_name: str) -> list[list[float]]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")
    return [_parse_number_list(item, field_name) for item in value]


def _parse_number_list(value: Any, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")

    numbers = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int | float):
            raise ValueError(f"{field_name} must contain only numeric values.")

        number = float(item)
        if not math.isfinite(number) or number < 0:
            raise ValueError(
                f"{field_name} must contain only non-negative finite values."
            )
        numbers.append(number)

    return numbers


def _parse_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value


def _parse_date(value: Any, field_name: str) -> dt.date:
    if not _is_iso_date(value):
        raise ValueError(f"{field_name} must contain dates in YYYY-MM-DD format.")
    return dt.date.fromisoformat(value)


def _is_iso_date(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:10].isdigit()
    )


def _validate_forecast_date_range(input_data: EpiAutoGPInput) -> None:
    date_range_days = (max(input_data.dates) - min(input_data.dates)).days
    days_buffer = max(30, math.ceil(date_range_days / 10))

    if input_data.forecast_date < min(input_data.dates) - dt.timedelta(
        days=days_buffer
    ):
        raise ValueError(
            "Forecast date "
            f"({input_data.forecast_date}) is too far before the data range "
            f"({min(input_data.dates)} to {max(input_data.dates)})."
        )

    if input_data.forecast_date > max(input_data.dates) + dt.timedelta(
        days=days_buffer
    ):
        raise ValueError(
            "Forecast date "
            f"({input_data.forecast_date}) is too far after the data range "
            f"({min(input_data.dates)} to {max(input_data.dates)})."
        )
