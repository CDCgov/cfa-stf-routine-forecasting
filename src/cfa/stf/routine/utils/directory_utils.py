"""Forecast batch directory naming, parsing, and discovery utilities."""

import datetime as dt
import os
import re
from pathlib import Path

from cfa.stf.data import ensure_list
from cfa.stf.forecasttools import LOCATION_LIST

DISEASE_NAMES = frozenset({"covid", "flu", "rsv"})
loc_abbrs_ = LOCATION_LIST


def get_model_batch_dir_name(
    disease: str,
    report_date: dt.date,
    first_training_date: dt.date,
    last_training_date: dt.date,
) -> str:
    """Build the standard model batch directory name."""
    return f"{disease}_r_{report_date}_f_{first_training_date}_t_{last_training_date}"


def parse_model_batch_dir_name(model_batch_dir_name: str) -> dict:
    """Parse a standard model batch directory name."""
    regex_match = re.match(r"(.+)_r_(.+)_f_(.+)_t_(.+)", model_batch_dir_name)
    if regex_match:
        disease, report_date, first_training_date, last_training_date = (
            regex_match.groups()
        )
    else:
        raise ValueError(
            f"Invalid model batch directory name format: {model_batch_dir_name}"
        )

    if disease not in DISEASE_NAMES:
        valid_diseases = ", ".join(sorted(DISEASE_NAMES))
        raise ValueError(
            f"Unknown disease '{disease}' in model batch directory name. "
            f"Valid diseases are: {valid_diseases}"
        )

    return {
        "disease": disease,
        "report_date": dt.datetime.strptime(report_date, "%Y-%m-%d").date(),
        "first_training_date": dt.datetime.strptime(
            first_training_date, "%Y-%m-%d"
        ).date(),
        "last_training_date": dt.datetime.strptime(
            last_training_date, "%Y-%m-%d"
        ).date(),
    }


def get_all_forecast_dirs(
    parent_dir: Path | str,
    diseases: str | list[str],
    report_date: str | dt.date = None,
) -> list[str]:
    """Return forecast-run subdirectories matching disease and report date."""
    diseases = ensure_list(diseases)

    if report_date is None:
        report_date_str = ""
    elif isinstance(report_date, str):
        report_date_str = report_date
    elif isinstance(report_date, dt.date):
        report_date_str = f"{report_date:%Y-%m-%d}"
    else:
        raise ValueError(
            "report_date must be one of None, "
            "a string in the format YYYY-MM-DD "
            "or a datetime.date instance. "
            f"Got {type(report_date)}."
        )

    valid_starts = tuple(f"{disease}_r_{report_date_str}" for disease in diseases)
    return [
        entry.name
        for entry in os.scandir(parent_dir)
        if entry.is_dir() and entry.name.startswith(valid_starts)
    ]
