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
    n_training_days: int,
    exclude_last_n_days: int,
) -> str:
    """Build the standard model batch directory name."""
    return f"{disease}_lookback-{n_training_days}_omit-{exclude_last_n_days}"


def parse_model_batch_dir_name(model_batch_dir_name: str) -> dict:
    """Parse a standard model batch directory name."""
    regex_match = re.fullmatch(
        r"(.+)_lookback-(\d+)_omit-(\d+)",
        model_batch_dir_name,
    )
    if regex_match:
        disease, n_training_days, exclude_last_n_days = regex_match.groups()
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
        "n_training_days": int(n_training_days),
        "exclude_last_n_days": int(exclude_last_n_days),
    }


def parse_forecast_output_dir_name(forecast_output_dir: Path | str) -> dt.date:
    """Parse the report date from a standard forecast output directory name."""
    forecast_output_dir_name = Path(forecast_output_dir).name
    regex_match = re.fullmatch(r"(.+)_forecasts", forecast_output_dir_name)
    if regex_match is None:
        raise ValueError(
            f"Invalid forecast output directory name format: {forecast_output_dir_name}"
        )
    try:
        return dt.date.fromisoformat(regex_match.group(1))
    except ValueError as error:
        raise ValueError(
            f"Invalid report date in forecast output directory name: "
            f"{forecast_output_dir_name}"
        ) from error


def get_all_model_batch_dirs(
    parent_dir: Path | str,
    diseases: str | list[str],
) -> list[str]:
    """Return model-batch subdirectories matching the requested diseases."""
    diseases = ensure_list(diseases)
    disease_pattern = "|".join(re.escape(disease) for disease in diseases)
    valid_name = re.compile(rf"(?:{disease_pattern})_lookback-\d+_omit-\d+").fullmatch
    return [
        entry.name
        for entry in os.scandir(parent_dir)
        if entry.is_dir() and valid_name(entry.name)
    ]
