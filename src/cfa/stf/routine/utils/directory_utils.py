"""Forecast batch directory naming, parsing, and discovery utilities."""

import os
import re
from pathlib import Path

from cfa.stf.data import ensure_list
from cfa.stf.forecasttools import LOCATION_LIST

from cfa.stf.routine.lookback import ALL_AVAILABLE_LOOKBACK, parse_lookback_days

DISEASE_NAMES = frozenset({"covid", "flu", "rsv"})
loc_abbrs_ = LOCATION_LIST


def parse_model_batch_dir_name(model_batch_dir_name: str) -> dict:
    """Parse a standard model batch directory name."""
    regex_match = re.fullmatch(
        rf"(.+)_lookback-(\d+|{ALL_AVAILABLE_LOOKBACK})_omit-(\d+)",
        model_batch_dir_name,
    )
    if regex_match:
        disease, n_lookback_days, exclude_last_n_days = regex_match.groups()
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
        "n_lookback_days": parse_lookback_days(n_lookback_days),
        "exclude_last_n_days": int(exclude_last_n_days),
    }


def get_all_forecast_dirs(
    parent_dir: Path | str,
    diseases: str | list[str],
) -> list[str]:
    """Return model-batch subdirectories matching the requested diseases."""
    diseases = ensure_list(diseases)
    valid_starts = tuple(f"{disease}_lookback-" for disease in diseases)
    return [
        entry.name
        for entry in os.scandir(parent_dir)
        if entry.is_dir() and entry.name.startswith(valid_starts)
    ]
