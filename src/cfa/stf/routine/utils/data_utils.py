"""Utilities for preparing forecast pipeline datasets."""

from pathlib import Path

from cfa.stf.routine._paths import DATA_DIR
from cfa.stf.routine.utils.language_utils import run_r_script


def generate_epiweekly_data(data_dir: Path, overwrite_daily: bool = False) -> None:
    """Generate epiweekly datasets from daily datasets using an R script."""
    args = [str(data_dir)]
    if overwrite_daily:
        args.append("--overwrite-daily")

    run_r_script(
        DATA_DIR / "generate_epiweekly_data.R",
        args,
        function_name="generate_epiweekly_data",
    )
