"""Run R regression tests for missing-data forecasts and calendar boundaries."""

import subprocess
from pathlib import Path


def test_missing_data_forecasts():
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        ["Rscript", str(Path(__file__).with_suffix(".R"))], cwd=root, check=True
    )
