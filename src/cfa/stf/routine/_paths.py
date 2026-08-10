"""Paths to non-Python resources distributed with the package."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
DATA_DIR = PACKAGE_ROOT / "data"
EPIAUTOGP_DIR = PACKAGE_ROOT / "epiautogp"
FABLE_DIR = PACKAGE_ROOT / "fable"
PYRENEW_HEW_DIR = PACKAGE_ROOT / "pyrenew_hew"
UTILS_DIR = PACKAGE_ROOT / "utils"

PRODUCTION_PRIORS = PYRENEW_HEW_DIR / "priors" / "prod_priors.py"
