"""Paths to non-Python resources distributed with the package."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
DATA_DIR = PACKAGE_ROOT / "data"
NOWCASTAUTOGP_DIR = PACKAGE_ROOT / "nowcastautogp"
FABLE_DIR = PACKAGE_ROOT / "fable"
MECH_NSSP_DAILY_DIR = PACKAGE_ROOT / "mech_nssp_daily"
PYRENEW_HEW_DIR = PACKAGE_ROOT / "pyrenew_hew"
UTILS_DIR = PACKAGE_ROOT / "utils"

PRODUCTION_PRIORS = PYRENEW_HEW_DIR / "priors" / "prod_priors.py"
