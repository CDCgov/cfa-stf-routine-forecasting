"""NowcastAutoGP integration for the routine forecasting pipeline."""

from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.data.nowcast import NowcastData, NowcastSource
from cfa.stf.routine.nowcastautogp.config import NowcastAutoGPConfig
from cfa.stf.routine.nowcastautogp.nowcast import FixedNowcast
from cfa.stf.routine.nowcastautogp.prep_nowcastautogp_data import (
    convert_to_nowcastautogp_json,
)
from cfa.stf.routine.nowcastautogp.reporting_delay_nowcast import (
    inflate_report,
    reporting_inflation_factors,
)

__all__ = [
    "convert_to_nowcastautogp_json",
    "NowcastAutoGPConfig",
    "FixedNowcast",
    "inflate_report",
    "HubverseNowcast",
    "NowcastData",
    "NowcastSource",
    "reporting_inflation_factors",
]
