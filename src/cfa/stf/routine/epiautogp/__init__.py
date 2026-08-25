"""EpiAutoGP integration for the routine forecasting pipeline."""

from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.data.nowcast import NowcastData, NowcastSource
from cfa.stf.routine.epiautogp.config import EpiAutoGPConfig
from cfa.stf.routine.epiautogp.nowcast import FixedNowcast
from cfa.stf.routine.epiautogp.prep_epiautogp_data import convert_to_epiautogp_json
from cfa.stf.routine.epiautogp.reporting_delay_nowcast import (
    inflate_report,
    reporting_inflation_factors,
)

__all__ = [
    "convert_to_epiautogp_json",
    "EpiAutoGPConfig",
    "FixedNowcast",
    "inflate_report",
    "HubverseNowcast",
    "NowcastData",
    "NowcastSource",
    "reporting_inflation_factors",
]
