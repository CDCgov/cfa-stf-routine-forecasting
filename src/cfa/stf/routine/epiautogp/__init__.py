"""
EpiAutoGP integration module for cfa-stf-routine-forecasting cfa.stf.routine.
"""

from cfa.stf.routine.data.hubverse_nowcast import HubverseNowcast
from cfa.stf.routine.data.nowcast import NowcastData, NowcastSource
from cfa.stf.routine.epiautogp.epiautogp_forecast_utils import setup_forecast_pipeline
from cfa.stf.routine.epiautogp.nowcast import FixedNowcast
from cfa.stf.routine.epiautogp.prep_epiautogp_data import convert_to_epiautogp_json
from cfa.stf.routine.epiautogp.reporting_delay import (
    inflate_report,
    reporting_inflation_factors,
)

__all__ = [
    "convert_to_epiautogp_json",
    "FixedNowcast",
    "inflate_report",
    "HubverseNowcast",
    "NowcastData",
    "NowcastSource",
    "reporting_inflation_factors",
    "setup_forecast_pipeline",
]
