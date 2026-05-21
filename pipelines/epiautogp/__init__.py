"""
EpiAutoGP integration module for cfa-stf-routine-forecasting pipelines.
"""

from pipelines.epiautogp.epiautogp_forecast_utils import setup_forecast_pipeline
from pipelines.epiautogp.nowcast import FixedNowcast, NowcastData, NowcastSource
from pipelines.epiautogp.prep_epiautogp_data import convert_to_epiautogp_json
from pipelines.epiautogp.reporting_delay import (
    inflate_report,
    reporting_inflation_factors,
)

__all__ = [
    "convert_to_epiautogp_json",
    "FixedNowcast",
    "inflate_report",
    "NowcastData",
    "NowcastSource",
    "reporting_inflation_factors",
    "setup_forecast_pipeline",
]
