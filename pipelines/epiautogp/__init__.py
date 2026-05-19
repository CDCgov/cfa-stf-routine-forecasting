"""
EpiAutoGP integration module for cfa-stf-routine-forecasting pipelines.
"""

from pipelines.epiautogp.epiautogp_forecast_utils import setup_forecast_pipeline
from pipelines.epiautogp.nowcast import NowcastData, NowcastModel
from pipelines.epiautogp.prep_epiautogp_data import convert_to_epiautogp_json

__all__ = [
    "convert_to_epiautogp_json",
    "NowcastData",
    "NowcastModel",
    "setup_forecast_pipeline",
]
