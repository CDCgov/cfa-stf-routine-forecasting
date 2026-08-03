"""
EpiAutoGP integration module for cfa-stf-routine-forecasting pipelines.
"""

from pipelines.data.hubverse_nowcast import HubverseNowcast
from pipelines.data.nowcast import FixedNowcast, NowcastData, NowcastSource
from pipelines.data.reporting_delay import inflate_report, reporting_inflation_factors
from pipelines.epiautogp.epiautogp_forecast_utils import setup_forecast_pipeline
from pipelines.epiautogp.prep_epiautogp_data import convert_to_epiautogp_json

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
