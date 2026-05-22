"""
Forecast specification value object.

Lives in its own module so that both `nowcast.py` (which the protocol and its
implementations live in) and `epiautogp_forecast_utils.py` (which builds and
consumes specs) can import it.
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ForecastSpec:
    """
    Specification of a single forecast run.

    Bundles the disease, location, vintage, data-source family, cadence, and
    ED-visit subtype that together identify and parameterize one forecast.
    These fields travel together through the pipeline and the validation in
    `prep_epiautogp_data._validate_epiautogp_parameters` enforces relationships
    between them, so they form a single cohesive unit rather than independent
    arguments.
    """

    disease: str
    loc: str
    report_date: date
    target: str
    frequency: str
    ed_visit_type: str
