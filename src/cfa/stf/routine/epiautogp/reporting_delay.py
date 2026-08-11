"""
Reporting-delay helpers shared across nowcast sources.
"""

from itertools import accumulate

REPORTING_FRACTION_TOL = 1e-9


def reporting_inflation_factors(pmf: list[float]) -> list[float]:
    """
    Per-row reporting fractions for the most-recent observations.

    The reporting-delay PMF is converted to a CDF; entries strictly less than 1
    correspond to dates whose reports are still partial. Returns those CDF
    entries in order (oldest-first), so callers can pair them with the
    most-recent observations (newest-first) to inflate toward fully-reported
    counts.
    """
    cdf = list(accumulate(pmf))
    return [fraction for fraction in cdf if fraction < 1.0 - REPORTING_FRACTION_TOL]


def inflate_report(report: float, fraction: float) -> float:
    """
    Inflation estimator for one partially-reported observation.

    Implements the zero-handling approximation documented at
    https://baselinenowcast.epinowcast.org/articles/model_definition.html#point-nowcast-generation
    so a zero report with a small reporting fraction stays well-behaved.
    """
    if fraction < 0.0:
        raise ValueError(f"Reporting fraction must be nonnegative: {fraction}")
    if fraction == 0.0:
        if report == 0.0:
            return 0.0
        raise ValueError(
            "Cannot inflate a positive report with zero reporting fraction"
        )
    return (report + 1.0 - fraction) / fraction
