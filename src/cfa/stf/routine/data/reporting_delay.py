"""
Reporting-delay (right-truncation) corrections for daily count series.

A right-truncation PMF gives the probability that a report for reference date
``d`` has arrived after each day of delay. Its cumulative sum is the expected
reporting fraction by lag, where entry ``k`` (0-based) is the fraction reported
at lag ``k + 1`` days: the first entry describes ``reference_date = report_date
- 1`` as of ``report_date`` (see ``ForecastRun.right_truncation_offset``).
"""

import datetime as dt
from itertools import accumulate

REPORTING_FRACTION_TOL = 1e-9


def reporting_inflation_factors(pmf: list[float]) -> list[float]:
    """Return incomplete reporting CDF entries, ordered oldest first."""
    return [
        fraction
        for fraction in accumulate(pmf)
        if fraction < 1.0 - REPORTING_FRACTION_TOL
    ]


def inflate_report(report: float, fraction: float) -> float:
    """Inflate one partial report using its expected reporting fraction."""
    if fraction < 0.0:
        raise ValueError(f"Reporting fraction must be nonnegative: {fraction}")
    if fraction == 0.0:
        if report == 0.0:
            return 0.0
        raise ValueError(
            "Cannot inflate a positive report with zero reporting fraction"
        )
    return (report + 1.0 - fraction) / fraction


def correct_reports_by_lag(
    *,
    dates: list[dt.date],
    reports: list[float],
    pmf: list[float],
    report_date: dt.date,
) -> tuple[list[float], list[float]]:
    """
    Nowcast each report by its true reporting lag.

    The lag of a reference date is ``report_date - date`` in days and must be at
    least one. A date whose lag lies beyond the incomplete part of the reporting
    CDF is treated as fully reported (fraction ``1.0``). Unlike a positional
    pairing of the newest reports with the CDF, this stays correct when the
    series stops before ``report_date - 1`` (a nonzero ``exclude_last_n_days``).

    Returns the corrected reports and the reporting fraction applied to each.
    """
    if len(dates) != len(reports):
        raise ValueError("dates and reports must have the same length")
    factors = reporting_inflation_factors(pmf)
    corrected: list[float] = []
    fractions: list[float] = []
    for date, report in zip(dates, reports, strict=True):
        lag = (report_date - date).days
        if lag < 1:
            raise ValueError(
                f"Reference date {date} is not before report date {report_date}"
            )
        fraction = factors[lag - 1] if lag <= len(factors) else 1.0
        corrected.append(inflate_report(float(report), fraction))
        fractions.append(fraction)
    return corrected, fractions
