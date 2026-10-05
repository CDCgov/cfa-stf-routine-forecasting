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


def reporting_fractions_by_lag(
    *,
    dates: list[dt.date],
    pmf: list[float],
    report_date: dt.date,
) -> list[float]:
    """
    Return the expected reporting fraction of each reference date by its true lag.

    This uses PyRenew's convention (``pyrenew.convolve.compute_prop_already_reported``
    with ``ForecastRun.right_truncation_offset``): the lag of a reference date is
    ``report_date - date`` in days and must be at least one, reporting CDF entry
    ``lag - 1`` applies to it, and a lag past the end of the CDF is fully
    reported (fraction ``1.0``). Indexing by date rather than by position from
    the end of the series keeps reporting gaps from shifting the CDF. A CDF
    that overshoots one through rounding is capped at ``1.0``.
    """
    cdf = list(accumulate(pmf))
    fractions: list[float] = []
    for date in dates:
        lag = (report_date - date).days
        if lag < 1:
            raise ValueError(
                f"Reference date {date} is not before report date {report_date}"
            )
        fractions.append(min(cdf[lag - 1], 1.0) if lag <= len(cdf) else 1.0)
    return fractions


def correct_reports_by_lag(
    *,
    dates: list[dt.date],
    reports: list[float],
    pmf: list[float],
    report_date: dt.date,
) -> tuple[list[float], list[float]]:
    """
    Nowcast each report by its true reporting lag.

    Fractions come from ``reporting_fractions_by_lag``. Unlike a positional
    pairing of the newest reports with the CDF, this stays correct when the
    series stops before ``report_date - 1`` (a nonzero ``exclude_last_n_days``).

    Returns the corrected reports and the reporting fraction applied to each.
    """
    if len(dates) != len(reports):
        raise ValueError("dates and reports must have the same length")
    fractions = reporting_fractions_by_lag(
        dates=dates, pmf=pmf, report_date=report_date
    )
    corrected = [
        inflate_report(float(report), fraction)
        for report, fraction in zip(reports, fractions, strict=True)
    ]
    return corrected, fractions
