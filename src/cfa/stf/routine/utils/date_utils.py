"""Date parsing and training-window utilities."""

import datetime as dt
import logging


def _parse_single_date(date_str: str) -> tuple[dt.date, dt.date]:
    """Parse a single date string into a one-day date range."""
    try:
        single_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        return (single_date, single_date)
    except ValueError as e:
        raise ValueError(
            f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD format. Error: {e}"
        ) from e


def _parse_date_range(range_str: str) -> tuple[dt.date, dt.date]:
    """Parse a date range string into a tuple of start and end dates."""
    if range_str.count(":") != 1:
        raise ValueError(
            f"Invalid date range format: '{range_str}'. "
            "Expected format: 'start_date:end_date' (e.g., '2024-01-15:2024-01-20')"
        )

    start_str, end_str = range_str.split(":", 1)
    try:
        start_date = dt.datetime.strptime(start_str.strip(), "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end_str.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(
            f"Invalid date format in range '{range_str}'. "
            f"Expected YYYY-MM-DD format. Error: {e}"
        ) from e

    if start_date > end_date:
        raise ValueError(
            f"Invalid date range '{range_str}': "
            f"start_date ({start_date}) must be before or equal to end_date ({end_date})"
        )

    return (start_date, end_date)


def parse_exclude_date_ranges(
    exclude_date_ranges_str: str | None,
) -> list[tuple[dt.date, dt.date]] | None:
    """Parse comma-separated single dates and inclusive date ranges."""
    if exclude_date_ranges_str is None or not exclude_date_ranges_str.strip():
        return None

    parsed_ranges = []
    for date_range_str in exclude_date_ranges_str.split(","):
        date_range_str = date_range_str.strip()
        if ":" in date_range_str:
            date_range = _parse_date_range(date_range_str)
        else:
            date_range = _parse_single_date(date_range_str)
        parsed_ranges.append(date_range)

    return parsed_ranges


def calculate_training_dates(
    report_date: dt.date,
    n_training_days: int,
    exclude_last_n_days: int,
    logger: logging.Logger,
) -> tuple[dt.date, dt.date]:
    """Calculate the inclusive first and last dates in a training window."""
    # Add one because the maximum date in the dataset is report_date - 1.
    last_training_date = report_date - dt.timedelta(days=exclude_last_n_days + 1)

    if last_training_date >= report_date:
        raise ValueError(
            "Last training date must be before the report date. "
            f"Got a last training date of {last_training_date} "
            f"with a report date of {report_date}."
        )

    logger.info(f"last training date: {last_training_date}")
    first_training_date = last_training_date - dt.timedelta(days=n_training_days - 1)
    logger.info(f"First training date {first_training_date}")

    return first_training_date, last_training_date
