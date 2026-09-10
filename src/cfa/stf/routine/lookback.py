"""Canonical encoding for finite and unlimited training lookbacks."""

ALL_AVAILABLE_LOOKBACK = "all"


def format_lookback_days(n_lookback_days: int | None) -> str:
    """Format a lookback value for use in paths and other identifiers."""
    return (
        str(n_lookback_days) if n_lookback_days is not None else ALL_AVAILABLE_LOOKBACK
    )


def parse_lookback_days(value: str) -> int | None:
    """Parse a lookback value from its canonical identifier representation."""
    return None if value == ALL_AVAILABLE_LOOKBACK else int(value)
