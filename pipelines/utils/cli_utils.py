"""Shared command-line arguments and utilities for forecast pipelines."""

import argparse
import subprocess
from pathlib import Path


def add_common_forecast_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common CLI arguments shared across forecast pipelines."""
    parser.add_argument(
        "--disease",
        type=str,
        required=True,
        help="Disease to model (e.g., COVID-19, Influenza, RSV).",
    )

    parser.add_argument(
        "--loc",
        type=str,
        required=True,
        help=(
            "Two-letter USPS abbreviation for the location to fit "
            "(e.g. 'AK', 'AL', 'AZ', etc.)."
        ),
    )

    parser.add_argument(
        "--facility-level-nssp-data-dir",
        type=Path,
        default=Path("private_data", "nssp_etl_gold"),
        help="Directory in which to look for facility-level NSSP ED visit data.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default="private_data",
        help="Directory in which to save output.",
    )

    parser.add_argument(
        "--n-training-days",
        type=int,
        default=180,
        help="Number of training days (default: 180).",
    )

    parser.add_argument(
        "--n-forecast-days",
        type=int,
        default=28,
        help=(
            "Number of days ahead to forecast relative to the "
            "report date (default: 28)."
        ),
    )

    parser.add_argument(
        "--exclude-last-n-days",
        type=int,
        default=0,
        help=(
            "Optionally exclude the final n days of available training "
            "data (default: 0, i.e. exclude no available data)."
        ),
    )

    parser.add_argument(
        "--credentials-path",
        type=Path,
        help="Path to a TOML file containing credentials such as API keys.",
    )

    parser.add_argument(
        "--nhsn-data-path",
        type=Path,
        default=None,
        help="Path to local NHSN data (for local testing).",
    )


def run_command(
    executable: str,
    args: list[str],
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run a command-line executable with arguments and handle errors.

    This is a general-purpose function for running any command-line tool
    (e.g., Rscript, julia, python) with proper error handling.

    Parameters
    ----------
    executable : str
        The command-line executable to run (e.g., "Rscript", "julia", "python").
    args : list[str]
        Arguments to pass to the executable (e.g., script path and its arguments).
    function_name : str | None
        Name of the calling function for error messages. If `None`, uses executable name.
    capture_output : bool, optional
        Whether to capture `stdout` and `stderr`, by default `True`.
    text : bool, optional
        Whether to decode output as text, by default `False`.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the command execution fails, returns a non-zero exit code and captures `stderr`.
    """
    command = [executable] + args

    result = subprocess.run(
        command,
        capture_output=capture_output,
        text=text,
    )

    if result.returncode != 0:
        error_name = function_name or executable
        error_msg = result.stderr.decode("utf-8") if not text else result.stderr
        raise RuntimeError(f"{error_name}: {error_msg}")

    return result
