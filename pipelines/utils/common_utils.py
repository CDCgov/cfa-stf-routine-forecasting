"""Common utility functions for forecast pipelines."""

import datetime as dt
import logging
import os
import re
import runpy
import subprocess
import tomllib
from pathlib import Path
from typing import Any

from forecasttools import ensure_listlike, location_table
from pyrenew_multisignal.hew import PyrenewHEWParam, build_pyrenew_hew_model

from pipelines.utils.cli_utils import run_command

# Disease mapping and location abbreviations
disease_map_lower_ = {
    "influenza": "Influenza",
    "covid-19": "COVID-19",
    "rsv": "RSV",
}
loc_abbrs_ = location_table["short_name"].to_list()


def load_credentials(
    credentials_path: Path | str | None, logger: logging.Logger
) -> dict[str, Any] | None:
    """Load credentials from a TOML file."""
    if credentials_path is not None:
        cp = Path(credentials_path)
        if not cp.suffix.lower() == ".toml":
            raise ValueError(
                "Credentials file must have the extension "
                "'.toml' (not case-sensitive). Got "
                f"{cp.suffix}"
            )
        logger.info(f"Reading in credentials from {cp}...")
        with open(cp, "rb") as fp:
            credentials_dict = tomllib.load(fp)
    else:
        logger.info("No credentials file given. Will proceed without one.")
        credentials_dict = None

    return credentials_dict


def get_available_reports(
    data_dir: str | Path, glob_pattern: str = "*.parquet"
) -> list[dt.date]:
    """Get available report dates from glob pattern matching files in a directory. Default pattern matches parquet files."""
    return [
        dt.datetime.strptime(f.stem, "%Y-%m-%d").date()
        for f in Path(data_dir).glob(glob_pattern)
    ]


def _parse_single_date(date_str: str) -> tuple[dt.date, dt.date]:
    """
    Parse a single date string into a date range tuple.
    """
    try:
        single_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        return (single_date, single_date)
    except ValueError as e:
        raise ValueError(
            f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD format. Error: {e}"
        ) from e


def _parse_date_range(range_str: str) -> tuple[dt.date, dt.date]:
    """
    Parse a date range string into a tuple of start and end dates.
    """
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
    """
    Parse comma-separated date ranges from string to list of tuples.

    This utility function is useful for parsing date exclusion parameters
    that may be used by various forecasting models to exclude periods with
    known reporting problems or other data quality issues.

    Parameters
    ----------
    exclude_date_ranges_str : str | None
        Comma-separated list of single dates or date ranges.
        Single dates: 'YYYY-MM-DD'
        Date ranges: 'start:end' where both dates are in YYYY-MM-DD format.
        Example: '2024-01-15,2024-03-01:2024-03-07'

    Returns
    -------
    list[tuple[dt.date, dt.date]] | None
        List of (start_date, end_date) tuples where both dates are inclusive.
        For single dates, start_date and end_date will be the same.
        Returns None if input is None/empty.

    Raises
    ------
    ValueError
        If date format is invalid, dates can't be parsed as YYYY-MM-DD,
        or start_date > end_date (for ranges).

    Examples
    --------
    >>> parse_exclude_date_ranges("2024-01-15")
    [(datetime.date(2024, 1, 15), datetime.date(2024, 1, 15))]

    >>> parse_exclude_date_ranges("2024-01-15:2024-01-20")
    [(datetime.date(2024, 1, 15), datetime.date(2024, 1, 20))]

    >>> parse_exclude_date_ranges("2024-01-15,2024-03-01:2024-03-07")
    [(datetime.date(2024, 1, 15), datetime.date(2024, 1, 15)),
     (datetime.date(2024, 3, 1), datetime.date(2024, 3, 7))]

    >>> parse_exclude_date_ranges(None)
    None
    """
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
    """
    Calculate first and last training dates.

    Parameters
    ----------
    report_date : dt.date
        The report date.
    n_training_days : int
        Number of training days.
    exclude_last_n_days : int
        Number of days to exclude from the end of training data.
    logger : logging.Logger
        Process logger.

    Returns
    -------
    tuple[dt.date, dt.date]
        Tuple of (first_training_date, last_training_date).

    Raises
    ------
    ValueError
        If last training date is not before report date.
    """
    # + 1 because max date in dataset is report_date - 1
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


def run_r_script(
    script_name: str,
    args: list[str],
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run an R script or command and handle errors.

    This is a convenience wrapper around `run_command` for R scripts.
    Supports the pattern: Rscript {FLAGS} {SCRIPT} {ARGS}

    Parameters
    ----------
    script_name : str
        Name of the R script to run.
    args : list[str]
        Arguments to pass to the R script.
    executor_flags : list[str] | None
        Flags to pass to the Rscript executable before the script name.
        For example: ["--vanilla", "--verbose"]
    function_name : str | None
        Name of the calling function for error messages. If None, uses script_name.
    capture_output : bool, optional
        Whether to capture stdout and stderr, by default True.
    text : bool, optional
        Whether to decode output as text, by default False.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the R script execution fails.

    Examples
    --------
    Run a script with vanilla mode:
        >>> run_r_script("script.R", ["--arg1", "value"], executor_flags=["--vanilla"])
    """
    command_args = (executor_flags or []) + [script_name] + args
    return run_command(
        "Rscript",
        command_args,
        function_name=function_name,
        capture_output=capture_output,
        text=text,
    )


def run_r_code(
    r_code: str,
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run inline R code and handle errors.

    This is a convenience wrapper around `run_r_script` for inline R code.
    Supports the pattern: Rscript {FLAGS} -e {CODE}

    Parameters
    ----------
    r_code : str
        The R code to execute.
    executor_flags : list[str] | None
        Flags to pass to the Rscript executable.
        For example: ["--vanilla", "--verbose"]
    function_name : str | None
        Name of the calling function for error messages.
    capture_output : bool, optional
        Whether to capture stdout and stderr, by default True.
    text : bool, optional
        Whether to decode output as text, by default False.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the R code execution fails.

    Examples
    --------
    Run R code with vanilla mode:
        >>> run_r_code("print('hello')", executor_flags=["--vanilla"])
    """
    flags_with_inline = (executor_flags or []) + ["-e"]
    return run_r_script(
        r_code,
        [],
        executor_flags=flags_with_inline,
        function_name=function_name,
        capture_output=capture_output,
        text=text,
    )


def run_julia_script(
    script_name: str,
    args: list[str],
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run a Julia script and handle errors.

    This is a convenience wrapper around `run_command` for Julia scripts.
    Supports the pattern:

    > julia {FLAGS} {SCRIPT} {ARGS}

    Parameters
    ----------
    script_name : str
        Name of the Julia script to run.
    args : list[str]
        Arguments to pass to the Julia script.
    executor_flags : list[str] | None
        Flags to pass to the julia executable before the script name.
        Common flags include:
        - ["--project=PATH"] or ["--project=@."] to specify the project environment
        - ["--threads=N"] or ["--threads=auto"] to set number of threads
        - ["--optimize=2"] to set optimization level
        - ["-O3", "--check-bounds=no"] for maximum performance
    function_name : str | None
        Name of the calling function for error messages. If None, uses script_name.
    capture_output : bool, optional
        Whether to capture stdout and stderr, by default True.
    text : bool, optional
        Whether to decode output as text, by default False.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the Julia script execution fails.
    """
    command_args = (
        (executor_flags or []) + [script_name] + args
    )  # use "truthy" to handle None
    return run_command(
        "julia",
        command_args,
        function_name=function_name,
        capture_output=capture_output,
        text=text,
    )


def run_julia_code(
    julia_code: str,
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run inline Julia code and handle errors.

    This is a convenience wrapper around `run_julia_script` for inline Julia code.
    Supports the pattern: julia {FLAGS} -e {CODE}

    Parameters
    ----------
    julia_code : str
        The Julia code to execute.
    executor_flags : list[str] | None
        Flags to pass to the julia executable.
        Common flags include:
        - ["--project=@."] to specify the project environment
        - ["--threads=N"] or ["--threads=auto"] to set number of threads
        - ["--optimize=2"] to set optimization level
    function_name : str | None
        Name of the calling function for error messages.
    capture_output : bool, optional
        Whether to capture stdout and stderr, by default True.
    text : bool, optional
        Whether to decode output as text, by default False.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the Julia code execution fails.
    """
    flags_with_inline = (executor_flags or []) + ["-e"]
    return run_julia_script(
        julia_code,
        [],
        executor_flags=flags_with_inline,
        function_name=function_name,
        capture_output=capture_output,
        text=text,
    )


def make_figures_from_model_fit_dir(
    model_fit_dir: Path,
    save_ci: bool = False,
    save_figs: bool = True,
) -> None:
    """Generate forecast figures from a model fit directory using R script.

    Parameters
    ----------
    model_fit_dir : Path
        Directory containing model fit data and output.
    save_ci : bool, optional
        Whether to save credible intervals to disk.
    save_figs : bool, optional
        Whether to save figures to disk.

    Returns
    -------
    None
    """
    args = [f"{model_fit_dir}"]
    if save_ci:
        args.append("--save-ci")
    if save_figs:
        args.append("--save-figs")

    run_r_script(
        "pipelines/make_figures_from_model_fit_dir.R",
        args,
        function_name="make_figures_from_model_fit_dir",
    )
    return None


def py_scalar_to_r_scalar(py_scalar):
    if py_scalar is None:
        return "NULL"
    return f"'{str(py_scalar)}'"


def create_hubverse_table(model_fit_path: Path) -> None:
    """Create hubverse table from model fit using R script.

    Parameters
    ----------
    model_fit_path : Path
        Path to the model fit directory.

    Returns
    -------
    None
    """
    run_r_code(
        f"""
            forecasttools::write_tabular(
            hewr::model_fit_dir_to_hub_q_tbl('{model_fit_path}'),
            fs::path('{model_fit_path}', "hubverse_table", ext = "parquet")
            )
            """,
        function_name="create_hubverse_table",
        text=True,
    )
    return None


def parse_model_batch_dir_name(model_batch_dir_name: str) -> dict:
    """
    Parse the name of a model batch directory,
    returning a dictionary of parsed values.

    Parameters
    ----------
    model_batch_dir_name
       Model batch directory name to parse.

    Returns
    -------
    dict
       A dictionary with keys 'disease', 'report_date',
       'first_training_date', and 'last_training_date'.
    """
    regex_match = re.match(r"(.+)_r_(.+)_f_(.+)_t_(.+)", model_batch_dir_name)
    if regex_match:
        disease, report_date, first_training_date, last_training_date = (
            regex_match.groups()
        )
    else:
        raise ValueError(
            f"Invalid model batch directory name format: {model_batch_dir_name}"
        )

    if disease not in disease_map_lower_:
        valid_diseases = ", ".join(disease_map_lower_.keys())
        raise ValueError(
            f"Unknown disease '{disease}' in model batch directory name. "
            f"Valid diseases are: {valid_diseases}"
        )

    return dict(
        disease=disease_map_lower_[disease],
        report_date=dt.datetime.strptime(report_date, "%Y-%m-%d").date(),
        first_training_date=dt.datetime.strptime(
            first_training_date, "%Y-%m-%d"
        ).date(),
        last_training_date=dt.datetime.strptime(last_training_date, "%Y-%m-%d").date(),
    )


def get_all_forecast_dirs(
    parent_dir: Path | str,
    diseases: str | list[str],
    report_date: str | dt.date = None,
) -> list[str]:
    """
    Get all the subdirectories within a parent directory
    that match the pattern for a forecast run for a
    given disease and optionally a given report date.

    Parameters
    ----------
    parent_dir
       Directory in which to look for forecast subdirectories.

    diseases
       Name of the diseases to match, as a list of strings,
       or a single disease as a string.

    Returns
    -------
    list[str]
        Names of matching directories, if any, otherwise an empty
        list.

    Raises
    ------
    ValueError
        Given an invalid ``report_date``.
    """
    diseases = ensure_listlike(diseases)

    if report_date is None:
        report_date_str = ""
    elif isinstance(report_date, str):
        report_date_str = report_date
    elif isinstance(report_date, dt.date):
        report_date_str = f"{report_date:%Y-%m-%d}"
    else:
        raise ValueError(
            "report_date must be one of None, "
            "a string in the format YYYY-MM-DD "
            "or a datetime.date instance. "
            f"Got {type(report_date)}."
        )
    valid_starts = tuple(
        [f"{disease.lower()}_r_{report_date_str}" for disease in diseases]
    )
    # by convention, disease names are
    # lowercase in directory patterns

    return [
        f.name
        for f in os.scandir(parent_dir)
        if f.is_dir() and f.name.startswith(valid_starts)
    ]


def build_pyrenew_hew_model_from_dir(
    model_dir: Path | str = None,
    prior_path: Path | str = None,
    model_params_path: Path | str = None,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
    fit_wastewater: bool = False,
):
    """
    Build a PyRenew HEW model from a directory or specified paths.

    Parameters
    ----------
    model_dir : Path | str, optional
        Directory containing priors.py and data/model_params.json.
        If provided, prior_path and model_params_path are ignored.
    prior_path : Path | str, optional
        Path to the priors.py file. Required if model_dir is not provided.
    model_params_path : Path | str, optional
        Path to the model_params.json file. Required if model_dir is not provided.
    fit_ed_visits : bool, optional
        Whether to fit ED visits data, by default False.
    fit_hospital_admissions : bool, optional
        Whether to fit hospital admissions data, by default False.
    fit_wastewater : bool, optional
        Whether to fit wastewater data, by default False.

    Returns
    -------
    model
        The built PyRenew HEW model.

    Raises
    ------
    ValueError
        If neither model_dir nor both prior_path and model_params_path are provided.
    """
    if model_dir is not None:
        prior_path = Path(model_dir) / "priors.py"
        model_params_path = Path(model_dir) / "data" / "model_params.json"
    else:
        if prior_path is None or model_params_path is None:
            raise ValueError(
                "Either model_dir must be provided, "
                "or both prior_path and model_params_path "
                "must be provided."
            )
        prior_path = Path(prior_path)
        model_params_path = Path(model_params_path)

    priors = runpy.run_path(str(prior_path))
    model_params = PyrenewHEWParam.from_json(model_params_path)
    my_model = build_pyrenew_hew_model(
        priors,
        model_params,
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
    )
    return my_model


def create_prop_samples(
    model_run_dir: Path | str,
    num_model_name: str,
    other_model_name: str,
    num_var_name: str = "observed_ed_visits",
    other_var_name: str = "other_ed_visits",
    prop_var_name: str = "prop_disease_ed_visits",
    augment_num_with_obs: bool = False,
    augment_other_with_obs: bool = True,
    aggregate_num: bool = False,
    aggregate_other: bool = False,
    save: bool = True,
) -> None:
    """Create proportion samples by calling the R script.

    Parameters
    ----------
    model_run_dir : Path | str
        Directory containing the model data and output.
    num_model_name : str
        Name of the model containing the numerator variable.
    other_model_name : str
        Name of the model containing the other variable.
    num_var_name : str, optional
        Name of the numerator variable, by default "observed_ed_visits".
    other_var_name : str, optional
        Name of the other variable, by default "other_ed_visits".
    prop_var_name : str, optional
        Name of the proportion variable, by default "prop_disease_ed_visits".
    augment_num_with_obs : bool, optional
        Whether to augment numerator samples with observations, by default False.
    augment_other_with_obs : bool, optional
        Whether to augment other samples with observations, by default True.
    aggregate_num : bool, optional
        Whether to aggregate numerator to epiweekly, by default False.
    aggregate_other : bool, optional
        Whether to aggregate other to epiweekly, by default False.
    save : bool, optional
        Whether to save the results, by default False.

    Returns
    -------
    None
    """
    args = [
        str(model_run_dir),
        "--num-model-name",
        num_model_name,
        "--other-model-name",
        other_model_name,
        "--num-var-name",
        num_var_name,
        "--other-var-name",
        other_var_name,
        "--prop-var-name",
        prop_var_name,
    ]
    if augment_num_with_obs:
        args.append("--augment-num-with-obs")
    if augment_other_with_obs:
        args.append("--augment-other-with-obs")
    if aggregate_num:
        args.append("--aggregate-num")
    if aggregate_other:
        args.append("--aggregate-other")
    if save:
        args.append("--save")

    run_r_script(
        "pipelines/create_prop_samples.R",
        args,
        function_name="create_prop_samples",
    )
