"""Utilities for running command-line tools."""

import subprocess


def run_command(
    executable: str,
    args: list[str],
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
    *,
    timeout: float | None = None,
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
    timeout : float | None, optional
        Maximum subprocess runtime in seconds. By default, wait indefinitely.

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result.

    Raises
    ------
    RuntimeError
        If the command execution fails, returns a non-zero exit code and captures `stderr`.
    subprocess.TimeoutExpired
        If the timeout expires, after killing and waiting for the child process.
    """
    command = [executable] + args

    result = subprocess.run(
        command,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )

    if result.returncode != 0:
        error_name = function_name or executable
        if not capture_output:
            raise RuntimeError(
                f"{error_name} failed with exit code {result.returncode}."
            )
        error_msg = result.stderr.decode("utf-8") if not text else result.stderr
        raise RuntimeError(f"{error_name}: {error_msg}")

    return result
