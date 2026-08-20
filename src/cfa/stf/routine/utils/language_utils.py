"""Convenience wrappers for R and Julia command execution."""

import subprocess
from pathlib import Path

from cfa.stf.routine.utils.cli_utils import run_command


def run_r_script(
    script_name: str | Path,
    args: list[str],
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """Run an R script and handle command failures."""
    command_args = (executor_flags or []) + [str(script_name)] + args
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
    """Run inline R code and handle command failures."""
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
    script_name: str | Path,
    args: list[str],
    executor_flags: list[str] | None = None,
    function_name: str | None = None,
    capture_output: bool = True,
    text: bool = False,
) -> subprocess.CompletedProcess:
    """Run a Julia script and handle command failures."""
    command_args = (executor_flags or []) + [str(script_name)] + args
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
    """Run inline Julia code and handle command failures."""
    flags_with_inline = (executor_flags or []) + ["-e"]
    return run_julia_script(
        julia_code,
        [],
        executor_flags=flags_with_inline,
        function_name=function_name,
        capture_output=capture_output,
        text=text,
    )
