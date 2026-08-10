"""Compatibility entrypoint for tooling that expects definitions at repo root."""

from cfa_dagster import start_dev_env

start_dev_env(__name__)

from cfa.stf.routine.dagster_defs import defs  # noqa: E402

__all__ = ["defs"]
