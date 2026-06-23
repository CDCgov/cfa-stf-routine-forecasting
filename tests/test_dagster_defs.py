"""Smoke tests for the Dagster definitions module."""

import importlib
import warnings

import dagster as dg


def test_dagster_defs_are_loadable(monkeypatch):
    """Verify Dagster can load and validate the repository definitions."""
    monkeypatch.setenv("DAGSTER_USER", "test-user")
    monkeypatch.setenv("DAGSTER_IS_DEV_CLI", "1")
    monkeypatch.setenv("DAGSTER_IS_DEFS_VALIDATION_CLI", "1")

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Parameter `use_user_code_server`.*",
        )
        dagster_defs = importlib.import_module("dagster_defs")

    dg.Definitions.validate_loadable(dagster_defs.defs)
