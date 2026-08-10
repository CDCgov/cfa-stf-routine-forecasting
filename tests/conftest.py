import os
import shutil
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--e2e-output-dir",
        type=Path,
        default=None,
        help="Directory where the pipeline end-to-end test workspace is retained.",
    )
    parser.addoption(
        "--e2e-force",
        action="store_true",
        default=False,
        help="Remove an existing --e2e-output-dir before running.",
    )
    parser.addoption(
        "--e2e-data-mode",
        choices=("auto", "real", "mock"),
        default="auto",
        help=(
            "Data source for pipeline end-to-end tests. "
            "'auto' uses real DataOps data in the external CFA environment "
            "and mocked data otherwise."
        ),
    )
    parser.addoption(
        "--model-test-location",
        default="CA",
        help="Location abbreviation for single-model integration tests.",
    )
    parser.addoption(
        "--model-test-disease",
        default="COVID-19",
        help="Disease for single-model integration tests.",
    )


@pytest.fixture
def pipeline_workspace(request, tmp_path, monkeypatch):
    retained_dir = request.config.getoption("--e2e-output-dir")
    force = request.config.getoption("--e2e-force")

    if retained_dir is None:
        workspace = tmp_path / "pipeline-e2e"
    else:
        workspace = Path(retained_dir)
        if workspace.exists():
            if not force:
                pytest.fail(
                    f"Retained workspace already exists: {workspace}. "
                    "Pass --e2e-force to remove it before running."
                )
            shutil.rmtree(workspace)

    workspace.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv(
        "XLA_FLAGS",
        os.environ.get("XLA_FLAGS", "--xla_force_host_platform_device_count=2"),
    )
    return workspace
