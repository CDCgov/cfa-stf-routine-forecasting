from pathlib import Path


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
