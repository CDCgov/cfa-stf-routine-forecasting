"""Unit tests for common utility and command-line argument functions"""

import argparse
import datetime as dt
import logging

import pytest

from pipelines.utils.cli_utils import (
    add_common_forecast_arguments,
    run_command,
)
from pipelines.utils.common_utils import (
    calculate_training_dates,
    get_available_reports,
    load_credentials,
    parse_exclude_date_ranges,
)


class TestValidationUtils:
    """Tests for validation and configuration utilities."""

    def test_load_credentials_with_none_returns_none(self):
        """Test that None credentials path returns None."""
        logger = logging.getLogger(__name__)

        result = load_credentials(None, logger)

        assert result is None

    def test_load_credentials_with_invalid_extension_raises_error(self, tmp_path):
        """Test that non-TOML file extension raises ValueError."""
        invalid_file = tmp_path / "credentials.txt"
        invalid_file.write_text("not a toml file")
        logger = logging.getLogger(__name__)

        with pytest.raises(ValueError, match="must have the extension '.toml'"):
            load_credentials(invalid_file, logger)

    @pytest.mark.parametrize(
        "n_training_days,exclude_last_n_days,expected_first,expected_last",
        [
            (90, 0, dt.date(2024, 9, 22), dt.date(2024, 12, 20)),
            (90, 5, dt.date(2024, 9, 17), dt.date(2024, 12, 15)),
        ],
    )
    def test_calculate_training_dates(
        self, n_training_days, exclude_last_n_days, expected_first, expected_last
    ):
        """Test training date calculation with various parameters."""
        report_date = dt.date(2024, 12, 21)
        logger = logging.getLogger(__name__)

        first_date, last_date = calculate_training_dates(
            report_date, n_training_days, exclude_last_n_days, logger
        )

        assert first_date == expected_first
        assert last_date == expected_last
        assert (last_date - first_date).days == n_training_days - 1

    @pytest.mark.parametrize(
        "input_str,expected",
        [
            (
                "2024-01-15:2024-01-20",
                [(dt.date(2024, 1, 15), dt.date(2024, 1, 20))],
            ),
            (
                "2024-01-15:2024-01-20,2024-03-01:2024-03-07",
                [
                    (dt.date(2024, 1, 15), dt.date(2024, 1, 20)),
                    (dt.date(2024, 3, 1), dt.date(2024, 3, 7)),
                ],
            ),
            (
                "2024-01-15",
                [(dt.date(2024, 1, 15), dt.date(2024, 1, 15))],
            ),
            (
                "2024-01-15:2024-01-15",
                [(dt.date(2024, 1, 15), dt.date(2024, 1, 15))],
            ),
            (
                "2024-01-15,2024-03-01:2024-03-07",
                [
                    (dt.date(2024, 1, 15), dt.date(2024, 1, 15)),
                    (dt.date(2024, 3, 1), dt.date(2024, 3, 7)),
                ],
            ),
            (None, None),
            ("", None),
            ("  ", None),
        ],
    )
    def test_parse_exclude_date_ranges_valid(self, input_str, expected):
        """Test parsing valid date range strings."""
        result = parse_exclude_date_ranges(input_str)
        assert result == expected

    @pytest.mark.parametrize(
        "input_str,error_match",
        [
            ("2024-01-15:2024-01-20:extra", "Invalid date range format"),
            (
                "2024-01-20:2024-01-15",
                "start_date.*must be before or equal to end_date",
            ),
            ("invalid:date", "Invalid date format"),
            ("not-a-date", "Invalid date format"),
        ],
    )
    def test_parse_exclude_date_ranges_invalid(self, input_str, error_match):
        """Test parsing invalid date range strings raises appropriate errors."""
        with pytest.raises(ValueError, match=error_match):
            parse_exclude_date_ranges(input_str)


class TestDataWranglingUtils:
    """Tests for data loading and processing utilities."""

    def test_get_available_reports_with_parquet_files(self, tmp_path):
        """Test discovering available report dates from parquet files."""
        (tmp_path / "2024-12-01.parquet").touch()
        (tmp_path / "2024-12-15.parquet").touch()
        (tmp_path / "2024-12-20.parquet").touch()

        result = get_available_reports(tmp_path)

        assert len(result) == 3
        assert dt.date(2024, 12, 1) in result
        assert dt.date(2024, 12, 15) in result
        assert dt.date(2024, 12, 20) in result


class TestCLIUtils:
    """Tests for CLI argument parsing utilities."""

    def test_add_common_forecast_arguments_smoke_test(self):
        """Smoke test that common arguments are added without errors."""
        parser = argparse.ArgumentParser()

        add_common_forecast_arguments(parser)

        # Parse with minimal required arguments to verify they exist
        args = parser.parse_args(
            [
                "--disease",
                "COVID-19",
                "--loc",
                "CA",
            ]
        )

        assert args.disease == "COVID-19"
        assert args.loc == "CA"
        assert args.n_training_days == 180  # default value
        assert args.n_forecast_days == 28  # default value
        assert args.exclude_last_n_days == 0  # default value

    def test_run_command_with_python_echo(self):
        """Smoke test run_command with simple Python echo."""
        result = run_command(
            "python",
            ["-c", "print('hello from python')"],
            text=True,
        )

        assert result.returncode == 0
        assert "hello from python" in result.stdout

    def test_run_command_inline_code_failure_raises_runtime_error(self):
        """Test that failed inline code raises RuntimeError."""
        with pytest.raises(RuntimeError):
            run_command(
                "python",
                ["-c", "import sys; sys.exit(1)"],
                text=True,
            )

    def test_run_command_with_executor_flags_python(self, tmp_path):
        """Test run_command with Python executor flags like -O for optimize."""
        # Create a simple Python script that checks if __debug__ is False (optimization on)
        # and therefore the executor flag worked.
        script = tmp_path / "test_optimize.py"
        script.write_text(
            "import sys; print('optimized' if not __debug__ else 'debug')"
        )

        # Run without optimization
        result_debug = run_command(
            "python",
            [str(script)],
            text=True,
        )
        assert result_debug.returncode == 0
        assert "debug" in result_debug.stdout

        # Run with -O flag (optimize)
        result_optimized = run_command(
            "python",
            ["-O", str(script)],
            text=True,
        )
        assert result_optimized.returncode == 0
        assert "optimized" in result_optimized.stdout
