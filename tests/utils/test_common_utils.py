"""Unit tests for shared utility functions."""

import datetime as dt
import logging
import sys

import pytest

from cfa.stf.routine.utils import language_utils
from cfa.stf.routine.utils.cli_utils import run_command
from cfa.stf.routine.utils.date_utils import (
    calculate_training_dates,
    parse_exclude_date_ranges,
)
from cfa.stf.routine.utils.language_utils import run_julia_script, run_r_script


class TestValidationUtils:
    """Tests for validation and configuration utilities."""

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


class TestCLIUtils:
    """Tests for command-line utilities."""

    @pytest.mark.parametrize(
        ("runner", "expected_executable"),
        [(run_r_script, "Rscript"), (run_julia_script, "julia")],
    )
    def test_script_runners_capture_output_by_default(
        self,
        monkeypatch,
        runner,
        expected_executable,
    ):
        calls = []
        sentinel = object()

        def fake_run_command(executable, args, **kwargs):
            calls.append((executable, args, kwargs))
            return sentinel

        monkeypatch.setattr(language_utils, "run_command", fake_run_command)

        result = runner("script", ["arg"])

        assert result is sentinel
        assert calls == [
            (
                expected_executable,
                ["script", "arg"],
                {
                    "function_name": None,
                    "capture_output": True,
                    "text": False,
                },
            )
        ]

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

    def test_run_command_can_inherit_output_streams(self, capfd):
        result = run_command(
            sys.executable,
            [
                "-c",
                "import sys; print('child out'); print('child err', file=sys.stderr)",
            ],
            capture_output=False,
        )

        captured = capfd.readouterr()
        assert result.returncode == 0
        assert "child out" in captured.out
        assert "child err" in captured.err

    def test_run_command_without_capture_reports_exit_code(self):
        with pytest.raises(RuntimeError, match="failed with exit code 2"):
            run_command(
                sys.executable,
                ["-c", "import sys; sys.exit(2)"],
                capture_output=False,
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
