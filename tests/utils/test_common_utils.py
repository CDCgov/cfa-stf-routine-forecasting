"""Unit tests for shared utility functions."""

import datetime as dt
import logging
import sys

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from cfa.stf.routine.utils import language_utils
from cfa.stf.routine.utils.cli_utils import run_command
from cfa.stf.routine.utils.date_utils import (
    calculate_training_dates,
    parse_exclude_date_ranges,
)
from cfa.stf.routine.utils.language_utils import run_julia_script, run_r_script
from cfa.stf.routine.utils.prop_utils import append_prop_data_to_combined_data


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


class TestDataWranglingUtils:
    """Tests for data loading and processing utilities."""

    def test_append_prop_data_to_combined_data_skips_non_nssp_data(self):
        original = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "location": ["US"],
                ".variable": ["observed_hospital_admissions"],
                ".value": [5],
            }
        )
        result = append_prop_data_to_combined_data(original)
        assert_frame_equal(result, original)

    @pytest.mark.parametrize(
        "present_var",
        ["observed_ed_visits", "other_ed_visits"],
    )
    def test_append_prop_data_to_combined_data_rejects_incomplete_nssp(
        self,
        present_var,
    ):
        data = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "location": ["US"],
                ".variable": [present_var],
                ".value": [5],
            }
        )

        with pytest.raises(ValueError, match="incomplete NSSP data"):
            append_prop_data_to_combined_data(data)

    def test_append_prop_data_to_combined_data_appends_proportions(self):
        data = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "location": ["US", "US"],
                ".variable": ["observed_ed_visits", "other_ed_visits"],
                ".value": [2, 8],
            }
        )

        result = append_prop_data_to_combined_data(data)
        expected = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "location": ["US", "US", "US"],
                ".variable": [
                    "observed_ed_visits",
                    "other_ed_visits",
                    "prop_disease_ed_visits",
                ],
                ".value": [2.0, 8.0, 0.2],
            }
        )
        assert_frame_equal(result, expected)

    def test_append_prop_data_to_combined_data_allows_variable_names(self):
        data = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "location": ["US", "US"],
                ".variable": ["num_visits", "denom_other_visits"],
                ".value": [3, 7],
            }
        )

        result = append_prop_data_to_combined_data(
            data,
            observed_var="num_visits",
            other_var="denom_other_visits",
            prop_var="prop_num_visits",
        )

        expected = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "location": ["US", "US", "US"],
                ".variable": [
                    "denom_other_visits",
                    "num_visits",
                    "prop_num_visits",
                ],
                ".value": [7.0, 3.0, 0.3],
            }
        )
        assert_frame_equal(result, expected)


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
