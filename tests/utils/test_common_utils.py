"""Unit tests for shared utility functions."""

import datetime as dt
import sys

import pytest

from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.utils import language_utils
from cfa.stf.routine.utils.cli_utils import run_command
from cfa.stf.routine.utils.date_utils import parse_exclude_date_ranges
from cfa.stf.routine.utils.directory_utils import (
    get_all_forecast_dirs,
    parse_model_batch_dir_name,
)
from cfa.stf.routine.utils.language_utils import run_julia_script, run_r_script


class TestValidationUtils:
    """Tests for validation and configuration utilities."""

    @pytest.mark.parametrize(
        (
            "n_lookback_days",
            "exclude_last_n_days",
            "expected_min_allowed",
            "expected_max_allowed",
        ),
        [
            (90, 0, dt.date(2024, 9, 22), dt.date(2024, 12, 20)),
            (90, 5, dt.date(2024, 9, 22), dt.date(2024, 12, 15)),
        ],
    )
    def test_forecast_window_boundaries(
        self,
        n_lookback_days,
        exclude_last_n_days,
        expected_min_allowed,
        expected_max_allowed,
    ):
        """Test training date calculation with various parameters."""
        report_date = dt.date(2024, 12, 21)
        window = ForecastWindow(
            report_date=report_date,
            n_lookback_days=n_lookback_days,
            exclude_last_n_days=exclude_last_n_days,
        )

        assert window.min_allowed_training_date == expected_min_allowed
        assert window.max_allowed_training_date == expected_max_allowed
        assert (report_date - window.min_allowed_training_date).days == (
            n_lookback_days
        )

    @pytest.mark.parametrize(
        ("n_lookback_days", "exclude_last_n_days", "message"),
        [
            (0, 0, "n_lookback_days must be positive"),
            (7, -1, "exclude_last_n_days must be nonnegative"),
            (7, 7, "exclude_last_n_days must be less than n_lookback_days"),
        ],
    )
    def test_forecast_window_rejects_invalid_configuration(
        self, n_lookback_days, exclude_last_n_days, message
    ):
        with pytest.raises(ValueError, match=message):
            ForecastWindow(
                report_date=dt.date(2024, 12, 21),
                n_lookback_days=n_lookback_days,
                exclude_last_n_days=exclude_last_n_days,
            )

    def test_forecast_window_owns_horizon_and_batch_name(self):
        window = ForecastWindow(
            report_date=dt.date(2026, 9, 8),
            n_lookback_days=150,
            exclude_last_n_days=3,
        )

        assert window.forecast_through == dt.date(2026, 10, 3)
        assert window.n_forecast_days_after(dt.date(2026, 8, 29)) == 35
        assert window.model_batch_dir_name("covid") == ("covid_lookback-150_omit-3")

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


class TestDirectoryUtils:
    def test_model_batch_directory_round_trip(self):
        window = ForecastWindow(
            report_date=dt.date(2026, 9, 8),
            n_lookback_days=150,
            exclude_last_n_days=3,
        )
        name = window.model_batch_dir_name("covid")

        assert name == "covid_lookback-150_omit-3"
        assert parse_model_batch_dir_name(name) == {
            "disease": "covid",
            "n_lookback_days": 150,
            "exclude_last_n_days": 3,
        }

    def test_get_all_forecast_dirs_matches_new_batch_prefix(self, tmp_path):
        (tmp_path / "covid_lookback-150_omit-1").mkdir()
        (tmp_path / "flu_lookback-90_omit-3").mkdir()
        (tmp_path / "covid_r_2026-09-02_f_2026-04-04_t_2026-08-31").mkdir()

        assert get_all_forecast_dirs(tmp_path, ["covid", "rsv"]) == [
            "covid_lookback-150_omit-1"
        ]


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
