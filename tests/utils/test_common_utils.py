"""Unit tests for common utility functions."""

import datetime as dt
import logging
import sys

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from cfa.stf.routine.utils import common_utils
from cfa.stf.routine.utils.cli_utils import run_command
from cfa.stf.routine.utils.common_utils import (
    append_prop_data_to_combined_data,
    calculate_training_dates,
    create_prop_samples,
    parse_exclude_date_ranges,
    run_julia_script,
    run_r_script,
)


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

    def test_append_prop_data_to_combined_data_skips_non_nssp_data(self, tmp_path):
        data_path = tmp_path / "combined_data.tsv"
        original = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "location": ["US"],
                ".variable": ["observed_hospital_admissions"],
                ".value": [5],
            }
        )
        original.write_csv(data_path, separator="\t")

        append_prop_data_to_combined_data(data_path)

        result = pl.read_csv(data_path, separator="\t")
        assert_frame_equal(result, original)

    @pytest.mark.parametrize(
        "present_var",
        ["observed_ed_visits", "other_ed_visits"],
    )
    def test_append_prop_data_to_combined_data_rejects_incomplete_nssp(
        self,
        tmp_path,
        present_var,
    ):
        data_path = tmp_path / "combined_data.tsv"
        pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "location": ["US"],
                ".variable": [present_var],
                ".value": [5],
            }
        ).write_csv(data_path, separator="\t")

        with pytest.raises(ValueError, match="incomplete NSSP data"):
            append_prop_data_to_combined_data(data_path)

    def test_append_prop_data_to_combined_data_updates_tsv(self, tmp_path):
        data_path = tmp_path / "combined_data.tsv"
        pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "location": ["US", "US"],
                ".variable": ["observed_ed_visits", "other_ed_visits"],
                ".value": [2, 8],
            }
        ).write_csv(data_path, separator="\t")

        append_prop_data_to_combined_data(data_path)

        result = pl.read_csv(data_path, separator="\t")
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

    def test_append_prop_data_to_combined_data_allows_variable_names(self, tmp_path):
        data_path = tmp_path / "combined_data.tsv"
        pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "location": ["US", "US"],
                ".variable": ["num_visits", "denom_other_visits"],
                ".value": [3, 7],
            }
        ).write_csv(data_path, separator="\t")

        append_prop_data_to_combined_data(
            data_path,
            observed_var="num_visits",
            other_var="denom_other_visits",
            prop_var="prop_num_visits",
        )

        result = pl.read_csv(data_path, separator="\t")
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

    def test_create_prop_samples_aggregates_with_python_forecasttools(self, tmp_path):
        num_model_dir = tmp_path / "num_model"
        other_model_dir = tmp_path / "other_model"
        (num_model_dir / "data").mkdir(parents=True)
        (other_model_dir / "data").mkdir(parents=True)

        daily_dates = [
            dt.date(2025, 1, 5) + dt.timedelta(days=day) for day in range(14)
        ]
        num_samples = pl.DataFrame(
            {
                "date": daily_dates,
                "geo_value": ["CA"] * 14,
                "disease": ["flu"] * 14,
                ".variable": ["observed_ed_visits"] * 14,
                ".value": list(range(1, 15)),
                ".draw": [1] * 14,
                ".chain": [1] * 14,
                ".iteration": list(range(1, 15)),
                "resolution": ["daily"] * 14,
                "data_type": ["forecast"] * 14,
            }
        )
        num_data = num_samples.drop(".draw", ".chain", ".iteration").with_columns(
            pl.lit("train").alias("data_type")
        )
        weekly_dates = [dt.date(2025, 1, 11), dt.date(2025, 1, 18)]
        other_samples = pl.DataFrame(
            {
                "date": weekly_dates,
                "geo_value": ["CA"] * 2,
                "disease": ["flu"] * 2,
                ".variable": ["other_ed_visits"] * 2,
                ".value": [63, 70],
                ".draw": [1, 1],
                "resolution": ["epiweekly"] * 2,
                "data_type": ["forecast"] * 2,
            }
        )
        other_data = other_samples.drop(".draw").with_columns(
            pl.lit("train").alias("data_type")
        )
        num_samples.write_parquet(num_model_dir / "samples.parquet")
        other_samples.write_parquet(other_model_dir / "samples.parquet")
        num_data.write_csv(num_model_dir / "data" / "combined_data.tsv", separator="\t")
        other_data.write_csv(
            other_model_dir / "data" / "combined_data.tsv", separator="\t"
        )

        create_prop_samples(
            model_run_dir=tmp_path,
            num_model_name="num_model",
            other_model_name="other_model",
            aggregate_num=True,
            augment_other_with_obs=False,
        )

        output_dir = tmp_path / "prop_epiweekly_aggregated_num_model_other_model"
        samples = pl.read_parquet(output_dir / "samples.parquet").sort("date")
        data = pl.read_csv(
            output_dir / "data" / "combined_data.tsv",
            separator="\t",
            try_parse_dates=True,
        ).sort("date")
        expected_values = [28 / (28 + 63), 77 / (77 + 70)]
        assert samples.get_column("date").to_list() == weekly_dates
        assert samples.get_column("resolution").unique().to_list() == ["epiweekly"]
        assert samples.get_column(".variable").unique().to_list() == [
            "prop_disease_ed_visits"
        ]
        assert samples.get_column(".value").to_list() == pytest.approx(expected_values)
        assert ".chain" not in samples.columns
        assert ".iteration" not in samples.columns
        assert data.get_column(".value").to_list() == pytest.approx(expected_values)


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

        monkeypatch.setattr(common_utils, "run_command", fake_run_command)

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
