"""Unit tests for EpiAutoGP data preparation date exclusion functionality.

This module tests the Polars-based date exclusion filtering used in
prep_epiautogp_data._apply_date_exclusions(). It includes both direct tests
of the actual production implementation and cross-validation against a reference
list-based implementation to ensure correctness.
"""

import datetime as dt
import json
import logging
from unittest.mock import MagicMock

import polars as pl
import pytest

from pipelines.data.data_access import DataFreshness, ForecastData, NHSNData, NSSPData
from pipelines.epiautogp.epiautogp_forecast_utils import (
    ForecastPipelineContext,
    ForecastSpec,
    ModelPaths,
)
from pipelines.epiautogp.nowcast import NowcastData
from pipelines.epiautogp.prep_epiautogp_data import (
    _apply_date_exclusions,
    convert_to_epiautogp_json,
)

N_DAYS = 10


def filter_dates_by_exclusions_lists(
    dates: list[dt.date],
    reports: list[float],
    exclude_date_ranges: list[tuple[dt.date, dt.date]] | None,
) -> tuple[list[dt.date], list[float]]:
    """
    List-based date exclusion filtering (reference implementation).

    This is kept as a reference implementation to cross-validate the
    Polars-based approach.
    """
    if exclude_date_ranges is None or len(exclude_date_ranges) == 0:
        return dates, reports

    def is_date_excluded(date: dt.date) -> bool:
        return any(start <= date <= end for start, end in exclude_date_ranges)

    filtered_data = [
        (date, report)
        for date, report in zip(dates, reports)
        if not is_date_excluded(date)
    ]

    if not filtered_data:
        return [], []

    filtered_dates, filtered_reports = zip(*filtered_data)
    return list(filtered_dates), list(filtered_reports)


class TestDateExclusionFiltering:
    """Tests for Polars-based date exclusion filtering logic."""

    @pytest.fixture
    def test_year(self):
        """Year for test dates."""
        return 2024

    @pytest.fixture
    def test_month(self):
        """Month for test dates."""
        return 1

    @pytest.fixture
    def test_dates(self, test_year, test_month):
        """Individual test dates as a dictionary for easy reference."""
        return {
            f"day{i}": dt.date(test_year, test_month, i) for i in range(1, N_DAYS + 1)
        }

    @pytest.fixture
    def sample_dataframe(self, test_year, test_month):
        """
        Create a sample Polars DataFrame matching the structure used in prep_epiautogp_data.

        The DataFrame has:
        - date: Date column
        - value: Float column representing observations (e.g., ED visits, admissions)
        - geo_value: Location identifier
        - disease: Disease name
        """
        dates = [dt.date(test_year, test_month, i) for i in range(1, N_DAYS + 1)]
        values = [float(i * 10) for i in range(1, N_DAYS + 1)]

        return pl.DataFrame(
            {
                "date": dates,
                "value": values,
                "geo_value": ["CA"] * N_DAYS,
                "disease": ["COVID-19"] * N_DAYS,
            }
        )

    @pytest.fixture
    def sample_lists(self, test_year, test_month):
        """Create sample dates and reports as lists (for reference implementation)."""
        dates = [dt.date(test_year, test_month, i) for i in range(1, N_DAYS + 1)]
        reports = [float(i * 10) for i in range(1, N_DAYS + 1)]
        return dates, reports

    def test_no_exclusions(self, sample_dataframe):
        """Test filtering with no exclusions (empty list)."""
        # With no exclusions, DataFrame should remain unchanged
        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(sample_dataframe, [], mock_logger)

        assert filtered_df.height == N_DAYS
        assert filtered_df["date"].to_list() == sample_dataframe["date"].to_list()
        assert filtered_df["value"].to_list() == sample_dataframe["value"].to_list()

    def test_exclude_single_date(self, sample_dataframe, test_dates):
        """Test excluding a single date using production code."""
        # Exclude day 5
        exclude_ranges = [(test_dates["day5"], test_dates["day5"])]

        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(
            sample_dataframe, exclude_ranges, mock_logger
        )

        assert filtered_df.height == 9
        filtered_dates = filtered_df["date"].to_list()
        filtered_values = filtered_df["value"].to_list()

        assert test_dates["day5"] not in filtered_dates
        assert test_dates["day4"] in filtered_dates
        assert test_dates["day6"] in filtered_dates
        assert 50.0 not in filtered_values
        assert 40.0 in filtered_values
        assert 60.0 in filtered_values

    def test_exclude_date_range(self, sample_dataframe, test_dates):
        """Test excluding a range of dates using production code."""
        # Exclude days 3-5 (inclusive)
        exclude_ranges = [(test_dates["day3"], test_dates["day5"])]

        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(
            sample_dataframe, exclude_ranges, mock_logger
        )

        assert filtered_df.height == 7
        filtered_dates = filtered_df["date"].to_list()
        filtered_values = filtered_df["value"].to_list()

        assert test_dates["day3"] not in filtered_dates
        assert test_dates["day4"] not in filtered_dates
        assert test_dates["day5"] not in filtered_dates
        assert test_dates["day2"] in filtered_dates
        assert test_dates["day6"] in filtered_dates

        # Check values
        assert 30.0 not in filtered_values
        assert 40.0 not in filtered_values
        assert 50.0 not in filtered_values

    def test_exclude_multiple_ranges(self, sample_dataframe, test_dates):
        """Test excluding multiple date ranges using production code."""
        # Exclude days 2-3 and days 7-8
        exclude_ranges = [
            (test_dates["day2"], test_dates["day3"]),
            (test_dates["day7"], test_dates["day8"]),
        ]

        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(
            sample_dataframe, exclude_ranges, mock_logger
        )

        assert filtered_df.height == 6
        filtered_dates = filtered_df["date"].to_list()

        assert test_dates["day2"] not in filtered_dates
        assert test_dates["day3"] not in filtered_dates
        assert test_dates["day7"] not in filtered_dates
        assert test_dates["day8"] not in filtered_dates
        assert test_dates["day1"] in filtered_dates
        assert test_dates["day4"] in filtered_dates
        assert test_dates["day9"] in filtered_dates

    def test_exclude_dates_outside_range(self, sample_dataframe, test_year):
        """Test excluding dates that don't exist in the data."""
        # Exclude dates before the data range
        exclude_ranges = [
            (dt.date(test_year - 1, 12, 1), dt.date(test_year - 1, 12, 31))
        ]

        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(
            sample_dataframe, exclude_ranges, mock_logger
        )

        # All dates should still be present
        assert filtered_df.height == N_DAYS
        assert filtered_df["date"].to_list() == sample_dataframe["date"].to_list()

    def test_exclude_all_dates_raises_error(self, sample_dataframe, test_dates):
        """Test that excluding all dates raises ValueError (production behavior)."""
        # Exclude entire range
        exclude_ranges = [(test_dates["day1"], test_dates[f"day{N_DAYS}"])]

        mock_logger = MagicMock()
        # Production code raises ValueError when all dates are excluded
        with pytest.raises(ValueError, match="All dates were excluded"):
            _apply_date_exclusions(sample_dataframe, exclude_ranges, mock_logger)

    def test_polars_matches_list_implementation(
        self, sample_dataframe, sample_lists, test_dates
    ):
        """
        Cross-validation: Verify production Polars implementation matches list-based reference.

        This test ensures both approaches produce identical results.
        """
        # Test with multiple exclusion scenarios
        test_scenarios = [
            [],  # No exclusions
            [(test_dates["day5"], test_dates["day5"])],  # Single date
            [(test_dates["day3"], test_dates["day5"])],  # Range
            [
                (test_dates["day2"], test_dates["day3"]),
                (test_dates["day7"], test_dates["day8"]),
            ],  # Multiple ranges
        ]

        dates_list, reports_list = sample_lists
        mock_logger = MagicMock()

        for exclude_ranges in test_scenarios:
            # Apply Polars filtering (production code)
            if len(exclude_ranges) == 0:
                filtered_df = sample_dataframe
            else:
                filtered_df = _apply_date_exclusions(
                    sample_dataframe, exclude_ranges, mock_logger
                )
            polars_dates = filtered_df["date"].to_list()
            polars_values = filtered_df["value"].to_list()

            # Apply list-based filtering (reference implementation)
            list_dates, list_values = filter_dates_by_exclusions_lists(
                dates_list, reports_list, exclude_ranges if exclude_ranges else None
            )

            # Both should produce identical results
            assert polars_dates == list_dates, (
                f"Date mismatch for exclusions {exclude_ranges}: "
                f"Polars={polars_dates}, Lists={list_dates}"
            )
            assert polars_values == list_values, (
                f"Value mismatch for exclusions {exclude_ranges}: "
                f"Polars={polars_values}, Lists={list_values}"
            )

    def test_dataframe_preserves_other_columns(self, sample_dataframe, test_dates):
        """Test that filtering preserves other columns in the DataFrame."""
        exclude_ranges = [(test_dates["day5"], test_dates["day5"])]

        mock_logger = MagicMock()
        filtered_df = _apply_date_exclusions(
            sample_dataframe, exclude_ranges, mock_logger
        )

        # Check that other columns are preserved
        assert "geo_value" in filtered_df.columns
        assert "disease" in filtered_df.columns
        assert all(filtered_df["geo_value"] == "CA")
        assert all(filtered_df["disease"] == "COVID-19")


class FakeNowcastSource:
    """Small test nowcast source that records the model series it receives."""

    def __init__(self):
        self.dates = None
        self.reports = None

    def get_nowcast_data(
        self,
        *,
        dates: list[dt.date],
        reports: list[float],
    ) -> NowcastData:
        self.dates = dates
        self.reports = reports
        return NowcastData(dates=[dates[-1]], reports=[[reports[-1] + 1.0]])


def _write_combined_data(path):
    pl.DataFrame(
        [
            {
                "date": dt.date(2024, 1, 1),
                "geo_value": "CA",
                "disease": "COVID-19",
                "data_type": "train",
                ".variable": "observed_ed_visits",
                ".value": 10.0,
            },
            {
                "date": dt.date(2024, 1, 2),
                "geo_value": "CA",
                "disease": "COVID-19",
                "data_type": "train",
                ".variable": "observed_ed_visits",
                ".value": 20.0,
            },
        ]
    ).write_csv(path, separator="\t")


def _epiautogp_context(tmp_path, nowcast_source=None):
    report_date = dt.date(2024, 1, 3)
    forecast_data = ForecastData(
        report_date=report_date,
        nssp=NSSPData(
            data=pl.DataFrame(),
            freshness=DataFreshness(
                source="nssp",
                selected_version_date=report_date,
                latest_observed_date=None,
                run_date=report_date,
                is_stale=False,
                reason="Test NSSP data",
            ),
        ),
        nhsn=NHSNData(
            data=pl.DataFrame(),
            freshness=DataFreshness(
                source="nhsn",
                selected_version_date=report_date,
                latest_observed_date=None,
                run_date=report_date,
                is_stale=False,
                reason="Test NHSN data",
            ),
            prelim=False,
        ),
    )
    return ForecastPipelineContext(
        forecast_spec=ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 1, 3),
            target="nssp",
            frequency="daily",
            ed_visit_type="observed",
        ),
        model_name="test_model",
        first_training_date=dt.date(2024, 1, 1),
        last_training_date=dt.date(2024, 1, 2),
        n_forecast_days=28,
        exclude_last_n_days=0,
        exclude_date_ranges=None,
        model_batch_dir=tmp_path / "batch",
        model_run_dir=tmp_path / "batch" / "model_runs" / "CA",
        forecast_data=forecast_data,
        logger=logging.getLogger(),
        nowcast_source=nowcast_source,
    )


class TestConvertToEpiAutoGpJson:
    """Tests for EpiAutoGP JSON serialization."""

    def test_no_nowcast_source_serializes_empty_nowcast_arrays(self, tmp_path):
        """Test converter writes empty nowcast arrays without a source."""
        data_path = tmp_path / "combined_data.tsv"
        _write_combined_data(data_path)
        paths = ModelPaths(
            model_output_dir=tmp_path / "model",
            data_dir=tmp_path,
            training_data=data_path,
        )

        output_path = convert_to_epiautogp_json(
            context=_epiautogp_context(tmp_path),
            paths=paths,
        )

        output = json.loads(output_path.read_text())
        assert output["nowcast_dates"] == []
        assert output["nowcast_reports"] == []

    def test_nowcast_source_serializes_nowcast_data(self, tmp_path):
        """Test converter writes nowcast data supplied by the context source."""
        data_path = tmp_path / "combined_data.tsv"
        _write_combined_data(data_path)
        paths = ModelPaths(
            model_output_dir=tmp_path / "model",
            data_dir=tmp_path,
            training_data=data_path,
        )
        source = FakeNowcastSource()

        output_path = convert_to_epiautogp_json(
            context=_epiautogp_context(tmp_path, nowcast_source=source),
            paths=paths,
        )

        output = json.loads(output_path.read_text())
        assert source.dates == [dt.date(2024, 1, 1), dt.date(2024, 1, 2)]
        assert source.reports == [10.0, 20.0]
        assert output["nowcast_dates"] == ["2024-01-02"]
        assert output["nowcast_reports"] == [[21.0]]
