"""Unit tests for forecast pipeline utility functions.

These tests use mocking via `@patch` decorators to isolate the units under test.
We mock these dependencies to:

1. Test the logic and control flow without side effects
2. Avoid requiring actual data files or external services
3. Make tests fast and deterministic
4. Focus on verifying correct function calls and parameter passing

Each test mocks the minimum dependencies needed for that specific test case.

The end-to-end functionality of the forecast pipeline is verified in a separate
shell-based integration test.
"""

import datetime as dt
import logging
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from pipelines.epiautogp.epiautogp_forecast_utils import (
    ForecastPipelineContext,
    ModelPaths,
    setup_forecast_pipeline,
)


@pytest.fixture
def base_context(tmp_path):
    """
    Fixture providing a ForecastPipelineContext with default test values.

    Tests can use this directly or override specific fields as needed.
    """
    return ForecastPipelineContext(
        disease="COVID-19",
        loc="CA",
        target="nssp",
        frequency="epiweekly",
        ed_visit_type="observed",
        model_name="test_model",
        nhsn_data_path=None,
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 20),
        n_forecast_days=28,
        exclude_last_n_days=0,
        exclude_date_ranges=None,
        model_batch_dir=tmp_path / "batch",
        model_run_dir=tmp_path / "batch" / "model_runs" / "CA",
        credentials_dict={},
        facility_level_nssp_data=pl.LazyFrame(),
        logger=logging.getLogger(),
    )


class TestForecastPipelineContext:
    """Tests for the ForecastPipelineContext dataclass."""

    def test_context_initialization(self):
        """Test that ForecastPipelineContext can be initialized with all fields."""
        context = ForecastPipelineContext(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            nhsn_data_path=None,
            report_date=dt.date(2024, 12, 20),
            first_training_date=dt.date(2024, 9, 22),
            last_training_date=dt.date(2024, 12, 20),
            n_forecast_days=28,
            exclude_last_n_days=0,
            exclude_date_ranges=None,
            model_batch_dir=Path("/output/batch"),
            model_run_dir=Path("/output/batch/model_runs/CA"),
            credentials_dict={"key": "value"},
            facility_level_nssp_data=pl.LazyFrame(),
            logger=logging.getLogger(),
        )

        assert context.disease == "COVID-19"
        assert context.loc == "CA"
        assert context.n_forecast_days == 28
        assert context.exclude_last_n_days == 0
        assert context.exclude_date_ranges is None


class TestModelPaths:
    """Tests for the ModelPaths dataclass."""

    def test_paths_initialization(self):
        """Test that ModelPaths can be initialized with all fields."""
        paths = ModelPaths(
            model_output_dir=Path("/output/model"),
            data_dir=Path("/output/model/data"),
            training_data=Path("/output/model/data/combined_data.tsv"),
        )

        assert paths.model_output_dir == Path("/output/model")
        assert paths.data_dir == Path("/output/model/data")
        assert paths.training_data.name == "combined_data.tsv"


class TestSetupForecastPipeline:
    """Tests for the setup_forecast_pipeline function."""

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.pl.scan_parquet")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_credentials")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.get_available_reports")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_setup_pipeline_returns_context(
        self,
        mock_calc_dates,
        mock_get_reports,
        mock_load_creds,
        mock_scan_parquet,
        tmp_path,
    ):
        """Test that setup_forecast_pipeline returns a properly configured context."""
        # Setup mocks
        mock_get_reports.return_value = [dt.date(2024, 12, 20)]
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_scan_parquet.return_value = pl.LazyFrame()

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            nhsn_data_path=None,
            facility_level_nssp_data_dir=tmp_path,
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            exclude_last_n_days=0,
            credentials_path=None,
            logger=None,
        )

        assert isinstance(context, ForecastPipelineContext)

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.pl.scan_parquet")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_credentials")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.get_available_reports")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_setup_pipeline_creates_directory_structure(
        self,
        mock_calc_dates,
        mock_get_reports,
        mock_load_creds,
        mock_scan_parquet,
        tmp_path,
    ):
        """Test that setup creates the expected directory structure."""
        mock_load_creds.return_value = {}
        mock_get_reports.return_value = [dt.date(2024, 12, 20)]
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_scan_parquet.return_value = pl.LazyFrame()

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            nhsn_data_path=None,
            facility_level_nssp_data_dir=tmp_path,
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
        )

        expected_batch_dir = (
            tmp_path / "covid-19_r_2024-12-20_f_2024-09-22_t_2024-12-20"
        )
        expected_run_dir = expected_batch_dir / "model_runs" / "CA"

        assert context.model_batch_dir == expected_batch_dir
        assert context.model_run_dir == expected_run_dir


class TestPrepareModelData:
    """Tests for the prepare_model_data function."""

    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data")
    def test_prepare_model_data_returns_paths(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,  # Fixture is injected here
    ):
        """Test that prepare_model_data returns ModelPaths."""
        # Use the fixture directly
        paths = base_context.prepare_model_data()

        assert isinstance(paths, ModelPaths)
        assert paths.model_output_dir.name == "test_model"
        assert paths.data_dir.name == "data"
        assert paths.training_data.name == "combined_data.tsv"

    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data")
    def test_prepare_model_data_creates_directories(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,  # Use fixture
    ):
        """Test that prepare_model_data creates the required directories."""
        # Use the fixture directly
        paths = base_context.prepare_model_data()

        assert paths.model_output_dir.exists()
        assert paths.data_dir.exists()

    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data")
    def test_prepare_model_data_passes_nhsn_data_path(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,  # Use fixture
        tmp_path,
    ):
        """Test that prepare_model_data passes nhsn_data_path to data functions."""
        nhsn_path = tmp_path / "nhsn_data.parquet"

        # Override just the fields we need for this test
        context = replace(
            base_context,
            target="nhsn",
            nhsn_data_path=nhsn_path,
        )

        _ = context.prepare_model_data()

        # Verify nhsn_data_path was passed to process_and_save_loc_data
        mock_process_loc.assert_called_once()
        assert mock_process_loc.call_args[1]["nhsn_data_path"] == nhsn_path

    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data")
    def test_prepare_model_data_with_nhsn_target(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,  # Use fixture
        tmp_path,
    ):
        """Test prepare_model_data with NHSN target and data."""
        nhsn_path = tmp_path / "nhsn_hospital_admissions.parquet"

        # Override multiple fields for NHSN test
        context = replace(
            base_context,
            target="nhsn",
            model_name="epiautogp_nhsn_epiweekly",
            nhsn_data_path=nhsn_path,
            credentials_dict={"key": "value"},
        )

        paths = context.prepare_model_data()

        # Verify the method returns valid paths
        assert isinstance(paths, ModelPaths)
        assert paths.model_output_dir.name == "epiautogp_nhsn_epiweekly"
        assert paths.data_dir.name == "data"


class TestPostprocessForecast:
    """Tests for the postprocess_forecast function."""

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.create_hubverse_table")
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.make_figures_from_model_fit_dir"
    )
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.create_samples_from_epiautogp_fit_dir"
    )
    def test_postprocess_calls_required_functions(
        self,
        mock_create_samples,
        mock_make_figures,
        mock_hubverse,
        base_context,  # Use fixture
    ):
        """Test that post_process_forecast calls all required functions."""
        # Override exclude_last_n_days for this test
        context = replace(
            base_context,
            exclude_last_n_days=5,
        )

        context.post_process_forecast()

        # Verify all functions were called
        mock_create_samples.assert_called_once()
        mock_make_figures.assert_called_once()
        mock_hubverse.assert_called_once()

        # Verify correct arguments to create_samples_from_epiautogp_fit_dir
        expected_model_fit_dir = context.model_run_dir / context.model_name
        assert (
            mock_create_samples.call_args[1]["model_fit_dir"] == expected_model_fit_dir
        )

        # Verify correct arguments to make_figures_from_model_fit_dir
        assert mock_make_figures.call_args[1]["model_fit_dir"] == expected_model_fit_dir
        assert mock_make_figures.call_args[1]["save_figs"] is True
        assert mock_make_figures.call_args[1]["save_ci"] is True

        # Verify create_hubverse_table was called with model_run_dir
        assert mock_hubverse.call_args[0][0] == context.model_run_dir

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.create_hubverse_table")
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.make_figures_from_model_fit_dir"
    )
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.create_samples_from_epiautogp_fit_dir"
    )
    def test_postprocess_creates_correct_paths(
        self,
        mock_create_samples,
        mock_make_figures,
        mock_hubverse,
        base_context,  # Use fixture
    ):
        """Test that post_process_forecast creates correct model_fit_dir path."""
        context = base_context

        context.post_process_forecast()

        # Verify model_fit_dir is correctly constructed as model_run_dir/model_name
        expected_model_fit_dir = context.model_run_dir / context.model_name
        assert (
            mock_create_samples.call_args[1]["model_fit_dir"] == expected_model_fit_dir
        )
        assert mock_make_figures.call_args[1]["model_fit_dir"] == expected_model_fit_dir
