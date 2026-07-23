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

from pipelines.data.data_access import DataFreshness, ForecastData
from pipelines.epiautogp.epiautogp_forecast_utils import (
    ForecastPipelineContext,
    ForecastSpec,
    ModelPaths,
    _resolve_nowcast_source,
    setup_forecast_pipeline,
)
from pipelines.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast


def _forecast_data(report_date: dt.date = dt.date(2024, 12, 20)) -> ForecastData:
    nssp_data = pl.DataFrame(
        {
            "date": [dt.date(2024, 12, 20), dt.date(2024, 12, 20)],
            "geo_value": ["CA", "CA"],
            "disease": ["COVID-19", "Total"],
            "ed_visits": [10, 100],
        }
    )
    nhsn_data = pl.DataFrame(
        {
            "weekendingdate": [dt.date(2024, 12, 14)],
            "jurisdiction": ["CA"],
            "disease": ["COVID-19"],
            "hospital_admissions": [5],
        }
    )
    return ForecastData.from_source_frames(
        loc_abb="CA",
        disease="COVID-19",
        report_date=report_date,
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=report_date,
        nssp_data=nssp_data,
        nssp_freshness=DataFreshness(
            source="nssp",
            selected_version_date=report_date,
            latest_observed_date=nssp_data.get_column("date").max(),
            run_date=report_date,
            is_stale=False,
            reason="Test NSSP data",
        ),
        nhsn_data=nhsn_data,
        nhsn_freshness=DataFreshness(
            source="nhsn",
            selected_version_date=report_date,
            latest_observed_date=nhsn_data.get_column("weekendingdate").max(),
            run_date=report_date,
            is_stale=False,
            reason="Test NHSN data",
        ),
        nhsn_prelim=False,
        loc_pop=1,
    )


@pytest.fixture
def base_context(tmp_path):
    """
    Fixture providing a ForecastPipelineContext with default test values.

    Tests can use this directly or override specific fields as needed.
    """
    return ForecastPipelineContext(
        forecast_spec=ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
        ),
        model_name="test_model",
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 20),
        n_forecast_days=28,
        exclude_last_n_days=0,
        exclude_date_ranges=None,
        model_batch_dir=tmp_path / "batch",
        model_run_dir=tmp_path / "batch" / "model_runs" / "CA",
        forecast_data=_forecast_data(),
        logger=logging.getLogger(),
    )


class TestForecastPipelineContext:
    """Tests for the ForecastPipelineContext dataclass."""

    def test_context_initialization(self):
        """Test that ForecastPipelineContext can be initialized with all fields."""
        context = ForecastPipelineContext(
            forecast_spec=ForecastSpec(
                disease="COVID-19",
                loc="CA",
                report_date=dt.date(2024, 12, 20),
                target="nssp",
                frequency="epiweekly",
                ed_visit_type="observed",
            ),
            model_name="test_model",
            first_training_date=dt.date(2024, 9, 22),
            last_training_date=dt.date(2024, 12, 20),
            n_forecast_days=28,
            exclude_last_n_days=0,
            exclude_date_ranges=None,
            model_batch_dir=Path("/output/batch"),
            model_run_dir=Path("/output/batch/model_runs/CA"),
            forecast_data=_forecast_data(),
            logger=logging.getLogger(),
        )

        assert context.forecast_spec.disease == "COVID-19"
        assert context.forecast_spec.loc == "CA"
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

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_setup_pipeline_returns_context(
        self,
        mock_calc_dates,
        mock_load_data,
        tmp_path,
    ):
        """Test that setup_forecast_pipeline returns a properly configured context."""
        # Setup mocks
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = _forecast_data()

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            run_date=dt.date(2024, 12, 20),
            exclude_last_n_days=0,
            logger=None,
            nowcast_source_name="none",
        )

        assert isinstance(context, ForecastPipelineContext)
        assert mock_calc_dates.call_args.args[0] == dt.date(2024, 12, 20)
        assert mock_load_data.call_args.kwargs["run_date"] == dt.date(
            2024, 12, 20
        )
        assert "report_date" not in mock_load_data.call_args.kwargs


    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_setup_pipeline_creates_directory_structure(
        self,
        mock_calc_dates,
        mock_load_data,
        tmp_path,
    ):
        """Test that setup creates the expected directory structure."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = _forecast_data()

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            run_date=dt.date(2024, 12, 20),
            nowcast_source_name="none",
        )

        expected_batch_dir = (
            tmp_path / "covid-19_r_2024-12-20_f_2024-09-22_t_2024-12-20"
        )
        expected_run_dir = expected_batch_dir / "model_runs" / "CA"

        assert context.model_batch_dir == expected_batch_dir
        assert context.model_run_dir == expected_run_dir

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.get_pmfs")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_reporting_delay_fetches_pmf_from_dataops(
        self,
        mock_calc_dates,
        mock_load_data,
        mock_get_pmfs,
        tmp_path,
    ):
        """Test reporting-delay nowcasting loads the PMF from DataOps."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = _forecast_data()
        mock_get_pmfs.return_value = {"right_truncation_pmf": [0.25, 0.75]}

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="daily",
            ed_visit_type="observed",
            model_name="test_model",
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            run_date=dt.date(2024, 12, 20),
            nowcast_source_name="reporting-delay",
        )

        assert isinstance(context.nowcast_source, ReportingDelayNowcast)
        assert context.nowcast_source.reporting_delay_pmf == [0.25, 0.75]
        assert mock_get_pmfs.call_args.kwargs["loc_abb"] == "CA"
        assert mock_get_pmfs.call_args.kwargs["disease"] == "COVID-19"
        assert mock_get_pmfs.call_args.kwargs["as_of"] == dt.date(2024, 12, 20)

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.get_pmfs")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch("pipelines.epiautogp.epiautogp_forecast_utils.calculate_training_dates")
    def test_direct_reporting_delay_pmf_wins_over_dataops(
        self,
        mock_calc_dates,
        mock_load_data,
        mock_get_pmfs,
        tmp_path,
    ):
        """Test a directly supplied PMF is used without calling get_pmfs."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = _forecast_data()

        context = setup_forecast_pipeline(
            disease="COVID-19",
            loc="CA",
            target="nssp",
            frequency="daily",
            ed_visit_type="other",
            model_name="test_model",
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            nowcast_source_name="reporting-delay",
            run_date=dt.date(2024, 12, 20),
            reporting_delay_pmf=[0.4, 0.6],
        )

        assert isinstance(context.nowcast_source, ReportingDelayNowcast)
        assert context.nowcast_source.reporting_delay_pmf == [0.4, 0.6]
        mock_get_pmfs.assert_not_called()

    def test_reporting_delay_errors_for_percentage_targets(self):
        """Test reporting-delay fails for percentage data (numerator/denominator)."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="daily",
            ed_visit_type="pct",
        )
        with pytest.raises(ValueError, match="not applicable"):
            _resolve_nowcast_source(
                forecast_spec=spec,
                nowcast_source_name="reporting-delay",
                reporting_delay_pmf=[1.0],
            )

    def test_reporting_delay_applies_to_nhsn_counts(self):
        """Test reporting-delay also applies to NHSN counts (not just NSSP)."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nhsn",
            frequency="daily",
            ed_visit_type="observed",
        )
        result = _resolve_nowcast_source(
            forecast_spec=spec,
            nowcast_source_name="reporting-delay",
            reporting_delay_pmf=[0.4, 0.6],
        )

        assert isinstance(result, ReportingDelayNowcast)

    def test_reporting_delay_returns_source_for_applicable_config(self):
        """Test reporting-delay builds a source for daily NSSP counts."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="daily",
            ed_visit_type="observed",
        )
        result = _resolve_nowcast_source(
            forecast_spec=spec,
            nowcast_source_name="reporting-delay",
            reporting_delay_pmf=[0.4, 0.6],
        )

        assert isinstance(result, ReportingDelayNowcast)
        assert result.reporting_delay_pmf == [0.4, 0.6]

    def test_reporting_delay_warns_for_non_daily_frequency(self, caplog):
        """Test reporting-delay logs a soft cadence warning on non-daily runs."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="epiweekly",
            ed_visit_type="observed",
        )
        with caplog.at_level(logging.WARNING):
            result = _resolve_nowcast_source(
                forecast_spec=spec,
                nowcast_source_name="reporting-delay",
                reporting_delay_pmf=[1.0],
            )

        assert isinstance(result, ReportingDelayNowcast)
        assert "reporting-delay PMF support matches the model cadence" in caplog.text

    def test_none_keyword_returns_no_source(self):
        """Test 'none' resolves to no nowcast source regardless of config."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="daily",
            ed_visit_type="observed",
        )
        result = _resolve_nowcast_source(
            forecast_spec=spec,
            nowcast_source_name="none",
            reporting_delay_pmf=[0.5, 0.5],
        )

        assert result is None

    def test_unknown_keyword_raises(self):
        """Test an unrecognised keyword raises a descriptive error."""
        spec = ForecastSpec(
            disease="COVID-19",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target="nssp",
            frequency="daily",
            ed_visit_type="observed",
        )
        with pytest.raises(ValueError, match="nowcast_source_name must be one of"):
            _resolve_nowcast_source(
                forecast_spec=spec,
                nowcast_source_name="auto",
                reporting_delay_pmf=None,
            )


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
    def test_prepare_model_data_passes_loaded_data(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,  # Use fixture
        tmp_path,
    ):
        """Test that prepare_model_data passes resolved data to data functions."""

        # Override just the fields we need for this test
        context = replace(
            base_context,
            forecast_spec=replace(base_context.forecast_spec, target="nhsn"),
        )

        _ = context.prepare_model_data()

        mock_process_loc.assert_called_once()
        assert mock_process_loc.call_args[1]["forecast_data"] is context.forecast_data

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
        # Override multiple fields for NHSN test
        context = replace(
            base_context,
            forecast_spec=replace(base_context.forecast_spec, target="nhsn"),
            model_name="epiautogp_nhsn_epiweekly",
        )

        paths = context.prepare_model_data()

        # Verify the method returns valid paths
        assert isinstance(paths, ModelPaths)
        assert paths.model_output_dir.name == "epiautogp_nhsn_epiweekly"
        assert paths.data_dir.name == "data"


class TestPostprocessForecast:
    """Tests for the postprocess_forecast function."""

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.model_fit_dir_to_hub_tbl")
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.make_figures_from_model_fit_dir"
    )
    def test_postprocess_calls_required_functions(
        self,
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
        mock_make_figures.assert_called_once()
        mock_hubverse.assert_called_once()

        expected_model_fit_dir = context.model_run_dir / context.model_name

        # Verify correct arguments to make_figures_from_model_fit_dir
        assert mock_make_figures.call_args[1]["model_fit_dir"] == expected_model_fit_dir
        assert mock_make_figures.call_args[1]["save_figs"] is True
        assert mock_make_figures.call_args[1]["save_ci"] is True

        # Verify model_fit_dir_to_hub_tbl was called with expected_model_fit_dir
        assert mock_hubverse.call_args[0][0] == expected_model_fit_dir

    @patch("pipelines.epiautogp.epiautogp_forecast_utils.model_fit_dir_to_hub_tbl")
    @patch(
        "pipelines.epiautogp.epiautogp_forecast_utils.make_figures_from_model_fit_dir"
    )
    def test_postprocess_creates_correct_paths(
        self,
        mock_make_figures,
        mock_hubverse,
        base_context,  # Use fixture
    ):
        """Test that post_process_forecast creates correct model_fit_dir path."""
        context = base_context

        context.post_process_forecast()

        # Verify model_fit_dir is correctly constructed as model_run_dir/model_name
        expected_model_fit_dir = context.model_run_dir / context.model_name
        assert mock_make_figures.call_args[1]["model_fit_dir"] == expected_model_fit_dir
