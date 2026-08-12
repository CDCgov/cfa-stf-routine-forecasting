"""Unit tests for forecast pipeline utility functions.

These tests use mocking via `@patch` decorators to isolate the units under test.
We mock these dependencies to:

1. Test the logic and control flow without side effects
2. Avoid requiring actual data files or external services
3. Make tests fast and deterministic
4. Focus on verifying correct function calls and parameter passing

Each test mocks the minimum dependencies needed for that specific test case.

The end-to-end functionality of the forecast pipeline is verified in a separate
integration test.
"""

import datetime as dt
import logging
from dataclasses import replace
from unittest.mock import patch

import pytest
from tests.factories import make_test_forecast_data

from cfa.stf.routine.epiautogp.epiautogp_forecast_utils import (
    ForecastPipelineContext,
    ForecastSpec,
    ModelPaths,
    _resolve_nowcast_source,
    setup_forecast_pipeline,
)
from cfa.stf.routine.epiautogp.reporting_delay_nowcast import ReportingDelayNowcast


@pytest.fixture
def base_context(tmp_path):
    """
    Fixture providing a ForecastPipelineContext with default test values.

    Tests can use this directly or override specific fields as needed.
    """
    return ForecastPipelineContext(
        forecast_spec=ForecastSpec(
            disease="covid",
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
        forecast_data=make_test_forecast_data(),
        logger=logging.getLogger(),
    )


class TestSetupForecastPipeline:
    """Tests for the setup_forecast_pipeline function."""

    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.calculate_training_dates"
    )
    def test_setup_pipeline_returns_context(
        self,
        mock_calc_dates,
        mock_load_data,
        tmp_path,
    ):
        """Test that setup_forecast_pipeline returns a properly configured context."""
        # Setup mocks
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = make_test_forecast_data()

        context = setup_forecast_pipeline(
            disease="covid",
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
            fail_on_stale_data=True,
        )

        assert isinstance(context, ForecastPipelineContext)
        assert mock_calc_dates.call_args.args[0] == dt.date(2024, 12, 20)
        assert mock_load_data.call_args.kwargs["run_date"] == dt.date(2024, 12, 20)
        assert mock_load_data.call_args.kwargs["fail_on_stale_data"] is True
        assert mock_load_data.call_args.kwargs["sources"] == {"nssp"}
        assert "report_date" not in mock_load_data.call_args.kwargs

    @pytest.mark.parametrize("target", ["nssp", "nhsn"])
    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.calculate_training_dates"
    )
    def test_setup_pipeline_requests_target_source(
        self,
        mock_calc_dates,
        mock_load_data,
        tmp_path,
        target,
    ):
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = make_test_forecast_data(sources={target})

        setup_forecast_pipeline(
            disease="covid",
            loc="CA",
            target=target,
            frequency="epiweekly",
            ed_visit_type="observed",
            model_name="test_model",
            output_dir=tmp_path,
            n_training_days=90,
            n_forecast_days=28,
            run_date=dt.date(2024, 12, 20),
        )

        assert mock_load_data.call_args.kwargs["sources"] == {target}

    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.calculate_training_dates"
    )
    def test_setup_pipeline_creates_directory_structure(
        self,
        mock_calc_dates,
        mock_load_data,
        tmp_path,
    ):
        """Test that setup creates the expected directory structure."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = make_test_forecast_data()

        context = setup_forecast_pipeline(
            disease="covid",
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

        expected_batch_dir = tmp_path / "covid_r_2024-12-20_f_2024-09-22_t_2024-12-20"
        expected_run_dir = expected_batch_dir / "model_runs" / "CA"

        assert context.model_batch_dir == expected_batch_dir
        assert context.model_run_dir == expected_run_dir

    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.get_nnh_right_truncation_pmf"
    )
    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.calculate_training_dates"
    )
    def test_reporting_delay_fetches_pmf_from_cfa_stf_data(
        self,
        mock_calc_dates,
        mock_load_data,
        mock_get_right_truncation_pmf,
        tmp_path,
    ):
        """Test reporting-delay nowcasting loads its PMF through cfa-stf-data."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = make_test_forecast_data()
        mock_get_right_truncation_pmf.return_value = [0.25, 0.75]

        context = setup_forecast_pipeline(
            disease="covid",
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
        mock_get_right_truncation_pmf.assert_called_once_with(
            state_abb="CA",
            disease="covid",
            as_of=dt.date(2024, 12, 20),
            reference_date=dt.date(2024, 12, 20),
        )

    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.get_nnh_right_truncation_pmf"
    )
    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.load_forecast_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.calculate_training_dates"
    )
    def test_direct_reporting_delay_pmf_wins_over_cfa_stf_data(
        self,
        mock_calc_dates,
        mock_load_data,
        mock_get_right_truncation_pmf,
        tmp_path,
    ):
        """Test a directly supplied PMF skips cfa-stf-data."""
        mock_calc_dates.return_value = (dt.date(2024, 9, 22), dt.date(2024, 12, 20))
        mock_load_data.return_value = make_test_forecast_data()

        context = setup_forecast_pipeline(
            disease="covid",
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
        mock_get_right_truncation_pmf.assert_not_called()

    def test_reporting_delay_errors_for_percentage_targets(self):
        """Test reporting-delay fails for percentage data (numerator/denominator)."""
        spec = ForecastSpec(
            disease="covid",
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

    @pytest.mark.parametrize("target", ["nssp", "nhsn"])
    def test_reporting_delay_returns_source_for_count_target(self, target):
        spec = ForecastSpec(
            disease="covid",
            loc="CA",
            report_date=dt.date(2024, 12, 20),
            target=target,
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
            disease="covid",
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
            disease="covid",
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
            disease="covid",
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
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data"
    )
    def test_prepare_model_data_returns_paths_and_creates_directories(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,
    ):
        paths = base_context.prepare_model_data()

        assert isinstance(paths, ModelPaths)
        assert paths.model_output_dir.name == "test_model"
        assert paths.data_dir.name == "data"
        assert paths.training_data.name == "combined_data.tsv"
        assert paths.model_output_dir.exists()
        assert paths.data_dir.exists()

    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.append_prop_data_to_combined_data"
    )
    @patch("cfa.stf.routine.epiautogp.epiautogp_forecast_utils.generate_epiweekly_data")
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.process_and_save_loc_data"
    )
    def test_prepare_model_data_passes_loaded_data(
        self,
        mock_process_loc,
        mock_gen_epiweekly,
        mock_append_prop,
        base_context,
    ):
        """Test that prepare_model_data passes resolved data to data functions."""
        context = replace(
            base_context,
            forecast_spec=replace(base_context.forecast_spec, target="nhsn"),
        )

        _ = context.prepare_model_data()

        mock_process_loc.assert_called_once()
        assert mock_process_loc.call_args[1]["forecast_data"] is context.forecast_data


class TestPostprocessForecast:
    """Tests for the postprocess_forecast function."""

    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.model_fit_dir_to_hub_tbl"
    )
    @patch(
        "cfa.stf.routine.epiautogp.epiautogp_forecast_utils.make_figures_from_model_fit_dir"
    )
    def test_postprocess_calls_required_functions(
        self,
        mock_make_figures,
        mock_hubverse,
        base_context,
    ):
        """Test that post_process_forecast calls all required functions."""
        context = replace(
            base_context,
            exclude_last_n_days=5,
        )

        context.post_process_forecast()

        mock_make_figures.assert_called_once()
        mock_hubverse.assert_called_once()

        expected_model_fit_dir = context.model_run_dir / context.model_name

        assert mock_make_figures.call_args[1]["model_fit_dir"] == expected_model_fit_dir
        assert mock_make_figures.call_args[1]["save_figs"] is True
        assert mock_make_figures.call_args[1]["save_ci"] is True
        assert mock_hubverse.call_args[0][0] == expected_model_fit_dir
