"""Unit tests for generate_test_data_lib.py functions."""

import datetime as dt

import numpy as np
import polars as pl
import pytest

from pipelines.data.generate_test_data_lib import (
    create_default_param_estimates,
    create_param_estimates,
)

# Module-level constants for reusable test data shared across multiple test classes
MAX_DATE_STR = "2024-01-31"
MAX_DATE = dt.date(2024, 1, 31)


class TestCreateParamEstimates:
    """Tests for create_param_estimates function."""

    @pytest.mark.parametrize(
        "gi_pmf,rt_pmf,delay_pmf,states,diseases",
        [
            (
                np.array([0.0, 0.3, 0.5, 0.2]),
                np.array([0.1, 0.4, 0.5]),
                np.array([0.2, 0.3, 0.3, 0.2]),
                ["MT", "DC"],
                ["COVID-19", "Influenza"],
            ),
            (
                np.array([0.5, 0.5]),
                np.array([0.5, 0.5]),
                np.array([0.5, 0.5]),
                ["MT"],
                ["COVID-19"],
            ),
            (
                np.array([0.2, 0.3, 0.3, 0.2]),
                np.array([0.25, 0.25, 0.25, 0.25]),
                np.array([0.1, 0.2, 0.3, 0.2, 0.2]),
                ["CA", "TX", "NY"],
                ["RSV"],
            ),
        ],
        ids=["2states_2diseases", "1state_1disease", "3states_1disease"],
    )
    def test_creates_param_estimates_dataframe(
        self, gi_pmf, rt_pmf, delay_pmf, states, diseases
    ):
        """Test that parameter estimates DataFrame is created."""
        result = create_param_estimates(
            gi_pmf, rt_pmf, delay_pmf, states, diseases, MAX_DATE_STR, MAX_DATE
        )

        assert isinstance(result, pl.DataFrame)
        assert "parameter" in result.columns
        assert "geo_value" in result.columns
        assert "disease" in result.columns

    def test_correct_number_of_rows(self):
        """Test that correct number of parameter estimate rows are created."""
        gi_pmf = np.array([0.5, 0.5])
        rt_pmf = np.array([0.5, 0.5])
        delay_pmf = np.array([0.5, 0.5])

        states = ["MT"]
        diseases = ["COVID-19"]

        result = create_param_estimates(
            gi_pmf,
            rt_pmf,
            delay_pmf,
            states,
            diseases,
            MAX_DATE_STR,
            MAX_DATE,
        )

        # Function creates one row per (parameter, location combo)
        # generation_interval: 1 row (global), right_truncation: 2 rows (MT, US), delay: 1 row (global)
        assert len(result) == 4

    def test_pmf_values_in_output(self):
        """Test that PMF values appear in the output."""
        gi_pmf = np.array([0.1, 0.9])
        rt_pmf = np.array([0.3, 0.7])
        delay_pmf = np.array([0.4, 0.6])

        result = create_param_estimates(
            gi_pmf,
            rt_pmf,
            delay_pmf,
            ["MT"],
            ["COVID-19"],
            MAX_DATE_STR,
            MAX_DATE,
        )

        # Check that PMF arrays are present (stored as arrays, not individual values)
        values = result["value"].to_list()
        assert any(np.array_equal(v, [0.1, 0.9]) for v in values)
        assert any(np.array_equal(v, [0.3, 0.7]) for v in values)
        assert any(np.array_equal(v, [0.4, 0.6]) for v in values)


class TestCreateDefaultParamEstimates:
    """Tests for create_default_param_estimates function."""

    STATE_DISEASE_GROUPS = [
        (["MT", "DC"], ["COVID-19"]),
        (["CA"], ["Influenza", "RSV"]),
        (["TX", "NY", "FL"], ["COVID-19"]),
        (["MT"], ["COVID-19"]),
    ]
    STATE_DISEASE_GROUPS_IDS = [
        "2states_1disease",
        "1state_2diseases",
        "3states_1disease",
        "1state_1disease",
    ]

    @pytest.mark.parametrize(
        "states,diseases",
        STATE_DISEASE_GROUPS,
        ids=STATE_DISEASE_GROUPS_IDS,
    )
    def test_creates_default_param_estimates(self, states, diseases):
        """Test that default parameter estimates are created."""
        result = create_default_param_estimates(
            states, diseases, MAX_DATE_STR, MAX_DATE
        )

        assert isinstance(result, pl.DataFrame)
        assert "parameter" in result.columns
        assert len(result) > 0

    def test_includes_all_required_parameters(self):
        """Test that all required parameters are included."""
        result = create_default_param_estimates(
            ["MT"], ["COVID-19"], MAX_DATE_STR, MAX_DATE
        )

        params = result["parameter"].unique().to_list()

        # Should include generation_interval, right_truncation, and delay
        assert "generation_interval" in params
        assert "right_truncation" in params
        assert any("delay" in p.lower() for p in params) or "inf_to_hosp" in params

    @pytest.mark.parametrize(
        "states,diseases",
        STATE_DISEASE_GROUPS,
        ids=STATE_DISEASE_GROUPS_IDS,
    )
    def test_multiple_states_and_diseases(self, states, diseases):
        """Test with multiple states and diseases."""
        result = create_default_param_estimates(
            states, diseases, MAX_DATE_STR, MAX_DATE
        )

        # Check that all states appear
        unique_states = result["geo_value"].unique().to_list()
        for state in states:
            assert state in unique_states

        # Check that all diseases appear
        unique_diseases = result["disease"].unique().to_list()
        for disease in diseases:
            assert disease in unique_diseases
