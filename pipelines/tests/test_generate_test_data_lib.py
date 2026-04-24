"""Unit tests for generate_test_data_lib.py functions."""

import datetime as dt
import json
import tempfile
from pathlib import Path

import arviz as az
import numpy as np
import polars as pl
import pytest

from pipelines.data.generate_test_data_lib import (
    create_default_param_estimates,
    create_param_estimates,
    create_var_df,
    dirichlet_integer_split,
    update_json_with_prior_predictive,
    update_tsv_with_prior_predictive,
)

# Module-level constants for reusable test data shared across multiple test classes
MAX_DATE_STR = "2024-01-31"
MAX_DATE = dt.date(2024, 1, 31)


def create_idata_from_values(hosp_vals, ed_vals, ww_vals):
    """Helper function to create DataTree from value lists."""
    hosp_data = np.array([[[[v] for v in hosp_vals]]])
    ed_data = np.array([[[[v] for v in ed_vals]]])
    ww_data = np.array([[[[v] for v in ww_vals]]])

    return az.from_dict(
        {
            "prior": {
                "observed_hospital_admissions": hosp_data,
                "observed_ed_visits": ed_data,
                "site_level_log_ww_conc": ww_data,
            }
        }
    )


class TestDirichletIntegerSplit:
    """Tests for dirichlet_integer_split function."""

    # Class-level constants for dirichlet-specific test parameters
    N_K_PARAMS = [
        (100, 5),
        (50, 10),
        (200, 3),
    ]
    N_K_IDS = ["n100_k5", "n50_k10", "n200_k3"]

    ALPHA_PARAMS = [
        (100, 5, 0.1),  # Small alpha (more concentration)
        (100, 5, 10.0),  # Large alpha (more uniform)
        (50, 3, 1.0),  # Default alpha
        (200, 8, 0.5),  # Small alpha
        (150, 6, 5.0),  # Large alpha
    ]
    ALPHA_PARAMS_IDS = [
        "small_alpha_0.1",
        "large_alpha_10.0",
        "default_alpha_1.0",
        "small_alpha_0.5",
        "large_alpha_5.0",
    ]

    @pytest.mark.parametrize(
        "n,k",
        N_K_PARAMS,
        ids=N_K_IDS,
    )
    def test_sum_equals_n(self, n, k):
        """Test that the sum of parts equals the input integer."""
        result = dirichlet_integer_split(n, k)
        assert result.sum() == n

    @pytest.mark.parametrize(
        "n,k",
        N_K_PARAMS,
        ids=N_K_IDS,
    )
    def test_correct_number_of_parts(self, n, k):
        """Test that the correct number of parts is returned."""
        result = dirichlet_integer_split(n, k)
        assert len(result) == k

    @pytest.mark.parametrize(
        "n,k",
        N_K_PARAMS,
        ids=N_K_IDS,
    )
    def test_all_non_negative(self, n, k):
        """Test that all parts are non-negative."""
        result = dirichlet_integer_split(n, k)
        assert (result >= 0).all()

    @pytest.mark.parametrize(
        "n,k",
        N_K_PARAMS,
        ids=N_K_IDS,
    )
    def test_all_integers(self, n, k):
        """Test that all parts are integers."""
        result = dirichlet_integer_split(n, k)
        assert result.dtype == np.int64 or result.dtype == np.int32

    @pytest.mark.parametrize(
        "n,k,alpha",
        ALPHA_PARAMS,
        ids=ALPHA_PARAMS_IDS,
    )
    def test_different_alpha_values(self, n, k, alpha):
        """Test with different alpha concentration parameters."""
        result = dirichlet_integer_split(n, k, alpha=alpha)
        assert result.sum() == n
        assert len(result) == k


class TestUpdateJsonWithPriorPredictive:
    """Tests for update_json_with_prior_predictive function."""

    @pytest.fixture
    def sample_idata_minimal(self):
        """Create minimal DataTree for basic tests."""
        return az.from_dict(
            {
                "prior": {
                    "observed_hospital_admissions": np.array([[[[10.0]]]]),
                    "observed_ed_visits": np.array([[[[5.0]]]]),
                    "site_level_log_ww_conc": np.array([[[[4.0]]]]),
                }
            }
        )

    @pytest.mark.parametrize(
        "state,disease,hosp_vals,ed_vals,ww_vals,expected_hosp,expected_ed",
        [
            (
                "MT",
                "COVID-19",
                [10.5, 20.3, 15.7],
                [5.2, 8.9, 6.1],
                [4.5, 3.2, 5.8],
                [10, 20, 15],
                [5, 8, 6],
            ),
            (
                "DC",
                "Influenza",
                [25.1, 30.8],
                [12.4, 15.9],
                [2.3, 3.7],
                [25, 30],
                [12, 15],
            ),
            (
                "CA",
                "RSV",
                [8.9, 12.2, 18.5, 22.1],
                [4.3, 6.7, 9.1, 11.8],
                [1.5, 2.2, 3.1, 4.0],
                [8, 12, 18, 22],
                [4, 6, 9, 11],
            ),
        ],
        ids=["MT-COVID19-3pts", "DC-Influenza-2pts", "CA-RSV-4pts"],
    )
    def test_updates_json_file(
        self, state, disease, hosp_vals, ed_vals, ww_vals, expected_hosp, expected_ed
    ):
        """Test that JSON file is updated with prior predictive values."""
        # Create temporary directory and JSON file
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test_data.json"

            # Create initial JSON structure with zeros matching the length
            n_points = len(hosp_vals)
            initial_data = {
                "nhsn_training_data": {"hospital_admissions": [0] * n_points},
                "nssp_training_data": {"observed_ed_visits": [0] * n_points},
                "nwss_training_data": {"log_genome_copies_per_ml": [0.0] * n_points},
            }
            with open(json_path, "w") as f:
                json.dump(initial_data, f)

            # Create mock DataTree using helper function
            idata = create_idata_from_values(hosp_vals, ed_vals, ww_vals)

            # Create state_disease_key
            state_disease_key = pl.DataFrame(
                {"draw": [0], "state": [state], "disease": [disease]}
            )

            # Call the function
            update_json_with_prior_predictive(
                json_path, idata, state_disease_key, state, disease
            )

            # Read updated JSON
            with open(json_path) as f:
                updated_data = json.load(f)

            # Verify updates
            assert (
                updated_data["nhsn_training_data"]["hospital_admissions"]
                == expected_hosp
            )
            assert (
                updated_data["nssp_training_data"]["observed_ed_visits"] == expected_ed
            )
            assert (
                len(updated_data["nwss_training_data"]["log_genome_copies_per_ml"])
                == n_points
            )

    def test_preserves_other_json_fields(self, sample_idata_minimal):
        """Test that other fields in JSON are preserved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test_data.json"

            initial_data = {
                "nhsn_training_data": {
                    "hospital_admissions": [0],
                    "other_field": "preserved",
                },
                "nssp_training_data": {"observed_ed_visits": [0]},
                "nwss_training_data": {"log_genome_copies_per_ml": [0.0]},
                "metadata": {"some": "data"},
            }
            with open(json_path, "w") as f:
                json.dump(initial_data, f)

            state_disease_key = pl.DataFrame(
                {"draw": [0], "state": ["DC"], "disease": ["Influenza"]}
            )

            update_json_with_prior_predictive(
                json_path, sample_idata_minimal, state_disease_key, "DC", "Influenza"
            )

            with open(json_path) as f:
                updated_data = json.load(f)

            assert updated_data["nhsn_training_data"]["other_field"] == "preserved"
            assert updated_data["metadata"] == {"some": "data"}


class TestUpdateTsvWithPriorPredictive:
    """Tests for update_tsv_with_prior_predictive function."""

    def test_updates_tsv_file(self):
        """Test that TSV file is updated with prior predictive values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tsv_path = Path(tmpdir) / "test_data.tsv"

            # Create initial TSV
            initial_df = pl.DataFrame(
                {
                    "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
                    ".variable": [
                        "observed_ed_visits",
                        "observed_ed_visits",
                        "site_level_log_ww_conc",
                    ],
                    ".value": [0.0, 0.0, 0.0],
                    "geo_value": ["MT", "MT", "MT"],
                    "disease": ["COVID-19", "COVID-19", "COVID-19"],
                    "data_type": ["train", "train", "train"],
                    "lab_site_index": [None, None, 0],
                }
            )
            initial_df.write_csv(tsv_path, separator="\t")

            # Create mock DataTree with correct shape
            hosp_data = np.array([[[[10.0]]]])  # (1, 1, 1, 1)
            ed_data = np.array([[[[25.0], [30.0]]]])  # (1, 1, 2, 1)
            ww_data = np.array([[[[4.5], [5.5]]]])  # (1, 1, 2, 1)

            idata = az.from_dict(
                {
                    "prior": {
                        "observed_hospital_admissions": hosp_data,
                        "observed_ed_visits": ed_data,
                        "site_level_log_ww_conc": ww_data,
                    }
                }
            )

            state_disease_key = pl.DataFrame(
                {"draw": [0], "state": ["MT"], "disease": ["COVID-19"]}
            )

            # Call the function
            update_tsv_with_prior_predictive(
                tsv_path, idata, state_disease_key, "MT", "COVID-19"
            )

            # Read updated TSV
            updated_df = pl.read_csv(tsv_path, separator="\t")

            # Check ED visits were updated
            ed_rows = updated_df.filter(pl.col(".variable") == "observed_ed_visits")
            assert ed_rows[".value"].to_list()[0] == 25.0
            assert ed_rows[".value"].to_list()[1] == 30.0


class TestCreateVarDf:
    """Tests for create_var_df function."""

    # Class-level constants for TestCreateVarDf-specific test data
    STATES = ["MT", "DC", "CA", "TX", "NY", "FL"]
    DISEASES = ["COVID-19", "Influenza", "RSV"]

    @pytest.mark.parametrize(
        "n_draws,n_times,n_sites,var_name",
        [
            (2, 10, 1, "test_variable"),
            (3, 5, 2, "infections"),
            (1, 20, 3, "hospitalizations"),
            (4, 8, 1, "observed_cases"),
        ],
        ids=[
            "2draws_10times_1site",
            "3draws_5times_2sites",
            "1draw_20times_3sites",
            "4draws_8times_1site",
        ],
    )
    def test_creates_dataframe_from_idata(self, n_draws, n_times, n_sites, var_name):
        """Test that a DataFrame is created from DataTree."""
        # Create mock DataTree
        data = np.random.randn(1, n_draws, n_times, n_sites)
        idata = az.from_dict({"prior": {var_name: data}})

        state_disease_key = pl.DataFrame(
            {
                "draw": list(range(n_draws)),
                "state": self.STATES[:n_draws],
                "disease": [self.DISEASES[0]] * n_draws,
            }
        )

        result = create_var_df(idata, var_name, state_disease_key)

        # Verify it's a DataFrame
        assert isinstance(result, pl.DataFrame)

        # Check it has the expected columns (variable name, not "value")
        expected_cols = ["state", "disease", "time", var_name]
        for col in expected_cols:
            assert col in result.columns

        # Check dimensions
        expected_rows = n_draws * n_times * n_sites
        assert len(result) == expected_rows

    @pytest.mark.parametrize(
        "n_draws,n_times,n_sites",
        [
            (3, 5, 2),
            (2, 10, 1),
            (4, 7, 3),
        ],
        ids=["3x5x2", "2x10x1", "4x7x3"],
    )
    def test_correct_dimensions(self, n_draws, n_times, n_sites):
        """Test that output DataFrame has correct dimensions."""
        data = np.random.randn(1, n_draws, n_times, n_sites)
        idata = az.from_dict({"prior": {"test_var": data}})

        state_disease_key = pl.DataFrame(
            {
                "draw": list(range(n_draws)),
                "state": self.STATES[:n_draws],
                "disease": [self.DISEASES[0]] * n_draws,
            }
        )

        result = create_var_df(idata, "test_var", state_disease_key)

        # Should have n_draws * n_times * n_sites rows
        expected_rows = n_draws * n_times * n_sites
        assert len(result) == expected_rows


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
