import datetime as dt
import json

import polars as pl
import pytest
from tests.factories import make_test_surveillance_inputs

from cfa.stf.routine.data import prep_data
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.utils.common_utils import append_prop_data_to_combined_data


@pytest.mark.parametrize(
    ("sources", "expected_variables"),
    [
        pytest.param(
            ("nssp",),
            {
                "observed_ed_visits",
                "other_ed_visits",
                "prop_disease_ed_visits",
            },
            id="nssp-only",
        ),
        pytest.param(
            ("nhsn",),
            {"observed_hospital_admissions"},
            id="nhsn-only",
        ),
        pytest.param(
            ("nssp", "nhsn"),
            {
                "observed_ed_visits",
                "other_ed_visits",
                "observed_hospital_admissions",
                "prop_disease_ed_visits",
            },
            id="both",
        ),
    ],
)
def test_serialize_data_handles_present_sources(
    monkeypatch,
    tmp_path,
    sources,
    expected_variables,
):
    def fail_if_called(**kwargs):
        pytest.fail(f"basicConfig called from serialize_data: {kwargs}")

    monkeypatch.setattr(prep_data.logging, "basicConfig", fail_if_called)
    forecast_run = ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 20),
        n_forecast_days=28,
        exclude_last_n_days=0,
        model_name="test_model",
        output_dir=tmp_path,
        surveillance=make_test_surveillance_inputs(sources=sources),
    )

    serialize_data(
        forecast_run=forecast_run,
        save_dir=tmp_path,
    )
    append_prop_data_to_combined_data(tmp_path / "combined_data.tsv")

    with open(tmp_path / "data_for_model_fit.json") as file:
        model_data = json.load(file)
    assert (model_data["nssp_training_data"] is not None) == ("nssp" in sources)
    assert (model_data["nhsn_training_data"] is not None) == ("nhsn" in sources)
    if "nssp" in sources:
        assert set(model_data["nssp_training_data"]) == {
            "date",
            "geo_value",
            "observed_ed_visits",
            "other_ed_visits",
        }
    if "nhsn" in sources:
        assert set(model_data["nhsn_training_data"]) == {
            "weekendingdate",
            "jurisdiction",
            "hospital_admissions",
        }

    combined_data = pl.read_csv(tmp_path / "combined_data.tsv", separator="\t")
    assert set(combined_data.get_column(".variable")) == expected_variables
