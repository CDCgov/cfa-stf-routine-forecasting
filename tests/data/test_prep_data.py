import datetime as dt
import json
from dataclasses import replace

import polars as pl
import pytest
from tests.factories import make_test_forecast_run

from cfa.stf.routine.data import prep_data
from cfa.stf.routine.data.prep_data import serialize_data
from cfa.stf.routine.utils.data_utils import aggregate_nssp_to_epiweekly


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
    forecast_run = make_test_forecast_run(
        output_dir=tmp_path,
        sources=sources,
    )

    serialize_data(forecast_run=forecast_run)

    with open(forecast_run.data_dir / "data_for_model_fit.json") as file:
        model_data = json.load(file)
    assert (model_data["nssp_training_data"] is not None) == ("nssp" in sources)
    assert (model_data["nhsn_training_data"] is not None) == ("nhsn" in sources)
    assert model_data["nssp_step_size"] == 1
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

    combined_data = pl.read_csv(
        forecast_run.data_dir / "combined_data.tsv", separator="\t"
    )
    assert set(combined_data.get_column(".variable")) == expected_variables


def test_serialize_data_uses_run_ed_visits_for_both_artifacts(tmp_path):
    forecast_run = make_test_forecast_run(
        output_dir=tmp_path,
        sources=("nssp",),
    )
    dates = pl.date_range(dt.date(2025, 1, 5), dt.date(2025, 1, 18), eager=True)
    nssp_data = pl.DataFrame(
        {
            "date": dates,
            "state_abb": ["CA"] * 14,
            "observed_ed_visits": list(range(1, 15)),
            "other_ed_visits": [9] * 14,
            "data_type": ["train"] * 7 + ["eval"] * 7,
            "resolution": ["daily"] * 14,
        }
    )
    nssp_data = aggregate_nssp_to_epiweekly(nssp_data)
    surveillance = replace(
        forecast_run.surveillance,
        nssp=replace(forecast_run.nssp, data=nssp_data),
    )
    forecast_run = replace(forecast_run, surveillance=surveillance)

    serialize_data(
        forecast_run=forecast_run,
        ed_visit_input_resolution="epiweekly",
    )

    with open(forecast_run.data_dir / "data_for_model_fit.json") as file:
        model_data = json.load(file)
    assert model_data["nssp_step_size"] == 7
    assert model_data["nssp_training_data"] == {
        "date": ["2025-01-11"],
        "geo_value": ["CA"],
        "observed_ed_visits": [28],
        "other_ed_visits": [63],
    }

    combined_data = pl.read_csv(
        forecast_run.data_dir / "combined_data.tsv",
        separator="\t",
        try_parse_dates=True,
    )
    weekly_ed_data = combined_data.filter(
        pl.col(".variable").is_in(
            [
                "observed_ed_visits",
                "other_ed_visits",
                "prop_disease_ed_visits",
            ]
        )
    ).select("date", "data_type", ".variable", ".value")
    assert weekly_ed_data.rows() == [
        (dt.date(2025, 1, 11), "train", "observed_ed_visits", 28.0),
        (dt.date(2025, 1, 11), "train", "other_ed_visits", 63.0),
        (dt.date(2025, 1, 11), "train", "prop_disease_ed_visits", 28 / 91),
        (dt.date(2025, 1, 18), "eval", "observed_ed_visits", 77.0),
        (dt.date(2025, 1, 18), "eval", "other_ed_visits", 63.0),
        (dt.date(2025, 1, 18), "eval", "prop_disease_ed_visits", 77 / 140),
    ]
