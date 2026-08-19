import datetime as dt
import json
from dataclasses import replace

import polars as pl
import pytest
from tests.factories import make_test_forecast_run

from cfa.stf.routine.data import prep_data
from cfa.stf.routine.data.prep_data import (
    aggregate_epiweekly_nssp,
    serialize_data,
)
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
    forecast_run = make_test_forecast_run(
        output_dir=tmp_path,
        sources=sources,
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


def test_aggregate_epiweekly_nssp_matches_r_forecasttools_semantics():
    dates = [dt.date(2024, 12, 15) + dt.timedelta(days=index) for index in range(28)]
    observed = list(range(1, 29))
    observed[3] = None
    data = pl.DataFrame(
        {
            "date": dates,
            "state_abb": ["CA"] * 28,
            "observed_ed_visits": observed,
            "other_ed_visits": list(range(101, 129)),
            "data_type": ["train"] * 26 + ["eval"] * 2,
            "resolution": ["daily"] * 28,
        }
    )

    result = aggregate_epiweekly_nssp(data)

    assert result.select(
        "date",
        "observed_ed_visits",
        "other_ed_visits",
    ).rows() == [
        (dt.date(2024, 12, 21), None, 728),
        (dt.date(2024, 12, 28), 77, 777),
        (dt.date(2025, 1, 4), 126, 826),
    ]


def test_aggregate_epiweekly_nssp_returns_typed_empty_frame():
    data = pl.DataFrame(
        {
            "date": [dt.date(2024, 1, 1)],
            "state_abb": ["CA"],
            "observed_ed_visits": [1],
            "other_ed_visits": [9],
            "data_type": ["train"],
            "resolution": ["daily"],
        }
    )

    result = aggregate_epiweekly_nssp(data)

    assert result.is_empty()
    assert result.schema == data.schema


def test_serialize_data_uses_shared_epiweekly_nssp_aggregation(tmp_path):
    forecast_run = make_test_forecast_run(output_dir=tmp_path, sources=("nssp",))
    dates = [dt.date(2024, 1, 7) + dt.timedelta(days=index) for index in range(7)]
    nssp_data = pl.DataFrame(
        {
            "date": dates,
            "state_abb": ["CA"] * 7,
            "observed_ed_visits": [1] * 7,
            "other_ed_visits": [9] * 7,
            "data_type": ["train"] * 7,
            "resolution": ["daily"] * 7,
        }
    )
    forecast_run = replace(
        forecast_run,
        surveillance=replace(
            forecast_run.surveillance,
            nssp=replace(forecast_run.nssp, data=nssp_data),
        ),
    )

    serialize_data(
        forecast_run=forecast_run,
        save_dir=tmp_path,
        nssp_frequency="epiweekly",
    )

    combined_data = pl.read_csv(
        tmp_path / "combined_data.tsv",
        separator="\t",
        try_parse_dates=True,
    )
    assert combined_data.get_column("date").unique().to_list() == [dt.date(2024, 1, 13)]
    assert combined_data.get_column("resolution").unique().to_list() == ["epiweekly"]
    assert dict(combined_data.select(".variable", ".value").iter_rows()) == {
        "observed_ed_visits": 7,
        "other_ed_visits": 63,
    }
