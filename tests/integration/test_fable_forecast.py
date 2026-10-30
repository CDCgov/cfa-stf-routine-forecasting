import polars as pl
import pytest
from tests.integration.model_test_utils import (
    MOCK_DATA_MODE,
    assert_model_outputs,
    configure_data_mode,
    model_batch_dir,
    patch_mock_nssp_with_missing_values,
    run_fable,
)


@pytest.mark.model_integration
@pytest.mark.parametrize(
    "missing_training_data", [False, True], ids=["complete", "missing"]
)
def test_fable_forecast(
    pipeline_workspace, monkeypatch, request, missing_training_data
):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    data_mode = configure_data_mode(request, monkeypatch)
    if missing_training_data:
        if data_mode != MOCK_DATA_MODE:
            pytest.skip("Missing training data is only injected into mock data")
        null_dates, dropped_date = patch_mock_nssp_with_missing_values(monkeypatch)

    run_fable(pipeline_workspace, disease, location)
    run_fable(
        pipeline_workspace,
        disease,
        location,
        ed_visit_input_resolution="epiweekly",
    )

    if missing_training_data:
        combined_data = pl.read_csv(
            model_batch_dir(pipeline_workspace, disease)
            / "model_runs"
            / location
            / "daily_fable_e_other"
            / "data"
            / "combined_data.tsv",
            separator="\t",
            null_values=["NA"],
            try_parse_dates=True,
        )
        dates = combined_data.get_column("date")
        null_value_dates = dates.filter(combined_data.get_column(".value").is_null())
        assert sorted(null_value_dates.unique()) == null_dates
        assert dropped_date not in dates

    assert_model_outputs(
        pipeline_workspace,
        disease,
        location,
        ["daily_fable_e_other", "epiweekly_fable_e_other"],
    )
