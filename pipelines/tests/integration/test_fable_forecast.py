import pytest

from pipelines.tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_fable,
)


@pytest.mark.model_integration
def test_fable_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)

    run_fable(pipeline_workspace, disease, location)
    run_fable(pipeline_workspace, disease, location, epiweekly=True)

    assert_model_outputs(
        pipeline_workspace,
        disease,
        location,
        ["daily_fable_e_other", "epiweekly_fable_e_other"],
    )
