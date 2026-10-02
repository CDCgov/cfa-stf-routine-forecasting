import pytest
from tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_mech_nssp_daily,
)


@pytest.mark.model_integration
def test_mech_nssp_daily_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)

    run_mech_nssp_daily(pipeline_workspace, disease, location)

    assert_model_outputs(pipeline_workspace, disease, location, ["mech_nssp_daily"])
