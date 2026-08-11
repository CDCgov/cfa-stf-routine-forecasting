import pytest
from tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_pyrenew,
)


@pytest.mark.model_integration
def test_pyrenew_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)

    model_letters = ("h", "e", "he")
    for letters in model_letters:
        run_pyrenew(
            pipeline_workspace,
            disease,
            location,
            model_letters=letters,
        )

    assert_model_outputs(
        pipeline_workspace,
        disease,
        location,
        [f"pyrenew_{letters}" for letters in model_letters],
    )
