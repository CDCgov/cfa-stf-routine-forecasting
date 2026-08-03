import pytest

from pipelines.tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_epiautogp,
)

EPIAUTOGP_CONFIGURATIONS = (
    ("nhsn", "epiweekly", "observed", "epiautogp_nhsn_epiweekly"),
    ("nssp", "epiweekly", "pct", "epiautogp_nssp_epiweekly_pct"),
    ("nssp", "daily", "observed", "epiautogp_nssp_daily"),
    ("nssp", "daily", "other", "epiautogp_nssp_daily_other"),
)


@pytest.mark.pipeline_e2e
@pytest.mark.model_integration
def test_epiautogp_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)

    for target, frequency, ed_visit_type, _ in EPIAUTOGP_CONFIGURATIONS:
        run_epiautogp(
            pipeline_workspace,
            disease,
            location,
            target=target,
            frequency=frequency,
            ed_visit_type=ed_visit_type,
        )

    assert_model_outputs(
        pipeline_workspace,
        disease,
        location,
        [model_name for *_, model_name in EPIAUTOGP_CONFIGURATIONS],
    )
