import pytest
from tests.integration.generate_test_data import (
    HUBVERSE_N_SAMPLES,
    HUBVERSE_NOWCAST_DIR_NAME,
    write_hubverse_nowcast,
)
from tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_nowcastautogp,
    selected_nhsn_observations,
)

NOWCASTAUTOGP_CONFIGURATIONS = (
    (
        "nhsn",
        "epiweekly",
        "observed",
        "hubverse",
        "nowcastautogp_nhsn_epiweekly",
    ),
    ("nssp", "epiweekly", "pct", "none", "nowcastautogp_nssp_epiweekly_pct"),
    (
        "nssp",
        "daily",
        "observed",
        "reporting-delay",
        "nowcastautogp_nssp_daily",
    ),
    (
        "nssp",
        "daily",
        "other",
        "none",
        "nowcastautogp_nssp_daily_other",
    ),
)


@pytest.mark.model_integration
def test_nowcastautogp_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)
    write_hubverse_nowcast(
        pipeline_workspace,
        disease=disease,
        location=location,
        nhsn_observations=selected_nhsn_observations(disease, location),
    )
    hubverse_nowcast_dir = (
        pipeline_workspace / "private_data" / HUBVERSE_NOWCAST_DIR_NAME / disease
    )

    for (
        target,
        frequency,
        ed_visit_type,
        nowcast_source_name,
        _,
    ) in NOWCASTAUTOGP_CONFIGURATIONS:
        run_nowcastautogp(
            pipeline_workspace,
            disease,
            location,
            target=target,
            frequency=frequency,
            ed_visit_type=ed_visit_type,
            nowcast_source_name=nowcast_source_name,
            hubverse_nowcast_dir=(
                hubverse_nowcast_dir if nowcast_source_name == "hubverse" else None
            ),
            n_forecast_draws=(
                HUBVERSE_N_SAMPLES if nowcast_source_name == "hubverse" else 40
            ),
        )

    assert_model_outputs(
        pipeline_workspace,
        disease,
        location,
        [model_name for *_, model_name in NOWCASTAUTOGP_CONFIGURATIONS],
    )
