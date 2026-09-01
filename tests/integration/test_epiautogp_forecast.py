import pytest
from tests.integration.model_test_utils import (
    assert_model_outputs,
    configure_data_mode,
    run_epiautogp,
    selected_nhsn_observation_dates,
)

from cfa.stf.routine.data.generate_test_data import (
    HUBVERSE_N_SAMPLES,
    HUBVERSE_NOWCAST_DIR_NAME,
    write_hubverse_nowcasts,
)

EPIAUTOGP_CONFIGURATIONS = (
    (
        "nhsn",
        "epiweekly",
        "observed",
        "hubverse",
        "epiautogp_nhsn_epiweekly",
    ),
    ("nssp", "epiweekly", "pct", "none", "epiautogp_nssp_epiweekly_pct"),
    ("nssp", "daily", "observed", "none", "epiautogp_nssp_daily"),
    (
        "nssp",
        "daily",
        "other",
        "reporting-delay",
        "epiautogp_nssp_daily_other",
    ),
)


@pytest.mark.model_integration
def test_epiautogp_forecast(pipeline_workspace, monkeypatch, request):
    disease = request.config.getoption("--model-test-disease")
    location = request.config.getoption("--model-test-location")
    configure_data_mode(request, monkeypatch)
    write_hubverse_nowcasts(
        pipeline_workspace,
        nhsn_observation_dates=selected_nhsn_observation_dates(disease, location),
        locations=[location],
        diseases=[disease],
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
    ) in EPIAUTOGP_CONFIGURATIONS:
        run_epiautogp(
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
        [model_name for *_, model_name in EPIAUTOGP_CONFIGURATIONS],
    )
