"""Regression tests for automation after an upstream model run terminates."""

import dagster as dg
import pytest
from dagster._core.storage.dagster_run import DagsterRunStatus

from cfa.stf.routine.dagster.defs.asset_config import OutputConfig, daily_partitions_def
from cfa.stf.routine.dagster.defs.assets import postprocess_forecasts


@pytest.mark.parametrize("terminal_status", ["failed", "canceled"])
@pytest.mark.parametrize("has_completed_input", [False, True])
def test_postprocess_after_nowcastautogp_terminates(
    terminal_status, has_completed_input
):
    # Simulate the upstream run lifecycle without fitting every location/disease.
    @dg.asset(name="nowcastautogp_e_pct_epiweekly", partitions_def=daily_partitions_def)
    def pending_gp():
        pass

    key = dg.AssetKey("postprocess_forecasts")
    gp_key = pending_gp.key
    deps = postprocess_forecasts.asset_deps[key]
    defs = dg.Definitions(
        assets=[
            postprocess_forecasts,
            pending_gp,
            *[
                dg.AssetSpec(dep, partitions_def=daily_partitions_def)
                for dep in deps - {gp_key}
            ],
        ],
        jobs=[dg.define_asset_job("gp", selection=dg.AssetSelection.assets(gp_key))],
        resources={"output_config": OutputConfig()},
    )
    partition = daily_partitions_def.get_last_partition_key()

    with dg.DagsterInstance.ephemeral() as instance:
        cursor = None

        def tick():
            nonlocal cursor
            result = dg.evaluate_automation_conditions(
                defs,
                instance=instance,
                asset_selection=dg.AssetSelection.assets(key),
                cursor=cursor,
            )
            cursor = result.cursor
            return result.get_requested_partitions(key)

        assert tick() == set()
        run = instance.create_run_for_job(
            defs.resolve_job_def("gp"),
            status=DagsterRunStatus.STARTED,
            tags={"dagster/partition": partition},
            asset_selection={gp_key},
        )
        if has_completed_input:
            instance.report_runless_asset_event(
                dg.AssetMaterialization(asset_key="fable_e_other", partition=partition)
            )
        assert tick() == set()

        if terminal_status == "failed":
            instance.report_run_failed(run)
        else:
            instance.report_run_canceled(run)

        assert tick() == ({partition} if has_completed_input else set())
