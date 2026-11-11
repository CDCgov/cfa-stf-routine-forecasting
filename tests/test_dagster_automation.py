import dagster as dg
import pytest

from cfa.stf.routine.dagster.defs import assets, automation
from cfa.stf.routine.dagster.defs.asset_config import OutputConfig, daily_partitions_def


@pytest.mark.parametrize(
    "model,expected_fusions",
    [
        (
            "fable_e_other",
            {"fuse_pyrenew_e_fable", "fuse_pyrenew_he_fable"},
        ),
        (
            "epiweekly_fable_e_other",
            {"fuse_pyrenew_e_fable_epiweekly", "fuse_pyrenew_he_fable_epiweekly"},
        ),
        (
            "pyrenew_e",
            {"fuse_pyrenew_e_fable", "fuse_pyrenew_e_fable_epiweekly"},
        ),
        (
            "pyrenew_he",
            {"fuse_pyrenew_he_fable", "fuse_pyrenew_he_fable_epiweekly"},
        ),
    ],
)
def test_standalone_model_triggers_fusion_then_postprocessing(model, expected_fusions):
    downstream = [
        assets.fuse_pyrenew_e_fable,
        assets.fuse_pyrenew_e_fable_epiweekly,
        assets.fuse_pyrenew_he_fable,
        assets.fuse_pyrenew_he_fable_epiweekly,
        assets.postprocess_forecasts,
    ]
    downstream_keys = {asset.key for asset in downstream}
    upstream_keys = {
        key
        for asset in downstream
        for key in asset.asset_deps[asset.key]
        if key not in downstream_keys
    }
    defs = dg.Definitions(
        assets=[
            *downstream,
            *[
                dg.AssetSpec(key, partitions_def=daily_partitions_def)
                for key in upstream_keys
            ],
        ],
        resources={"output_config": OutputConfig()},
    )
    partition = daily_partitions_def.get_last_partition_key()
    postprocess_key = assets.postprocess_forecasts.key

    with dg.DagsterInstance.ephemeral() as instance:
        # Evaluate separately, matching the production sensors' independent cursors.
        def evaluate(sensor, previous=None):
            return dg.evaluate_automation_conditions(
                defs=defs,
                instance=instance,
                asset_selection=sensor.asset_selection,
                cursor=previous.cursor if previous else None,
            )

        def complete(key):
            instance.report_runless_asset_event(
                dg.AssetMaterialization(asset_key=key, partition=partition)
            )

        fusion = evaluate(automation.fusion_sensor)
        postprocess = evaluate(automation.postprocess_sensor)
        assert fusion.total_requested == postprocess.total_requested == 0

        # Both the initial standalone run and a later rerun must propagate.
        # All other upstream models remain missing throughout the test.
        for _ in range(2):
            complete(dg.AssetKey(model))
            fusion = evaluate(automation.fusion_sensor, fusion)
            assert fusion.total_requested == len(expected_fusions)
            for name in expected_fusions:
                assert fusion.get_requested_partitions(dg.AssetKey(name)) == {partition}

            # Models are also direct dependencies of postprocessing.
            postprocess = evaluate(automation.postprocess_sensor, postprocess)
            assert postprocess.get_requested_partitions(postprocess_key) == {partition}
            complete(postprocess_key)
            postprocess = evaluate(automation.postprocess_sensor, postprocess)
            assert postprocess.total_requested == 0

            # A subsequent fusion completion must trigger postprocessing again.
            for name in expected_fusions:
                complete(dg.AssetKey(name))
            postprocess = evaluate(automation.postprocess_sensor, postprocess)
            assert postprocess.get_requested_partitions(postprocess_key) == {partition}
            complete(postprocess_key)
            postprocess = evaluate(automation.postprocess_sensor, postprocess)
            fusion = evaluate(automation.fusion_sensor, fusion)
            assert postprocess.total_requested == fusion.total_requested == 0
