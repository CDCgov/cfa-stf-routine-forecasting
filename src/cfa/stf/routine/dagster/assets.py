from pathlib import Path

import dagster as dg
from cfa_dagster import dynamic_graph_asset

from cfa.stf.routine.dagster.asset_config import (
    EModelExclusions,
    EpiAutoGPEPctEpiweeklyConfig,
    FableEOtherConfig,
    ModelBaseConfig,
    PostProcessConfig,
    PyrenewConfig,
    daily_partitions_def,
)
from cfa.stf.routine.dagster.asset_helpers import (
    _fuse_pyrenew_fable_e_other,
    _run_epiautogp_e_pct_epiweekly,
    _run_fable_e_other,
    _run_pyrenew_model,
    _throw_if_backfill,
)
from cfa.stf.routine.dagster.automation import eager_on_wednesday
from cfa.stf.routine.utils.postprocess_forecast_batches import main as postprocess

# Shared Asset Decorator Arguments

# It's helpful (and helps reduce DRY issues) to specify some common
# arguments that we give to the asset decorators, as well as some tags

common_asset_args = {
    "partitions_def": daily_partitions_def,  # every asset uses this partitions def
    "retry_policy": dg.RetryPolicy(),  # allow the assets to retry once on failure
}

# Dagster tag keys cannot contain spaces. These tags make it easy to select all
# assets that need rerunning after their corresponding source data changes.
E_DATA_RERUN_TAGS = {
    "e-data-rerun": "",
    "e-data-rerun-no-epiautogp": "",
}
EPIAUTOGP_E_DATA_RERUN_TAGS = {"e-data-rerun": ""}
H_DATA_RERUN_TAGS = {"h-data-rerun": ""}
HE_DATA_RERUN_TAGS = E_DATA_RERUN_TAGS | H_DATA_RERUN_TAGS

# ============================================================================
# ASSET DEFINITIONS
# ============================================================================

# External Asset Specs

# These allow us to model external assets we do not have locally
# while in development. They do not materialize.
# They are replaced with true assets in production where
# other code locations are able to be referenced.

comprehensive_nssp_gold = dg.AssetSpec(
    "comprehensive_nssp_gold",
    partitions_def=daily_partitions_def,
    group_name="Upstream",
)

nhsn_hrd_prelim = dg.AssetSpec(
    "nhsn_hrd_prelim", partitions_def=daily_partitions_def, group_name="Upstream"
)


# Forecast Assets


# Fable E Other
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="Fable",
    ins={"comprehensive_nssp_gold": dg.In(dg.Nothing)},
    tags=E_DATA_RERUN_TAGS,
)
def fable_e_other(
    context: dg.OpExecutionContext,
    fable_e_other_config: FableEOtherConfig,
    model_base_config: ModelBaseConfig,
):
    _run_fable_e_other(
        context,
        fable_e_other_config,
        model_base_config,
        ed_visit_input_resolution="daily",
    )


# Epiweekly Fable E Other
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="Fable",
    ins={"comprehensive_nssp_gold": dg.In(dg.Nothing)},
    tags=E_DATA_RERUN_TAGS,
)
def epiweekly_fable_e_other(
    context: dg.OpExecutionContext,
    fable_e_other_config: FableEOtherConfig,
    model_base_config: ModelBaseConfig,
):
    _run_fable_e_other(
        context,
        fable_e_other_config,
        model_base_config,
        ed_visit_input_resolution="epiweekly",
    )


# Pyrenew E
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="Pyrenew",
    ins={
        "comprehensive_nssp_gold": dg.In(dg.Nothing),
    },
    tags=E_DATA_RERUN_TAGS,
)
def pyrenew_e(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _run_pyrenew_model(context, pyrenew_config, model_base_config, "e")


# Pyrenew H
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="Pyrenew",
    ins={
        "nhsn_hrd_prelim": dg.In(dg.Nothing),
    },
    tags=H_DATA_RERUN_TAGS,
)
def pyrenew_h(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
):
    _run_pyrenew_model(context, pyrenew_config, model_base_config, "h")


# Pyrenew HE
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="Pyrenew",
    ins={
        "comprehensive_nssp_gold": dg.In(dg.Nothing),
        "nhsn_hrd_prelim": dg.In(dg.Nothing),
    },
    tags=HE_DATA_RERUN_TAGS,
)
def pyrenew_he(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _run_pyrenew_model(context, pyrenew_config, model_base_config, "he")


# EpiAutoGP E-pct (epiweekly)
@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=eager_on_wednesday,
    group_name="EpiAutoGP",
    ins={"comprehensive_nssp_gold": dg.In(dg.Nothing)},
    tags=EPIAUTOGP_E_DATA_RERUN_TAGS,
)
def epiautogp_e_pct_epiweekly(
    context: dg.OpExecutionContext,
    epiautogp_e_pct_epiweekly_config: EpiAutoGPEPctEpiweeklyConfig,
    model_base_config: ModelBaseConfig,
):
    _run_epiautogp_e_pct_epiweekly(
        context,
        epiautogp_e_pct_epiweekly_config,
        model_base_config,
    )


# Fusion Assets


@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=dg.AutomationCondition.eager(),
    group_name="Fusion",
    ins={"pyrenew_e": dg.In(dg.Nothing), "fable_e_other": dg.In(dg.Nothing)},
    tags=E_DATA_RERUN_TAGS,
)
def fuse_pyrenew_e_fable(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _fuse_pyrenew_fable_e_other(
        context,
        model_base_config,
        pyrenew_model_name="pyrenew_e",
        epiweekly=False,
    )


@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=dg.AutomationCondition.eager(),
    group_name="Fusion",
    ins={
        "pyrenew_e": dg.In(dg.Nothing),
        "epiweekly_fable_e_other": dg.In(dg.Nothing),
    },
    tags=E_DATA_RERUN_TAGS,
)
def fuse_pyrenew_e_fable_epiweekly(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _fuse_pyrenew_fable_e_other(
        context,
        model_base_config,
        pyrenew_model_name="pyrenew_e",
        epiweekly=True,
    )


@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=dg.AutomationCondition.eager(),
    group_name="Fusion",
    ins={"pyrenew_he": dg.In(dg.Nothing), "fable_e_other": dg.In(dg.Nothing)},
    tags=HE_DATA_RERUN_TAGS,
)
def fuse_pyrenew_he_fable(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _fuse_pyrenew_fable_e_other(
        context,
        model_base_config,
        pyrenew_model_name="pyrenew_he",
        epiweekly=False,
    )


@dynamic_graph_asset(
    **common_asset_args,
    automation_condition=dg.AutomationCondition.eager(),
    group_name="Fusion",
    ins={
        "pyrenew_he": dg.In(dg.Nothing),
        "epiweekly_fable_e_other": dg.In(dg.Nothing),
    },
    tags=HE_DATA_RERUN_TAGS,
)
def fuse_pyrenew_he_fable_epiweekly(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _fuse_pyrenew_fable_e_other(
        context,
        model_base_config,
        pyrenew_model_name="pyrenew_he",
        epiweekly=True,
    )


# Postprocessing Asset


@dg.asset(
    deps=[
        "fuse_pyrenew_e_fable",
        "fuse_pyrenew_e_fable_epiweekly",
        "fuse_pyrenew_he_fable",
        "fuse_pyrenew_he_fable_epiweekly",
        "pyrenew_h",
        "epiautogp_e_pct_epiweekly",
    ],
    partitions_def=daily_partitions_def,
    # Runs when any dependency has been updated as long as at least one exists
    automation_condition=(
        dg.AutomationCondition.eager().replace(
            old=~dg.AutomationCondition.any_deps_missing(),
            new=dg.AutomationCondition.any_deps_match(
                ~dg.AutomationCondition.missing()
                | dg.AutomationCondition.will_be_requested()
            ),
        )
    ).with_label("postprocess_custom_eager"),
    group_name="Postprocess",
    retry_policy=dg.RetryPolicy(),  # allow the asset to retry once on failure
    tags=HE_DATA_RERUN_TAGS,
)
def postprocess_forecasts(
    context: dg.AssetExecutionContext,
    config: PostProcessConfig,
):
    """
    Postprocess forecast batches
    """

    _throw_if_backfill(context, daily_partitions_def)

    daily_forecast_output_dir: Path = Path(
        config.output_basedir, f"{context.partition_key}_forecasts"
    )

    context.log.info(f"config: '{config}'")
    postprocess(
        base_forecast_dir=daily_forecast_output_dir,
        diseases=config.postprocess_diseases,
        skip_existing=config.skip_existing,
        local_copy_dir=daily_forecast_output_dir,
    )
