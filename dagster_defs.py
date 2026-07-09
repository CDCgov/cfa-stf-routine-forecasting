# Basic Imports
import datetime as dt
import logging
import os
import subprocess
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo

# Direct use of dagster
import dagster as dg

# Helper Libraries
from cfa.stf.forecasttools import LOCATION_LIST
from cfa_dagster import (
    ADLS2PickleIOManager,
    ExecutionConfig,
    GraphDimension,
    GraphDimensionExclusion,
    SelectorConfig,
    azure_batch_executor,
    collect_definitions,
    docker_executor,
    dynamic_executor,
    dynamic_graph_asset,
    start_dev_env,
)
from cfa_dagster import (
    is_production as is_prod,
)
from pydantic import BaseModel, Field
from pygit2.repository import Repository
from pyrenew_multisignal.hew.utils import flags_from_hew_letters

# Model Code
from pipelines.fable.forecast_fable import main as forecast_fable
from pipelines.pyrenew_hew.forecast_pyrenew import main as forecast_pyrenew
from pipelines.utils.common_utils import (
    calculate_training_dates,
    create_prop_samples,
    get_model_batch_dir_name,
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)
from pipelines.utils.postprocess_forecast_batches import main as postprocess

# ============================================================================
# DAGSTER INITIALIZATION
# ============================================================================

# function to start the dev server
start_dev_env(__name__)

# get the user running the Dagster instance
user = os.getenv("DAGSTER_USER")

is_production = is_prod()

# ============================================================================
# RUNTIME CONFIGURATION: WORKING DIRECTORY, EXECUTORS, VOLUME MOUNTS
# ============================================================================
# Executors define the runtime-location of an asset job
# See later on for Asset job definitions

# ---------- Working Directory, Branch, and Image Tag ----------

# Instead of hardcoding the repo name, this will always find the containing directory of this defs file
# As of 6/2026, cfa-stf-routine-forecasting
local_workdir = Path(__file__).parent.resolve()  # absolute path to the workdir
container_workdir = Path(
    f"/{local_workdir.name}"
)  # in the container, workdir is mounted at /

# Get branch name from git, defaulting to main if not in a git repo
try:
    current_branch_name = str(Repository(local_workdir).head.shorthand)
    print(f"Branch name from git: {current_branch_name}")
except Exception:
    current_branch_name = "main"
    print("No .git folder detected; using main as the branch name")

# Use 'latest' tag for production or main branch, otherwise use branch name
registry = "cfaprdbatchcr.azurecr.io"
tag = (
    "latest"
    if (is_production or current_branch_name == "main")
    else current_branch_name
)
image = f"{registry}/{local_workdir.name}:{tag}"

# ----------- Azure blob storage mount strings ---------------

# Used in the cloud, and combined with the local mounting dir, used locally
blob_mounts = [
    f"stf-routine-forecasting-config:{container_workdir}/config",
    f"stf-routine-forecasting-prod-output:{container_workdir}/output",
    f"stf-routine-forecasting-test-output:{container_workdir}/test-output",
]

# Used when mounting with docker
local_mounting_dir = f"{local_workdir}/blobfuse/mounts/"

# ---------- Execution Configuration ----------

# Launches locally in a new system process
# Used for lightweight assets and jobs, etc. where volume mounts are not needed
basic_execution_config = ExecutionConfig(
    executor=SelectorConfig(class_name=dg.multiprocess_executor.__name__),
)

# Launches locally, executes in a docker container as configured below
# Allows for rapid local testing in a similar-to-batch environment
docker_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=docker_executor.__name__,
        config={
            "image": image,
            "retries": {"enabled": {}},
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    f"{__file__}:{container_workdir}/{os.path.basename(__file__)}",
                    # blob container mounts for cfa-stf-routine-forecasting
                    # with the docker executor we need the local mounting dir as well
                ]
                + [local_mounting_dir + mount for mount in blob_mounts]
            },
        },
    ),
)

# Cloud execution. This is what we want for any model run.
azure_batch_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "stf-routine-forecasting-pool",
            **(
                {}
                if is_production  # image will come from the code location in prod
                else {"image": image}
            ),
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    # f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    # blob container mounts for cfa-stf-routine-forecasting
                    # we do not need the local mounting dir for azure batch
                ]
                + blob_mounts,
                "working_dir": f"{container_workdir}",
            },
        },
    ),
)

# ============================================================================
# GRAPH DIMENSIONS AND PARTITIONS
# How are the data split and processed in Azure Batch?
# ============================================================================

DEFAULT_EXCLUDED_LOCATIONS = ["AS", "GU", "MP", "PR", "UM", "VI"]
SUPPORTED_DISEASES = ["COVID-19"]

# Disease dimensions
DISEASES = SUPPORTED_DISEASES
Disease = StrEnum("Disease", {v: v for v in DISEASES})

# Location dimensions
LOCATIONS = [
    location for location in LOCATION_LIST if location not in DEFAULT_EXCLUDED_LOCATIONS
]
Location = StrEnum("Location", {v: v for v in LOCATIONS})

# Daily Partitions
tz = "America/New_York"
daily_partitions_def = dg.DailyPartitionsDefinition(
    start_date=dt.datetime.now(ZoneInfo(tz)) - dt.timedelta(days=1),
    end_offset=1,
    timezone=tz,
)

# ============================================================================
# ASSET CONFIGURATIONS
# ============================================================================


# using default_factory to prevent ConfigOverrides from populating fields in the Launchpad
class _ModelTrainingFields(BaseModel):
    output_basedir: str = Field(default_factory=lambda: "")
    n_training_days: int = Field(default_factory=lambda: 0)
    exclude_last_n_days: int = Field(default_factory=lambda: 0)
    fail_on_stale_data: bool = Field(default_factory=lambda: is_production)


class ConfigOverride(_ModelTrainingFields, dg.Config):
    location: Location  # type: ignore[reportInvalidTypeForm]

    def as_dict(self) -> dict:  # type: ignore[reportInvalidTypeForm]
        return self.model_dump(mode="json", exclude_unset=True)


class ModelBaseConfig(_ModelTrainingFields, dg.ConfigurableResource):
    """
    Base configuration for all model assets.
    Contains parameters common to Fable and Pyrenew models.
    """

    output_basedir: str = "output" if is_production else "test-output"
    n_training_days: int = 150
    exclude_last_n_days: int = 1
    fail_on_stale_data: bool = is_production
    diseases: GraphDimension[Disease] = GraphDimension(DISEASES)  # type: ignore[reportInvalidTypeForm]
    locations: GraphDimension[Location] = GraphDimension(LOCATIONS)  # type: ignore[reportInvalidTypeForm]
    # Add defaults here, or add in the launchpad with ctrl+space
    config_overrides: list[ConfigOverride] = Field(
        default=[
            # ConfigOverride(location="GA", exclude_last_n_days=2).as_dict(),
        ],
        description=(
            "Provide location-specific overrides as a list of dicts. "
            "The Launchpad accepts both yaml and json-style lists e.g."
            "config_overrides: [{ location: GA, exclude_last_n_days: 2 }]"
            ""
        ),
    )  # type: ignore[reportInvalidTypeForm]

    def get_by_location(self, loc: Location) -> "ModelBaseConfig":  # type: ignore[reportInvalidTypeForm]
        """Returns location-specific config if provided via config_overrides"""
        overrides = {}
        for entry in self.config_overrides:
            if entry["location"] == loc:
                overrides = {k: v for k, v in entry.items() if k != "location"}
                break
        return self.model_copy(update=overrides)


class FableEOtherConfig(dg.ConfigurableResource):  # used to inherit ModelBaseConfig
    """
    Configuration for fable E-other model assets
    (fable_e_other, epiweekly_fable_e_other).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_samples: int = 400 if not is_production else 2000


class PyrenewConfig(dg.ConfigurableResource):  # used to inherit ModelBaseConfig
    """
    Configuration for Pyrenew model assets (pyrenew_e, pyrenew_h, pyrenew_he, etc.).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_warmup: int = 200 if not is_production else 1000
    n_samples: int = 200 if not is_production else 500
    n_chains: int = 2 if not is_production else 4
    rng_key: int = 12345
    additional_forecast_letters: str = ""


class EModelExclusions(
    dg.ConfigurableResource
):  # used to inherit ModelBaseConfig, used to be called FusionConfig
    # filter out WY
    locations: GraphDimensionExclusion[Location] = GraphDimensionExclusion(["WY"])  # type: ignore[reportInvalidTypeForm]


class WModelExclusions(
    dg.ConfigurableResource
):  # used to inherit PyrenewConfig, used to be called PyrenewWConfig
    # only COVID-19 is valid for W
    diseases: GraphDimension[Disease] = GraphDimension(["COVID-19"])  # type: ignore[reportInvalidTypeForm]


class PostProcessConfig(dg.Config):
    """
    Configuration for the Post-Processing asset.
    """

    output_basedir: str = "output" if is_production else "test-output"
    skip_existing: bool = False
    save_local_copy: bool = False
    local_copy_dir: str = ""  # "stf_forecast_fig_share"
    postprocess_diseases: list[str] = ["COVID-19"]


# ============================================================================
# MODEL CONSTRUCTOR FUNCTIONS - these are used later, in Asset Definitions
# ============================================================================


def _throw_if_backfill(
    context: dg.OpExecutionContext | dg.AssetExecutionContext,
    partition_def: dg.PartitionsDefinition,
):
    current_partition = context.partition_key
    latest_partition = partition_def.get_last_partition_key()
    if current_partition != latest_partition:
        raise RuntimeError("STF forecast models do not support backfills")


def _run_fable_e_other(
    context: dg.OpExecutionContext,
    fable_e_other_config: FableEOtherConfig,
    model_base_config: ModelBaseConfig,
    epiweekly: bool,
) -> str | None:
    """
    Helper function to run fable E-other model with optional epiweekly mode.
    """
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    # we let the user potentially override the basedir,
    # but subdir is locked to the partition date
    daily_forecast_output_dir: Path = Path(
        model_base_config.output_basedir,
        f"{context.partition_key}_forecasts",
    )
    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    context.log.info(f"fable_e_other_config: '{fable_e_other_config}'")
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_fable(
        disease=disease,
        loc=location,
        output_dir=daily_forecast_output_dir,
        n_training_days=model_base_config.n_training_days,
        n_forecast_days=28,
        n_samples=fable_e_other_config.n_samples,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        epiweekly=epiweekly,
        run_date=run_date,
        fail_on_stale_data=model_base_config.fail_on_stale_data,
    )


def _run_pyrenew_model(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
    model_letters: str,
) -> str | None:
    """
    Helper to run Pyrenew models with common arguments.
    """
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    # we let the user potentially override the basedir,
    # but subdir is locked to the partition date
    daily_forecast_output_dir: Path = Path(
        model_base_config.output_basedir, f"{context.partition_key}_forecasts"
    )

    fit_flags = flags_from_hew_letters(model_letters)
    forecast_flags = flags_from_hew_letters(
        f"{model_letters}{pyrenew_config.additional_forecast_letters}",
        flag_prefix="forecast",
    )
    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    context.log.info(f"config: '{pyrenew_config}'")
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_pyrenew(
        disease=disease,
        loc=location,
        priors_path=Path("pipelines/pyrenew_hew/priors/prod_priors.py"),
        output_dir=daily_forecast_output_dir,
        n_training_days=model_base_config.n_training_days,
        n_forecast_days=28,
        n_chains=pyrenew_config.n_chains,
        n_warmup=pyrenew_config.n_warmup,
        n_samples=pyrenew_config.n_samples,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        rng_key=pyrenew_config.rng_key,
        run_date=run_date,
        fail_on_stale_data=model_base_config.fail_on_stale_data,
        **fit_flags,
        **forecast_flags,
    )


def get_model_loc_dir(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
) -> Path:
    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    report_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    first_training_date, last_training_date = calculate_training_dates(
        report_date=report_date,
        n_training_days=model_base_config.n_training_days,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        logger=context.log,
    )

    model_batch_dir_name = get_model_batch_dir_name(
        disease=disease,
        report_date=report_date,
        first_training_date=first_training_date,
        last_training_date=last_training_date,
    )

    model_loc_dir = Path(
        model_base_config.output_basedir,
        f"{context.partition_key}_forecasts",
        model_batch_dir_name,
        "model_runs",
        location,
    )
    return model_loc_dir


def _run_fusion_model(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    num_model_name,
    other_model_name,
    aggregate_num,
    aggregate_other,
    fusion_model_name,
) -> str | None:
    """
    Helper function to run fusion model.
    """
    _throw_if_backfill(context, daily_partitions_def)
    model_loc_dir = get_model_loc_dir(context, model_base_config)
    create_prop_samples(
        model_run_dir=model_loc_dir,
        num_model_name=num_model_name,
        other_model_name=other_model_name,
        aggregate_num=aggregate_num,
        aggregate_other=aggregate_other,
        save=True,
    )

    fusion_model_fit_dir = Path(model_loc_dir, fusion_model_name)

    make_figures_from_model_fit_dir(fusion_model_fit_dir)

    make_figures_from_model_fit_dir(
        fusion_model_fit_dir,
        save_figs=True,
        save_ci=True,
    )
    model_fit_dir_to_hub_tbl(fusion_model_fit_dir)

    context.log.debug(f"config: '{model_base_config}'")


def _fuse_pyrenew_fable_e_other(
    context,
    model_base_config: ModelBaseConfig,
    pyrenew_model_name,
    epiweekly: bool,
):
    other_model_name = "epiweekly_fable_e_other" if epiweekly else "daily_fable_e_other"
    fusion_model_name = (
        f"prop_epiweekly_aggregated_{pyrenew_model_name}_epiweekly_fable_e_other"
        if epiweekly
        else f"prop_{pyrenew_model_name}_daily_fable_e_other"
    )
    aggregate_num = epiweekly
    _run_fusion_model(
        context=context,
        model_base_config=model_base_config,
        num_model_name=pyrenew_model_name,
        other_model_name=other_model_name,
        aggregate_num=aggregate_num,
        aggregate_other=False,
        fusion_model_name=fusion_model_name,
    )


# ============================================================================
# SCHEDULES AND AUTOMATION CONDITION SENSORS
# ============================================================================


# Custom Automation Condition. Relies on use_user_code_server=True on the sensor
class IsWeekday(dg.AutomationCondition):
    def __init__(self, weekday: int):
        """
        Check if evaluation time falls on a specific weekday.
        This is is a simple evaluation, rather than a stateful operation,
        such as with cron_tick_passed().

        Args:
            weekday: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday,
                    4=Friday, 5=Saturday, 6=Sunday
        """
        self.weekday = weekday
        super().__init__()

    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        # If the current weekday is equal to the desired weekday,
        # return the candidate_subset -> a dagster context's "true" case
        if context.evaluation_time.weekday() == self.weekday:
            true_subset = context.candidate_subset
        else:
            true_subset = context.get_empty_subset()

        return dg.AutomationResult(context=context, true_subset=true_subset)

    @property
    def name(self) -> str:
        """Define the label that will appear in the UI"""
        days = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        return f"is_{days[self.weekday].lower()}"


weekly_forecast_initial_sensor = dg.AutomationConditionSensorDefinition(
    name="WeeklyForecastInitial",
    target=dg.AssetSelection.groups("WeeklyForecastInitial"),
    use_user_code_server=True,  # allows for custom automation conditions
)

weekly_forecast_fusion_sensor = dg.AutomationConditionSensorDefinition(
    name="WeeklyForecastFusion",
    target=dg.AssetSelection.groups("WeeklyForecastFusion"),
    use_user_code_server=False,  # does NOT allow custom conditions
)

# ---------- Shared Asset Decorator Arguments ----------

# All of our forecast assets should materialize with the same
# partitions, graph_dimensions, automation conditions, and asset groups
# The only thing that differs between them are their dependencies

weekly_forecast_base_asset_args = {
    "partitions_def": daily_partitions_def,
}

weekly_forecast_initial_asset_args = {
    **weekly_forecast_base_asset_args,
    "group_name": "WeeklyForecastInitial",
    "automation_condition": (
        # We specifically don't want these to run unless it's Wednesday
        # 0=monday,1=tuesday,2=wednesday,etc.
        # Note this is different from cron which is 1-indexed
        dg.AutomationCondition.eager() & IsWeekday(2)
    ).with_label("eager_on_wed"),
}

weekly_forecast_fusion_asset_args = {
    **weekly_forecast_base_asset_args,
    "group_name": "WeeklyForecastFusion",
    # we want vanilla eager for the fusion assets
    "automation_condition": dg.AutomationCondition.eager(),
}

# ============================================================================
# ASSET DEFINITIONS
# ============================================================================
# These are the core of Dagster - functions that specify data

# ---------- External Asset Specs -------------

# These allow us to model external assets we do not have locally
# while in development. They do not materialize.
# They are replaced with true assets in production where
# other code locations are able to be referenced.

nssp_gold_v1 = dg.AssetSpec(
    "nssp_gold_v1", partitions_def=daily_partitions_def, group_name="Upstream"
)

nhsn_hrd_prelim = dg.AssetSpec(
    "nhsn_hrd_prelim", partitions_def=daily_partitions_def, group_name="Upstream"
)


# ---------------- Weekly Forecasts --------------


# Fable E Other
@dynamic_graph_asset(
    **weekly_forecast_initial_asset_args,
    ins={"nssp_gold_v1": dg.In(dg.Nothing)},
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
        epiweekly=False,
    )


# Epiweekly Fable E Other
@dynamic_graph_asset(
    **weekly_forecast_initial_asset_args,
    ins={"nssp_gold_v1": dg.In(dg.Nothing)},
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
        epiweekly=True,
    )


# Pyrenew E
@dynamic_graph_asset(
    **weekly_forecast_initial_asset_args,
    ins={
        "nssp_gold_v1": dg.In(dg.Nothing),
    },
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
    **weekly_forecast_initial_asset_args,
    ins={
        "nhsn_hrd_prelim": dg.In(dg.Nothing),
    },
)
def pyrenew_h(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
):
    _run_pyrenew_model(context, pyrenew_config, model_base_config, "h")


# Pyrenew HE
@dynamic_graph_asset(
    **weekly_forecast_initial_asset_args,
    ins={
        "nssp_gold_v1": dg.In(dg.Nothing),
        "nhsn_hrd_prelim": dg.In(dg.Nothing),
    },
)
def pyrenew_he(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
    e_model_exclusions: EModelExclusions,
):
    _run_pyrenew_model(context, pyrenew_config, model_base_config, "he")


# ---------- Fusion Forecasts ----------


@dynamic_graph_asset(
    **weekly_forecast_fusion_asset_args,
    ins={"pyrenew_e": dg.In(dg.Nothing), "fable_e_other": dg.In(dg.Nothing)},
)
def fuse_pyrenew_e_ts(
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
    **weekly_forecast_fusion_asset_args,
    ins={
        "pyrenew_e": dg.In(dg.Nothing),
        "epiweekly_fable_e_other": dg.In(dg.Nothing),
    },
)
def fuse_pyrenew_e_ts_epiweekly(
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
    **weekly_forecast_fusion_asset_args,
    ins={"pyrenew_he": dg.In(dg.Nothing), "fable_e_other": dg.In(dg.Nothing)},
)
def fuse_pyrenew_he_ts(
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
    **weekly_forecast_fusion_asset_args,
    ins={
        "pyrenew_he": dg.In(dg.Nothing),
        "epiweekly_fable_e_other": dg.In(dg.Nothing),
    },
)
def fuse_pyrenew_he_ts_epiweekly(
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


# ---------- Postprocessing Forecast Batches ----------


@dg.asset(
    deps=[
        "fuse_pyrenew_e_ts",
        "fuse_pyrenew_e_ts_epiweekly",
        "fuse_pyrenew_he_ts",
        "fuse_pyrenew_he_ts_epiweekly",
        "pyrenew_h",
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
    group_name="WeeklyForecastFusion",
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


# ============================================================================
# JOBS AND OPS
# These can create images.
# ============================================================================

# These are only used in dev - they should not appear on the production webserver
if not is_production:
    # Build and Push Image ---------------------------

    @dg.op
    def build_image_op(
        context: dg.OpExecutionContext,
        should_push: bool,
        should_deploy_to_prod: bool,
        dockerfile_path: str,
        build_context: str,
        image: str,
    ):
        """
        Builds the image used by dagster. Requires that your VM be registered with an Azure managed identity.

        should_push: bool - should the image be pushed to the Container Registry?
        should_deploy_to_prod: bool - should the prod server be updated with the newest image? (usually you do not want to do this)
        dockerfile_path: str - where is the Dockerfile located locally? (has a default)
        build_context: str - where should we build from? (has a default)
        image: str - the full name (including registry and tag) of the image
        """

        build_command = [
            "docker",
            "buildx",
            "build",
            "-t",
            image,
            "-f",
            dockerfile_path,
            build_context,
        ]

        if should_push:
            subprocess.run(
                ["az", "login", "--identity"],
                check=True,
            )
            subprocess.run(["az", "acr", "login", "-n", registry], check=True)
            build_command.append("--push")

        context.log.info(f"Running {' '.join(build_command)}")
        subprocess.run(build_command, check=True)

        update_script_url = (
            # repo
            "https://raw.githubusercontent.com/CDCgov/cfa-dagster/"
            # ref
            "refs/heads/main/"
            # file
            "scripts/update_code_location.py"
        )

        if should_deploy_to_prod:
            context.log.info(f"Deploying {image} to the dagster prod server.")
            subprocess.run(
                ["uv", "run", update_script_url, "--registry_image", image], check=True
            )

    @dg.job(
        description=(
            "Build the container image used by dagster to run this project's asset pipelines."
            "Run after making any change and before running the pipelines."
        ),
        config=dg.RunConfig(
            ops={
                "build_image_op": {
                    "inputs": {
                        "should_push": True,
                        "should_deploy_to_prod": False,
                        "dockerfile_path": f"{local_workdir}/Dockerfile",
                        # the build context should be the top level of the repo
                        "build_context": str(local_workdir),
                        "image": image,
                    }
                }
            },
            # configure this job to run on your computer
            execution=basic_execution_config.to_run_config(),
        ),
        executor_def=dynamic_executor(),
    )
    def build_image():
        build_image_op()

    # Explore the image you built as it will be run with dagster ---------------------------

    @dg.op
    def explore_image_op(
        context: dg.OpExecutionContext,
    ):
        """
        Allows you to run the container you previously built and explore the filesystem that will be used by dagster.
        """
        context.log.info(
            "Check the terminal from which you ran the webserver to interact; stdout from your terminal will appear below."
        )
        explore_cmd = (
            ["docker", "run", "-it"]
            + [
                item
                for mount in blob_mounts
                for item in ("-v", local_mounting_dir + mount)
            ]
            + ["--rm", image, "bash"]
        )
        subprocess.run(explore_cmd, check=True)

    @dg.job(
        description=(
            "Interactively navigate the filesystem of your last-built container, "
            "as it would be used in Docker or Azure Batch execution."
        ),
        executor_def=dg.in_process_executor,
    )
    def explore_image():
        explore_image_op()

# ============================================================================
# DAGSTER DEFINITIONS OBJECT
# ============================================================================
# This code allows us to collect all of the above definitions into a single
# Definitions object for Dagster to read. By doing this, we can keep our
# Dagster code in a single file instead of splitting it across multiple files.

# collect Dagster definitions from the current file
collected_defs = collect_definitions(globals())

# Set Azure HTTP Logging Level
# this will limit excessive IO logs in stderr
# for any assets making azure http requests
azure_http_logger = logging.getLogger(
    "azure.core.pipeline.policies.http_logging_policy"
)
azure_http_logger.setLevel(logging.WARNING)

# Create Definitions object
defs = dg.Definitions(
    **collected_defs,
    resources={
        # These IOManagers let Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # Shared resources for model assets
        "model_base_config": ModelBaseConfig(),
        "pyrenew_config": PyrenewConfig(),
        "fable_e_other_config": FableEOtherConfig(),
        "e_model_exclusions": EModelExclusions(),
        "w_model_exclusions": WModelExclusions(),
    },
    executor=dynamic_executor(
        default_config=azure_batch_execution_config,
        # default_config=basic_execution_config,
        # default_config=docker_execution_config,
        alternate_configs=[
            basic_execution_config,
            docker_execution_config,
            azure_batch_execution_config,
        ],
    ),
)
