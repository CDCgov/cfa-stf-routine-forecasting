# Basic Imports
import datetime as dt
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from cfa_dagster import (
    ADLS2PickleIOManager,
    DynamicGraphAssetExecutionContext,
    ExecutionConfig,
    SelectorConfig,
    azure_batch_executor,
    azure_container_app_job_executor,
    collect_definitions,
    docker_executor,
    dynamic_executor,
    dynamic_graph_asset,
    start_dev_env,
)
from dagster_azure.blob import (
    AzureBlobStorageDefaultCredential,
    AzureBlobStorageResource,
)

# Helper Libraries
from forecasttools import location_table
from pygit2.repository import Repository
from pyrenew_multisignal.hew.utils import flags_from_hew_letters

# Direct use of dagster
import dagster as dg

# Model Code
from pipelines.fable.forecast_timeseries import main as forecast_timeseries
from pipelines.pyrenew_hew.forecast_pyrenew import main as forecast_pyrenew
from pipelines.utils.postprocess_forecast_batches import main as postprocess

# ============================================================================
# DAGSTER INITIALIZATION
# ============================================================================

# function to start the dev server
start_dev_env(__name__)

# shared time helpers
DEFAULT_EXCLUDED_LOCATIONS = ["AS", "GU", "MP", "PR", "UM", "VI"]
SUPPORTED_DISEASES = ["COVID-19", "Influenza", "RSV"]

# env variable set by Dagster CLI
is_production: bool = not os.getenv("DAGSTER_IS_DEV_CLI")

# get the user running the Dagster instance
user = os.getenv("DAGSTER_USER")

# ============================================================================
# RUNTIME CONFIGURATION: WORKING DIRECTORY, EXECUTORS
# ============================================================================
# Executors define the runtime-location of an asset job
# See later on for Asset job definitions

# ---------- Working Directory, Branch, and Image Tag ----------

workdir = "cfa-stf-routine-forecasting"
local_workdir = Path(__file__).parent.resolve()

# If the tag is prod, use 'latest'.
# Else iteratively test on our dev images
# (You can always manually specify an override in the GUI)
try:
    print("You are running inside a .git repository; getting branchname from .git")
    repo = Repository(os.getcwd())
    current_branch_name = str(repo.head.shorthand)
    git_commit_sha = str(repo.head.target)
except Exception:
    print(
        "No .git folder detected; attempting to get branch name from build-arg $GIT_BRANCH_NAME"
    )
    current_branch_name = os.getenv("GIT_BRANCH_NAME", "unknown_branch")
    git_commit_sha = os.getenv("GIT_COMMIT_SHA", "unknown_commit_hash")

print(f"Current branch name is {current_branch_name}")

tag = (
    "latest"
    if (is_production or current_branch_name == "main")
    else current_branch_name
)
image = f"ghcr.io/cdcgov/cfa-stf-routine-forecasting:{tag}"

# ---------- Execution Configuration ----------

# Most basic execution - in dev, launches and runs locally
# In prod, launches on the code location but runs in Azure Container App Jobs
# Used for lightweight assets and jobs, etc. where volume mounts are not needed
basic_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_container_app_job_executor.__name__
        if is_production
        else dg.multiprocess_executor.__name__
    ),
)

# Launches locally, executes in a docker container as configured below
# Allows for rapid local testing in a similar-to-batch environment
docker_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=docker_executor.__name__,
        config={
            "image": image,
            "env_vars": [
                f"DAGSTER_USER={user}",
                "VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv",
            ],
            "retries": {"enabled": {}},
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    f"{__file__}:/{workdir}/{os.path.basename(__file__)}",
                    # blob container mounts for cfa-stf-routine-forecasting
                    f"{local_workdir}/blobfuse/mounts/nssp-archival-vintages:/cfa-stf-routine-forecasting/nssp-archival-vintages",
                    f"{local_workdir}/blobfuse/mounts/nssp-etl:/cfa-stf-routine-forecasting/nssp-etl",
                    f"{local_workdir}/blobfuse/mounts/nwss-vintages:/cfa-stf-routine-forecasting/nwss-vintages",
                    f"{local_workdir}/blobfuse/mounts/params:/cfa-stf-routine-forecasting/params",
                    f"{local_workdir}/blobfuse/mounts/config:/cfa-stf-routine-forecasting/config",
                    f"{local_workdir}/blobfuse/mounts/output:/cfa-stf-routine-forecasting/output",
                    f"{local_workdir}/blobfuse/mounts/test-output:/cfa-stf-routine-forecasting/test-output",
                ]
            },
        },
    ),
)

# Cloud execution. This is what we want for any model run.
azure_batch_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "pyrenew-dagster-pool",
            **(
                {}
                if is_production  # image will come from the code location in prod
                else {"image": image}
            ),
            "env_vars": [
                "VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv",
            ],
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    # f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    # blob container mounts for cfa-stf-routine-forecasting
                    "nssp-archival-vintages:/cfa-stf-routine-forecasting/nssp-archival-vintages",
                    "nssp-etl:/cfa-stf-routine-forecasting/nssp-etl",
                    "nwss-vintages:/cfa-stf-routine-forecasting/nwss-vintages",
                    "prod-param-estimates:/cfa-stf-routine-forecasting/params",
                    "pyrenew-hew-config:/cfa-stf-routine-forecasting/config",
                    "pyrenew-hew-prod-output:/cfa-stf-routine-forecasting/output",
                    "pyrenew-test-output:/cfa-stf-routine-forecasting/test-output",
                ],
                "working_dir": "/cfa-stf-routine-forecasting",
            },
        },
    ),
)

# ============================================================================
# GRAPH DIMENSIONS AND PARTITIONS
# ============================================================================
# How are the data split and processed in Azure Batch?

# Disease dimensions
DISEASES = SUPPORTED_DISEASES

# Location dimensions
RAW_LOCATIONS = location_table.get_column("short_name").to_list()
LOCATIONS = [
    location for location in RAW_LOCATIONS if location not in DEFAULT_EXCLUDED_LOCATIONS
]

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


class ModelBaseConfig(dg.Config):
    """
    Base configuration for all model assets.
    Contains parameters common to both Timeseries and Pyrenew models.
    """

    output_basedir: str = "output" if is_production else "test-output"
    n_training_days: int = 150
    exclude_last_n_days: int = 1
    diseases: list[str] = DISEASES
    locations: list[str] = LOCATIONS


class TimeseriesConfig(ModelBaseConfig):
    """
    Configuration for timeseries model assets (timeseries_e, epiweekly_timeseries_e).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_samples: int = 400 if not is_production else 2000  # Total samples for timeseries


class PyrenewConfig(ModelBaseConfig):
    """
    Configuration for Pyrenew model assets (pyrenew_e, pyrenew_h, pyrenew_he, etc.).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_warmup: int = 200 if not is_production else 1000
    n_samples: int = 200 if not is_production else 500
    n_chains: int = 2 if not is_production else 4
    rng_key: int = 12345
    additional_forecast_letters: str = ""


class PyrenewEConfig(PyrenewConfig):
    # filter out WY
    locations: list[str] = [loc for loc in LOCATIONS if loc != "WY"]


class PyrenewWConfig(PyrenewConfig):
    # only COVID-19 is valid for W
    diseases: list[str] = ["COVID-19"]


class PyrenewEWConfig(PyrenewConfig):
    # only COVID-19 is valid for W
    diseases: list[str] = ["COVID-19"]
    # filter out WY
    locations: list[str] = [loc for loc in LOCATIONS if loc != "WY"]


class PostProcessConfig(dg.Config):
    """
    Configuration for the Post-Processing asset.
    """

    output_basedir: str = "output" if is_production else "test-output"
    skip_existing: bool = False
    save_local_copy: bool = False
    local_copy_dir: str = ""  # "stf_forecast_fig_share"
    postprocess_diseases: list[str] = ["COVID-19", "Influenza", "RSV"]


# ============================================================================
# ASSET DEFINITIONS
# ============================================================================
# These are the core of Dagster - functions that specify data

# ----------- Model Constructor Functions --------------------------


def _throw_if_backfill(
    context: DynamicGraphAssetExecutionContext | dg.AssetExecutionContext,
    partition_def: dg.PartitionsDefinition,
):
    current_partition = context.partition_key
    latest_partition = partition_def.get_last_partition_key()
    if current_partition != latest_partition:
        raise RuntimeError("STF forecast models do not support backfills")


def _run_timeseries_e(
    context: DynamicGraphAssetExecutionContext,
    config: TimeseriesConfig,
    epiweekly: bool,
) -> str | None:
    """
    Helper function to run timeseries-e model with optional epiweekly mode.
    """
    _throw_if_backfill(context, daily_partitions_def)

    disease = context.graph_dimension["diseases"]
    location = context.graph_dimension["locations"]

    # we let the user potentially override the basedir,
    # but subdir is locked to the partition date
    daily_forecast_output_dir: Path = Path(
        f"{config.output_basedir}/{context.partition_key}_forecasts"
    )

    context.log.info(f"config: '{config}'")
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_timeseries(
        disease=disease,
        loc=location,
        facility_level_nssp_data_dir=Path("nssp-etl/gold"),
        output_dir=daily_forecast_output_dir,
        n_training_days=config.n_training_days,
        n_forecast_days=28,
        n_samples=config.n_samples,
        exclude_last_n_days=config.exclude_last_n_days,
        epiweekly=epiweekly,
        credentials_path=Path("config/creds.toml"),
    )


def _run_pyrenew_model(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    model_letters: str,
) -> str | None:
    """
    Helper to run Pyrenew models with common arguments.
    """
    _throw_if_backfill(context, daily_partitions_def)

    disease = context.graph_dimension["diseases"]
    location = context.graph_dimension["locations"]

    # we let the user potentially override the basedir,
    # but subdir is locked to the partition date
    daily_forecast_output_dir: Path = Path(
        f"{config.output_basedir}/{context.partition_key}_forecasts"
    )

    fit_flags = flags_from_hew_letters(model_letters)
    forecast_flags = flags_from_hew_letters(
        f"{model_letters}{config.additional_forecast_letters}",
        flag_prefix="forecast",
    )
    context.log.info(f"config: '{config}'")
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_pyrenew(
        disease=disease,
        loc=location,
        facility_level_nssp_data_dir=Path("nssp-etl/gold"),
        nwss_data_dir=Path("nwss-vintages"),
        param_data_dir=Path("params"),
        priors_path=Path("pipelines/priors/prod_priors.py"),
        output_dir=daily_forecast_output_dir,
        n_training_days=config.n_training_days,
        n_forecast_days=28,
        n_chains=config.n_chains,
        n_warmup=config.n_warmup,
        n_samples=config.n_samples,
        exclude_last_n_days=config.exclude_last_n_days,
        credentials_path=Path("config/creds.toml"),
        rng_key=config.rng_key,
        **fit_flags,
        **forecast_flags,
    )


# ---------- Automation Conditions ----------

# A condition that will run as soon as all dependencies are available, but only
# if the asset has not been materialized yet for that day
eager_once = dg.AutomationCondition.eager() & dg.AutomationCondition.missing()

# ---------- Pyrenew Assets ----------


# Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # Run as soon as the nssp gold data is available only on Wednesdays
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule="0 0 * * WED", cron_timezone="America/New_York"
    ),
    group_name="WeeklyForecast",
    ins={"nssp_gold_v1": dg.In(dg.Nothing)},
)
def timeseries_e(context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig):
    _run_timeseries_e(context, config, epiweekly=False)


# Epiweekly Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # Run as soon as the nssp gold data is available only on Wednesdays
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule="0 0 * * WED", cron_timezone="America/New_York"
    ),
    group_name="WeeklyForecast",
    ins={"nssp_gold_v1": dg.In(dg.Nothing)},
)
def epiweekly_timeseries_e(
    context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig
):
    _run_timeseries_e(context, config, epiweekly=True)


# Pyrenew E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=eager_once,
    group_name="WeeklyForecast",
    ins={
        "timeseries_e": dg.In(dg.Nothing),
        "epiweekly_timeseries_e": dg.In(dg.Nothing),
    },
)
def pyrenew_e(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewEConfig,
):
    _run_pyrenew_model(context, config, "e")


# Pyrenew H
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=eager_once,
    group_name="WeeklyForecast",
    ins={
        "nhsn_hrd": dg.In(dg.Nothing),
    },
)
def pyrenew_h(context: DynamicGraphAssetExecutionContext, config: PyrenewConfig):
    _run_pyrenew_model(context, config, "h")


# Pyrenew HE
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=eager_once,
    group_name="WeeklyForecast",
    ins={
        "timeseries_e": dg.In(dg.Nothing),
        "epiweekly_timeseries_e": dg.In(dg.Nothing),
        "nhsn_hrd": dg.In(dg.Nothing),
    },
)
def pyrenew_he(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewEConfig,
):
    _run_pyrenew_model(context, config, "he")


# Pyrenew HW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # automation_condition=eager_once,
    group_name="WeeklyForecastArchived",
    ins={
        "nwss": dg.In(dg.Nothing),
        "nhsn_hrd": dg.In(dg.Nothing),
    },
)
def pyrenew_hw(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewWConfig,
):
    _run_pyrenew_model(context, config, "hw")


# Pyrenew HEW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # automation_condition=eager_once,
    group_name="WeeklyForecastArchived",
    ins={
        "timeseries_e": dg.In(dg.Nothing),
        "epiweekly_timeseries_e": dg.In(dg.Nothing),
        "nwss": dg.In(dg.Nothing),
        "nhsn_hrd": dg.In(dg.Nothing),
    },
)
def pyrenew_hew(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewEWConfig,
):
    _run_pyrenew_model(context, config, "hew")


# ---------- Postprocessing Forecast Batches ----------


@dg.asset(
    deps=[
        "pyrenew_e",
        "pyrenew_h",
        "pyrenew_he",
    ],
    partitions_def=daily_partitions_def,
    # Runs when any dependency has been updated as long as at least one exists
    automation_condition=(
        dg.AutomationCondition.eager().replace(
            ~dg.AutomationCondition.any_deps_missing(),
            dg.AutomationCondition.any_deps_match(
                ~dg.AutomationCondition.missing()
                | dg.AutomationCondition.will_be_requested()
            ),
        )
    ),
    group_name="WeeklyForecast",
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
        f"{config.output_basedir}/{context.partition_key}_forecasts"
    )

    context.log.info(f"config: '{config}'")
    postprocess(
        base_forecast_dir=daily_forecast_output_dir,
        diseases=config.postprocess_diseases,
        skip_existing=config.skip_existing,
        local_copy_dir=daily_forecast_output_dir,
    )


# ============================================================================
# SCHEDULES AND AUTOMATION CONDITION SENSORS
# ============================================================================

# TODO: investigate use_user_code_server and custom/default automation conditions

# ---------- Weekly Forecast Sensor ----------

weekly_forecast_sensor = dg.AutomationConditionSensorDefinition(
    name="WeeklyForecast",
    target=dg.AssetSelection.groups("WeeklyForecast"),
)

# ============================================================================
# DAGSTER DEFINITIONS OBJECT
# ============================================================================
# This code allows us to collect all of the above definitions into a single
# Definitions object for Dagster to read. By doing this, we can keep our
# Dagster code in a single file instead of splitting it across multiple files.

# change storage accounts between dev and prod
storage_account = "cfadagster" if is_production else "cfadagsterdev"

# collect Dagster definitions from the current file
collected_defs = collect_definitions(globals())

# Create Definitions object
defs = dg.Definitions(
    assets=collected_defs["assets"],
    asset_checks=collected_defs["asset_checks"],
    jobs=collected_defs["jobs"],
    sensors=collected_defs["sensors"],
    schedules=collected_defs["schedules"],
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # an example storage account
        "azure_blob_storage": AzureBlobStorageResource(
            account_url=f"{storage_account}.blob.core.windows.net",
            credential=AzureBlobStorageDefaultCredential(),
        ),
    },
    # You can put a comment after azure_batch_config to solely execute with Azure batch
    executor=dynamic_executor(
        default_config=azure_batch_execution_config,
        # default_config=basic_execution_config,
        # default_config=docker_execution_config,
        alternate_configs=[basic_execution_config, docker_execution_config],
    ),
)
