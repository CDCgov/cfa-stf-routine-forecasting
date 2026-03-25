# Basic Imports
import datetime as dt
import os
from pathlib import Path

# Direct use of dagster
import dagster as dg
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
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
from pytz import timezone

# Local constant imports
from pipelines.batch.common_batch_utils import (
    DEFAULT_EXCLUDED_LOCATIONS,
    SUPPORTED_DISEASES,
)

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
NY_TZ = timezone("America/New_York")
DATE_FMT = "%Y-%m-%d"


def current_date_str() -> str:
    return dt.datetime.now(NY_TZ).strftime(DATE_FMT)


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
    launcher=SelectorConfig(class_name=dg.DefaultRunLauncher.__name__),
    executor=SelectorConfig(
        class_name=azure_container_app_job_executor.__name__
        if is_production
        else dg.multiprocess_executor.__name__
    ),
)

# Launches locally, executes in a docker container as configured below
# Allows for rapid local testing in a similar-to-batch environment
docker_execution_config = ExecutionConfig(
    launcher=SelectorConfig(class_name=dg.DefaultRunLauncher.__name__),
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
    launcher=SelectorConfig(class_name=dg.DefaultRunLauncher.__name__),
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "pyrenew-dagster-pool",
            "image": image,
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
daily_partitions_def = dg.DailyPartitionsDefinition(
    start_date="2026-01-01", end_offset=1, timezone="America/New_York"
)

# ============================================================================
# ASSET CONFIGURATIONS
# ============================================================================


class ModelBaseConfig(dg.Config):
    """
    Base configuration for all model assets.
    Contains parameters common to both Timeseries and Pyrenew models.
    """

    _output_basedir: str = "output" if is_production else "test-output"
    output_dir: str = f"{_output_basedir}/{current_date_str()}_forecasts"
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


class PostProcessConfig(dg.Config):
    """
    Configuration for the Post-Processing asset.
    """

    _output_basedir: str = "output" if is_production else "test-output"
    output_dir: str = f"{_output_basedir}/{current_date_str()}_forecasts"
    skip_existing: bool = True
    save_local_copy: bool = False
    local_copy_dir: str = ""  # "stf_forecast_fig_share"
    postprocess_diseases: list[str] = ["COVID-19", "Influenza", "RSV"]


# ============================================================================
# ASSET DEFINITIONS
# ============================================================================
# These are the core of Dagster - functions that specify data


# ---------- Data Availability Check Functions ----------

# TODO: either add these to pipelines/utils or deprecate altogether by virtue
# of having these upstream data materialized in dagster directly


def _check_nhsn_data_availability(context: dg.AssetExecutionContext):
    current_date = context.partition_key
    nhsn_target_url = "https://data.cdc.gov/api/views/mpgq-jmmr.json"
    try:
        resp = requests.get(nhsn_target_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        nhsn_update_date_raw = data.get("rowsUpdatedAt")
        if nhsn_update_date_raw is None:
            return {"exists": False, "reason": "Key 'rowsUpdatedAt' not found"}
        nhsn_update_date = dt.datetime.fromtimestamp(
            nhsn_update_date_raw, dt.UTC
        ).strftime("%Y-%m-%d")

        nhsn_check = nhsn_update_date == current_date
        print(f"NHSN data available for date {current_date}: {nhsn_check}")
        result = {
            "exists": nhsn_check,
            "update_date": nhsn_update_date,
            "current_date": current_date,
        }
        context.log.debug(result)
        return result
    except Exception as e:
        print(f"Error checking NHSN data availability: {e}")
        return {"exists": False, "reason": str(e)}


def _check_nssp_gold_data_availability(
    context: dg.AssetExecutionContext,
    account_name="cfaazurebatchprd",
    container_name="nssp-etl",
):
    current_date = context.partition_key
    blob_name = f"gold/{current_date}.parquet"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        f"https://{account_name}.blob.core.windows.net", credential=credential
    )
    container_client = blob_service_client.get_container_client(container_name)
    blobs = list(container_client.list_blobs(name_starts_with=blob_name))
    nssp_gold_check = bool(blobs)
    latest_blob = None
    blobs_gold = list(container_client.list_blobs(name_starts_with="gold/"))
    if blobs_gold:
        latest_blob = max(blobs_gold, key=lambda b: b.last_modified).name
    print(f"NSSP gold data available for date {current_date}: {nssp_gold_check}")
    result = {
        "exists": nssp_gold_check,
        "blob_name": blob_name,
        "latest_blob": latest_blob,
        "current_date": current_date,
    }
    context.log.debug(result)
    return result


def _check_nwss_gold_data_availability(
    context: dg.AssetExecutionContext,
    account_name="cfaazurebatchprd",
    container_name="nwss-vintages",
):
    current_date = context.partition_key
    folder_prefix = "NWSS-ETL-covid-"
    target_folder = f"{folder_prefix}{current_date}/"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        f"https://{account_name}.blob.core.windows.net", credential=credential
    )
    container_client = blob_service_client.get_container_client(container_name)
    all_blobs = list(container_client.list_blobs(name_starts_with=folder_prefix))
    latest_blob = max(all_blobs, key=lambda b: b.last_modified).name
    target_blob = list(container_client.list_blobs(name_starts_with=target_folder))
    nwss_gold_check = latest_blob == target_blob
    result = {
        "exists": nwss_gold_check,
        "latest_blob": latest_blob,
        "target_folder": target_folder,
        "target_blob": target_blob,
        "current_date": current_date,
    }
    context.log.debug(result)
    return result


# ----------- Upstream Data Availability Assets ----


# NHSN
@dg.asset(
    partitions_def=daily_partitions_def,
    automation_condition=(
        # Check every hour 6am-4pm on Wednesday for new data;
        dg.AutomationCondition.on_cron(
            cron_schedule="0 6-16 * * WED", cron_timezone="America/New_York"
        )
        &
        # don't check if not-missing for that day
        dg.AutomationCondition.on_missing()
    ),
    group_name="UpstreamData",
    output_required=False,
)
def nhsn_data_stf(context: dg.AssetExecutionContext):
    result = _check_nhsn_data_availability(context)
    if result["exists"]:
        context.log.info(f"NHSN data available: {result}")
        yield dg.Output("nhsn_data_stf")
    else:
        context.log.error(f"NHSN data not available: {result}")
        return


# NSSP
@dg.asset(
    partitions_def=daily_partitions_def,
    automation_condition=(
        # Check every hour 6am-4pm on Wednesday for new data;
        dg.AutomationCondition.on_cron(
            cron_schedule="0 6-16 * * WED", cron_timezone="America/New_York"
        )
        &
        # don't check if not-missing for that day
        dg.AutomationCondition.on_missing()
    ),
    group_name="UpstreamData",
    output_required=False,
)
def nssp_gold_stf(context: dg.AssetExecutionContext):
    result = _check_nssp_gold_data_availability(context)
    if result["exists"]:
        context.log.info(f"NSSP gold data available: {result}")
        yield dg.Output("nssp_gold_stf")
    else:
        context.log.error(f"NSSP gold data not available: {result}")
        return


# NWSS
@dg.asset(
    partitions_def=daily_partitions_def,
    automation_condition=(
        # Check every hour 6am-4pm on Wednesday for new data;
        dg.AutomationCondition.on_cron(
            cron_schedule="0 6-16 * * WED", cron_timezone="America/New_York"
        )
        &
        # don't check if not-missing for that day
        dg.AutomationCondition.on_missing()
    ),
    group_name="UpstreamData",
    output_required=False,
)
def nwss_gold_stf(context: dg.AssetExecutionContext):
    result = _check_nwss_gold_data_availability(context)
    if result["exists"]:
        context.log.info(f"NWSS gold data available: {result}")
        yield dg.Output("nwss_gold_stf")
    else:
        context.log.error(f"NWSS gold data not available: {result}")
        return


# ----------- Model Constructor Functions --------------------------


def _get_valid_date_disease_location(
    context: DynamicGraphAssetExecutionContext,
    model_letters: str,
) -> tuple[str, str, str, bool]:
    """
    Function used by assets to parse which disease or location they should run as, and the daily partition.
    TODO: Update for signals in addition to (in alternative to) model letters for timeseries.
    """
    context.register_output(lambda: dg.Output("dummy_return"))
    # Disease and Locations are our "Graph Dimensions".
    disease = context.graph_dimension["diseases"]
    location = context.graph_dimension["locations"]

    # Date is the daily partition we use
    date = context.partition_key
    if date < current_date_str():
        raise RuntimeError("STF forecast models do not support backfills.")

    is_valid_to_proceed: bool = True

    # TODO: encode this logic in the config classes, rather than a functional check after-the-fact
    # This will prevent wasted execution overhead
    if "w" in model_letters and disease != "COVID-19":
        context.log.warning(
            f"Model letter 'w' is only applicable for COVID-19. Skipping model run for disease {disease}."
        )
        is_valid_to_proceed: bool = False

    if "e" in model_letters and location == "WY":
        context.log.warning(
            "Model letter 'e' is not applicable for location WY. Skipping model run."
        )
        is_valid_to_proceed: bool = False

    return is_valid_to_proceed, date, disease, location


def _run_timeseries_e(
    context: DynamicGraphAssetExecutionContext,
    config: TimeseriesConfig,
    epiweekly: bool,
) -> str | None:
    """
    Helper function to run timeseries-e model with optional epiweekly mode.
    """

    is_valid_to_proceed, date, disease, location = _get_valid_date_disease_location(
        context, model_letters="e"
    )

    # We can have dagster skip execution if conditions don't apply
    if is_valid_to_proceed:
        context.log.debug(f"config: '{config}'")
        forecast_timeseries(
            disease=disease,
            loc=location,
            facility_level_nssp_data_dir=Path("nssp-etl/gold"),
            output_dir=Path(config.output_dir),
            n_training_days=config.n_training_days,
            n_forecast_days=28,
            n_samples=config.n_samples,
            exclude_last_n_days=config.exclude_last_n_days,
            epiweekly=epiweekly,
            credentials_path=Path("config/creds.toml"),
        )
        yield dg.Output("epiweekly_timeseries_e" if epiweekly else "timeseries_e")
    else:
        return


def _run_pyrenew_model(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    model_letters: str,
) -> str | None:
    """
    Helper to run Pyrenew models with common arguments.
    """

    is_valid_to_proceed, date, disease, location = _get_valid_date_disease_location(
        context, model_letters
    )

    if is_valid_to_proceed:
        fit_flags = flags_from_hew_letters(model_letters)
        forecast_flags = flags_from_hew_letters(
            f"{model_letters}{config.additional_forecast_letters}",
            flag_prefix="forecast",
        )
        context.log.debug(f"config: '{config}'")
        forecast_pyrenew(
            disease=disease,
            loc=location,
            facility_level_nssp_data_dir=Path("nssp-etl/gold"),
            nwss_data_dir=Path("nwss-vintages"),
            param_data_dir=Path("params"),
            priors_path=Path("pipelines/priors/prod_priors.py"),
            output_dir=Path(config.output_dir),
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
        yield dg.Output(f"pyrenew_{model_letters}")
    else:
        return


# ---------- Pyrenew Assets ----------


# Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # We materialize this asset whenever its deps are met and it is missing for a given day
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def timeseries_e(
    context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig, nssp_gold_stf
):
    """
    Run Timeseries-e model and produce outputs.
    """
    return _run_timeseries_e(context, config, epiweekly=False)


# Epiweekly Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # We materialize this asset whenever its deps are met and it is missing for a given day
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def epiweekly_timeseries_e(
    context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig, nssp_gold_stf
):
    """
    Run Epiweekly Timeseries-e model and produce outputs.
    """
    return _run_timeseries_e(context, config, epiweekly=True)


# Pyrenew E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def pyrenew_e(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    timeseries_e,
    epiweekly_timeseries_e,
):
    """
    Run Pyrenew-e model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "e")


# Pyrenew H
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def pyrenew_h(
    context: DynamicGraphAssetExecutionContext, config: PyrenewConfig, nhsn_data_stf
):
    """
    Run Pyrenew-h model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "h")


# Pyrenew HE
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def pyrenew_he(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    timeseries_e,
    epiweekly_timeseries_e,
    nhsn_data_stf,
):
    """
    Run Pyrenew-he model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "he")


# Pyrenew HW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecastArchived",
)
def pyrenew_hw(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    nhsn_data_stf,
    nwss_gold_stf,
):
    """
    Run Pyrenew-hw model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "hw")


# Pyrenew HEW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    # automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecastArchived",
)
def pyrenew_hew(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    timeseries_e,
    epiweekly_timeseries_e,
    nhsn_data_stf,
    nwss_gold_stf,
):
    """
    Run Pyrenew-hew model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "hew")


# ---------- Postprocessing Forecast Batches ----------


@dg.asset(
    deps=[
        "pyrenew_e",
        "pyrenew_h",
        "pyrenew_he",
    ],
    partitions_def=daily_partitions_def,
    # Run if it can, whenever something upstream runs
    automation_condition=dg.AutomationCondition.eager(),
    group_name="WeeklyForecast",
    output_required=False,
)
def postprocess_forecasts(
    context: dg.AssetExecutionContext,
    config: PostProcessConfig,
):
    """
    Postprocess forecast batches.
    """

    date = context.partition_key
    if date < current_date_str():
        context.log.error(
            "Postprocessing does not support backfills. Skipping materialization."
        )
        return

    postprocess(
        base_forecast_dir=config.output_dir,
        diseases=config.postprocess_diseases,
        skip_existing=config.skip_existing,
        local_copy_dir=config.output_dir,
    )

    yield dg.Output("postprocess_forecasts")


# ============================================================================
# SCHEDULES AND AUTOMATION CONDITION SENSORS
# ============================================================================

# TODO: investigate use_user_code_server and custom/default automation conditions

# ---------- Upstream Data Sensor ------------

upstream_data_sensor = dg.AutomationConditionSensorDefinition(
    name="UpstreamData",
    target=dg.AssetSelection.groups("UpstreamData"),
    run_tags=basic_execution_config.to_run_tags(),
)

# ---------- Weekly Forecast Sensor ----------

weekly_forecast_sensor = dg.AutomationConditionSensorDefinition(
    name="WeeklyForecast",
    target=dg.AssetSelection.groups("WeeklyForecast"),
    run_tags=azure_batch_execution_config.to_run_tags(),
)

# --- legacy/classic schedule definitions ----
# this serves as an additional way to launch runs;
# in general, we want to use automation conditions and their sensors,
# not top-down schedules


# Launches upstream jobs; will run anything downstream by virtue of automation conditions
@dg.schedule(
    target=dg.AssetSelection.groups("UpstreamData"),
    cron_schedule="0 6-16 * * MON",
)
def optional_monday(context: dg.ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(
        partition_key=scheduled_date,
        tags={"partition": scheduled_date},
        run_config=dg.RunConfig(
            execution=basic_execution_config.to_run_config(),
        ),
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
        # default_config=docker_execution_config,
        alternate_configs=[basic_execution_config, docker_execution_config],
    ),
)
