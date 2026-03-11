# Basic Imports
import datetime as dt
import os
from pathlib import Path

# Dagster and cloud Imports
import dagster as dg
import pydantic
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from cfa_dagster import (
    ADLS2PickleIOManager,
    AzureContainerAppJobRunLauncher,
    DynamicGraphAssetExecutionContext,
    ExecutionConfig,
    SelectorConfig,
    azure_batch_executor,
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
is_production = not os.getenv("DAGSTER_IS_DEV_CLI")

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

# Most basic execution - launches locally, runs locally
default_config = ExecutionConfig(
    launcher=SelectorConfig(class_name=dg.DefaultRunLauncher.__name__),
    executor=SelectorConfig(class_name=dg.multiprocess_executor.__name__),
)


# Launches in a container app job, then executes in the same process
azure_caj_config = ExecutionConfig(
    launcher=SelectorConfig(class_name=AzureContainerAppJobRunLauncher.__name__),
    executor=SelectorConfig(class_name=dg.in_process_executor.__name__),
)

# Launches locally, executes in a docker container as configured below
docker_config = ExecutionConfig(
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

# Config for full backfills - launches with container app jobs, sends execution to Azure Batch
azure_batch_config = ExecutionConfig(
    launcher=SelectorConfig(
        class_name=AzureContainerAppJobRunLauncher.__name__, config={"image": image}
    ),
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
# PARTITIONS
# ============================================================================
# How are the data split and processed in Azure Batch?

DISEASES = SUPPORTED_DISEASES
disease_partitions = dg.StaticPartitionsDefinition(DISEASES)

# location Partitions
LOCATIONS = location_table.get_column("short_name").to_list()
location_partitions = dg.StaticPartitionsDefinition(
    [location for location in LOCATIONS if location not in DEFAULT_EXCLUDED_LOCATIONS]
)

# Multi Partitions
pyrenew_multi_partition_def = dg.MultiPartitionsDefinition(
    {"disease": disease_partitions, "location": location_partitions}
)

# Daily Partitions
daily_partitions_def = dg.DailyPartitionsDefinition(
    start_date="2026-01-01", end_offset=1, timezone="America/New_York"
)

# ============================================================================
# ASSET CONFIGURATIONS
# ============================================================================


class CommonConfig(dg.Config):
    """
    Common configuration for both Model and Post-Processing assets.
    Both ModelConfigBase and PostProcessConfig inherit from this.
    """

    forecast_date: str = pydantic.Field(default_factory=current_date_str)
    output_basedir: str = "output" if is_production else "test-output"
    # output_basedir: str = "test-output" # uncomment to force testing even on prod server


class ModelConfigBase(CommonConfig):
    """
    Base configuration for all model assets.
    Contains parameters common to both Timeseries and Pyrenew models.
    """

    n_training_days: int = 150
    exclude_last_n_days: int = 1
    diseases: list[str] = DISEASES
    locations: list[str] = LOCATIONS


class TimeseriesConfig(ModelConfigBase):
    """
    Configuration for timeseries model assets (timeseries_e, epiweekly_timeseries_e).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_samples: int = 400 if not is_production else 2000  # Total samples for timeseries


class PyrenewConfig(ModelConfigBase):
    """
    Configuration for Pyrenew model assets (pyrenew_e, pyrenew_h, pyrenew_he, etc.).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_warmup: int = 200 if not is_production else 1000
    n_samples: int = 200 if not is_production else 500
    n_chains: int = 2 if not is_production else 4
    rng_key: int = 12345
    additional_forecast_letters: str = ""


# class PipelineConfig(dg.Config):
#     """
#     Unified pipeline configuration containing configs for both model types.
#     This is used by the pipeline launcher to configure all model runs.

#     You can either provide nested 'timeseries' and 'pyrenew' configs directly,
#     or use the default values which will be automatically initialized.
#     """

#     timeseries: TimeseriesConfig = pydantic.Field(default_factory=TimeseriesConfig)
#     pyrenew: PyrenewConfig = pydantic.Field(default_factory=PyrenewConfig)


class PostProcessConfig(CommonConfig):
    """
    Configuration for the Post-Processing asset.
    """

    skip_existing: bool = True
    save_local_copy: bool = False
    local_copy_dir: str = ""  # "stf_forecast_fig_share"
    postprocess_diseases: list[str] = ["COVID-19", "Influenza", "RSV"]


# ============================================================================
# MODEL HELPER FUNCTIONS
# ============================================================================
# These helpers are not asset themselves


def get_disease_location_date(
    context: DynamicGraphAssetExecutionContext,
    model_letters: str,
) -> tuple[str | None, str | None]:
    """
    Function used by assets to parse which disease or location they should run as, and the daily partition.
    TODO: Update for signals in addition to (in alternative to) model letters for timeseries.
    """

    # Disease and Locations are our "Graph Dimensions".
    disease = context.graph_dimension["disease"]
    location = context.graph_dimension["location"]

    # Date is the daily partition we use
    date = context.partition_key

    if "w" in model_letters and disease != "COVID-19":
        context.log.info(
            f"Model letter 'w' is only applicable for COVID-19. Skipping model run for disease {disease}."
        )
        return None, None
    if "e" in model_letters and location == "WY":
        context.log.info(
            "Model letter 'e' is not applicable for location WY. Skipping model run."
        )
        return None, None

    return disease, location, date


def _run_timeseries_e(
    context: DynamicGraphAssetExecutionContext,
    config: TimeseriesConfig,
    epiweekly: bool,
) -> str:
    """
    Helper function to run timeseries-e model with optional epiweekly mode.
    """

    disease, location, date = get_disease_location_date(context, model_letters="e")
    output_dir = f"{config.output_basedir}/{date}_forecasts"

    if disease is not None and location is not None:
        context.log.debug(f"config: '{config}'")
        forecast_timeseries(
            disease=disease,
            loc=location,
            facility_level_nssp_data_dir=Path("nssp-etl/gold"),
            output_dir=Path(output_dir),
            n_training_days=config.n_training_days,
            n_forecast_days=28,
            n_samples=config.n_samples,
            exclude_last_n_days=config.exclude_last_n_days,
            epiweekly=epiweekly,
            credentials_path=Path("config/creds.toml"),
        )
    return "epiweekly_timeseries_e" if epiweekly else "timeseries_e"


def _run_pyrenew_model(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    model_letters: str,
):
    """
    Helper to run Pyrenew models with common arguments.
    """
    disease, location, date = get_disease_location_date(context, model_letters)
    output_dir = f"{config.output_basedir}/{date}_forecasts"

    if disease is not None and location is not None:
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
            output_dir=Path(output_dir),
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
    return f"pyrenew_{model_letters}"


# ============================================================================
# ASSET DEFINITIONS
# ============================================================================
# These are the core of Dagster - functions that specify data

# ---------- Pyrenew Assets ----------


# Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=dg.AutomationCondition.on_cron(cron_schedule="0 8 * * *"),
    group_name="WeeklyForecast",
)
def timeseries_e(context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig):
    """
    Run Timeseries-e model and produce outputs.
    """
    return _run_timeseries_e(context, config, epiweekly=False)


# Epiweekly Timeseries E
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule="0 8 * * TUE,WED"
    ),
    group_name="WeeklyForecast",
)
def epiweekly_timeseries_e(
    context: DynamicGraphAssetExecutionContext, config: TimeseriesConfig
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
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule="0 14 * * TUE,WED"
    ),
    group_name="WeeklyForecast",
)
def pyrenew_h(context: DynamicGraphAssetExecutionContext, config: PyrenewConfig):
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
):
    """
    Run Pyrenew-he model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "he")


# Pyrenew HW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    group_name="WeeklyForecastArchived",
)
def pyrenew_hw(context: DynamicGraphAssetExecutionContext, config: PyrenewConfig):
    """
    Run Pyrenew-hw model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "hw")


# Pyrenew HEW
@dynamic_graph_asset(
    partitions_def=daily_partitions_def,
    graph_dimensions=["diseases", "locations"],
    group_name="WeeklyForecastArchived",
)
def pyrenew_hew(
    context: DynamicGraphAssetExecutionContext,
    config: PyrenewConfig,
    timeseries_e,
    epiweekly_timeseries_e,
):
    """
    Run Pyrenew-hew model and produce outputs.
    """
    return _run_pyrenew_model(context, config, "hew")


# ---------- Epi AutoGP Asset ----------


@dg.asset(group_name="ExperimentalEpiAutoGP")
def epiautogp(context: dg.AssetExecutionContext):
    """
    Placeholder asset for Epi AutoGP forecasts.
    """
    # Placeholder logic for Epi AutoGP forecasts
    context.log.info("Epi AutoGP forecast asset executed.")
    # TODO: implement Epi AutoGP model and invoke its pipeline entrypoint.
    return "epiautogp"


# ---------- Postprocessing Forecast Batches ----------
# TODO: integrate this asset into the DAG fully, and/or trigger it via sensors


@dg.asset(
    deps=[
        "timeseries_e",
        "epiweekly_timeseries_e",
        "pyrenew_e",
        "pyrenew_h",
        "pyrenew_he",
    ],
    automation_condition=dg.AutomationCondition.on_missing(),
    group_name="WeeklyForecast",
)
def postprocess_forecasts(
    context: dg.AssetExecutionContext,
    config: PostProcessConfig,
):
    """
    Postprocess forecast batches.
    """
    postprocess(
        base_forecast_dir=config.output_dir,
        diseases=config.postprocess_diseases,
        skip_existing=config.skip_existing,
        local_copy_dir=config.output_dir,
    )
    return "postprocess_forecasts"


# ============================================================================
# ORCHESTRATION: PARTITIONED ASSETS, JOBS, AND SCHEDULES
# ============================================================================
# Model runs and post-processing via jobs and schedules
# Scheduling full pipeline runs and defining a flexible configuration
# We use dagster ops and jobs here to launch asset backfills with custom configuration

# ---------- Data Availability Check Ops ----------


@dg.op
def check_nhsn_data_availability():
    current_date = current_date_str()
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
        return {
            "exists": nhsn_check,
            "update_date": nhsn_update_date,
            "current_date": current_date,
        }
    except Exception as e:
        print(f"Error checking NHSN data availability: {e}")
        return {"exists": False, "reason": str(e)}


@dg.op
def check_nssp_gold_data_availability(
    account_name="cfaazurebatchprd", container_name="nssp-etl"
):
    current_date = current_date_str()
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
    print(f"NSSP gold data avaialble for date {current_date}: {nssp_gold_check}")
    return {
        "exists": nssp_gold_check,
        "blob_name": blob_name,
        "latest_blob": latest_blob,
        "current_date": current_date,
    }


@dg.op
def check_nwss_gold_data_availability(
    account_name="cfaazurebatchprd", container_name="nwss-vintages"
):
    current_date = current_date_str()
    folder_prefix = f"NWSS-ETL-covid-{current_date}/"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        f"https://{account_name}.blob.core.windows.net", credential=credential
    )
    container_client = blob_service_client.get_container_client(container_name)
    blobs = list(container_client.list_blobs(name_starts_with=folder_prefix))
    nwss_gold_check = bool(blobs)
    print(f"NWSS gold data avaialble for date {current_date}: {nwss_gold_check}")
    return {
        "exists": nwss_gold_check,
        "folder_prefix": folder_prefix,
        "current_date": current_date,
    }


# ---------- Pipeline Launch Op ----------


# @dg.op
# def launch_forecast_pipeline(
#     context: dg.OpExecutionContext, config: PipelineConfig
# ) -> str | None:
#     # We are referencing the global pyrenew_multi_partition_def defined earlier
#     partition_keys = pyrenew_multi_partition_def.get_partition_keys()

#     asset_config_map = {
#         "timeseries_e": config.timeseries,
#         "epiweekly_timeseries_e": config.timeseries,
#         "pyrenew_e": config.pyrenew,
#         "pyrenew_h": config.pyrenew,
#         "pyrenew_he": config.pyrenew,
#         "pyrenew_hw": config.pyrenew,
#         "pyrenew_hew": config.pyrenew,
#     }

#     # Determine which assets to backfill based on data availability
#     nhsn_available = check_nhsn_data_availability()["exists"]  # H Data
#     nssp_available = check_nssp_gold_data_availability()["exists"]  # E Data
#     # nwss_available = check_nwss_gold_data_availability()["exists"] # W Data

#     context.log.debug(f"NHSN available: {nhsn_available}")
#     context.log.debug(f"NSSP available: {nssp_available}")
#     # context.log.debug(f"NWSS available: {nwss_available}")

#     # Determine which assets to backfill based on data availability
#     # if nhsn_available and nssp_available and nwss_available:
#     #     context.log.info("NHSN, NSSP gold, and NWSS gold data are all available - launching full pipeline.")
#     #     context.log.info("Launching full pyrenew_hew backfill.")
#     #     asset_selection = ("timeseries_e", "pyrenew_e", "pyrenew_h", "pyrenew_he", "pyrenew_hw", "pyrenew_hew")

#     if nhsn_available and nssp_available:
#         # elif nhsn_available and nssp_available:
#         context.log.info(
#             "Both NHSN data and NSSP gold data are available, but NWSS gold data is not."
#         )
#         context.log.info(
#             "Launching a timeseries_e, pyrenew_e, pyrenew_h, and pyrenew_he backfill."
#         )
#         asset_selection = [
#             "epiweekly_timeseries_e",
#             "timeseries_e",
#             "pyrenew_e",
#             "pyrenew_h",
#             "pyrenew_he",
#         ]

#     # elif nhsn_available and nwss_available:
#     #     context.log.info("NHSN data and NWSS data are available, but NSSP gold data is not.")
#     #     context.log.info("Launching pyrenew_h and pyrenew_hw backfill.")
#     #     asset_selection = ("pyrenew_h", "pyrenew_hw")

#     elif nssp_available:
#         context.log.info("Only NSSP gold data are available.")
#         context.log.info("Launching a timeseries_e and pyrenew_e backfill.")
#         asset_selection = ["epiweekly_timeseries_e", "timeseries_e", "pyrenew_e"]

#     elif nhsn_available:
#         context.log.info("Only NHSN data are available.")
#         context.log.info("Launching a pyrenew_h backfill.")
#         asset_selection = ["pyrenew_h"]

#     else:
#         context.log.info("No required data is available.")
#         asset_selection = []
#         context.log.info("Execution will not be sent to Azure batch!")
#         return

#     # Build ops config dict based on asset types using the joint config
#     ops_config: dict[str, TimeseriesConfig | PyrenewConfig] = {
#         asset: asset_config_map[asset] for asset in asset_selection
#     }

#     # Launch the backfill
#     # Returns: a backfill ID,
#     # side-effect: launches the backfill run in Dagster via a GraphQL query
#     backfill_id = launch_asset_backfill(
#         asset_selection,
#         partition_keys,
#         run_config=dg.RunConfig(
#             ops=ops_config,
#             execution=azure_batch_config.to_run_config(),
#         ),
#         tags={
#             "run": "pyrenew",
#             "available_data": str(
#                 {
#                     "nhsn": nhsn_available,
#                     "nssp_gold": nssp_available,
#                     "nwss_gold": False,  # nwss_available,
#                 }
#             ),
#             "user": user,
#             "models_attempted": ", ".join(asset_selection),
#             "forecast_date": config.pyrenew.forecast_date,
#             "output_dir": config.pyrenew.output_dir,
#             "git_commit_sha": git_commit_sha,
#         },
#     )
#     context.log.info(
#         f"Launched backfill with id: '{backfill_id}'. "
#         "Click the output metadata url to monitor"
#     )
#     context.add_output_metadata({"url": dg.MetadataValue.url(f"/runs/b/{backfill_id}")})
#     return backfill_id


# ---------- Job Definitions ----------

# # These specify the resources used to initially launch our backfill job.
# forecast_pipeline_caj_launch_config = {
#     "config": {
#         "launcher": {"AzureContainerAppJobRunLauncher": {"cpu": 2.0, "memory": 4.0}}
#     }
# }
# forecast_pipeline_local_launch_config = {
#     "config": {}
# }  # We can let the default take over

# # Define run config for the backfil launcher and for the scheduler
# weekly_forecast_config = dg.RunConfig(
#     ops={"launch_forecast_pipeline": PipelineConfig()},
#     execution=forecast_pipeline_caj_launch_config
#     if is_production
#     else forecast_pipeline_local_launch_config,
# )


# This wraps our launch_pipeline op in a job that can be scheduled or manually launched via the GUI
# @dg.job(
#     executor_def=dynamic_executor(
#         default_config=azure_caj_config if is_production else default_config
#     ),
#     config=weekly_forecast_config,
# )
# def weekly_forecast_via_backfill():
#     launch_forecast_pipeline()


@dg.job(
    executor_def=dg.multiprocess_executor,
)
def check_all_data():
    check_nhsn_data_availability()  # H Data
    check_nssp_gold_data_availability()  # E Data
    check_nwss_gold_data_availability()  # W Data


# ---------- Schedule Definitions ----------

# weekly_forecast_via_backfill_schedule = dg.ScheduleDefinition(
#     default_status=(
#         dg.DefaultScheduleStatus.RUNNING
#         # don't run locally by default
#         if is_production
#         else dg.DefaultScheduleStatus.STOPPED
#     ),
#     job=weekly_forecast_via_backfill,
#     run_config=weekly_forecast_config,
#     cron_schedule="0 8,14 * * TUE,WED",
#     execution_timezone="America/New_York",
# )

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
        default_config=azure_batch_config  # if is_production else docker_config
    ),
)
