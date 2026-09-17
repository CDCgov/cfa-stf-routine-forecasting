# ruff: noqa: E402

import logging
import os
import warnings

import dagster as dg
from cfa_dagster import (
    ADLS2PickleIOManager,
    dynamic_executor,
    start_dev_env,
)

# Initialization must precede imports that construct the definitions (E402).
from cfa.stf.routine.dagster import asset_config, assets, automation, execution, jobs

# ============================================================================
# DAGSTER INITIALIZATION
# ============================================================================

log = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message=r".*AutomationConditionSensorDefinition.*is currently in beta.*",
)

# Get the user running the Dagster instance.
user = os.getenv("DAGSTER_USER")

start_dev_env(__name__)

# ============================================================================
# DAGSTER DEFINITIONS OBJECT
# ============================================================================
# Collect the imported modules' definitions into one Dagster entrypoint.

# Set Azure HTTP Logging Level
# this will limit excessive IO logs in stderr
# for any assets making azure http requests
azure_http_logger = logging.getLogger(
    "azure.core.pipeline.policies.http_logging_policy"
)
azure_http_logger.setLevel(logging.WARNING)

# Create Definitions object
defs = dg.load_definitions_from_modules(
    modules=[assets, automation, jobs],
    resources={
        # These IOManagers let Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # Shared resources for model assets
        "model_base_config": asset_config.ModelBaseConfig(),
        "pyrenew_config": asset_config.PyrenewConfig(),
        "epiautogp_e_pct_epiweekly_config": asset_config.EpiAutoGPEPctEpiweeklyConfig(),
        "fable_e_other_config": asset_config.FableEOtherConfig(),
        "e_model_exclusions": asset_config.EModelExclusions(),
        "w_model_exclusions": asset_config.WModelExclusions(),
    },
    executor=dynamic_executor(
        default_config=execution.azure_batch_4cpu_execution_config,
        # default_config=execution.basic_execution_config,
        # default_config=execution.docker_execution_config,
        alternate_configs=[
            execution.basic_execution_config,
            execution.docker_execution_config,
        ],
    ),
)
