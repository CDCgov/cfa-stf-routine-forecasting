# ruff: noqa: E402, F403, F405

import logging
import os
import warnings

import dagster as dg
from cfa_dagster import (
    ADLS2PickleIOManager,
    collect_definitions,
    dynamic_executor,
    start_dev_env,
)
from cfa_dagster import is_production as is_prod

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
is_production = is_prod()

start_dev_env(__name__)

# Initialization must precede imports that construct the definitions (E402).

from cfa.stf.routine.dagster.asset_config import *
from cfa.stf.routine.dagster.asset_helpers import *
from cfa.stf.routine.dagster.assets import *
from cfa.stf.routine.dagster.automation import *
from cfa.stf.routine.dagster.execution import *
from cfa.stf.routine.dagster.jobs import *

# ============================================================================
# DAGSTER DEFINITIONS OBJECT
# ============================================================================
# Collect the imported modules' definitions into one Dagster entrypoint.

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
        "epiautogp_e_pct_epiweekly_config": EpiAutoGPEPctEpiweeklyConfig(),
        "fable_e_other_config": FableEOtherConfig(),
        "e_model_exclusions": EModelExclusions(),
        "w_model_exclusions": WModelExclusions(),
    },
    executor=dynamic_executor(
        default_config=azure_batch_4cpu_execution_config,
        # default_config=basic_execution_config,
        # default_config=docker_execution_config,
        alternate_configs=[
            basic_execution_config,
            docker_execution_config,
        ],
    ),
)
