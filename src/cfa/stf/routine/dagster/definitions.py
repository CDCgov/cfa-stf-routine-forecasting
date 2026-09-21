"Initialization and definitions object for our dagster project."

# ruff: noqa: E402

import logging
import os
import warnings
from pathlib import Path

import dagster as dg
from cfa_dagster import (
    ADLS2PickleIOManager,
    start_dev_env,
)

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

# Set Azure HTTP Logging Level
# this will limit excessive IO logs in stderr
# for any assets making azure http requests
azure_http_logger = logging.getLogger(
    "azure.core.pipeline.policies.http_logging_policy"
)
azure_http_logger.setLevel(logging.WARNING)


# ============================================================================
# DAGSTER DEFINITIONS
# ============================================================================
# Load the modules' definitions into one Dagster definitions entrypoint.


@dg.definitions
def defs():

    path = Path(__file__).parent

    # What are we loading from the CFA Dagster package directly?
    cfa_definitions = dg.Definitions(
        resources={
            # These IOManagers let Dagster serialize asset outputs and store them
            # in Azure to pass between assets
            "io_manager": ADLS2PickleIOManager(),
        },
    )
    # What are we loading from our project's definitions modules?
    project_definitions = dg.load_from_defs_folder(project_root=path)

    # Return the union of both/all definitions collections
    return dg.Definitions.merge(cfa_definitions, project_definitions)
