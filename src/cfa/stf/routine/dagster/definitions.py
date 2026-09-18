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
from dagster import definitions, load_from_defs_folder

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


@definitions
def defs():
    path = Path(__file__).parent

    return dg.Definitions.merge(
        load_from_defs_folder(project_root=path),
        dg.Definitions(
            resources={
                # These IOManagers let Dagster serialize asset outputs and store them
                # in Azure to pass between assets
                "io_manager": ADLS2PickleIOManager(),
            },
        ),
    )
