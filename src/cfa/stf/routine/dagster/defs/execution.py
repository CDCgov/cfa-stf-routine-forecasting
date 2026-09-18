import logging
import os
from pathlib import Path

import dagster as dg
from cfa_dagster import (
    ExecutionConfig,
    SelectorConfig,
    azure_batch_executor,
    docker_executor,
    dynamic_executor,
)
from cfa_dagster import is_production as is_prod
from pygit2.repository import Repository

log = logging.getLogger(__name__)
user = os.getenv("DAGSTER_USER")

# ============================================================================
# RUNTIME CONFIGURATION: WORKING DIRECTORY, EXECUTORS, VOLUME MOUNTS
# ============================================================================
# Executors define the runtime-location of an asset job
# See later on for Asset job definitions

# ---------- Working Directory, Branch, and Image Tag ----------


def _find_project_root() -> Path:
    """Find the checkout containing the Dagster project configuration."""
    working_dir = Path.cwd().resolve()
    module_dir = Path(__file__).resolve().parent
    for candidate in (
        working_dir,
        *working_dir.parents,
        module_dir,
        *module_dir.parents,
    ):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return working_dir


local_workdir = _find_project_root()
container_workdir = Path(
    f"/{local_workdir.name}"
)  # in the container, workdir is mounted at /

# Get branch name from git, defaulting to main if not in a git repo
try:
    current_branch_name = os.environ.get("GITHUB_HEAD_REF") or str(
        Repository(local_workdir).head.shorthand
    )
    log.debug(f"Branch name from git: {current_branch_name}")
except Exception:
    current_branch_name = "main"
    log.warning("No .git folder detected; using main as the branch name")

# Use 'latest' tag for production or main branch, otherwise use branch name
registry = "cfaprdbatchcr.azurecr.io"
tag = (
    "latest"
    if (is_prod() or current_branch_name == "main")
    else current_branch_name.replace("/", "-")
)
image = f"{registry}/{local_workdir.name}:{tag}"

# ----------- Output volume mount strings ---------------

# Azure Batch writes outputs directly to blob storage.
azure_blob_mounts = [
    f"stf-routine-forecasting-prod-output:{container_workdir}/output",
    f"stf-routine-forecasting-test-output:{container_workdir}/test-output",
]

# Local runs are non-production, so they use one output directory.
local_output_mount = (
    f"{local_workdir / 'test-output'}:{container_workdir / 'test-output'}"
)

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
                    f"{local_workdir}:{container_workdir / 'src/cfa/stf/routine/dagster'}",
                    # Store outputs on the host so they persist after the
                    # container exits.
                ]
                + [local_output_mount]
            },
        },
    ),
)

# Cloud execution. This is what we want for any model run.
# Shared config for all Azure Batch pools; only pool_name differs between them.
_azure_batch_shared_config = {
    **(
        {}
        if is_prod()  # image will come from the code location in prod
        else {"image": image}
    ),
    "container_kwargs": {
        "volumes": [
            # bind the ~/.azure folder for optional cli login
            # f"/home/{user}/.azure:/root/.azure",
            # bind current file so we don't have to rebuild
            # the container image for workflow changes
            # Azure blob output mounts
        ]
        + azure_blob_mounts,
        "working_dir": f"{container_workdir}",
    },
}

azure_batch_2cpu_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "stf-routine-2cpu",
            **_azure_batch_shared_config,
        },
    ),
)

azure_batch_4cpu_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "stf-routine-4cpu",
            **_azure_batch_shared_config,
        },
    ),
)

azure_batch_64cpu_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            "pool_name": "stf-routine-64cpu",
            **_azure_batch_shared_config,
        },
    ),
)


@dg.definitions
def execution():
    return dg.Definitions(
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
