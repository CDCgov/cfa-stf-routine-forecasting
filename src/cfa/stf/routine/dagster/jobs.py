import json
import subprocess

import dagster as dg
from cfa_dagster import GraphDimension, dynamic_executor
from cfa_dagster import is_production as is_prod

from cfa.stf.routine.dagster.asset_config import ModelBaseConfig, tz
from cfa.stf.routine.dagster.execution import (
    azure_batch_4cpu_execution_config,
    basic_execution_config,
    image,
    local_output_mount,
    local_workdir,
    registry,
)

# ============================================================================
# JOBS AND OPS
# These can create images.
# ============================================================================

update_script_url = (
    # repo
    "https://raw.githubusercontent.com/CDCgov/cfa-dagster/"
    # ref
    "refs/heads/main/"
    # file
    "scripts/update_code_location.py"
)

prod_server_image = f"{registry}/{local_workdir.name}:latest"


# Plain helper so the deploy logic can be reused directly (e.g. from build_image_op)
# without invoking the op imperatively, which Dagster does not support.
def _refresh_prod_server_image(logger, image_to_deploy: str):
    logger.info(f"Deploying {image_to_deploy} to the dagster prod server.")
    subprocess.run(
        ["uv", "run", update_script_url, "--registry_image", image_to_deploy],
        check=True,
    )


# Used both in the schedule and in the build_image_op
@dg.op
def refresh_prod_server_image_op(context: dg.OpExecutionContext, image_to_deploy: str):
    """
    Deploys the dagster image to the prod server. Can deploy a working branch's image or the latest image.
    """
    _refresh_prod_server_image(context.log, image_to_deploy)


refresh_prod_server_image_config = dg.RunConfig(
    ops={
        "refresh_prod_server_image_op": {
            "inputs": {
                "image_to_deploy": prod_server_image,
            }
        }
    },
    # configure this job to run on your computer
    execution=basic_execution_config.to_run_config(),
)


@dg.job(
    description=(
        "Standalone job that simply (re)deploys the latest image to the prod server (you can override the tag if necessary). "
        "Note - the build_image job that is available in dev can be passed a flag that executes this when complete. "
    ),
    config=refresh_prod_server_image_config,
    executor_def=dynamic_executor(),
)
def refresh_prod_server_image():
    refresh_prod_server_image_op()


E2E_LOCATIONS = ["CA", "US"]


def e2e_config() -> dg.RunConfig:
    return dg.RunConfig(
        resources={
            "model_base_config": ModelBaseConfig(
                locations=GraphDimension(E2E_LOCATIONS)
            ),
        },
        execution=azure_batch_4cpu_execution_config.to_run_config(),
    )


def e2e_json() -> str:
    return json.dumps(e2e_config().to_config_dict())


end_to_end = dg.define_asset_job(
    name="end_to_end",
    selection=dg.AssetSelection.groups("Fable", "Pyrenew", "EpiAutoGP", "Fusion"),
    config=e2e_config(),
)


@dg.schedule(
    cron_schedule="00 23 * * TUE",
    execution_timezone=tz,
    job_name="refresh_prod_server_image",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def reset_prod_server_image_for_wednesday():
    return dg.RunRequest(run_config=refresh_prod_server_image_config)


# These are only used in dev - they should not appear on the production webserver
if not is_prod():
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

        if should_deploy_to_prod:
            _refresh_prod_server_image(context.log, image_to_deploy=image)

    @dg.job(
        description=(
            "Build the container image used by dagster to run this project's asset cfa.stf.routine."
            "Run after making any change and before running the cfa.stf.routine."
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
            + ["-v", local_output_mount]
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
