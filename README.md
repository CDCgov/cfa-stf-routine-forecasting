# CFA STF Routine Forecasting
| stfroutineforecasting | pipelines |
| --- | --- |
| [![stfroutineforecasting](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting/branch/main/graph/badge.svg?flag=stfroutineforecasting)](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting) | [![pipelines](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting/graph/badge.svg?flag=pipelines)](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting) |

The STF Routine Forecasting project contains code for producing short-term forecasts of respiratory disease burden using several models:
- [EpiAutoGp](pipelines/epiautogp)
- [fable](pipelines/fable)
- [pyrenew_hew](pipelines/pyrenew_hew)

EpiAutoGP test coverage is included in the `pipelines` coverage badge above rather than reported separately.

These forecasts are submitted to CDC's forecasting hubs:
- [RSV Forecast Hub](https://github.com/CDCgov/rsv-forecast-hub/)
- [FluSight Forecast Hub](https://github.com/cdcepi/FluSight-forecast-hub)
- [COVID-19 Forecast Hub](https://github.com/CDCgov/covid19-forecast-hub)

The modeling pipeline is orchestrated with [Dagster](dagster_defs.py).

## Containers

The project uses GitHub Actions for automatically building container images based on the project's [Dockerfile](Dockerfile). The images are currently hosted on Azure Container Registry and are built and pushed via the [containers.yaml](.github/workflows/containers.yaml) GitHub Actions workflow.

Container images pushed to the Azure Container Registry are automatically tagged as either `latest` (if the commit is on the `main` branch) or with the branch name (if the commit is on a different branch). After a branch is deleted, the image tag is removed from the registry via the [delete-container-tag.yaml](.github/workflows/delete-container-tag.yaml) GitHub Actions workflow.

Containers can also be built using a dagster job on the dev webserver as defined in `dagster_defs.py`. You can choose whether to push the image (generally you should) or to even push to the dagster production server (do so only in coordination with the STF team).

## Running Model Pipelines with Dagster
> [!NOTE]
> Azure Batch Forecasting Pipelines can only be run by CDC internal users on the CFA Virtual Analyst Platform.

To execute dagster workflows fully locally with this project, you'll need to have blobs mounted. However, you can also launch jobs locally and have them submit to Azure Batch.

#### Local Development and Testing
> Prerequisites:
> - `uv`, `docker`, a VAP VM with a registered managed identity in Azure.
> - Permissions to push to the Azure Container Registry.

The following instructions will set up Dagster on your VAP. However, based on the current configuration, actual execution will still run in the cloud via Azure Batch. You can change the `executor` option in `dagster_defs.py` or in the dagster launchpad to test using the local Docker Executor - this will require you to have setup Blobfuse. See [Using the local docker executor](#using-the-local-docker-executor).

1. Build and push the `cfa-stf-routine-forecasting` container, as also described above:
    - Use the `build_image` job in dagster, making sure to push the image.
2. Run `uv run dagster_defs.py` and open the terminal link (usually http://127.0.0.1:4000/)

Dagster is now ready to use locally.

> [!NOTE]
> The following process has been changing frequently. We will work to firm it up over the coming weeks and months.

In development, whenever you update code, rerun the `build_image` job and then `Reload Definitions` from the dagster lineage page.
Pushing your code to github will also re-build and push the container image, but will typically take longer and you will have to wait for completion in Github Actions.

By default, on this repository, Dagster will submit tasks to Azure Batch for execution.

#### Using the local docker executor
If you'd like to test a few "tasks" locally, you can have dagster execute on your machine, which is much faster than waiting for Azure Batch to pick up jobs. Dagster can leverage your VM's own docker daemon to emulate Azure Batch. When doing this, take care not to run more than two or three state x disease combinations at a time or you will quickly put your VM into a coma.

When using the `Docker Executor`, Dagster assumes mounts at `./blobfuse/mounts/` in the working directory.
- `sudo bash "./blobfuse/cleanup.sh"`: gracefully unmounts the relevant blobs. It is often worth running this first to make sure you're mounting to a clean setup.
- `sudo bash "./blobfuse/mount.sh"`: mounts the relevant blobs using blobfuse. Use this before launching locally-executed dagster jobs.

#### Production Scheduling

From our [production dagster server](https://dagster.apps.edav.ext.cdc.gov/), you can run and schedule model runs and see other projects' pipelines at CFA.
- Pushes to `main` that include changes to `dagster_defs.py` will automatically update this server via a Github Actions Workflow. (See next section.)
- Before pushing to `main`, make sure you have thoroughly tested your own branch and gotten a PR review.
- It is good practice to periodically re-sync (`uv sync`) and even re-create your virtual environment if your branch has been open a while to make sure dependencies are up to date. `cfa-dagster`, our own implementation of dagster, updates frequently. To specifically update that package, run `uv lock --upgrade-package cfa-dagster`.

#### How to push to the dagster server
1. You can use the Github Actions workflow in `containers.yaml` via workflow dispatch. Use this for testing in pre-prod with a non-`main` branch.
    - Let people know when you do this so they don't override your test with their own.
    - Pushes to main that do not include changes to the `dagster_defs.py` file will NOT automatically update the server.
2. As mentioned, pushes to `main` that include a change to `dagster_defs.py` since the previous commit (or PR) will push to the server.
3. Powerusers: `build_image` in dagster's Jobs will build and push your own local branch to the server if you set `should_push` to `True`.
    - Communicate that you are running this job to the STF team before doing so.

## General Disclaimer
This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/organization/mission.htm).  GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
This repository is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).
