# CFA STF Routine Forecasting
| hewr                                                                                                                                                                | EpiAutoGP                                                                                                                                           | pipelines                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [![hewr](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting/branch/main/graph/badge.svg?flag=hewr)](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting) | [![EpiAutoGP](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting/graph/badge.svg?flag=epiautogp)](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting) | [![pipelines](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting/graph/badge.svg?flag=pipelines)](https://codecov.io/gh/CDCgov/cfa-stf-routine-forecasting) |

The PyRenew-HEW project aims to create short-term forecasts of respiratory disease burden using the [PyRenew library](https://github.com/CDCgov/PyRenew) and several data sources:

- [x] **H**ospital Admissions from the [National Healthcare Safety Network](https://www.cdc.gov/nhsn/index.html)
- [x] **E**mergency Department Visits from the [National Syndromic Surveillance Program](https://www.cdc.gov/nssp/index.html)
- [x] **W**astewater virus concentration from the [National Wastewater Surveillance System](https://www.cdc.gov/nwss/index.html)

This is a work in progress, and not all data sources are currently integrated into the model.

This repository contains code for the [PyRenew-HEW model](pyrenew_hew/pyrenew_hew_model.py) itself, as well as [pipelines](pipelines) for running the model in production, and [utilities](hewr) for working with model outputs.

## Containers

The project uses GitHub Actions for automatically building container images based on the project's [Containerfile](Containerfile). The images are currently hosted on Github Container Registry and are built and pushed via the [containers.yaml](.github/workflows/containers.yaml) GitHub Actions workflow.

Images can also be built locally. The [Makefile](Makefile) contains several targets for building and pushing images. Although the Makefile uses Docker as the default engine, the `ENGINE` environment variable can be set to `podman` to use Podman instead, for example:

```bash
ENGINE=podman make container_build
# Equivalent to:
# podman build . -t cfa-stf-routine-forecasting -f Containerfile
```

Container images pushed to the Azure Container Registry are automatically tagged as either `latest` (if the commit is on the `main` branch) or with the branch name (if the commit is on a different branch). After a branch is deleted, the image tag is remove from the registry via the [delete-container-tag.yaml](.github/workflows/delete-container-tag.yaml) GitHub Actions workflow.

## Running Model Pipelines
> [!NOTE]
> Azure Batch Forecasting Pipelines can only be run by CDC internal users on the CFA Virtual Analyst Platform.

There are two ways to run Azure Batch Modeling Code:
1. [The Azure Command Center](#3-azure-command-center) - interactive/manual.
2. [Dagster Workflow Orchestration](#2-dagster-workflow-orchestration) - automated, feature rich GUI.

### 1. Azure Command Center
> Specific environment setup steps required can be found in the [Routine Forecasting Standard Operating Procedure](https://cdcent.github.io/cfa-stf-handbook/routine_forecast_sop.html).

You can run `uv run pipelines/azure_command_center.py` (or `make acc`) to launch the Azure Command Center.
- The Azure Command Center will check for necessary data before offering to run pipelines.
- You must have previously configured your Azure Credentials and Environment Variables. To do this, run `make config`, or follow the steps in the SOP.
- The Azure Command Center is meant to be a streamlined interface for interactively running in production.

### 2. Dagster Workflow Orchestration

Development is ongoing - you can test an early version by following the steps below.

#### Local Development and Testing
> Prerequisites: `uv`. `docker`, a VAP VM with a registered managed identity in Azure, and rights to the cfaprdbatchcr container registry. Contact cfatoolsteam@cdc.gov for assistance with the latter two.

The following instructions will set up Dagster on your VAP. However, based on the current configuration, actual execution will still run in the cloud via Azure Batch. You can change the `executor` option in `dagster_defs.py` to test using the local Docker Executor - this will require you to have setup Blobfuse.

1. Setup your `uv virtual environment`:
    - `uv sync`
    - `source .venv/bin/activate`
2. Login to Azure and the Batch Container Registry:
    - `az login --identity && az acr login -n cfaprdbatchcr`
3. Build and push the `cfa-stf-routine-forecasting:dagster_latest` image:
    - `docker build -t ghcr.io/cdcgov/cfa-stf-routine-forecasting:dagster_latest -f Containerfile . --push`
3. Start the Dagster UI by running `uv run dagster_defs.py` and clicking the link in your terminal (usually [http://127.0.0.1:3000/])
4. You should now see the dagster UI for Pyrenew-HEW. This is a local server that will only show PyRenew-HEW asssets as defined in your local git repository.
5. Try materializing an asset by navigating to "Lineage" on the left sidebar. By default, these assets will submit jobs to Azure Batch and write to the `pyrenew-test-output` blob.
    - We recommend materializing a few partitions at a time for testing purposes.
    ![alt text](.github/img/dagster_lineage.png)
    - You will get a pop-up directing you to your asset runs, which provide progress logs.
    ![alt text](.github/img/dagster_runs.png)
6. Using the run ID dagster provides, you can also find your jobs in Azure Batch Explorer.

#### Production Scheduling

Pushes to main will automatically update the central Dagster Code Location for PyRenew-HEW via a Github Actions Workflow. From the central code server, you can run and schedule Pyrenew-HEW runs and see other projects' pipelines at CFA. You can also manually update the code server with a makefile recipe (see next section).

To manually update the code location while we evaluate dagster, you can run `make dagster_push_prod`. This manual approach will be deprecated and discouraged once we move to using dagster in production.

To login to the production server, head to https://dagster.apps.edav.ext.cdc.gov/.

#### Makefile Targets for Dagster
After you've familiarized yourself with the above instructions, feel free to use these convenient `make` targets:
- `make dagster_build`: builds your dagster image.
- `make dagster_push`: builds your dagster image, then pushes it.
- `make dagster_push_prod`: runs dagster_push, then uploads the central dagster scheduling server's code for pyrenew-hew.
- `make dagster`: runs the dagster UI locally.
- `make mount`: mounts the pyrenew-relevant blobs using blobfuse. Use this before launching locally-executed dagster jobs.
- `make unmount`: gracefully unmounts the pyrenew-relevant blobs.

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
