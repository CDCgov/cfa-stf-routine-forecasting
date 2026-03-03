# Build parameters

ifndef ENGINE
ENGINE = docker
endif

ifndef CONTAINER_REGISTRY
CONTAINER_REGISTRY = ghcr.io/cdcgov
endif

ifndef CONTAINER_NAME
CONTAINER_NAME = cfa-stf-routine-forecasting
endif

# Determine container tag from git branch: "main" -> "latest", otherwise use branch name
GIT_BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)
GIT_COMMIT_SHA := $(shell git rev-parse HEAD)

ifeq ($(GIT_BRANCH_NAME),main)
CONTAINER_TAG ?= latest
else
CONTAINER_TAG ?= $(GIT_BRANCH_NAME)
endif


ifndef CONTAINER_REMOTE_NAME
CONTAINER_REMOTE_NAME = $(CONTAINER_REGISTRY)/$(CONTAINER_NAME):$(CONTAINER_TAG)
endif

ifndef CONTAINERFILE
CONTAINERFILE = Containerfile
endif

# Post-processing Parameters

ifndef FORECAST_DATE
FORECAST_DATE = $(shell date +%Y-%m-%d)
endif

# ----------- #
# Help Target #
# ----------- #

help:
	@echo "Usage: make [target] [ARGS]"
	@echo ""

	@echo "Blobfuse Mount Targets: "
	@echo "  mount              : Mount blob storage containers using blobfuse2"
	@echo "  unmount            : Unmount blob storage containers and clean up"
	@echo ""
	@echo "Container Build Targets: "
	@echo "  ghcr_login          : Log in to the Github Container Registry. Requires GH_USERNAME and GH_PAT env vars"
	@echo "  container_build     : Build the container image"
	@echo "  container_tag	     : Tag the container image for pushing to the registry"
	@echo "  container_push	     : Push the container image"
	@echo "  container_explore   : Run the last locally-built container interactively in your shell"
	@echo ""
# 	@echo "Dagster Targets: "
# 	@echo "  dagster_push_prod   : Push the dagster container image to the Azure Container Registry and code location for production"
	@echo ""
	@echo "Model Fit Targets: "
	@echo "  config              : Source the azureconfig.sh file to set environment variables for Azure access"
	@echo "  acc                 : Run the Azure Command Center for routine production jobs"
	@echo "  post_process        : Post-process model outputs."
	@echo "Passing a flag through ARGS will also override the flags set previously."

#------------------------ #
# Blobfuse Mount Targets
# ----------------------- #

mount:
	sudo bash -c "source ./blobfuse/mount.sh"

unmount:
	sudo bash -c "source ./blobfuse/cleanup.sh"

# ----------------------- #
# Container Build Targets
# ----------------------- #

ghcr_login:
	@if [ -z "$(GH_PAT)" ] || [ -z "$(GH_USERNAME)" ]; then \
		echo "Error: GH_PAT and GH_USERNAME environment variables must be set to log in to GitHub Container Registry"; \
		exit 1; \
	fi; \
	echo "$$GH_PAT" | $(ENGINE) login ghcr.io -u "$(GH_USERNAME)" --password-stdin

container_build:
	$(ENGINE) build . -t $(CONTAINER_REMOTE_NAME) -f $(CONTAINERFILE) \
	--build-arg GIT_COMMIT_SHA=$(GIT_COMMIT_SHA) \
	--build-arg GIT_BRANCH_NAME=$(GIT_BRANCH_NAME)

container_tag:
	$(ENGINE) tag $(CONTAINER_REMOTE_NAME) $(CONTAINER_REMOTE_NAME)

container_push: ghcr_login container_build
	$(ENGINE) push $(CONTAINER_REMOTE_NAME)

container_explore:
	$(ENGINE) run -it --rm $(CONTAINER_REMOTE_NAME) bash

config:
	bash -c "source ./azureconfig.sh"

# dagster_push_prod:
# 	docker build . -t ghcr.io/cdcgov/cfa-stf-routine-forecasting:latest -f Containerfile && \
# 	docker push ghcr.io/cdcgov/cfa-stf-routine-forecasting:latest && \
# 	uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/update_code_location.py \
#     	--registry_image ghcr.io/cdcgov/cfa-stf-routine-forecasting:latest

# ---------------- #
# Model Fit Targets
# ---------------- #

acc: mount config
	uv run pipelines/batch/azure_command_center.py

post_process: config
	uv run python pipelines/utils/postprocess_forecast_batches.py \
    	--input "./blobfuse/mounts/pyrenew-hew-prod-output/${FORECAST_DATE}_forecasts" \
    	--output "./blobfuse/mounts/nssp-etl/gold/${FORECAST_DATE}_forecasts.parquet" \
		${ARGS}
