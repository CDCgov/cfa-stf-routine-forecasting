# Build parameters

ifndef ENGINE
ENGINE = docker
endif

ifndef CONTAINER_REGISTRY
CONTAINER_REGISTRY = ghcr.io/cdcgov
endif

ifndef CONTAINER_IMAGE_NAME
CONTAINER_IMAGE_NAME = cfa-stf-routine-forecasting
endif

ifndef CONTAINER_IMAGE_VERSION
CONTAINER_IMAGE_VERSION = latest
endif

ifndef CONTAINER_REMOTE_NAME
CONTAINER_REMOTE_NAME = $(CONTAINER_REGISTRY)/$(CONTAINER_IMAGE_NAME):$(CONTAINER_IMAGE_VERSION)
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
	@echo ""
	@echo "Dagster Targets: "
	@echo "  dagster_push_prod   : Push the dagster container image to the Azure Container Registry and code location for production"
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
	$(ENGINE) build . -t $(CONTAINER_REMOTE_NAME) -f $(CONTAINERFILE)

container_tag:
	$(ENGINE) tag $(CONTAINER_REMOTE_NAME) $(CONTAINER_REMOTE_NAME)

container_push: ghcr_login
	$(ENGINE) push $(CONTAINER_REMOTE_NAME)

config:
	bash -c "source ./azureconfig.sh"

dagster_push_prod:
	docker build . -t ghcr.io/cdcgov/cfa-stf-routine-forecasting:dagster_latest -f Containerfile && \
	docker push ghcr.io/cdcgov/cfa-stf-routine-forecasting:dagster_latest && \
	uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/update_code_location.py \
    	--registry_image ghcr.io/cdcgov/cfa-stf-routine-forecasting:dagster_latest

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
