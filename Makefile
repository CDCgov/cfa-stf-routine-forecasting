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
BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)

ifeq ($(BRANCH_NAME),main)
CONTAINER_TAG ?= latest
else
CONTAINER_TAG ?= $(BRANCH_NAME)
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
	@echo "  ghcr_login          : Log in to the Github Container Registry using gh auth or GH_USERNAME/GH_PAT"
	@echo "  container_build     : Build the container image"
	@echo "  container_tag	     : Tag the container image for pushing to the registry"
	@echo "  container_push	     : Push the container image"
	@echo "  container_explore   : Run the last locally-built container interactively in your shell"
	@echo '  prod_test           : Push your local image to the dagster prod server (use sparingly and with agreement from team)'
	@echo ""
	@echo ""
	@echo "Model Fit Targets: "
	@echo "  config              : Source the azureconfig.sh file to set environment variables for Azure access"
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
	@if [ -n "$(GH_PAT)" ] && [ -n "$(GH_USERNAME)" ]; then \
		echo "$$GH_PAT" | $(ENGINE) login ghcr.io -u "$(GH_USERNAME)" --password-stdin; \
	elif command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then \
		if ! gh auth status -h github.com 2>&1 | grep -q "write:packages"; then \
			gh auth refresh -h github.com -s write:packages; \
		fi; \
		GH_LOGIN=$$(gh api user --jq .login); \
		gh auth token | $(ENGINE) login ghcr.io -u "$$GH_LOGIN" --password-stdin; \
	else \
		echo "Error: run 'gh auth login' or set GH_USERNAME and GH_PAT to log in to GitHub Container Registry"; \
		exit 1; \
	fi

container_build:
	$(ENGINE) build . -t $(CONTAINER_REMOTE_NAME) -f $(CONTAINERFILE) \
	--platform linux/amd64

container_tag:
	$(ENGINE) tag $(CONTAINER_REMOTE_NAME) $(CONTAINER_REMOTE_NAME)

container_push: container_build ghcr_login
	$(ENGINE) push $(CONTAINER_REMOTE_NAME)

container_explore:
	$(ENGINE) run -it --rm $(CONTAINER_REMOTE_NAME) bash

prod_test: container_push
	uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/update_code_location.py \
	--registry_image $(CONTAINER_REMOTE_NAME)

config:
	bash -c "source ./azureconfig.sh"

# ---------------- #
# Model Fit Targets
# ---------------- #

post_process: config
	uv run python pipelines/utils/postprocess_forecast_batches.py \
    	--input "./blobfuse/mounts/stf-routine-forecasting-prod-output/${FORECAST_DATE}_forecasts" \
    	--output "./blobfuse/mounts/nssp-etl/gold/${FORECAST_DATE}_forecasts.parquet" \
		${ARGS}
