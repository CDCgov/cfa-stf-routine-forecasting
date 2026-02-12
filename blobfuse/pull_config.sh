#!/bin/bash
# Execute this script from top level directory: bash -c ./blobfuse/pull_config.sh

# Download the Azure configuration script from blob storage
az storage blob download \
	--account-name "cfaazurebatchprd" \
	--container-name "pyrenew-hew-config" \
	--name "azureconfig.sh" \
	--file "./azureconfig.sh" \
	--auth-mode login \
	--overwrite

source ./azureconfig.sh

# Download the blobfuse config yaml from blob storage
az storage blob download \
	--account-name "cfaazurebatchprd" \
	--container-name "pyrenew-hew-config" \
	--name "blobfuse_config.yaml" \
	--file "./config.yaml" \
	--auth-mode login \
	--overwrite
