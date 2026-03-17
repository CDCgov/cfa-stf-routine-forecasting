#!/bin/bash

# ensure logged in via Azure CLI.
./blobfuse/verifylogin.sh

# pull azure configuration files
./blobfuse/pull_config.sh

if [[ "$?" -ne 0 ]]; then
	exit 1
fi

# ensure cache exists
mkdir -p .cache

echo "Mounting containers specified in mounts.txt using blobfuse2..."
TO_MOUNT=(
	"nssp-etl"
	"nssp-archival-vintages"
	"prod-param-estimates"
	"pyrenew-hew-prod-output"
	"pyrenew-test-output"
	"nwss-vintages"
	"pyrenew-hew-config"
)

for dir in "${TO_MOUNT[@]}"; do
	echo "Mounting" $dir
	mkdir -p /mnt/$dir
	blobfuse2 mount --container-name $dir /mnt/$dir --allow-other
done

echo ""
mkdir -p ./blobfuse/mounts
echo "Creating symlinks in $(pwd)/blobfuse/mounts..."

# Create symlinks only if they do not already exist, and inform the user
if [[ -L "./blobfuse/mounts/params" ]]; then
	echo "Symlink './blobfuse/mounts/params' already exists, skipping."
else
	ln -s "/mnt/prod-param-estimates" "./blobfuse/mounts/params"
	echo "Created symlink './blobfuse/mounts/params' -> '/mnt/prod-param-estimates'"
fi

if [[ -L "./blobfuse/mounts/output" ]]; then
	echo "Symlink './blobfuse/mounts/output' already exists, skipping."
else
	ln -s "/mnt/pyrenew-hew-prod-output" "./blobfuse/mounts/output"
	echo "Created symlink './blobfuse/mounts/output' -> '/mnt/pyrenew-hew-prod-output'"
fi

if [[ -L "./blobfuse/mounts/test-output" ]]; then
	echo "Symlink './blobfuse/mounts/test-output' already exists, skipping."
else
	ln -s "/mnt/pyrenew-test-output" "./blobfuse/mounts/test-output"
	echo "Created symlink './blobfuse/mounts/test-output' -> '/mnt/pyrenew-test-output'"
fi

if [[ -L "./blobfuse/mounts/nwss-vintages" ]]; then
	echo "Symlink './blobfuse/mounts/nwss-vintages' already exists, skipping."
else
	ln -s "/mnt/nwss-vintages" "./blobfuse/mounts/nwss-vintages"
	echo "Created symlink './blobfuse/mounts/nwss-vintages' -> '/mnt/nwss-vintages'"
fi

if [[ -L "./blobfuse/mounts/config" ]]; then
	echo "Symlink './blobfuse/mounts/config' already exists, skipping."
else
	ln -s "/mnt/pyrenew-hew-config" "./blobfuse/mounts/config"
	echo "Created symlink './blobfuse/mounts/config' -> '/mnt/pyrenew-hew-config'"
fi

if [[ -L "./blobfuse/mounts/nssp-etl" ]]; then
	echo "Symlink './blobfuse/mounts/nssp-etl' already exists, skipping."
else
	ln -s "/mnt/nssp-etl" "./blobfuse/mounts/nssp-etl"
	echo "Created symlink './blobfuse/mounts/nssp-etl' -> '/mnt/nssp-etl'"
fi

if [[ -L "./blobfuse/mounts/nssp-archival-vintages" ]]; then
	echo "Symlink './blobfuse/mounts/nssp-archival-vintages' already exists, skipping."
else
	ln -s "/mnt/nssp-archival-vintages" "./blobfuse/mounts/nssp-archival-vintages"
	echo "Created symlink './blobfuse/mounts/nssp-archival-vintages' -> '/mnt/nssp-archival-vintages'"
fi

echo "Setting environment variables for the Azure Command Center..."
export NSSP_ETL_PATH="$(pwd)/blobfuse/mounts/nssp-etl"
export PYRENEW_HEW_PROD_OUTPUT_PATH="$(pwd)/blobfuse/mounts/output"
export NWSS_VINTAGES_PATH="$(pwd)/blobfuse/mounts/nwss-vintages"

echo "Done."
