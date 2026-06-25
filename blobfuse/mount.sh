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
	"nwss-vintages"
	"prod-param-estimates"
	"stf-routine-forecasting-prod-output"
	"stf-routine-forecasting-test-output"
	"stf-routine-forecasting-config"
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
if [[ -L "./blobfuse/mounts/prod-param-estimates" ]]; then
	echo "Symlink './blobfuse/mounts/prod-param-estimates' already exists, skipping."
else
	ln -s "/mnt/prod-param-estimates" "./blobfuse/mounts/prod-param-estimates"
	echo "Created symlink './blobfuse/mounts/prod-param-estimates' -> '/mnt/prod-param-estimates'"
fi

if [[ -L "./blobfuse/mounts/output" ]]; then
	echo "Symlink './blobfuse/mounts/stf-routine-forecasting-prod-output' already exists, skipping."
else
	ln -s "/mnt/stf-routine-forecasting-prod-output" "./blobfuse/mounts/stf-routine-forecasting-prod-output"
	echo "Created symlink './blobfuse/mounts/stf-routine-forecasting-prod-output' -> '/mnt/stf-routine-forecasting-prod-output'"
fi

if [[ -L "./blobfuse/mounts/test-output" ]]; then
	echo "Symlink './blobfuse/mounts/stf-routine-forecasting-test-output' already exists, skipping."
else
	ln -s "/mnt/stf-routine-forecasting-test-output" "./blobfuse/mounts/stf-routine-forecasting-test-output"
	echo "Created symlink './blobfuse/mounts/stf-routine-forecasting-test-output' -> '/mnt/stf-routine-forecasting-test-output'"
fi

if [[ -L "./blobfuse/mounts/nwss-vintages" ]]; then
	echo "Symlink './blobfuse/mounts/nwss-vintages' already exists, skipping."
else
	ln -s "/mnt/nwss-vintages" "./blobfuse/mounts/nwss-vintages"
	echo "Created symlink './blobfuse/mounts/nwss-vintages' -> '/mnt/nwss-vintages'"
fi

if [[ -L "./blobfuse/mounts/config" ]]; then
	echo "Symlink './blobfuse/mounts/stf-routine-forecasting-config' already exists, skipping."
else
	ln -s "/mnt/stf-routine-forecasting-config" "./blobfuse/mounts/stf-routine-forecasting-config"
	echo "Created symlink './blobfuse/mounts/stf-routine-forecasting-config' -> '/mnt/stf-routine-forecasting-config'"
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

echo "Done."
