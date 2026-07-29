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
for dir in "${TO_MOUNT[@]}"; do
	link="./blobfuse/mounts/$dir"
	if [[ -L "$link" ]]; then
		echo "Symlink '$link' already exists, skipping."
	else
		ln -s "/mnt/$dir" "$link"
		echo "Created symlink '$link' -> '/mnt/$dir'"
	fi
done

echo "Done."
