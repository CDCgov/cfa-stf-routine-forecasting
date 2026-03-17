#!/bin/bash

# ensure logged in via Azure CLI.
./blobfuse/verifylogin.sh

if [[ "$?" -ne 0 ]]; then
	exit 1
fi

echo "Cleaning up blobfuse mounts"

echo "Unmounting any mounted blob storage containers"
blobfuse2 unmount all

echo "Removing all empty entries in /mnt/"
find /mnt/ -mindepth 1 -type d -empty -delete

echo "Clearing the cache"
rm -rf .blobfuse_cache/*

echo "Removing empty directories"
find . -type d -empty -delete

echo "Removing symlinks"
find . -type l -delete

echo "Done!"
