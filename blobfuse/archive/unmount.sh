#!/bin/bash

# ensure logged in via Azure CLI.
./blobfuse/verifylogin.sh

if [[ "$?" -ne 0 ]]; then
	exit 1
fi

echo "Unmounting containers specified in mounts.txt with blobfuse2..."

TO_UNMOUNT=(
	"nssp-etl"
	"nssp-archival-vintages"
	"prod-param-estimates"
	"pyrenew-hew-prod-output"
	"pyrenew-test-output"
	"nwss-vintages"
	"pyrenew-hew-config"
)

for dir in "${TO_UNMOUNT[@]}"; do
	echo "Unmounting" $dir
	blobfuse2 unmount $dir
	rmdir $dir
done

echo "Done."
