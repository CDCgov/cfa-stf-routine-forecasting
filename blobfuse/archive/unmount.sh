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
	"stf-routine-forecasting-prod-output"
	"stf-routine-forecasting-test-output"
	"nwss-vintages"
	"stf-routine-forecasting-config"
)

for dir in "${TO_UNMOUNT[@]}"; do
	echo "Unmounting" $dir
	blobfuse2 unmount $dir
	rmdir $dir
done

echo "Done."
