#!/bin/bash

which az &>/dev/null
if [[ "$?" -ne 0 ]]; then
	echo "Could not find the Azure CLI 'az'. Check that it is installed and on your PATH."
	exit 1
fi

az account show &>/dev/null
if [[ "$?" -ne 0 ]]; then
	echo "User does not appear to be logged in via the Azure CLI. Attempting to log in with managed identity..."
	az login --identity &>/dev/null
	if [[ "$?" -ne 0 ]]; then
		echo "Failed to log in with managed identity. Please run 'az login' manually and try again."
		exit 1
	fi
	echo "Logged in with managed identity."
fi

exit 0
