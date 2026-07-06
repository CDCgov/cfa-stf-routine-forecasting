# Pyrenew Blobfuse Configuration

This directory serves as a project-specific fork of the [cfa-blobfuse-tutorial](https://github.com/cdcent).
> [!NOTE]
> This is useful when testing, and is necessary (in this repository) for using the dagster `docker_executor` or `in_process_executor`.

> Make sure you have blobfuse2 installed before running this module.

This directory will mount blobs to `/mnt` and then symlinks to `./blobfuse/mounts/`.

To run, make sure you're in the top level as your working directory (`cfa-stf-routine-forecasting`, and not `cfa-stf-routine-forecasting/blobfuse`).
1. Run `sudo "./blobfuse/mount.sh"`. It is good practice to run `sudo "./blobfuse/cleanup.sh"` to make sure your setup is clean (see below).
1. Check to make sure `/mnt` has blobs mounted and that symlinks have been created under `cfa-stf-routine-forecasting/blobfuse/mounts/`.
1. Before attempting to remount, always run the cleanup script `sudo "./blobfuse/cleanup.sh"` and make sure there are no empty directories in `./blobfuse/mounts/`.
