# Pyrenew Blobfuse Configuration

This directory serves as a project-specific fork of the [cfa-blobfuse-tutorial](https://github.com/cdcent).
> [!NOTE]
> This is useful when testing, and is necessary (in this repository) for using the dagster `docker_executor` or `in_process_executor`.

> Make sure you have blobfuse2 installed before running this module.

This directory will mount blobs to `/mnt` and then symlink to a directory you specify (or the current directory if you don't supply an argument).

To run, make sure you're in the top level as your working directory (`cfa-stf-routine-forecasting`, and not `cfa-stf-routine-forecasting/blobfuse`).
1. Run `sudo chmod +x ./blobfuse/mount.sh`.
2. Run `sudo source bash -c "./blobfuse/mount.sh"`.
3. Check to make sure `/mnt` has blobs mounted and that symlinks have been created under `cfa-stf-routine-forecasting/blobfuse/mounts/`.
4. Before attempting to remount, always run the cleanup script `sudo source bash -c "./blobfuse/cleanup.sh"` and make sure there are no empty directories in `./blobfuse/mounts/`.
