"""Create the Azure Batch pool used by routine forecasting."""

import cfa.cloudops


def main() -> None:
    """Create or replace the routine forecasting Azure Batch pool."""
    client = cfa.cloudops.CloudClient(keyvault="cfa-predict")
    client.create_pool(
        pool_name="stf-routine-forecasting-pool",
        vm_size="small",
        mounts=[
            "stf-routine-forecasting-prod-output",
            "stf-routine-forecasting-test-output",
        ],
        max_autoscale_nodes=400,
        low_priority_nodes=0,
        cache_blobfuse=True,
        replace_existing_pool=True,
    )


if __name__ == "__main__":
    main()
