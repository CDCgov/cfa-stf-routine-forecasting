import cfa.cloudops

client = cfa.cloudops.CloudClient(keyvault="cfa-predict")

client.create_pool(
    pool_name="stf-routine-forecasting-pool",
    container_image_name="ubuntu-hpc-2404",
    max_autoscale_nodes=400,
    low_priority_nodes=0,
    cache_blobfuse=False,
    replace_existing_pool=True,
)
