import cfa.cloudops

client = cfa.cloudops.CloudClient(keyvault="cfa-predict")

client.create_pool(
    pool_name="stf-routine-forecasting-pool",
    vm_size="small",
    mounts=[
        "nssp-archival-vintages",
        "nssp-etl",
        "nwss-vintages",
        "prod-param-estimates",
        "stf-routine-forecasting-config",
        "stf-routine-forecasting-prod-output",
        "stf-routine-forecasting-test-output",
    ],
    max_autoscale_nodes=400,
    low_priority_nodes=0,
    cache_blobfuse=True,
    replace_existing_pool=True,
)
