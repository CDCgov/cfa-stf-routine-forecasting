#!/usr/bin/env python3

import argparse

import azuretools.defaults as d
from azure.mgmt.batch import models
from azuretools import blob
from azuretools.auth import EnvCredentialHandler
from azuretools.client import get_batch_management_client


def main(pool_name: str) -> None:
    """
    Set up a pool with a given name
    and default configuration.

    Parameters
    ----------
    pool_name
       name for the pool

    Returns
    -------
    None
    """

    creds = EnvCredentialHandler()
    client = get_batch_management_client(creds)
    node_id_ref = creds.compute_node_identity_reference
    # To run models with wastewater signal (which require more resources),
    # pass `vm_size="standard_d8s_v3"` to override the default size.
    pool_config = d.get_default_pool_config(
        pool_name=pool_name,
        subnet_id=creds.azure_subnet_id,
        user_assigned_identity=creds.azure_user_assigned_identity,
    )

    pool_config.mount_configuration = blob.get_node_mount_config(
        storage_containers=[
            "nssp-etl",
            "nssp-archival-vintages",
            "nwss-vintages",
            "prod-param-estimates",
            "pyrenew-hew-prod-output",
            "pyrenew-hew-config",
            "pyrenew-test-output",
        ],
        account_names=creds.azure_blob_storage_account,
        identity_references=node_id_ref,
    )

    (
        pool_config.deployment_configuration.virtual_machine_configuration.container_configuration
    ) = models.ContainerConfiguration(
        type="dockerCompatible",
        container_registries=[creds.azure_container_registry],
    )

    client.pool.create(
        resource_group_name=creds.azure_resource_group_name,
        account_name=creds.azure_batch_account,
        pool_name=pool_name,
        parameters=pool_config,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Set up an Azure batch pool using the azuretools defaults")
    )
    parser.add_argument(
        "pool_name",
        type=str,
        help="A name for the pool",
    )

    parsed = vars(parser.parse_args())

    main(parsed["pool_name"])
