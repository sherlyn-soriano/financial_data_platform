import pulumi
import pulumi_azure_native as azure_native

def create_data_lake(name: str, resource_group_name: str, location: str):
    """Create Data Lake Storage Gen2 with herarchical namespace"""

    storage_account = azure_native.storage.StorageAccount(
        name,
        account_name=name,
        resource_group_name=resource_group_name,
        location=location,
        sku=azure_native.storage.SkuArgs(
            name=azure_native.storage.SkuName.STANDARD_LRS,
        ),
        kind=azure_native.storage.Kind.STORAGE_V2,
        is_hns_enabled=True,
        enable_https_traffic_only=True,
        minimum_tls_version=azure_native.storage.MinimumTlsVersion.TLS1_2,
        allow_blob_public_access=False,
        tags={
            "Purpose":"DataLake",
            "Layer": "Storage"
        }
    )

    bronze_container = azure_native.storage.BlobContainer(
        "bronze-container",
        account_name=storage_account.name,
        resource_group_name=resource_group_name,
        container_name="bronze",
        public_access=azure_native.storage.PublicAccess.NONE
    )

    silver_container = azure_native.storage.BlobContainer(
        "silver-container",
        account_name= storage_account.name,
        resource_group_name=resource_group_name,
        container_name="silver",
        public_access=azure_native.storage.PublicAccess.NONE
    )

    gold_container = azure_native.storage.BlobContainer(
        "gold-container",
        account_name=storage_account.name,
        resource_group_name=resource_group_name,
        container_name="gold",
        public_access=azure_native.storage.PublicAccess.NONE
    )

    lifecycle_policy = azure_native.storage.ManagementPolicy(
        "lifecycle-policy",
        account_name = storage_account.name,
        resource_group_name = resource_group_name,
        policy=azure_native.storage.ManagementPolicySchemaArgs(
            rules=[
                azure_native.storage.ManagementPolicyRuleArgs(
                    enabled=True,
                    name="move-to-cool-tier",
                    type=azure_native.storage.RuleType.LIFECYCLE,
                    definition=azure_native.storage.ManagementPolicyDefinitionArgs(
                        actions=azure_native.storage.ManagementPolicyActionArgs(
                            base_blob=azure_native.storage.ManagementPolicyBaseBlobArgs(
                                tier_to_cool=azure_native.storage.DateAfterModificationArgs(
                                    days_after_modification_greater_than = 30
                                ),
                                tier_to_archive=azure_native.storage.DateAfterModificationArgs(
                                    days_after_modification_greater_than=90
                                ),
                                delete=azure_native.storage.DateAfterModificationArgs(
                                    days_after_modification_greater_than=365
                                )
                            )
                        ),
                        filters=azure_native.storage.ManagementPolicyFilterArgs(
                            blob_types=["blockBlob"],
                            prefix_match=["bronze"]
                        )
                    )
                )
            ]
        )
    )

    pulumi.log.info(f"Data Lake created: {name}")
    return storage_account