import pulumi
import pulumi_azure_native as azure_native

def create_resource_group(name: str, location: str, tags: dict):
    """Create resource group name """
    rg = azure_native.resources.ResourceGroup(
        name,
        resource_group_name = name,
        location=location,
        tags=tags
    )
    pulumi.log.info(f"Resource Group Created: {name}")
    return rg