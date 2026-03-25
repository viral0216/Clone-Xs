"""Multi-cloud support — handle AWS, Azure, and GCP workspace credentials and routing."""

import logging
import os
from dataclasses import dataclass

from src.client import get_workspace_client

logger = logging.getLogger(__name__)


@dataclass
class CloudWorkspace:
    """Represents a Databricks workspace on a specific cloud provider."""
    name: str
    cloud: str  # aws, azure, gcp
    host: str
    token: str | None = None
    warehouse_id: str | None = None
    # Azure-specific
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None
    # GCP-specific
    gcp_service_account_key: str | None = None
    # AWS-specific
    aws_profile: str | None = None


def detect_cloud_provider(host: str) -> str:
    """Detect the cloud provider from the workspace URL.

    Returns:
        'aws', 'azure', or 'gcp'
    """
    host_lower = host.lower()
    if "azuredatabricks.net" in host_lower:
        return "azure"
    elif "gcp.databricks.com" in host_lower:
        return "gcp"
    elif "cloud.databricks.com" in host_lower or "databricks.com" in host_lower:
        return "aws"
    else:
        logger.warning(f"Could not detect cloud provider from host: {host}. Assuming AWS.")
        return "aws"


def load_workspaces_from_config(config: dict) -> list[CloudWorkspace]:
    """Load workspace definitions from config.

    Config format:
        workspaces:
          - name: prod-aws
            cloud: aws
            host: https://xxx.cloud.databricks.com
            token: dapi...
            warehouse_id: abc123
          - name: staging-azure
            cloud: azure
            host: https://xxx.azuredatabricks.net
            token: dapi...
            warehouse_id: def456
    """
    workspaces = []
    for ws_config in config.get("workspaces", []):
        ws = CloudWorkspace(
            name=ws_config["name"],
            cloud=ws_config.get("cloud", detect_cloud_provider(ws_config["host"])),
            host=ws_config["host"],
            token=ws_config.get("token"),
            warehouse_id=ws_config.get("warehouse_id"),
            azure_tenant_id=ws_config.get("azure_tenant_id"),
            azure_client_id=ws_config.get("azure_client_id"),
            azure_client_secret=ws_config.get("azure_client_secret"),
            gcp_service_account_key=ws_config.get("gcp_service_account_key"),
            aws_profile=ws_config.get("aws_profile"),
        )
        workspaces.append(ws)

    return workspaces


def get_client_for_workspace(workspace: CloudWorkspace):
    """Get a WorkspaceClient configured for a specific cloud workspace.

    Handles authentication differences across cloud providers.
    """
    if workspace.token:
        return get_workspace_client(host=workspace.host, token=workspace.token)

    # Azure Service Principal auth
    if workspace.cloud == "azure" and workspace.azure_client_id:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=workspace.host,
            azure_tenant_id=workspace.azure_tenant_id,
            azure_client_id=workspace.azure_client_id,
            azure_client_secret=workspace.azure_client_secret,
        )

    # GCP Service Account auth
    if workspace.cloud == "gcp" and workspace.gcp_service_account_key:
        from databricks.sdk import WorkspaceClient
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = workspace.gcp_service_account_key
        return WorkspaceClient(host=workspace.host)

    # AWS Profile auth
    if workspace.cloud == "aws" and workspace.aws_profile:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=workspace.host,
            profile=workspace.aws_profile,
        )

    # Fall back to default auth
    return get_workspace_client(host=workspace.host)


def clone_across_clouds(
    config: dict,
    source_workspace: CloudWorkspace,
    dest_workspace: CloudWorkspace,
) -> dict:
    """Clone a catalog from one cloud workspace to another.

    Cross-cloud cloning is done via deep clone (shallow clone across clouds is not supported).
    The process:
    1. Read schema from source workspace
    2. Create structures in destination workspace
    3. Deep clone tables (data is copied across clouds)

    Args:
        config: Clone configuration
        source_workspace: Source cloud workspace
        dest_workspace: Destination cloud workspace

    Returns:
        Clone summary.
    """
    source_cloud = source_workspace.cloud.upper()
    dest_cloud = dest_workspace.cloud.upper()

    logger.info(f"Cross-cloud clone: {source_cloud} ({source_workspace.name}) -> {dest_cloud} ({dest_workspace.name})")

    if config.get("clone_type", "DEEP").upper() == "SHALLOW":
        logger.warning("Shallow clone is not supported across clouds. Switching to DEEP clone.")
        config["clone_type"] = "DEEP"

    # Get clients for both workspaces
    get_client_for_workspace(source_workspace)
    get_client_for_workspace(dest_workspace)

    # Use the multi-workspace clone mechanism
    from src.multi_workspace_clone import clone_to_multiple_workspaces

    clone_config = {
        **config,
        "sql_warehouse_id": source_workspace.warehouse_id or config.get("sql_warehouse_id"),
    }

    destinations = [{
        "host": dest_workspace.host,
        "token": dest_workspace.token,
        "sql_warehouse_id": dest_workspace.warehouse_id,
        "destination_catalog": config.get("destination_catalog"),
    }]

    result = clone_to_multiple_workspaces(clone_config, destinations)

    logger.info(f"Cross-cloud clone complete: {source_cloud} -> {dest_cloud}")
    return result


def list_workspaces(config: dict) -> None:
    """List all configured workspaces with their cloud providers."""
    workspaces = load_workspaces_from_config(config)

    if not workspaces:
        logger.info("No workspaces configured. Add them to your config under 'workspaces:'")
        return

    logger.info("=" * 60)
    logger.info("CONFIGURED WORKSPACES")
    logger.info("=" * 60)

    for ws in workspaces:
        cloud_icon = {"aws": "☁️ AWS", "azure": "🔷 Azure", "gcp": "🟢 GCP"}.get(ws.cloud, ws.cloud)
        auth = "token" if ws.token else (
            "service_principal" if ws.azure_client_id else (
                "service_account" if ws.gcp_service_account_key else "default"
            )
        )
        logger.info(f"  {ws.name}")
        logger.info(f"    Cloud:     {cloud_icon}")
        logger.info(f"    Host:      {ws.host}")
        logger.info(f"    Auth:      {auth}")
        logger.info(f"    Warehouse: {ws.warehouse_id or 'N/A'}")
        logger.info("")
