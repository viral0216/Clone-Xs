"""Parallel cross-workspace clone — clone one source to multiple destinations."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.client import get_workspace_client
from src.clone_catalog import clone_catalog

logger = logging.getLogger(__name__)


def clone_to_multiple_workspaces(
    config: dict,
    destinations: list[dict],
    max_parallel: int = 2,
) -> dict:
    """Clone a source catalog to multiple destination workspaces in parallel.

    Args:
        config: Base clone configuration.
        destinations: List of destination configs, each with:
            - host: Workspace URL
            - token: Auth token
            - sql_warehouse_id: Warehouse ID in dest workspace
            - destination_catalog: Target catalog name
        max_parallel: Max concurrent workspace clones.

    Returns:
        Summary with per-workspace results.
    """
    source = config.get("source_catalog", "unknown")
    logger.info(f"Starting parallel clone of '{source}' to {len(destinations)} workspaces")

    results = {}

    def _clone_to_workspace(dest_config: dict) -> dict:
        host = dest_config["host"]
        dest_catalog = dest_config.get("destination_catalog", config.get("destination_catalog"))
        warehouse_id = dest_config["sql_warehouse_id"]

        workspace_label = f"{host} / {dest_catalog}"
        logger.info(f"[{workspace_label}] Starting clone...")

        try:
            client = get_workspace_client(host=host, token=dest_config["token"])
            clone_config = {
                **config,
                "destination_catalog": dest_catalog,
                "sql_warehouse_id": warehouse_id,
            }
            summary = clone_catalog(client, clone_config)
            total_failed = sum(
                summary[t]["failed"] for t in ("tables", "views", "functions", "volumes")
            )
            status = "success" if total_failed == 0 else "partial_failure"
            logger.info(f"[{workspace_label}] Clone completed: {status}")
            return {
                "workspace": host,
                "catalog": dest_catalog,
                "status": status,
                "summary": summary,
            }
        except Exception as e:
            logger.error(f"[{workspace_label}] Clone failed: {e}")
            return {
                "workspace": host,
                "catalog": dest_catalog,
                "status": "failed",
                "error": str(e),
            }

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(_clone_to_workspace, dest): dest
            for dest in destinations
        }

        workspace_results = []
        for future in as_completed(futures):
            result = future.result()
            workspace_results.append(result)

    succeeded = sum(1 for r in workspace_results if r["status"] == "success")
    failed = sum(1 for r in workspace_results if r["status"] == "failed")
    partial = sum(1 for r in workspace_results if r["status"] == "partial_failure")

    summary = {
        "source_catalog": source,
        "total_destinations": len(destinations),
        "succeeded": succeeded,
        "failed": failed,
        "partial_failures": partial,
        "results": workspace_results,
    }

    logger.info(f"Multi-workspace clone complete: {succeeded} succeeded, {failed} failed, {partial} partial")
    return summary
