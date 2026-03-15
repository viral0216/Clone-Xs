"""Warehouse auto-scaling — manage SQL warehouse lifecycle around clone operations."""

import logging
import time

logger = logging.getLogger(__name__)


def get_warehouse_status(client, warehouse_id: str) -> dict:
    """Get the current status of a SQL warehouse.

    Returns:
        Dict with id, name, state, size, cluster_size, auto_stop_mins, etc.
    """
    try:
        wh = client.warehouses.get(warehouse_id)
        return {
            "id": wh.id,
            "name": wh.name,
            "state": wh.state.value if wh.state else "UNKNOWN",
            "size": wh.cluster_size,
            "num_clusters": wh.num_clusters,
            "min_num_clusters": wh.min_num_clusters,
            "max_num_clusters": wh.max_num_clusters,
            "auto_stop_mins": wh.auto_stop_mins,
            "warehouse_type": wh.warehouse_type.value if wh.warehouse_type else "UNKNOWN",
            "enable_serverless_compute": getattr(wh, "enable_serverless_compute", None),
        }
    except Exception as e:
        logger.error(f"Failed to get warehouse status: {e}")
        return {"id": warehouse_id, "state": "ERROR", "error": str(e)}


def ensure_warehouse_running(
    client,
    warehouse_id: str,
    timeout_minutes: int = 10,
    poll_interval_seconds: int = 10,
) -> bool:
    """Ensure the warehouse is running. Start it if stopped.

    Args:
        client: WorkspaceClient
        warehouse_id: SQL warehouse ID
        timeout_minutes: Max time to wait for startup
        poll_interval_seconds: Time between status checks

    Returns:
        True if warehouse is running, False if timeout.
    """
    status = get_warehouse_status(client, warehouse_id)
    state = status.get("state", "UNKNOWN")

    if state == "RUNNING":
        logger.info(f"Warehouse {warehouse_id} is already running")
        return True

    if state in ("STOPPED", "STOPPING"):
        logger.info(f"Warehouse {warehouse_id} is {state}, starting it...")
        try:
            client.warehouses.start(warehouse_id)
        except Exception as e:
            logger.error(f"Failed to start warehouse: {e}")
            return False
    elif state in ("STARTING",):
        logger.info(f"Warehouse {warehouse_id} is already starting...")
    else:
        logger.warning(f"Warehouse {warehouse_id} is in state: {state}")
        try:
            client.warehouses.start(warehouse_id)
        except Exception:
            pass

    # Poll until running
    deadline = time.time() + (timeout_minutes * 60)
    while time.time() < deadline:
        status = get_warehouse_status(client, warehouse_id)
        state = status.get("state", "UNKNOWN")

        if state == "RUNNING":
            logger.info(f"Warehouse {warehouse_id} is now running")
            return True

        if state in ("DELETED", "DELETING"):
            logger.error(f"Warehouse {warehouse_id} has been deleted")
            return False

        remaining = int((deadline - time.time()) / 60)
        logger.info(f"Warehouse state: {state}. Waiting... ({remaining}m remaining)")
        time.sleep(poll_interval_seconds)

    logger.error(f"Warehouse {warehouse_id} did not start within {timeout_minutes} minutes")
    return False


def scale_warehouse(
    client,
    warehouse_id: str,
    new_size: str | None = None,
    min_clusters: int | None = None,
    max_clusters: int | None = None,
) -> dict:
    """Scale a SQL warehouse up or down.

    Args:
        new_size: Cluster size (2X-Small, X-Small, Small, Medium, Large, X-Large, etc.)
        min_clusters: Minimum number of clusters
        max_clusters: Maximum number of clusters

    Returns:
        Updated warehouse status.
    """
    current = get_warehouse_status(client, warehouse_id)
    logger.info(f"Current warehouse size: {current.get('size')}, clusters: {current.get('num_clusters')}")

    update_kwargs = {}
    if new_size:
        update_kwargs["cluster_size"] = new_size
    if min_clusters is not None:
        update_kwargs["min_num_clusters"] = min_clusters
    if max_clusters is not None:
        update_kwargs["max_num_clusters"] = max_clusters

    if not update_kwargs:
        logger.info("No scaling changes requested")
        return current

    try:
        client.warehouses.edit(
            warehouse_id,
            name=current.get("name", ""),
            cluster_size=new_size or current.get("size", "Small"),
            min_num_clusters=min_clusters or current.get("min_num_clusters", 1),
            max_num_clusters=max_clusters or current.get("max_num_clusters", 1),
            auto_stop_mins=current.get("auto_stop_mins", 120),
        )
        logger.info(f"Warehouse {warehouse_id} scaling updated")
    except Exception as e:
        logger.error(f"Failed to scale warehouse: {e}")
        return {"error": str(e)}

    return get_warehouse_status(client, warehouse_id)


def auto_manage_warehouse(
    client,
    warehouse_id: str,
    config: dict,
    catalog_size_gb: float = 0,
) -> dict:
    """Automatically manage warehouse for a clone operation.

    Logic:
    - Start warehouse if stopped
    - Scale up if catalog is large
    - Returns original settings to restore after clone

    Args:
        client: WorkspaceClient
        warehouse_id: SQL warehouse ID
        config: Clone config
        catalog_size_gb: Estimated catalog size

    Returns:
        Dict with original_settings for restoration after clone.
    """
    # Save original settings
    original = get_warehouse_status(client, warehouse_id)

    # Ensure running
    if not ensure_warehouse_running(client, warehouse_id):
        return {"status": "failed", "error": "Warehouse could not be started"}

    # Auto-scale based on catalog size
    recommended_size = _recommend_size(catalog_size_gb)
    current_size = original.get("size", "Small")

    if _size_rank(recommended_size) > _size_rank(current_size):
        logger.info(f"Catalog is {catalog_size_gb:.0f} GB — scaling warehouse from {current_size} to {recommended_size}")
        scale_warehouse(client, warehouse_id, new_size=recommended_size)
    else:
        logger.info(f"Warehouse size {current_size} is adequate for {catalog_size_gb:.0f} GB catalog")

    return {
        "status": "ready",
        "original_settings": {
            "size": original.get("size"),
            "min_clusters": original.get("min_num_clusters"),
            "max_clusters": original.get("max_num_clusters"),
        },
    }


def restore_warehouse(client, warehouse_id: str, original_settings: dict) -> None:
    """Restore warehouse to its original settings after clone."""
    try:
        scale_warehouse(
            client, warehouse_id,
            new_size=original_settings.get("size"),
            min_clusters=original_settings.get("min_clusters"),
            max_clusters=original_settings.get("max_clusters"),
        )
        logger.info(f"Warehouse {warehouse_id} restored to original settings")
    except Exception as e:
        logger.warning(f"Failed to restore warehouse settings: {e}")


def _recommend_size(catalog_size_gb: float) -> str:
    """Recommend warehouse size based on catalog size."""
    if catalog_size_gb < 10:
        return "Small"
    elif catalog_size_gb < 100:
        return "Medium"
    elif catalog_size_gb < 500:
        return "Large"
    elif catalog_size_gb < 1000:
        return "X-Large"
    else:
        return "2X-Large"


SIZE_RANKS = {
    "2X-Small": 1, "X-Small": 2, "Small": 3, "Medium": 4,
    "Large": 5, "X-Large": 6, "2X-Large": 7, "3X-Large": 8, "4X-Large": 9,
}


def _size_rank(size: str) -> int:
    """Get numeric rank for a warehouse size."""
    return SIZE_RANKS.get(size, 3)
