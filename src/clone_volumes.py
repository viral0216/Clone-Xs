import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, get_max_parallel_queries, list_volumes_sdk
from src.permissions import copy_volume_permissions, update_ownership
from src.rollback import record_object

logger = logging.getLogger(__name__)


def get_volumes(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> list[dict]:
    """List all volumes in a schema."""
    volumes = list_volumes_sdk(client, catalog, schema)
    # Ensure volume_type is a string (SDK may return enum)
    for v in volumes:
        if v.get("volume_type") is not None:
            v["volume_type"] = str(v["volume_type"]).replace("VolumeType.", "")
    return volumes


def get_existing_volumes(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> set[str]:
    """Get set of existing volume names in destination schema."""
    rows = list_volumes_sdk(client, catalog, schema)
    return {row["volume_name"] for row in rows}


def clone_volume(
    client: WorkspaceClient,
    warehouse_id: str,
    dest_catalog: str,
    schema: str,
    volume_name: str,
    volume_type: str,
    storage_location: str | None,
    comment: str | None,
    dry_run: bool = False,
) -> bool:
    """Create a volume in the destination catalog."""
    dest = f"`{dest_catalog}`.`{schema}`.`{volume_name}`"

    if volume_type == "EXTERNAL" and storage_location:
        sql = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {dest} LOCATION '{storage_location}'"
    else:
        sql = f"CREATE VOLUME IF NOT EXISTS {dest}"

    if comment:
        sql += f" COMMENT '{comment}'"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Created volume: {dest} ({volume_type})")
        return True
    except Exception as e:
        if "LOCATION_OVERLAP" in str(e):
            logger.info(f"Skipping volume {volume_name}: external volume location overlaps with source (expected for cross-catalog clones)")
            return False
        logger.error(f"Failed to create volume {dest}: {e}")
        return False


def _clone_single_volume(
    client, warehouse_id, source_catalog, dest_catalog, schema,
    vol_name, vol_type, storage_location, comment,
    dry_run, copy_permissions, copy_ownership, rollback_log,
) -> tuple[str, bool]:
    """Clone a single volume with post-clone operations. Returns (name, success)."""
    success = clone_volume(
        client, warehouse_id, dest_catalog, schema, vol_name,
        vol_type, storage_location, comment, dry_run=dry_run,
    )
    if success:
        if rollback_log and not dry_run:
            record_object(rollback_log, "volumes", f"`{dest_catalog}`.`{schema}`.`{vol_name}`")
        if copy_permissions and not dry_run:
            copy_volume_permissions(client, source_catalog, dest_catalog, schema, vol_name)
        if copy_ownership and not dry_run:
            update_ownership(
                client, SecurableType.VOLUME,
                f"{source_catalog}.{schema}.{vol_name}",
                f"{dest_catalog}.{schema}.{vol_name}",
            )
    return vol_name, success


def clone_volumes_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    load_type: str,
    dry_run: bool = False,
    copy_permissions: bool = False,
    copy_ownership: bool = False,
    rollback_log: str | None = None,
    max_workers: int | None = None,
) -> dict:
    """Clone all volumes in a schema in parallel. Returns summary of results."""
    max_workers = max_workers or get_max_parallel_queries()
    volumes = get_volumes(client, warehouse_id, source_catalog, schema)
    results = {"success": 0, "failed": 0, "skipped": 0}

    existing = set()
    if load_type == "INCREMENTAL":
        existing = get_existing_volumes(client, warehouse_id, dest_catalog, schema)

    # Filter volumes
    vols_to_clone = []
    for vol_row in volumes:
        vol_name = vol_row["volume_name"]
        if load_type == "INCREMENTAL" and vol_name in existing:
            results["skipped"] += 1
            continue
        vols_to_clone.append(vol_row)

    # Clone in parallel
    if len(vols_to_clone) > 1 and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _clone_single_volume,
                    client, warehouse_id, source_catalog, dest_catalog, schema,
                    v["volume_name"],
                    v.get("volume_type", "MANAGED"),
                    v.get("storage_location"),
                    v.get("comment"),
                    dry_run, copy_permissions, copy_ownership, rollback_log,
                ): v["volume_name"]
                for v in vols_to_clone
            }
            for future in as_completed(futures):
                _, success = future.result()
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
    else:
        for v in vols_to_clone:
            _, success = _clone_single_volume(
                client, warehouse_id, source_catalog, dest_catalog, schema,
                v["volume_name"],
                v.get("volume_type", "MANAGED"),
                v.get("storage_location"),
                v.get("comment"),
                dry_run, copy_permissions, copy_ownership, rollback_log,
            )
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1

    return results
