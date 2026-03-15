import logging
import time
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.diff import compare_catalogs
from src.schema_drift import detect_schema_drift

logger = logging.getLogger(__name__)


def monitor_once(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    check_drift: bool = True,
    check_counts: bool = False,
) -> dict:
    """Run a single monitoring check and return results."""
    timestamp = datetime.now().isoformat()

    result = {
        "timestamp": timestamp,
        "source_catalog": source_catalog,
        "dest_catalog": dest_catalog,
        "in_sync": True,
        "diff": None,
        "drift": None,
    }

    # Object-level diff
    diff = compare_catalogs(client, warehouse_id, source_catalog, dest_catalog, exclude_schemas)
    total_missing = sum(len(diff[t]["only_in_source"]) for t in diff)
    total_extra = sum(len(diff[t]["only_in_dest"]) for t in diff)

    result["diff"] = {
        "missing_in_dest": total_missing,
        "extra_in_dest": total_extra,
    }

    if total_missing > 0 or total_extra > 0:
        result["in_sync"] = False

    # Schema drift check
    if check_drift:
        drift = detect_schema_drift(
            client, warehouse_id, source_catalog, dest_catalog, exclude_schemas,
        )
        result["drift"] = {
            "tables_with_drift": drift["tables_with_drift"],
        }
        if drift["tables_with_drift"] > 0:
            result["in_sync"] = False

    # Row count check
    if check_counts:
        count_mismatches = _check_row_counts(
            client, warehouse_id, source_catalog, dest_catalog, exclude_schemas,
        )
        result["count_mismatches"] = count_mismatches
        if count_mismatches:
            result["in_sync"] = False

    return result


def _check_row_counts(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
) -> list[dict]:
    """Spot-check row counts across catalogs. Returns list of mismatches."""
    mismatches = []
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)

    sql = f"""
        SELECT table_schema, table_name
        FROM {dest_catalog}.information_schema.tables
        WHERE table_schema NOT IN ({exclude_clause})
        AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    tables = execute_sql(client, warehouse_id, sql)

    for row in tables:
        schema = row["table_schema"]
        table = row["table_name"]
        try:
            src_rows = execute_sql(
                client, warehouse_id,
                f"SELECT COUNT(*) AS cnt FROM `{source_catalog}`.`{schema}`.`{table}`",
            )
            dst_rows = execute_sql(
                client, warehouse_id,
                f"SELECT COUNT(*) AS cnt FROM `{dest_catalog}`.`{schema}`.`{table}`",
            )
            src_count = int(src_rows[0]["cnt"]) if src_rows else None
            dst_count = int(dst_rows[0]["cnt"]) if dst_rows else None

            if src_count is not None and dst_count is not None and src_count != dst_count:
                mismatches.append({
                    "schema": schema,
                    "table": table,
                    "source_count": src_count,
                    "dest_count": dst_count,
                })
        except Exception:
            pass

    return mismatches


def monitor_loop(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    interval_minutes: int = 30,
    max_iterations: int = 0,
    check_drift: bool = True,
    check_counts: bool = False,
    on_out_of_sync: callable = None,
) -> None:
    """Continuously monitor catalogs at a given interval.

    Args:
        interval_minutes: Minutes between checks.
        max_iterations: Max number of checks (0 = infinite).
        on_out_of_sync: Optional callback function(result) when out of sync.
    """
    logger.info(
        f"Starting continuous monitoring: {source_catalog} vs {dest_catalog} "
        f"(every {interval_minutes}m)"
    )

    iteration = 0
    while True:
        iteration += 1
        logger.info(f"--- Monitor check #{iteration} ---")

        result = monitor_once(
            client, warehouse_id, source_catalog, dest_catalog, exclude_schemas,
            check_drift=check_drift, check_counts=check_counts,
        )

        if result["in_sync"]:
            logger.info("Catalogs are in sync.")
        else:
            logger.warning("Catalogs are OUT OF SYNC!")
            if result["diff"]:
                logger.warning(
                    f"  Missing in dest: {result['diff']['missing_in_dest']}, "
                    f"Extra in dest: {result['diff']['extra_in_dest']}"
                )
            if result.get("drift"):
                logger.warning(f"  Tables with schema drift: {result['drift']['tables_with_drift']}")

            if on_out_of_sync:
                try:
                    on_out_of_sync(result)
                except Exception as e:
                    logger.error(f"on_out_of_sync callback failed: {e}")

        if max_iterations > 0 and iteration >= max_iterations:
            logger.info(f"Reached max iterations ({max_iterations}). Stopping.")
            break

        logger.info(f"Next check in {interval_minutes} minutes...")
        time.sleep(interval_minutes * 60)
