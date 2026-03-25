"""Table maintenance — OPTIMIZE and VACUUM with predictive optimization detection.

Runs OPTIMIZE and VACUUM on selected tables in parallel with progress tracking.
Checks for Predictive Optimization to warn users when manual maintenance is unnecessary.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, get_max_parallel_queries
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def check_predictive_optimization(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
) -> dict:
    """Check if Predictive Optimization is enabled for tables in a catalog.

    Looks for the 'delta.enableOptimizedAutolayout' table property and
    catalog-level predictive optimization settings.
    """
    exclude_schemas = exclude_schemas or ["information_schema", "default"]
    result = {
        "enabled": False,
        "catalog_level": False,
        "tables_with_po": [],
        "total_checked": 0,
    }

    try:
        # Check catalog-level properties
        try:
            props = execute_sql(
                client, warehouse_id,
                f"SHOW TBLPROPERTIES `{catalog}`.information_schema.tables",
            )
            for p in props:
                key = (p.get("key") or p.get("property_key") or "").lower()
                if "predictive" in key or "optimizedautolayout" in key:
                    result["catalog_level"] = True
                    result["enabled"] = True
        except Exception:
            pass  # Not all catalogs support SHOW TBLPROPERTIES on info_schema

        # Check table-level properties across all schemas
        exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
        sql = f"""
            SELECT table_schema, table_name
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ({exclude_clause})
              AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, sql)
        result["total_checked"] = len(tables)

        # Sample up to 20 tables for property checks (avoid excessive queries)
        sample = tables[:20] if len(tables) > 20 else tables
        for t in sample:
            schema = t["table_schema"]
            table = t["table_name"]
            try:
                props = execute_sql(
                    client, warehouse_id,
                    f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table}`",
                )
                for p in props:
                    key = (p.get("key") or p.get("property_key") or "").lower()
                    val = (p.get("value") or p.get("property_value") or "").lower()
                    if ("predictive" in key or "optimizedautolayout" in key) and val in ("true", "1"):
                        result["tables_with_po"].append(f"{schema}.{table}")
                        result["enabled"] = True
                        break
            except Exception:
                continue

    except Exception as e:
        logger.debug(f"Predictive optimization check failed: {e}")

    return result


def _enumerate_tables(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema_filter: str | None = None,
    table_filter: str | None = None,
    exclude_schemas: list[str] | None = None,
) -> list[dict]:
    """Enumerate tables to operate on."""
    exclude_schemas = exclude_schemas or ["information_schema", "default"]

    if table_filter and schema_filter:
        return [{"catalog": catalog, "schema": schema_filter, "table": table_filter}]

    if schema_filter:
        sql = f"""
            SELECT table_name
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema_filter}' AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        rows = execute_sql(client, warehouse_id, sql)
        return [{"catalog": catalog, "schema": schema_filter, "table": r["table_name"]} for r in rows]

    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    sql = f"""
        SELECT table_schema, table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema NOT IN ({exclude_clause})
          AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    rows = execute_sql(client, warehouse_id, sql)
    return [{"catalog": catalog, "schema": r["table_schema"], "table": r["table_name"]} for r in rows]


def run_optimize(
    client: WorkspaceClient,
    warehouse_id: str,
    tables: list[dict],
    dry_run: bool = False,
    max_workers: int | None = None,
) -> dict:
    """Run OPTIMIZE on a list of tables.

    Args:
        tables: List of {"catalog": str, "schema": str, "table": str}
        dry_run: If True, log SQL without executing
        max_workers: Parallel execution threads
    """
    max_workers = max_workers or get_max_parallel_queries()
    logger.info(f"Running OPTIMIZE on {len(tables)} table(s){' (dry run)' if dry_run else ''}")

    results = []
    progress = ProgressTracker(len(tables), "OPTIMIZE")
    progress.start()

    def _optimize_one(t: dict) -> dict:
        fqn = f"`{t['catalog']}`.`{t['schema']}`.`{t['table']}`"
        try:
            if dry_run:
                logger.info(f"[DRY RUN] OPTIMIZE {fqn}")
                return {"schema": t["schema"], "table": t["table"], "status": "dry_run", "error": None}
            execute_sql(client, warehouse_id, f"OPTIMIZE {fqn}")
            return {"schema": t["schema"], "table": t["table"], "status": "success", "error": None}
        except Exception as e:
            logger.warning(f"OPTIMIZE failed for {fqn}: {e}")
            return {"schema": t["schema"], "table": t["table"], "status": "failed", "error": str(e)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_optimize_one, t): t for t in tables}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            progress.update(success=result["status"] != "failed")

    progress.stop()

    succeeded = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")

    summary = {
        "operation": "OPTIMIZE",
        "total": len(tables),
        "succeeded": succeeded,
        "failed": failed,
        "dry_run": dry_run,
        "results": results,
    }

    logger.info(f"OPTIMIZE complete: {succeeded} succeeded, {failed} failed out of {len(tables)}")
    return summary


def run_vacuum(
    client: WorkspaceClient,
    warehouse_id: str,
    tables: list[dict],
    retention_hours: int = 168,
    dry_run: bool = False,
    max_workers: int | None = None,
) -> dict:
    """Run VACUUM on a list of tables.

    Args:
        tables: List of {"catalog": str, "schema": str, "table": str}
        retention_hours: Data retention period in hours (default 168 = 7 days)
        dry_run: If True, log SQL without executing
        max_workers: Parallel execution threads
    """
    max_workers = max_workers or get_max_parallel_queries()
    logger.info(
        f"Running VACUUM on {len(tables)} table(s) "
        f"(retain {retention_hours}h){' (dry run)' if dry_run else ''}"
    )

    results = []
    progress = ProgressTracker(len(tables), "VACUUM")
    progress.start()

    def _vacuum_one(t: dict) -> dict:
        fqn = f"`{t['catalog']}`.`{t['schema']}`.`{t['table']}`"
        sql = f"VACUUM {fqn} RETAIN {retention_hours} HOURS"
        try:
            if dry_run:
                logger.info(f"[DRY RUN] {sql}")
                return {"schema": t["schema"], "table": t["table"], "status": "dry_run", "error": None}
            execute_sql(client, warehouse_id, sql)
            return {"schema": t["schema"], "table": t["table"], "status": "success", "error": None}
        except Exception as e:
            logger.warning(f"VACUUM failed for {fqn}: {e}")
            return {"schema": t["schema"], "table": t["table"], "status": "failed", "error": str(e)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_vacuum_one, t): t for t in tables}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            progress.update(success=result["status"] != "failed")

    progress.stop()

    succeeded = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")

    summary = {
        "operation": "VACUUM",
        "total": len(tables),
        "succeeded": succeeded,
        "failed": failed,
        "retention_hours": retention_hours,
        "dry_run": dry_run,
        "results": results,
    }

    logger.info(f"VACUUM complete: {succeeded} succeeded, {failed} failed out of {len(tables)}")
    return summary
