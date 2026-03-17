import logging
import os

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def check_connectivity(client: WorkspaceClient) -> dict:
    """Verify workspace connectivity by listing catalogs."""
    try:
        catalogs = list(client.catalogs.list())
        return {"check": "connectivity", "status": "OK", "detail": f"{len(catalogs)} catalogs accessible"}
    except Exception as e:
        return {"check": "connectivity", "status": "FAIL", "detail": str(e)}


def check_warehouse(client: WorkspaceClient, warehouse_id: str) -> dict:
    """Verify SQL warehouse exists and is running."""
    try:
        wh = client.warehouses.get(warehouse_id)
        state = wh.state.value if wh.state else "UNKNOWN"
        if state in ("RUNNING", "STARTING"):
            return {"check": "warehouse", "status": "OK", "detail": f"{wh.name} ({state})"}
        return {
            "check": "warehouse", "status": "WARN",
            "detail": f"{wh.name} is {state} — may need to start",
        }
    except Exception as e:
        return {"check": "warehouse", "status": "FAIL", "detail": str(e)}


def check_catalog_access(
    client: WorkspaceClient, warehouse_id: str, catalog: str, label: str = "catalog",
) -> dict:
    """Verify the caller can access a catalog via SDK (falls back to SQL)."""
    # Try SDK first — no warehouse needed
    try:
        info = client.catalogs.get(catalog)
        return {
            "check": f"{label}_access ({catalog})", "status": "OK",
            "detail": f"Accessible (owner: {getattr(info, 'owner', 'unknown')})",
        }
    except Exception as sdk_err:
        sdk_detail = str(sdk_err)
        if label == "destination" and ("cannot be found" in sdk_detail.lower() or "not found" in sdk_detail.lower() or "NO_SUCH_CATALOG" in sdk_detail or "does not exist" in sdk_detail.lower()):
            return {
                "check": f"{label}_access ({catalog})", "status": "WARN",
                "detail": f"Catalog '{catalog}' does not exist yet — it will be created automatically during clone",
            }
    # Fallback to SQL
    try:
        rows = execute_sql(
            client, warehouse_id,
            f"SELECT schema_name FROM {catalog}.information_schema.schemata LIMIT 1",
        )
        return {
            "check": f"{label}_access ({catalog})", "status": "OK",
            "detail": f"Accessible ({len(rows)} schema(s) readable)",
        }
    except Exception as e:
        detail = str(e)
        if label == "destination" and ("cannot be found" in detail.lower() or "not found" in detail.lower() or "NO_SUCH_CATALOG" in detail):
            return {
                "check": f"{label}_access ({catalog})", "status": "WARN",
                "detail": f"Catalog '{catalog}' does not exist yet — it will be created automatically during clone",
            }
        return {"check": f"{label}_access ({catalog})", "status": "FAIL", "detail": detail}


def check_permissions(
    client: WorkspaceClient, warehouse_id: str, dest_catalog: str,
) -> dict:
    """Check if the caller can create objects in the destination catalog.

    Uses SDK schemas.create/delete first; falls back to SQL if SDK fails.
    """
    schema_name = "__preflight_check__"
    full_name = f"{dest_catalog}.{schema_name}"

    # Try SDK first — no warehouse needed
    try:
        try:
            client.schemas.create(name=schema_name, catalog_name=dest_catalog)
        except Exception as create_err:
            # Schema may already exist — that's OK
            if "already exists" not in str(create_err).lower():
                raise
        try:
            client.schemas.delete(full_name)
        except Exception:
            pass  # Best-effort cleanup
        return {"check": "write_permissions", "status": "OK", "detail": "Can create/drop schemas (SDK)"}
    except Exception as sdk_err:
        sdk_detail = str(sdk_err)
        if "not found" in sdk_detail.lower() or "NO_SUCH_CATALOG" in sdk_detail or "does not exist" in sdk_detail.lower():
            return {
                "check": "write_permissions", "status": "WARN",
                "detail": f"Catalog '{dest_catalog}' does not exist yet — will be created automatically during clone",
            }
        # SDK failed for another reason — fall through to SQL
        pass

    # Fallback to SQL
    try:
        execute_sql(
            client, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS `{dest_catalog}`.`{schema_name}`",
        )
        execute_sql(
            client, warehouse_id,
            f"DROP SCHEMA IF EXISTS `{dest_catalog}`.`{schema_name}`",
        )
        return {"check": "write_permissions", "status": "OK", "detail": "Can create/drop schemas"}
    except Exception as e:
        detail = str(e)
        if "not found" in detail.lower() or "NO_SUCH_CATALOG" in detail:
            return {
                "check": "write_permissions", "status": "WARN",
                "detail": f"Catalog '{dest_catalog}' does not exist yet — will be created automatically during clone",
            }
        return {"check": "write_permissions", "status": "FAIL", "detail": detail}


def check_env_vars() -> dict:
    """Check for required environment variables."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if host and token:
        return {"check": "env_vars", "status": "OK", "detail": "DATABRICKS_HOST and TOKEN set"}
    missing = []
    if not host:
        missing.append("DATABRICKS_HOST")
    if not token:
        missing.append("DATABRICKS_TOKEN")
    return {
        "check": "env_vars", "status": "WARN",
        "detail": f"Missing: {', '.join(missing)} (may use other auth)",
    }


def run_preflight(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    check_write: bool = True,
) -> dict:
    """Run all pre-flight checks and return a summary."""
    logger.info("Running pre-flight checks...")

    checks = [
        check_env_vars(),
        check_connectivity(client),
        check_warehouse(client, warehouse_id),
        check_catalog_access(client, warehouse_id, source_catalog, "source"),
        check_catalog_access(client, warehouse_id, dest_catalog, "destination"),
    ]

    if check_write:
        checks.append(check_permissions(client, warehouse_id, dest_catalog))

    passed = sum(1 for c in checks if c["status"] == "OK")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    failed = sum(1 for c in checks if c["status"] == "FAIL")

    # Print results
    logger.info("=" * 60)
    logger.info("PRE-FLIGHT CHECK RESULTS")
    logger.info("=" * 60)
    for c in checks:
        icon = {"OK": "PASS", "WARN": "WARN", "FAIL": "FAIL"}[c["status"]]
        logger.info(f"  [{icon}] {c['check']}: {c['detail']}")
    logger.info("-" * 60)
    logger.info(f"  {passed} passed, {warnings} warnings, {failed} failed")
    logger.info("=" * 60)

    ready = failed == 0
    if ready:
        logger.info("All critical checks passed. Ready to proceed.")
    else:
        logger.error("Pre-flight checks failed. Fix issues before proceeding.")

    return {
        "checks": checks,
        "passed": passed,
        "warnings": warnings,
        "failed": failed,
        "ready": ready,
    }
