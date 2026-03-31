"""Health check endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health_check():
    import os
    runtime = os.getenv("CLONE_XS_RUNTIME", "standalone")

    # SDK version info
    sdk_version = None
    try:
        import databricks.sdk
        sdk_version = getattr(databricks.sdk, "__version__", "unknown")
    except ImportError:
        pass

    # REST API fallback availability
    rest_api_available = False
    try:
        from src.rest_api_client import DatabricksRestClient
        rest_api_available = True
    except ImportError:
        pass

    from fastapi import Query, Depends
    return {
        "status": "ok",
        "service": "Clone-Xs",
        "runtime": runtime,
        "sdk_version": sdk_version,
        "rest_api_fallback": rest_api_available,
    }


@router.get("/health/deep")
async def deep_health_check():
    """Deep health check — tests warehouse connectivity, config, and table existence."""
    import logging
    logger = logging.getLogger(__name__)
    checks = {}

    # 1. Config
    try:
        from src.config import load_config_cached
        config = load_config_cached()
        audit = config.get("audit_trail", {})
        catalog = audit.get("catalog", "")
        wid = config.get("sql_warehouse_id", "")
        checks["config"] = {"status": "ok", "catalog": catalog, "warehouse_id": wid[:8] + "..." if wid else ""}
    except Exception as e:
        checks["config"] = {"status": "error", "error": str(e)}
        return {"status": "error", "checks": checks}

    # 2. Auth
    try:
        from src.auth import get_client
        client = get_client()
        checks["auth"] = {"status": "ok"}
    except Exception as e:
        checks["auth"] = {"status": "error", "error": str(e)}
        return {"status": "degraded", "checks": checks}

    # 3. Warehouse connectivity
    if wid:
        try:
            from src.client import execute_sql
            execute_sql(client, wid, "SELECT 1 AS health_check", max_retries=1)
            checks["warehouse"] = {"status": "ok"}
        except Exception as e:
            checks["warehouse"] = {"status": "error", "error": str(e)[:200]}
    else:
        checks["warehouse"] = {"status": "skipped", "reason": "No warehouse configured"}

    # 4. Required schemas
    if catalog and wid:
        try:
            from src.client import execute_sql
            rows = execute_sql(client, wid, f"SHOW SCHEMAS IN `{catalog}`", max_retries=1)
            existing = {r.get("databaseName", r.get("namespace", r.get("schema_name", ""))).lower() for r in (rows or [])}
            expected = {"logs", "governance", "reconciliation", "data_quality", "pii", "metrics", "lineage"}
            missing = expected - existing
            checks["schemas"] = {
                "status": "ok" if not missing else "degraded",
                "existing": sorted(existing & expected),
                "missing": sorted(missing),
            }
        except Exception as e:
            checks["schemas"] = {"status": "error", "error": str(e)[:200]}

    # 5. Table count from registry
    if catalog and wid:
        try:
            from src.table_registry import get_all_table_fqns
            reg_config = {"audit_trail": {"catalog": catalog}}
            sections = get_all_table_fqns(reg_config)
            all_tables = [t for s in sections for t in s["tables"]]
            total_expected = len(all_tables)
            # Cap table validation to avoid overloading the warehouse
            max_check = min(total_expected, 20)
            found = 0
            checked = 0
            for t in all_tables[:max_check]:
                checked += 1
                try:
                    from src.client import execute_sql
                    execute_sql(client, wid, f"DESCRIBE TABLE {t['fqn']}", max_retries=1)
                    found += 1
                except Exception:
                    pass
            checks["tables"] = {
                "status": "ok" if found == checked else "degraded",
                "expected": total_expected,
                "checked": checked,
                "found": found,
                "missing": checked - found,
            }
        except Exception as e:
            checks["tables"] = {"status": "error", "error": str(e)[:200]}

    # Overall status
    statuses = [c.get("status") for c in checks.values()]
    if "error" in statuses:
        overall = "error"
    elif "degraded" in statuses:
        overall = "degraded"
    else:
        overall = "ok"

    return {"status": overall, "checks": checks}
