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


def _check_create_catalog(
    client: WorkspaceClient, warehouse_id: str, identity: str,
) -> list[dict]:
    """Check if the caller has CREATE CATALOG on the metastore."""
    results = []
    try:
        # Check metastore-level grants
        grants = execute_sql(client, warehouse_id, "SHOW GRANTS ON METASTORE")
        can_create = any(
            priv in str(g.get("privilege", "")).upper()
            for g in grants
            for priv in ("CREATE_CATALOG", "CREATE CATALOG", "ALL_PRIVILEGES", "ALL PRIVILEGES")
        )
        if can_create:
            results.append({
                "check": "create_catalog_permission",
                "status": "OK",
                "detail": "CREATE CATALOG granted on metastore",
            })
        else:
            # Check if the identity specifically has the grant
            my_grants = [
                g for g in grants
                if identity.lower() in str(g.get("principal", "")).lower()
                or "account users" in str(g.get("principal", "")).lower()
            ]
            has_create = any(
                priv in str(g.get("privilege", "")).upper()
                for g in my_grants
                for priv in ("CREATE_CATALOG", "CREATE CATALOG", "ALL_PRIVILEGES", "ALL PRIVILEGES")
            )
            if has_create:
                results.append({
                    "check": "create_catalog_permission",
                    "status": "OK",
                    "detail": f"CREATE CATALOG granted to {identity}",
                })
            else:
                results.append({
                    "check": "create_catalog_permission",
                    "status": "FAIL",
                    "detail": (
                        f"No CREATE CATALOG on metastore — cannot create new catalog. "
                        f"Grant with: GRANT CREATE CATALOG ON METASTORE TO `{identity}`"
                    ),
                })
    except Exception as e:
        results.append({
            "check": "create_catalog_permission",
            "status": "WARN",
            "detail": f"Could not check CREATE CATALOG: {e}",
        })
    return results


def check_uc_permissions(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
) -> list[dict]:
    """Check Unity Catalog permissions needed for clone operations.

    Verifies: MANAGE on destination, USE CATALOG on source, ownership transfer,
    and tag policy assignment permissions.
    """
    results = []

    # 1. Check current identity
    try:
        me = client.current_user.me()
        identity = me.user_name or me.display_name or "unknown"
        results.append({
            "check": "current_identity",
            "status": "OK",
            "detail": f"Authenticated as: {identity}",
        })
    except Exception as e:
        identity = "unknown"
        results.append({
            "check": "current_identity",
            "status": "WARN",
            "detail": f"Could not determine identity: {e}",
        })

    # 2. Check MANAGE permission on destination catalog (needed for ownership transfer)
    try:
        # Check ownership first — owner has all privileges implicitly
        try:
            cat_info = client.catalogs.get(dest_catalog)
            is_owner = getattr(cat_info, "owner", "") == identity
        except Exception:
            is_owner = False

        if is_owner:
            results.append({
                "check": "dest_manage_permission",
                "status": "OK",
                "detail": f"MANAGE granted on {dest_catalog} (owner)",
            })
        else:
            grants_sql = f"SHOW GRANTS ON CATALOG `{dest_catalog}`"
            grants = execute_sql(client, warehouse_id, grants_sql)
            MANAGE_PRIVS = ("MANAGE", "ALL_PRIVILEGES", "ALL PRIVILEGES")

            # Check if any grant (for any principal) includes MANAGE
            has_manage = any(
                priv in str(g.get("privilege", "")).upper()
                for g in grants
                for priv in MANAGE_PRIVS
            )

            # Also check if identity-specific or group grants cover it
            if not has_manage:
                my_grants = [
                    g for g in grants
                    if identity.lower() in str(g.get("principal", "")).lower()
                    or "account users" in str(g.get("principal", "")).lower()
                ]
                has_manage = any(
                    priv in str(g.get("privilege", "")).upper()
                    for g in my_grants
                    for priv in MANAGE_PRIVS
                )

            if has_manage:
                results.append({
                    "check": "dest_manage_permission",
                    "status": "OK",
                    "detail": f"MANAGE granted on {dest_catalog}",
                })
            else:
                # Check schema-level MANAGE — if all schemas grant MANAGE, it's functionally OK
                try:
                    schemas = execute_sql(
                        client, warehouse_id,
                        f"SELECT schema_name FROM `{dest_catalog}`.information_schema.schemata "
                        f"WHERE schema_name NOT IN ('information_schema', 'default') LIMIT 3",
                    )
                    schemas_with_manage = 0
                    for s_row in schemas:
                        s = s_row.get("schema_name", s_row.get("SCHEMA_NAME", ""))
                        if not s:
                            continue
                        try:
                            sg = execute_sql(
                                client, warehouse_id,
                                f"SHOW GRANTS ON SCHEMA `{dest_catalog}`.`{s}`",
                            )
                            if any(
                                priv in str(g.get("privilege", "")).upper()
                                for g in sg
                                for priv in MANAGE_PRIVS
                            ):
                                schemas_with_manage += 1
                        except Exception:
                            pass
                    if schemas and schemas_with_manage == len(schemas):
                        results.append({
                            "check": "dest_manage_permission",
                            "status": "OK",
                            "detail": f"MANAGE granted on {dest_catalog} schemas (schema-level)",
                        })
                    else:
                        results.append({
                            "check": "dest_manage_permission",
                            "status": "WARN",
                            "detail": (
                                f"No MANAGE on {dest_catalog} — ownership transfer and tag assignment will fail. "
                                f"Grant with: GRANT MANAGE ON CATALOG `{dest_catalog}` TO `{identity}`"
                            ),
                        })
                except Exception:
                    results.append({
                        "check": "dest_manage_permission",
                        "status": "WARN",
                        "detail": (
                            f"No MANAGE on {dest_catalog} — ownership transfer and tag assignment will fail. "
                            f"Grant with: GRANT MANAGE ON CATALOG `{dest_catalog}` TO `{identity}`"
                        ),
                    })
    except Exception as e:
        detail = str(e)
        if "not found" in detail.lower() or "does not exist" in detail.lower():
            # Catalog doesn't exist — check CREATE CATALOG on metastore
            results.append({
                "check": "dest_manage_permission",
                "status": "WARN",
                "detail": f"Catalog '{dest_catalog}' does not exist yet — will be created during clone",
            })
            results.extend(_check_create_catalog(client, warehouse_id, identity))
        else:
            results.append({
                "check": "dest_manage_permission",
                "status": "WARN",
                "detail": f"Could not check MANAGE on {dest_catalog}: {e}",
            })

    # 3. Check USE CATALOG on source
    try:
        execute_sql(
            client, warehouse_id,
            f"SELECT 1 FROM `{source_catalog}`.information_schema.schemata LIMIT 1",
        )
        # Check if owner for richer messaging
        try:
            src_info = client.catalogs.get(source_catalog)
            src_owner = getattr(src_info, "owner", "")
            if src_owner == identity:
                results.append({
                    "check": "source_use_catalog",
                    "status": "OK",
                    "detail": f"USE CATALOG on {source_catalog} (owner)",
                })
            else:
                results.append({
                    "check": "source_use_catalog",
                    "status": "OK",
                    "detail": f"USE CATALOG on {source_catalog}",
                })
        except Exception:
            results.append({
                "check": "source_use_catalog",
                "status": "OK",
                "detail": f"USE CATALOG on {source_catalog}",
            })
    except Exception as e:
        results.append({
            "check": "source_use_catalog",
            "status": "FAIL",
            "detail": (
                f"Cannot read {source_catalog}: {e}. "
                f"Grant with: GRANT USE CATALOG ON CATALOG `{source_catalog}` TO `{identity}`"
            ),
        })

    # 4. Check CREATE TABLE permission on destination
    # Skip if user is owner or has MANAGE — those imply full privileges
    try:
        cat_info = client.catalogs.get(dest_catalog)
        is_owner = getattr(cat_info, "owner", "") == identity
    except Exception:
        is_owner = False

    # Check if MANAGE was already confirmed in step 2
    manage_ok = any(
        c.get("check") == "dest_manage_permission" and c.get("status") == "OK"
        for c in results
    )

    if is_owner or manage_ok:
        reason = "owner" if is_owner else "MANAGE granted"
        results.append({
            "check": "dest_create_table",
            "status": "OK",
            "detail": f"CREATE TABLE implied on {dest_catalog} ({reason})",
        })
    else:
        try:
            grants_sql = f"SHOW GRANTS ON CATALOG `{dest_catalog}`"
            grants = execute_sql(client, warehouse_id, grants_sql)

            # Check catalog-level grants
            PRIV_KEYWORDS = ("CREATE_TABLE", "CREATE TABLE", "CREATE", "ALL_PRIVILEGES", "ALL PRIVILEGES", "MANAGE")
            cat_can_create = any(
                priv in str(g.get("privilege", "")).upper()
                for g in grants
                for priv in PRIV_KEYWORDS
            )

            if cat_can_create:
                results.append({
                    "check": "dest_create_table",
                    "status": "OK",
                    "detail": f"CREATE TABLE on {dest_catalog}",
                })
            else:
                # Check schema-level grants — sample first schema
                try:
                    schemas = execute_sql(
                        client, warehouse_id,
                        f"SELECT schema_name FROM `{dest_catalog}`.information_schema.schemata "
                        f"WHERE schema_name NOT IN ('information_schema', 'default') LIMIT 1",
                    )
                    if schemas:
                        s = schemas[0].get("schema_name", schemas[0].get("SCHEMA_NAME", ""))
                        if s:
                            schema_grants = execute_sql(
                                client, warehouse_id,
                                f"SHOW GRANTS ON SCHEMA `{dest_catalog}`.`{s}`",
                            )
                            schema_can_create = any(
                                priv in str(g.get("privilege", "")).upper()
                                for g in schema_grants
                                for priv in PRIV_KEYWORDS
                            )
                            if schema_can_create:
                                results.append({
                                    "check": "dest_create_table",
                                    "status": "OK",
                                    "detail": f"CREATE TABLE on {dest_catalog}.{s} (schema-level grant)",
                                })
                                return results
                except Exception:
                    pass

                results.append({
                    "check": "dest_create_table",
                    "status": "WARN",
                    "detail": (
                        f"No explicit CREATE TABLE on {dest_catalog}. "
                        f"Clone may fail. Grant with: GRANT CREATE TABLE ON CATALOG `{dest_catalog}` TO `{identity}`"
                    ),
                })
        except Exception:
            pass  # Already covered by other checks

    return results


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

    # UC permission checks
    uc_checks = check_uc_permissions(client, warehouse_id, source_catalog, dest_catalog)
    checks.extend(uc_checks)

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
