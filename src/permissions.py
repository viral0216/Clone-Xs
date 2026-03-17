"""Permission and ownership copying for Unity Catalog objects.

Tries the SDK Grants API first (no SQL warehouse needed), then falls
back to SQL-based SHOW GRANTS / GRANT if the SDK fails (known issue
with managed catalog workspaces where the SDK returns
"SECURABLETYPE.X is not a valid securable type").
"""

import json
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql

logger = logging.getLogger(__name__)

_SESSION_FILE = os.path.expanduser("~/.clxs-session.json")


def _resolve_warehouse_id(warehouse_id: str) -> str:
    """Resolve warehouse ID — use session file or config if empty."""
    if warehouse_id and warehouse_id != "SERVERLESS":
        return warehouse_id
    # Try session file
    try:
        with open(_SESSION_FILE) as f:
            session = json.load(f)
            wid = session.get("warehouse_id", "")
            if wid and wid != "SERVERLESS":
                return wid
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Try config file
    try:
        from src.config import load_config
        cfg = load_config("config/clone_config.yaml")
        return cfg.get("sql_warehouse_id", "")
    except Exception:
        pass
    return warehouse_id

# Internal privileges that should not be copied
_SKIP_PRIVILEGES = {"INHERITED_FROM", "SYSTEM"}


def _show_grants_sql(securable_type: str, full_name: str) -> str:
    """Build SHOW GRANTS SQL for a securable object."""
    return f"SHOW GRANTS ON {securable_type} `{'`.`'.join(full_name.split('.'))}`"


def _securable_type_enum(securable_type: str) -> SecurableType:
    """Map SQL securable type string to SDK SecurableType enum."""
    mapping = {
        "CATALOG": SecurableType.CATALOG,
        "SCHEMA": SecurableType.SCHEMA,
        "TABLE": SecurableType.TABLE,
        "VOLUME": SecurableType.VOLUME,
        "FUNCTION": SecurableType.FUNCTION,
    }
    return mapping.get(securable_type.upper(), SecurableType.TABLE)


def _copy_grants_via_sdk(
    client: WorkspaceClient,
    securable_type: str,
    source_name: str,
    dest_name: str,
    label: str,
) -> bool:
    """Try to copy grants using the SDK Grants API.

    Returns True if successful, False if SDK grants API fails
    (known issue with managed catalogs).
    """
    from databricks.sdk.service.catalog import PermissionsChange, Privilege
    try:
        sec_type = _securable_type_enum(securable_type)
        effective = client.grants.get_effective(sec_type, source_name)
        if not effective.privilege_assignments:
            return True

        changes = []
        for assignment in effective.privilege_assignments:
            principal = assignment.principal or ""
            if not principal:
                continue
            privs = []
            for p in (assignment.privileges or []):
                priv_name = p.privilege.value if hasattr(p.privilege, "value") else str(p.privilege)
                if priv_name.upper() in _SKIP_PRIVILEGES:
                    continue
                try:
                    privs.append(Privilege(priv_name))
                except Exception:
                    continue
            if privs:
                changes.append(PermissionsChange(add=privs, principal=principal))

        if changes:
            client.grants.update(sec_type, dest_name, changes=changes)
            logger.info(f"Copied {len(changes)} grant(s) via SDK: {source_name} -> {dest_name}")
        return True
    except Exception as e:
        logger.debug(f"SDK grants failed for {label}, will fall back to SQL: {e}")
        return False


def _copy_grants_via_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    securable_type: str,
    source_name: str,
    dest_name: str,
    label: str,
) -> None:
    """Copy grants from source to destination.

    Tries SDK Grants API first; falls back to SQL SHOW GRANTS / GRANT
    if SDK fails (known issue with managed catalogs).
    """
    # Try SDK first — no warehouse needed
    if _copy_grants_via_sdk(client, securable_type, source_name, dest_name, label):
        return

    # Fallback to SQL
    warehouse_id = _resolve_warehouse_id(warehouse_id)
    if not warehouse_id:
        logger.debug(f"Skipping permissions for {label}: no warehouse ID available")
        return
    try:
        sql = _show_grants_sql(securable_type, source_name)
        rows = execute_sql(client, warehouse_id, sql)

        if not rows:
            return

        grants_applied = 0
        for row in rows:
            principal = row.get("Principal") or row.get("principal") or ""
            privilege = row.get("ActionType") or row.get("privilege") or row.get("action_type") or ""

            if not principal or not privilege:
                continue
            if privilege.upper() in _SKIP_PRIVILEGES:
                continue

            dest_escaped = "`" + "`.`".join(dest_name.split(".")) + "`"
            grant_sql = f"GRANT {privilege} ON {securable_type} {dest_escaped} TO `{principal}`"
            try:
                execute_sql(client, warehouse_id, grant_sql)
                grants_applied += 1
            except Exception as ge:
                logger.debug(f"Could not grant {privilege} to {principal} on {dest_name}: {ge}")

        if grants_applied:
            logger.info(f"Copied {grants_applied} grants: {source_name} -> {dest_name}")

    except Exception as e:
        logger.warning(f"Could not copy {label} permissions: {e}")


def copy_catalog_permissions(
    client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str = "",
) -> None:
    """Copy catalog-level permissions from source to destination."""
    _copy_grants_via_sql(
        client, warehouse_id, "CATALOG",
        source_catalog, dest_catalog, "catalog",
    )


def copy_schema_permissions(
    client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    warehouse_id: str = "",
) -> None:
    """Copy schema-level permissions from source to destination."""
    _copy_grants_via_sql(
        client, warehouse_id, "SCHEMA",
        f"{source_catalog}.{schema}", f"{dest_catalog}.{schema}",
        f"schema {schema}",
    )


def copy_table_permissions(
    client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    warehouse_id: str = "",
) -> None:
    """Copy table-level permissions from source to destination."""
    _copy_grants_via_sql(
        client, warehouse_id, "TABLE",
        f"{source_catalog}.{schema}.{table_name}",
        f"{dest_catalog}.{schema}.{table_name}",
        f"table {schema}.{table_name}",
    )


def copy_volume_permissions(
    client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    volume_name: str,
    warehouse_id: str = "",
) -> None:
    """Copy volume-level permissions from source to destination."""
    _copy_grants_via_sql(
        client, warehouse_id, "VOLUME",
        f"{source_catalog}.{schema}.{volume_name}",
        f"{dest_catalog}.{schema}.{volume_name}",
        f"volume {schema}.{volume_name}",
    )


def copy_function_permissions(
    client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    function_name: str,
    warehouse_id: str = "",
) -> None:
    """Copy function-level permissions from source to destination."""
    _copy_grants_via_sql(
        client, warehouse_id, "FUNCTION",
        f"{source_catalog}.{schema}.{function_name}",
        f"{dest_catalog}.{schema}.{function_name}",
        f"function {schema}.{function_name}",
    )


def update_ownership(
    client: WorkspaceClient,
    securable_type: SecurableType,
    source_full_name: str,
    dest_full_name: str,
) -> None:
    """Copy ownership from source object to destination object."""
    try:
        source_info = None
        if securable_type == SecurableType.CATALOG:
            source_info = client.catalogs.get(source_full_name)
        elif securable_type == SecurableType.SCHEMA:
            source_info = client.schemas.get(source_full_name)
        elif securable_type == SecurableType.TABLE:
            source_info = client.tables.get(source_full_name)
        elif securable_type == SecurableType.VOLUME:
            source_info = client.volumes.read(source_full_name)

        if source_info and hasattr(source_info, "owner") and source_info.owner:
            owner = source_info.owner
            # Skip if source is system-owned — don't overwrite the creator's ownership
            if owner.lower() in ("system user", "system"):
                logger.debug(
                    f"Skipping ownership copy for {dest_full_name} (source owner is '{owner}')"
                )
                return

            if securable_type == SecurableType.CATALOG:
                client.catalogs.update(dest_full_name, owner=source_info.owner)
            elif securable_type == SecurableType.SCHEMA:
                client.schemas.update(dest_full_name, owner=source_info.owner)
            elif securable_type == SecurableType.TABLE:
                client.tables.update(dest_full_name, owner=source_info.owner)
            elif securable_type == SecurableType.VOLUME:
                client.volumes.update(dest_full_name, owner=source_info.owner)

            logger.info(
                f"Updated ownership for {dest_full_name} to {source_info.owner}"
            )
    except Exception as e:
        logger.warning(f"Could not update ownership for {dest_full_name}: {e}")


def _has_sql_executor() -> bool:
    """Check if a custom SQL executor (spark.sql) is configured."""
    from src.client import _sql_executor
    return _sql_executor is not None
