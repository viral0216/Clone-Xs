"""Lakehouse Federation: browse foreign catalogs, manage connections, migrate to managed Delta."""

import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def list_foreign_catalogs(client: WorkspaceClient) -> list[dict]:
    """List all foreign (federated) catalogs in the metastore."""
    results = []
    try:
        catalogs = client.catalogs.list()
        for c in catalogs:
            if str(getattr(c, "catalog_type", "")).upper() == "FOREIGN_CATALOG":
                results.append({
                    "name": c.name,
                    "catalog_type": "FOREIGN",
                    "comment": c.comment,
                    "owner": c.owner,
                    "connection_name": getattr(c, "connection_name", None),
                    "created_at": str(c.created_at) if c.created_at else None,
                })
    except Exception as e:
        logger.error(f"Failed to list foreign catalogs: {e}")
    return results


def list_connections(client: WorkspaceClient) -> list[dict]:
    """List all connections in the metastore."""
    results = []
    try:
        connections = client.connections.list()
        for conn in connections:
            results.append({
                "name": conn.name,
                "connection_type": str(conn.connection_type) if conn.connection_type else None,
                "comment": conn.comment,
                "owner": conn.owner,
                "full_name": conn.full_name,
                "created_at": str(conn.created_at) if conn.created_at else None,
                "updated_at": str(conn.updated_at) if conn.updated_at else None,
                "read_only": getattr(conn, "read_only", None),
            })
    except Exception as e:
        logger.error(f"Failed to list connections: {e}")
    return results


def export_connection(client: WorkspaceClient, connection_name: str) -> dict | None:
    """Export a connection's configuration (redacting sensitive options)."""
    try:
        conn = client.connections.get(name=connection_name)

        # Redact sensitive fields from options
        options = dict(conn.options) if conn.options else {}
        redacted_keys = {"password", "token", "secret", "client_secret", "private_key"}
        for key in redacted_keys:
            if key in options:
                options[key] = "***REDACTED***"

        return {
            "name": conn.name,
            "connection_type": str(conn.connection_type) if conn.connection_type else None,
            "comment": conn.comment,
            "owner": conn.owner,
            "options": options,
            "properties": dict(conn.properties) if conn.properties else {},
            "read_only": getattr(conn, "read_only", None),
        }
    except Exception as e:
        logger.error(f"Failed to export connection {connection_name}: {e}")
        return None


def clone_connection(
    client: WorkspaceClient,
    connection_def: dict,
    new_name: str | None = None,
    credentials: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Create a connection from an exported definition.

    The caller must supply credentials (password, token, etc.) since
    those are redacted in the export.
    """
    name = new_name or connection_def["name"]
    result = {"name": name, "success": False}

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        options = dict(connection_def.get("options", {}))
        # Remove redacted placeholders
        options = {k: v for k, v in options.items() if v != "***REDACTED***"}
        # Merge in supplied credentials
        if credentials:
            options.update(credentials)

        from databricks.sdk.service.catalog import ConnectionType

        conn_type = connection_def.get("connection_type", "")
        client.connections.create(
            name=name,
            connection_type=ConnectionType(conn_type) if conn_type else ConnectionType.MYSQL,
            options=options,
            comment=connection_def.get("comment", f"Cloned from {connection_def['name']}"),
        )

        result["success"] = True
        logger.info(f"Created connection: {name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create connection {name}: {e}")

    return result


def list_foreign_tables(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str | None = None,
) -> list[dict]:
    """List tables in a foreign catalog using SDK."""
    exclude = {"information_schema", "default"}
    schemas = [schema] if schema else [
        s.name for s in client.schemas.list(catalog_name=catalog)
        if s.name not in exclude
    ]

    results = []
    for s in schemas:
        try:
            for t in client.tables.list(catalog_name=catalog, schema_name=s):
                results.append({
                    "table_catalog": catalog,
                    "table_schema": s,
                    "table_name": t.name,
                    "table_type": str(t.table_type) if t.table_type else None,
                    "full_name": t.full_name,
                })
        except Exception as e:
            logger.debug(f"Could not list tables in {catalog}.{s}: {e}")
    return results


def migrate_foreign_to_managed(
    client: WorkspaceClient, warehouse_id: str,
    foreign_fqn: str, dest_fqn: str,
    dry_run: bool = False,
) -> dict:
    """Migrate a foreign table to a managed Delta table using CTAS.

    Materializes the foreign table data into a managed Delta table in the
    destination catalog.
    """
    result = {
        "source": foreign_fqn,
        "destination": dest_fqn,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        result["sql"] = f"CREATE TABLE {dest_fqn} AS SELECT * FROM {foreign_fqn}"
        return result

    try:
        # Ensure destination schema exists (SDK)
        parts = dest_fqn.replace("`", "").split(".")
        if len(parts) >= 2:
            try:
                client.schemas.create(name=parts[1], catalog_name=parts[0])
            except Exception as e:
                if "ALREADY_EXISTS" not in str(e):
                    raise

        # CTAS is SQL-only — no SDK equivalent
        sql = f"CREATE OR REPLACE TABLE `{dest_fqn.replace('.', '`.`')}` AS SELECT * FROM `{foreign_fqn.replace('.', '`.`')}`"
        execute_sql(client, warehouse_id, sql)

        result["success"] = True
        logger.info(f"Migrated foreign table: {foreign_fqn} -> {dest_fqn}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to migrate {foreign_fqn}: {e}")

    return result
