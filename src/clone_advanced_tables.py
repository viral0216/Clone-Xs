"""Clone advanced Unity Catalog table types: materialized views, streaming tables, online tables."""

import logging
import re

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

EXCLUDE_SCHEMAS = {"information_schema", "default"}


def _list_schemas(client: WorkspaceClient, catalog: str) -> list[str]:
    """List schema names in a catalog using SDK."""
    try:
        return [
            s.name for s in client.schemas.list(catalog_name=catalog)
            if s.name not in EXCLUDE_SCHEMAS
        ]
    except Exception as e:
        logger.error(f"Failed to list schemas in {catalog}: {e}")
        return []


# ---------------------------------------------------------------------------
# Listing — uses SDK client.tables.list() instead of information_schema SQL
# ---------------------------------------------------------------------------

def list_materialized_views(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List materialized views in a catalog using SDK."""
    schemas = [schema] if schema else _list_schemas(client, catalog)
    results = []
    for s in schemas:
        try:
            for t in client.tables.list(catalog_name=catalog, schema_name=s):
                if str(t.table_type) == "MATERIALIZED_VIEW":
                    results.append({
                        "table_catalog": catalog,
                        "table_schema": s,
                        "table_name": t.name,
                        "full_name": t.full_name,
                    })
        except Exception as e:
            logger.debug(f"Could not list tables in {catalog}.{s}: {e}")
    return results


def list_streaming_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List streaming tables (DLT-managed) in a catalog using SDK."""
    schemas = [schema] if schema else _list_schemas(client, catalog)
    results = []
    for s in schemas:
        try:
            for t in client.tables.list(catalog_name=catalog, schema_name=s):
                if str(t.table_type) == "STREAMING_TABLE":
                    results.append({
                        "table_catalog": catalog,
                        "table_schema": s,
                        "table_name": t.name,
                        "full_name": t.full_name,
                    })
        except Exception as e:
            logger.debug(f"Could not list tables in {catalog}.{s}: {e}")
    return results


def list_online_tables(
    client: WorkspaceClient, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List online tables using SDK."""
    results = []
    try:
        online_tables = client.online_tables.list()
        for ot in online_tables:
            name = ot.name or ""
            if not name.startswith(f"{catalog}."):
                continue
            if schema:
                parts = name.split(".")
                if len(parts) >= 2 and parts[1] != schema:
                    continue
            results.append({
                "name": ot.name,
                "status": str(ot.status.detailed_state) if ot.status else None,
                "spec": {
                    "source_table_full_name": ot.spec.source_table_full_name if ot.spec else None,
                    "primary_key_columns": list(ot.spec.primary_key_columns) if ot.spec and ot.spec.primary_key_columns else [],
                    "run_triggered": ot.spec.run_triggered if ot.spec else None,
                    "run_continuously": ot.spec.run_continuously if ot.spec else None,
                } if ot.spec else None,
            })
    except Exception as e:
        logger.debug(f"Could not list online tables: {e}")
    return results


def list_all_advanced_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str | None = None,
) -> dict:
    """List all advanced table types in a catalog."""
    mvs = list_materialized_views(client, warehouse_id, catalog, schema)
    sts = list_streaming_tables(client, warehouse_id, catalog, schema)
    ots = list_online_tables(client, catalog, schema)

    return {
        "catalog": catalog,
        "materialized_views": mvs,
        "streaming_tables": sts,
        "online_tables": ots,
        "totals": {
            "materialized_views": len(mvs),
            "streaming_tables": len(sts),
            "online_tables": len(ots),
        },
    }


# ---------------------------------------------------------------------------
# Export definitions
# ---------------------------------------------------------------------------

def export_materialized_view_definition(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, name: str,
) -> dict | None:
    """Export a materialized view's CREATE statement.

    SHOW CREATE TABLE is SQL-only — no SDK equivalent.
    """
    fqn = f"`{catalog}`.`{schema}`.`{name}`"
    try:
        rows = execute_sql(client, warehouse_id, f"SHOW CREATE TABLE {fqn}")
        if rows:
            create_sql = rows[0].get("createtab_stmt", "") or rows[0].get("result", "")
            return {
                "type": "MATERIALIZED_VIEW",
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "fqn": f"{catalog}.{schema}.{name}",
                "create_sql": create_sql,
            }
    except Exception as e:
        logger.error(f"Failed to export MV definition for {fqn}: {e}")
    return None


def export_streaming_table_definition(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, name: str,
) -> dict | None:
    """Export a streaming table's definition.

    SHOW CREATE TABLE is SQL-only — no SDK equivalent.
    """
    fqn = f"`{catalog}`.`{schema}`.`{name}`"
    try:
        rows = execute_sql(client, warehouse_id, f"SHOW CREATE TABLE {fqn}")
        if rows:
            create_sql = rows[0].get("createtab_stmt", "") or rows[0].get("result", "")
            return {
                "type": "STREAMING_TABLE",
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "fqn": f"{catalog}.{schema}.{name}",
                "create_sql": create_sql,
            }
    except Exception as e:
        logger.error(f"Failed to export streaming table definition for {fqn}: {e}")
    return None


def export_online_table_definition(
    client: WorkspaceClient, table_name: str,
) -> dict | None:
    """Export an online table's specification via SDK."""
    try:
        ot = client.online_tables.get(name=table_name)
        return {
            "type": "ONLINE_TABLE",
            "name": ot.name,
            "spec": {
                "source_table_full_name": ot.spec.source_table_full_name if ot.spec else None,
                "primary_key_columns": list(ot.spec.primary_key_columns) if ot.spec and ot.spec.primary_key_columns else [],
                "run_triggered": ot.spec.run_triggered if ot.spec else None,
                "run_continuously": ot.spec.run_continuously if ot.spec else None,
                "timeseries_key": ot.spec.timeseries_key if ot.spec else None,
            } if ot.spec else None,
        }
    except Exception as e:
        logger.error(f"Failed to export online table {table_name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Clone / recreate on target
# ---------------------------------------------------------------------------

def _rewrite_catalog_refs(sql: str, source_catalog: str, dest_catalog: str) -> str:
    """Rewrite catalog references in a SQL statement."""
    sql = sql.replace(f"`{source_catalog}`", f"`{dest_catalog}`")
    pattern = re.compile(rf'\b{re.escape(source_catalog)}\b', re.IGNORECASE)
    sql = pattern.sub(dest_catalog, sql)
    return sql


def clone_materialized_view(
    client: WorkspaceClient, warehouse_id: str,
    definition: dict, dest_catalog: str, source_catalog: str,
    dry_run: bool = False,
) -> dict:
    """Recreate a materialized view in the destination catalog.

    CREATE MATERIALIZED VIEW is SQL-only — no SDK equivalent.
    """
    result = {
        "source": definition["fqn"],
        "destination": definition["fqn"].replace(f"{source_catalog}.", f"{dest_catalog}.", 1),
        "type": "MATERIALIZED_VIEW",
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        result["sql"] = _rewrite_catalog_refs(definition["create_sql"], source_catalog, dest_catalog)
        return result

    try:
        # Ensure schema exists (SDK)
        try:
            client.schemas.create(name=definition["schema"], catalog_name=dest_catalog)
        except Exception as e:
            if "ALREADY_EXISTS" not in str(e):
                raise

        # Rewrite and execute (SQL-only)
        new_sql = _rewrite_catalog_refs(definition["create_sql"], source_catalog, dest_catalog)
        new_sql = re.sub(r'^CREATE\s+MATERIALIZED\s+VIEW',
                         'CREATE OR REPLACE MATERIALIZED VIEW', new_sql, flags=re.IGNORECASE)
        execute_sql(client, warehouse_id, new_sql)
        result["success"] = True
        logger.info(f"Created MV: {result['destination']}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to create MV {result['destination']}: {e}")

    return result


def clone_online_table(
    client: WorkspaceClient,
    definition: dict, dest_catalog: str, source_catalog: str,
    dry_run: bool = False,
) -> dict:
    """Create an online table in the destination catalog via SDK."""
    source_name = definition["name"]
    dest_name = source_name.replace(f"{source_catalog}.", f"{dest_catalog}.", 1)

    result = {
        "source": source_name,
        "destination": dest_name,
        "type": "ONLINE_TABLE",
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        spec = definition.get("spec", {})
        source_table = spec.get("source_table_full_name", "")
        if source_table:
            source_table = source_table.replace(f"{source_catalog}.", f"{dest_catalog}.", 1)

        from databricks.sdk.service.catalog import OnlineTableSpec

        ot_spec = OnlineTableSpec(
            source_table_full_name=source_table,
            primary_key_columns=spec.get("primary_key_columns", []),
            run_triggered=spec.get("run_triggered"),
            run_continuously=spec.get("run_continuously"),
            timeseries_key=spec.get("timeseries_key"),
        )

        client.online_tables.create(name=dest_name, spec=ot_spec)
        result["success"] = True
        logger.info(f"Created online table: {dest_name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create online table {dest_name}: {e}")

    return result


def clone_all_advanced_tables(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    schema: str | None = None,
    include_mvs: bool = True,
    include_streaming: bool = True,
    include_online: bool = True,
    dry_run: bool = False,
) -> dict:
    """Clone all advanced table types from source to destination catalog."""
    results = {"materialized_views": [], "streaming_tables": [], "online_tables": [], "errors": []}

    if include_mvs:
        mvs = list_materialized_views(client, warehouse_id, source_catalog, schema)
        for mv in mvs:
            defn = export_materialized_view_definition(
                client, warehouse_id, mv["table_catalog"], mv["table_schema"], mv["table_name"],
            )
            if defn:
                r = clone_materialized_view(client, warehouse_id, defn, dest_catalog, source_catalog, dry_run)
                results["materialized_views"].append(r)
                if not r.get("success"):
                    results["errors"].append(r)

    if include_streaming:
        sts = list_streaming_tables(client, warehouse_id, source_catalog, schema)
        for st in sts:
            defn = export_streaming_table_definition(
                client, warehouse_id, st["table_catalog"], st["table_schema"], st["table_name"],
            )
            if defn:
                results["streaming_tables"].append({
                    "source": defn["fqn"],
                    "type": "STREAMING_TABLE",
                    "exported": True,
                    "note": "Streaming tables require DLT pipeline — definition exported for reference",
                    "create_sql": defn["create_sql"],
                })

    if include_online:
        ots = list_online_tables(client, source_catalog, schema)
        for ot in ots:
            defn = export_online_table_definition(client, ot["name"])
            if defn:
                r = clone_online_table(client, defn, dest_catalog, source_catalog, dry_run)
                results["online_tables"].append(r)
                if not r.get("success"):
                    results["errors"].append(r)

    return results
