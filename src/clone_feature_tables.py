"""Clone Unity Catalog feature tables (Feature Store)."""

import logging

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


def list_feature_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List feature tables in a catalog.

    Feature tables are Delta tables with the 'is_feature_table' or
    'databricks.feature_store.table' property set. Uses SDK for listing
    and property inspection.
    """
    schemas = [schema] if schema else _list_schemas(client, catalog)

    feature_tables = []
    for s in schemas:
        try:
            tables = client.tables.list(catalog_name=catalog, schema_name=s)
        except Exception:
            continue

        for t in tables:
            if str(t.table_type) not in ("MANAGED", "EXTERNAL"):
                continue

            # Check properties for feature table markers
            props = dict(t.properties) if t.properties else {}
            is_feature = (
                props.get("is_feature_table", "").lower() in ("true", "1")
                or props.get("databricks.feature_store.table", "").lower() in ("true", "1")
            )
            if is_feature:
                feature_tables.append({
                    "table_catalog": catalog,
                    "table_schema": s,
                    "table_name": t.name,
                    "full_name": t.full_name,
                    "comment": t.comment,
                    "is_feature_table": True,
                })

    logger.info(f"Found {len(feature_tables)} feature tables in {catalog}")
    return feature_tables


def clone_feature_table(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    schema: str, table_name: str,
    clone_type: str = "DEEP",
    dry_run: bool = False,
) -> dict:
    """Clone a feature table from source to destination.

    Performs a Delta CLONE (SQL-only) and preserves feature table properties via SDK.
    """
    source_fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dest_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"
    dest_full_name = f"{dest_catalog}.{schema}.{table_name}"
    result = {
        "source": f"{source_catalog}.{schema}.{table_name}",
        "destination": dest_full_name,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        # Ensure destination schema exists (SDK)
        try:
            client.schemas.create(name=schema, catalog_name=dest_catalog)
        except Exception as e:
            if "ALREADY_EXISTS" not in str(e):
                raise

        # Clone the table (SQL-only — no SDK equivalent for Delta CLONE)
        clone_keyword = "DEEP CLONE" if clone_type == "DEEP" else "SHALLOW CLONE"
        execute_sql(client, warehouse_id,
                    f"CREATE OR REPLACE TABLE {dest_fqn} {clone_keyword} {source_fqn}")

        # Copy feature table properties via SDK
        try:
            source_table = client.tables.get(full_name=f"{source_catalog}.{schema}.{table_name}")
            source_props = dict(source_table.properties) if source_table.properties else {}
            feature_props = {
                k: v for k, v in source_props.items()
                if k.startswith("databricks.feature_store") or k == "is_feature_table"
            }
            if feature_props:
                client.tables.update(full_name=dest_full_name, properties=feature_props)
        except Exception as pe:
            logger.warning(f"Could not copy feature properties for {table_name}: {pe}")

        result["success"] = True
        logger.info(f"Cloned feature table {source_fqn} -> {dest_fqn}")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to clone feature table {source_fqn}: {e}")

    return result
