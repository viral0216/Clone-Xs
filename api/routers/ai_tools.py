"""Databricks Unity Catalog tools for the AI Assistant.

Thin wrappers over an already-authenticated databricks-sdk WorkspaceClient.
The client is resolved by get_db_client() (handles PAT / session / App auth)
and passed in directly — no raw credentials needed here.

Ported from dbx-coding-agent/tools/dbx_tools.py — keeping only the 5 UC
exploration functions relevant to chat-based data discovery.
"""

from __future__ import annotations


def _rows_to_text(cols: list[str], rows: list[list], limit: int) -> str:
    head = " | ".join(cols)
    body = "\n".join(
        " | ".join("" if c is None else str(c) for c in r) for r in rows[:limit]
    )
    more = f"\n… ({len(rows) - limit} more rows)" if len(rows) > limit else ""
    return f"{head}\n{body}{more}" if body else head + "\n(no rows)"


def dbx_sql(query: str, client, warehouse_id: str, limit: int = 100) -> str:
    """Run a read-only SQL query on a Databricks SQL Warehouse."""
    try:
        if not warehouse_id:
            return "ERROR: warehouse_id required for SQL execution."
        resp = client.statement_execution.execute_statement(
            statement=query, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        result = resp.result
        if result is None:
            state = getattr(getattr(resp, "status", None), "state", None)
            return f"(no result; statement state: {state})"
        cols = [c.name for c in (resp.manifest.schema.columns or [])] if resp.manifest else []
        rows = result.data_array or []
        return _rows_to_text(cols, rows, limit)
    except Exception as e:
        return f"ERROR: {e}"


def dbx_list_catalogs(client) -> str:
    """List Unity Catalog catalogs."""
    try:
        names = [c.name for c in client.catalogs.list()]
        return "\n".join(names) if names else "(no catalogs)"
    except Exception as e:
        return f"ERROR: {e}"


def dbx_list_schemas(catalog: str, client) -> str:
    """List schemas in a Unity Catalog catalog."""
    try:
        names = [s.name for s in client.schemas.list(catalog_name=catalog)]
        return "\n".join(names) if names else f"(no schemas in {catalog})"
    except Exception as e:
        return f"ERROR: {e}"


def dbx_list_tables(catalog: str, schema: str, client) -> str:
    """List tables in a catalog.schema with their type."""
    try:
        rows = [
            f"{t.name}\t{getattr(t, 'table_type', '')}"
            for t in client.tables.list(catalog_name=catalog, schema_name=schema)
        ]
        return "\n".join(rows) if rows else f"(no tables in {catalog}.{schema})"
    except Exception as e:
        return f"ERROR: {e}"


def dbx_describe_table(
    catalog: str, schema: str, table: str, client, warehouse_id: str = "", sample_rows: int = 3
) -> str:
    """Describe a Unity Catalog table: columns, row count, and sample rows."""
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    cols   = dbx_sql(f"DESCRIBE TABLE {fq}", client, warehouse_id, limit=300)
    count  = dbx_sql(f"SELECT COUNT(*) AS row_count FROM {fq}", client, warehouse_id, limit=1)
    sample = dbx_sql(f"SELECT * FROM {fq} LIMIT {int(sample_rows)}", client, warehouse_id, limit=int(sample_rows))
    return (
        f"# {catalog}.{schema}.{table}\n\n"
        f"## Columns\n{cols}\n\n"
        f"## Row count\n{count}\n\n"
        f"## Sample rows\n{sample}"
    )
