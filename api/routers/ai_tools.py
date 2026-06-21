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
    parts = [
        f"# {catalog}.{schema}.{table}\n",
        f"## Columns\n{cols}\n",
        f"## Row count\n{count}\n",
    ]
    if sample_rows and int(sample_rows) > 0:
        sample = dbx_sql(
            f"SELECT * FROM {fq} LIMIT {int(sample_rows)}",
            client, warehouse_id, limit=int(sample_rows),
        )
        parts.append(f"## Sample rows\n{sample}")
    return "\n".join(parts)


def dbx_search_tables(term: str, client, catalog: str = "") -> str:
    """Search Unity Catalog for tables (and columns) whose name matches `term`.
    Walks catalogs → schemas → tables via the SDK (no SQL warehouse needed).
    If `catalog` is given, restricts the search to that catalog."""
    try:
        term_l = (term or "").lower().strip()
        if not term_l:
            return "ERROR: search term is required."

        catalogs = [catalog] if catalog else [c.name for c in client.catalogs.list()]
        matches: list[str] = []
        for cat in catalogs:
            if not cat:
                continue
            try:
                schemas = client.schemas.list(catalog_name=cat)
            except Exception:
                continue
            for sch in schemas:
                if not sch.name or sch.name == "information_schema":
                    continue
                try:
                    tables = client.tables.list(catalog_name=cat, schema_name=sch.name)
                except Exception:
                    continue
                for t in tables:
                    name = t.name or ""
                    col_hits = [
                        c.name for c in (t.columns or [])
                        if c.name and term_l in c.name.lower()
                    ]
                    if term_l in name.lower() or col_hits:
                        fq = f"{cat}.{sch.name}.{name}"
                        if col_hits:
                            matches.append(f"{fq}  (matched columns: {', '.join(col_hits[:5])})")
                        else:
                            matches.append(fq)
                    if len(matches) >= 50:
                        break
                if len(matches) >= 50:
                    break
            if len(matches) >= 50:
                break

        if not matches:
            return f"No tables or columns matching '{term}' found."
        more = "\n… (showing first 50 matches)" if len(matches) >= 50 else ""
        return "\n".join(matches[:50]) + more
    except Exception as e:
        return f"ERROR: {e}"


def dbx_table_lineage(table_fqn: str, client) -> str:
    """Return upstream and downstream tables for a fully-qualified table
    (`catalog.schema.table`) via the Databricks lineage-tracking REST API."""
    try:
        resp = client.api_client.do(
            "GET",
            "/api/2.0/lineage-tracking/table-lineage",
            query={"table_name": table_fqn, "include_entity_lineage": "true"},
        )
        ups = [
            (u.get("tableInfo") or {}).get("name")
            for u in (resp.get("upstreams") or [])
            if (u.get("tableInfo") or {}).get("name")
        ]
        downs = [
            (d.get("tableInfo") or {}).get("name")
            for d in (resp.get("downstreams") or [])
            if (d.get("tableInfo") or {}).get("name")
        ]
        up_text = "\n".join(f"  ← {u}" for u in ups) if ups else "  (none)"
        down_text = "\n".join(f"  → {d}" for d in downs) if downs else "  (none)"
        return (
            f"# Lineage for {table_fqn}\n\n"
            f"## Upstream (feeds this table)\n{up_text}\n\n"
            f"## Downstream (depends on this table)\n{down_text}"
        )
    except Exception as e:
        return f"ERROR: {e}"


def dbx_profile_column(
    catalog: str, schema: str, table: str, column: str, client, warehouse_id: str = ""
) -> str:
    """Profile a single column: null %, distinct count, min/max, and a few sample values."""
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    col = f"`{column}`"
    sql = (
        f"SELECT COUNT(*) AS total_rows, "
        f"COUNT({col}) AS non_null, "
        f"COUNT(*) - COUNT({col}) AS null_count, "
        f"ROUND((COUNT(*) - COUNT({col})) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_pct, "
        f"COUNT(DISTINCT {col}) AS distinct_count, "
        f"CAST(MIN({col}) AS STRING) AS min_value, "
        f"CAST(MAX({col}) AS STRING) AS max_value "
        f"FROM {fq}"
    )
    stats = dbx_sql(sql, client, warehouse_id, limit=1)
    samples = dbx_sql(
        f"SELECT DISTINCT {col} FROM {fq} WHERE {col} IS NOT NULL LIMIT 5",
        client, warehouse_id, limit=5,
    )
    return (
        f"# Column profile: {catalog}.{schema}.{table}.{column}\n\n"
        f"## Statistics\n{stats}\n\n"
        f"## Sample distinct values\n{samples}"
    )


def dbx_explain_query(query: str, client, warehouse_id: str = "") -> str:
    """Return the physical execution plan for a SQL query using EXPLAIN FORMATTED."""
    q = (query or "").strip().rstrip(";")
    if not q:
        return "ERROR: query is required."
    return dbx_sql(f"EXPLAIN FORMATTED {q}", client, warehouse_id, limit=200)


def dbx_assessment_findings(severity: str = "", category: str = "", status: str = "") -> str:
    """Return the latest workspace assessment (WAF) findings, optionally filtered.
    Loads from local scan storage — no WorkspaceClient needed."""
    try:
        from api.routers.assessment._storage import _latest_result, _load_result
    except Exception as e:
        return f"ERROR: assessment storage unavailable: {e}"

    meta = _latest_result()
    if not meta:
        return "No assessment has been run yet. Run a scan in the Assessment portal first."
    result = _load_result(meta.get("scan_id", ""))
    findings = (result or {}).get("findings", []) or []
    if not findings:
        return "The latest assessment produced no findings."

    if severity:
        wanted = {s.strip().lower() for s in severity.split(",")}
        findings = [f for f in findings if str(f.get("severity", "")).lower() in wanted]
    if category:
        findings = [f for f in findings if str(f.get("category", "")).lower() == category.lower()]
    if status:
        wanted_st = {s.strip().upper() for s in status.split(",")}
        findings = [f for f in findings if str(f.get("status", "")).upper() in wanted_st]

    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    findings.sort(key=lambda f: order.get(str(f.get("severity", "")).lower(), 4))

    lines = [f"# Assessment findings ({len(findings)} shown)\n"]
    for f in findings[:40]:
        sev = str(f.get("severity", "?")).upper()
        st = f.get("status", "")
        title = f.get("title") or f.get("check") or f.get("name") or f.get("category", "finding")
        rec = f.get("recommendation") or f.get("remediation") or ""
        lines.append(f"- [{sev}] {title}" + (f" ({st})" if st else ""))
        if rec:
            lines.append(f"    → {rec}")
    if len(findings) > 40:
        lines.append(f"\n… ({len(findings) - 40} more)")
    return "\n".join(lines)


def dbx_pii_columns(catalog: str, client, warehouse_id: str = "") -> str:
    """List columns tagged as PII in Unity Catalog for a catalog, via
    information_schema.column_tags. Used to warn before querying sensitive data."""
    if not catalog:
        return "ERROR: catalog is required."
    sql = (
        f"SELECT schema_name, table_name, column_name, tag_name, tag_value "
        f"FROM `{catalog}`.information_schema.column_tags "
        f"WHERE LOWER(tag_name) IN ('pii','pii_type','sensitive','classification') "
        f"ORDER BY schema_name, table_name LIMIT 200"
    )
    return dbx_sql(sql, client, warehouse_id, limit=200)
