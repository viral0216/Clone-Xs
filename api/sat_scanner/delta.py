"""SAT Scanner — Delta table export via SQL Statement Execution REST API."""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import SATScanResult
from .checks import SAT_CHECKS, _get_effort, CHECK_API_ENDPOINTS
from .scoring import _build_prioritised_recommendations


# ─────────────────────────────────────────────────────────────────────────────
# Delta Table Export
# ─────────────────────────────────────────────────────────────────────────────

def _list_sql_warehouses(host: str, token: str) -> list[dict]:
    """Fetch all SQL warehouses from the workspace."""
    import httpx as _httpx
    try:
        resp = _httpx.get(
            f"{host}/api/2.0/sql/warehouses",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        return resp.json().get("warehouses", [])
    except Exception:
        return []


def _select_sql_warehouse(host: str, token: str) -> str:
    """Prompt the user to select a SQL warehouse. Returns the warehouse ID."""
    warehouses = _list_sql_warehouses(host, token)

    # Sort: RUNNING first, then STARTING, then others
    _state_order = {"RUNNING": 0, "STARTING": 1, "STOPPED": 2, "STOPPING": 3, "DELETED": 4}
    warehouses.sort(key=lambda w: _state_order.get(w.get("state", ""), 99))

    print("\n  Select a SQL warehouse for Delta table writes:\n")
    for i, wh in enumerate(warehouses, 1):
        state = wh.get("state", "unknown")
        size = wh.get("cluster_size", "")
        name = wh.get("name", wh["id"])
        marker = " (recommended)" if state == "RUNNING" else ""
        print(f"    {i}. {name}  [{state}] {size}{marker}")
    n = len(warehouses)
    print(f"    {n + 1}. [ Enter warehouse ID manually ]")
    print()

    while True:
        choice = input(f"  Choose [1-{n + 1}]: ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < n:
                wh = warehouses[idx]
                print(f"  Using SQL warehouse: {wh.get('name', '')} ({wh.get('state', 'unknown')})")
                return wh["id"]
            if idx == n:
                wh_id = input("  Enter SQL warehouse ID: ").strip()
                if wh_id:
                    print(f"  Using SQL warehouse: {wh_id}")
                    return wh_id
                print("  Warehouse ID cannot be empty.")
                continue
        except ValueError:
            pass
        print(f"  Invalid choice. Enter a number between 1 and {n + 1}.")


def _resolve_warehouse_id(host: str, token: str, warehouse_id: str = "") -> str:
    """Return a SQL warehouse ID — either the provided one or interactively selected."""
    if warehouse_id:
        print(f"  Using SQL warehouse: {warehouse_id}")
        return warehouse_id
    return _select_sql_warehouse(host, token)


def _query_sql_statement(host: str, token: str, warehouse_id: str, sql: str) -> list[list]:
    """Execute SQL via the Statement API and return result rows.

    Handles PENDING/RUNNING states by polling until the statement completes.
    Also follows pagination via next_chunk_internal_link.
    """
    import httpx as _httpx
    import time as _time

    url = f"{host}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
    }
    resp = _httpx.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"SQL Statement API error {resp.status_code}: {resp.text}")
    data = resp.json()

    # Poll if statement is still pending/running (warehouse may be starting)
    statement_id = data.get("statement_id", "")
    status = data.get("status", {}).get("state", "")
    poll_count = 0
    while status in ("PENDING", "RUNNING") and poll_count < 60:
        if poll_count == 0:
            print("  Waiting for SQL warehouse...", end="", flush=True)
        elif poll_count % 5 == 0:
            print(".", end="", flush=True)
        _time.sleep(2)
        poll_count += 1
        poll_resp = _httpx.get(
            f"{url}/{statement_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if poll_resp.status_code != 200:
            raise RuntimeError(f"SQL poll error {poll_resp.status_code}: {poll_resp.text}")
        data = poll_resp.json()
        status = data.get("status", {}).get("state", "")
    if poll_count > 0:
        print()  # newline after dots

    if status == "FAILED":
        err = data.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise RuntimeError(f"SQL statement failed: {err}")

    # Extract rows, following pagination chunks
    result = data.get("result", {})
    rows = result.get("data_array", [])

    # Follow pagination if there are more chunks
    next_link = result.get("next_chunk_internal_link")
    while next_link:
        chunk_resp = _httpx.get(
            f"{host}{next_link}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if chunk_resp.status_code != 200:
            break
        chunk_data = chunk_resp.json()
        rows.extend(chunk_data.get("data_array", []))
        next_link = chunk_data.get("next_chunk_internal_link")

    return rows


def _select_catalog(host: str, token: str, warehouse_id: str, default_catalog: str) -> str:
    """List available Unity Catalog catalogs and let the user pick one or type a name."""
    try:
        rows = _query_sql_statement(host, token, warehouse_id, "SHOW CATALOGS")
        catalogs = [r[0] for r in rows]
    except Exception:
        catalogs = []

    # If default exists, highlight it
    default_idx = None
    if default_catalog in catalogs:
        default_idx = catalogs.index(default_catalog)

    print("\n  Select a catalog for Delta table storage:\n")
    for i, cat in enumerate(catalogs, 1):
        marker = " (default)" if cat == default_catalog else ""
        print(f"    {i}. {cat}{marker}")
    n = len(catalogs)
    print(f"    {n + 1}. [ Enter catalog name manually ]")
    print()

    prompt = f"  Choose [1-{n + 1}]"
    if default_idx is not None:
        prompt += f" (Enter for {default_catalog})"
    prompt += ": "

    while True:
        choice = input(prompt).strip()
        if not choice and default_idx is not None:
            print(f"  Using catalog: {default_catalog}")
            return default_catalog
        try:
            idx = int(choice) - 1
            if 0 <= idx < n:
                print(f"  Using catalog: {catalogs[idx]}")
                return catalogs[idx]
            if idx == n:
                name = input("  Enter catalog name: ").strip()
                if name:
                    print(f"  Using catalog: {name}")
                    return name
                print("  Catalog name cannot be empty.")
                continue
        except ValueError:
            pass
        print(f"  Invalid choice. Enter a number between 1 and {n + 1}.")


def _select_schema(host: str, token: str, warehouse_id: str, catalog: str, default_schema: str) -> str:
    """List schemas in a catalog and let the user pick one, or enter a new name."""
    _skip = {"default", "information_schema"}
    try:
        rows = _query_sql_statement(host, token, warehouse_id, f"SHOW SCHEMAS IN `{catalog}`")
        schemas = [r[0] for r in rows if r[0] not in _skip]
    except Exception:
        schemas = []

    # If default exists, highlight it
    default_idx = None
    if default_schema in schemas:
        default_idx = schemas.index(default_schema)

    print(f"\n  Select a schema in `{catalog}` (or enter a new name to create one):\n")
    for i, sch in enumerate(schemas, 1):
        marker = " (default)" if sch == default_schema else ""
        print(f"    {i}. {sch}{marker}")
    n = len(schemas)
    print(f"    {n + 1}. [ Create new schema ]")
    print()

    prompt = f"  Choose [1-{n + 1}]"
    if default_idx is not None:
        prompt += f" (Enter for {default_schema})"
    prompt += ": "

    while True:
        choice = input(prompt).strip()
        if not choice and default_idx is not None:
            print(f"  Using schema: {default_schema}")
            return default_schema
        try:
            idx = int(choice) - 1
            if 0 <= idx < n:
                print(f"  Using schema: {schemas[idx]}")
                return schemas[idx]
            if idx == n:
                new_name = input("  Enter new schema name: ").strip()
                if new_name:
                    print(f"  Will create schema: {new_name}")
                    return new_name
                print("  Schema name cannot be empty.")
                continue
        except ValueError:
            pass
        print(f"  Invalid choice. Enter a number between 1 and {n + 1}.")


def _exec_sql_parallel(host: str, token: str, warehouse_id: str, statements: list[str], max_workers: int = 6) -> None:
    """Execute multiple SQL statements in parallel using a thread pool."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_exec_sql_statement, host, token, warehouse_id, sql): i
            for i, sql in enumerate(statements)
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                errors.append(str(e))
    if errors:
        raise RuntimeError(f"Parallel SQL failed ({len(errors)} errors): {errors[0]}")


_EXPECTED_TABLES = {"scan_runs", "findings", "reports", "category_scores", "api_endpoints", "all_checks", "scan_changes", "prioritised_recommendations"}

# Expected columns per table — used for automatic schema migration (ALTER TABLE ADD COLUMNS)
# Only columns added *after* the initial release need to be listed here.
_EXPECTED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "prioritised_recommendations": [
        ("cost_low", "INT"),
        ("cost_high", "INT"),
        ("cost_reason", "STRING"),
    ],
}


def _migrate_existing_tables(fqn: str, existing: set[str], host: str, token: str, warehouse_id: str) -> None:
    """Add missing columns to existing tables via ALTER TABLE ADD COLUMNS."""
    alter_stmts: list[str] = []
    for table_name, expected_cols in _EXPECTED_COLUMNS.items():
        if table_name not in existing:
            continue  # table doesn't exist yet — CREATE TABLE will handle it
        try:
            rows = _query_sql_statement(host, token, warehouse_id, f"DESCRIBE {fqn}.{table_name}")
            current_cols = {r[0].lower() for r in rows}
        except Exception:
            continue
        missing_cols = [(name, dtype) for name, dtype in expected_cols if name.lower() not in current_cols]
        if missing_cols:
            cols_sql = ", ".join(f"{name} {dtype}" for name, dtype in missing_cols)
            alter_stmts.append(f"ALTER TABLE {fqn}.{table_name} ADD COLUMNS ({cols_sql})")
    if alter_stmts:
        _exec_sql_parallel(host, token, warehouse_id, alter_stmts)


def ensure_delta_tables(catalog: str, schema: str, host: str, token: str, warehouse_id: str) -> tuple[str, str]:
    """Prompt for catalog/schema, create tables if needed via REST API. Returns (catalog, schema)."""
    catalog = _select_catalog(host, token, warehouse_id, catalog)
    schema = _select_schema(host, token, warehouse_id, catalog, schema)
    fqn = f"`{catalog}`.`{schema}`"

    # Create schema if it doesn't exist (must complete before table creation)
    _exec_sql_statement(host, token, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {fqn}")

    # Check which tables already exist — skip CREATE for those that do
    try:
        rows = _query_sql_statement(host, token, warehouse_id, f"SHOW TABLES IN {fqn}")
        existing = {r[1] for r in rows}  # column 1 = tableName
    except Exception:
        existing = set()

    missing = _EXPECTED_TABLES - existing

    # ── Schema migration: add missing columns to existing tables ──
    _migrate_existing_tables(fqn, existing, host, token, warehouse_id)

    if not missing:
        return catalog, schema  # all tables exist, nothing to do

    # Only create missing tables (in parallel)
    create_stmts = []
    if "scan_runs" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.scan_runs (
            run_id STRING, workspace_url STRING, workspace_name STRING,
            scanned_at TIMESTAMP, overall_score INT, total_checks INT,
            passed INT, failed INT, warnings INT, not_applicable INT,
            api_errors INT, category_scores STRING, databricks_version STRING
        ) USING DELTA""")
    if "findings" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.findings (
            run_id STRING, check_id STRING, category STRING, title STRING,
            severity STRING, status STRING, current_state STRING,
            recommendation STRING, description STRING, reference_url STRING,
            is_api_error BOOLEAN, portal_link STRING, benefits STRING,
            effort STRING, evidence STRING, details STRING
        ) USING DELTA""")
    if "reports" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.reports (
            run_id STRING, workspace_name STRING, report_type STRING,
            file_name STRING, report_bytes BINARY, created_at TIMESTAMP
        ) USING DELTA""")
    if "category_scores" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.category_scores (
            run_id STRING, workspace_name STRING, category STRING,
            score INT, grade STRING, grade_definition STRING
        ) USING DELTA""")
    if "api_endpoints" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.api_endpoints (
            run_id STRING, workspace_name STRING, endpoint STRING,
            status STRING, items_count INT, error_code INT
        ) USING DELTA""")
    if "all_checks" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.all_checks (
            check_id STRING, category STRING, severity STRING, title STRING,
            description STRING, recommendation STRING, effort STRING,
            reference_url STRING
        ) USING DELTA""")
    if "scan_changes" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.scan_changes (
            run_id STRING, previous_run_id STRING, workspace_name STRING,
            scanned_at STRING, previous_scanned_at STRING,
            score_before INT, score_after INT, score_delta INT,
            change_type STRING, check_id STRING, category STRING,
            severity STRING, title STRING, status_before STRING,
            status_after STRING
        ) USING DELTA""")
    if "prioritised_recommendations" in missing:
        create_stmts.append(f"""CREATE TABLE IF NOT EXISTS {fqn}.prioritised_recommendations (
            run_id STRING, workspace_name STRING, priority_label STRING,
            priority_score INT, check_id STRING, category STRING,
            severity STRING, status STRING, effort STRING,
            title STRING, recommendation STRING, benefits STRING,
            portal_link STRING, cost_low INT, cost_high INT,
            cost_reason STRING
        ) USING DELTA""")

    if create_stmts:
        _exec_sql_parallel(host, token, warehouse_id, create_stmts)

    return catalog, schema


def _sql_escape(val: str) -> str:
    """Escape a string value for safe inclusion in SQL literals."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("\\", "\\\\").replace("'", "\\'") + "'"


# Tables that carry a run_id column (all except all_checks)
_TABLES_WITH_RUN_ID = [
    "scan_runs", "findings", "category_scores", "api_endpoints",
    "prioritised_recommendations", "scan_changes", "reports",
]


def _build_merge(fqn: str, table: str, keys: list[str], columns: list[str],
                 rows: list[tuple], batch_size: int = 50) -> list[str]:
    """Build MERGE statements for idempotent upsert.

    keys:    columns used in the ON clause (e.g. ["run_id", "check_id"])
    columns: all columns including keys
    rows:    list of tuples, one per row, values already SQL-escaped
    """
    stmts = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        unions = " UNION ALL ".join(
            "SELECT " + ", ".join(f"{v} AS {c}" for v, c in zip(row, columns))
            for row in batch
        )
        on_clause = " AND ".join(
            f"{fqn}.{table}.{k} = source.{k}" for k in keys
        )
        update_cols = [c for c in columns if c not in keys]
        update_set = ", ".join(f"{c} = source.{c}" for c in update_cols)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"source.{c}" for c in columns)
        stmts.append(
            f"MERGE INTO {fqn}.{table} USING ({unions}) AS source "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    return stmts


def _exec_sql_statement(host: str, token: str, warehouse_id: str, sql: str) -> None:
    """Execute SQL via the Databricks SQL Statement Execution API.

    Handles PENDING/RUNNING states by polling until the statement completes.
    """
    import httpx as _httpx
    import time as _time

    url = f"{host}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
    }
    resp = _httpx.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"SQL Statement API error {resp.status_code}: {resp.text}")
    data = resp.json()

    # Poll if statement is still pending/running
    statement_id = data.get("statement_id", "")
    status = data.get("status", {}).get("state", "")
    poll_count = 0
    while status in ("PENDING", "RUNNING") and poll_count < 60:
        _time.sleep(2)
        poll_count += 1
        poll_resp = _httpx.get(
            f"{url}/{statement_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if poll_resp.status_code != 200:
            raise RuntimeError(f"SQL poll error {poll_resp.status_code}: {poll_resp.text}")
        data = poll_resp.json()
        status = data.get("status", {}).get("state", "")

    if status == "FAILED":
        err = data.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise RuntimeError(f"SQL statement failed: {err}")


def export_delta(
    result: SATScanResult,
    run_id: str,
    catalog: str,
    schema: str,
    host: str,
    token: str,
    warehouse_id: str,
    mode: str = "merge",
) -> None:
    """Write scan results to Delta tables using the SQL Statement API.

    mode: "merge" (upsert, default), "append" (blind insert), "overwrite" (delete + insert).
    Fires all independent statements in parallel for speed.
    """
    fqn = f"`{catalog}`.`{schema}`"
    batch_size = 50

    # ── Overwrite mode: delete existing rows for this run_id first ──
    if mode == "overwrite":
        del_stmts = [f"DELETE FROM {fqn}.{t} WHERE run_id = {_sql_escape(run_id)}" for t in _TABLES_WITH_RUN_ID]
        try:
            _exec_sql_parallel(host, token, warehouse_id, del_stmts)
        except Exception:
            pass  # tables may not exist on first run

    def _upsert(table: str, keys: list[str], columns: list[str],
                rows: list[tuple], bs: int = batch_size) -> list[str]:
        """Build MERGE or INSERT statements based on mode."""
        if mode == "merge":
            return _build_merge(fqn, table, keys, columns, rows, bs)
        # append / overwrite — plain INSERT
        stmts = []
        for i in range(0, len(rows), bs):
            batch = rows[i : i + bs]
            vals = ", ".join("(" + ", ".join(row) + ")" for row in batch)
            stmts.append(f"INSERT INTO {fqn}.{table} VALUES {vals}")
        return stmts

    # ── Build all SQL statements up front ──

    all_statements: list[str] = []

    # 1. scan_runs (MERGE on run_id)
    _sr_cols = ["run_id", "workspace_url", "workspace_name", "scanned_at",
                "overall_score", "total_checks", "passed", "failed", "warnings",
                "not_applicable", "api_errors", "category_scores", "databricks_version"]
    all_statements += _upsert("scan_runs", ["run_id"], _sr_cols, [(
        _sql_escape(run_id),
        _sql_escape(result.workspace_url),
        _sql_escape(result.workspace_name),
        _sql_escape(result.scanned_at),
        str(result.overall_score),
        str(result.total_checks),
        str(result.passed),
        str(result.failed),
        str(result.warnings),
        str(result.not_applicable),
        str(result.api_errors),
        _sql_escape(json.dumps(result.category_scores, default=str)),
        _sql_escape(result.databricks_version or ""),
    )])

    # 2. findings (MERGE on run_id + check_id)
    if result.findings:
        _f_cols = ["run_id", "check_id", "category", "title", "severity", "status",
                   "current_state", "recommendation", "description", "reference_url",
                   "is_api_error", "portal_link", "benefits", "effort", "evidence", "details"]
        f_rows = []
        for f in result.findings:
            evidence = json.dumps(f.evidence, default=str) if f.evidence else ""
            details = json.dumps(f.details, default=str) if f.details else ""
            f_rows.append((
                _sql_escape(run_id), _sql_escape(f.check_id),
                _sql_escape(f.category), _sql_escape(f.title),
                _sql_escape(f.severity), _sql_escape(f.status),
                _sql_escape(f.current_state), _sql_escape(f.recommendation),
                _sql_escape(f.description), _sql_escape(f.reference_url),
                str(f.is_api_error).lower(), _sql_escape(f.portal_link or ''),
                _sql_escape(f.benefits or ''), _sql_escape(f.effort or ''),
                _sql_escape(evidence), _sql_escape(details),
            ))
        all_statements += _upsert("findings", ["run_id", "check_id"], _f_cols, f_rows, batch_size)

    # 3. category_scores (MERGE on run_id + category)
    if result.category_scores:
        _cs_cols = ["run_id", "workspace_name", "category", "score", "grade", "grade_definition"]
        cs_rows = []
        for cat, score in sorted(result.category_scores.items()):
            grade = "Good" if score >= 80 else ("Needs Improvement" if score >= 60 else "Critical")
            grade_def = (
                "Strong security posture. Address remaining findings as maintenance items." if score >= 80
                else "Gaps exist that weaken security posture. Prioritize High and Critical findings." if score >= 60
                else "Significant security risks are present. Immediate remediation is required."
            )
            cs_rows.append((
                _sql_escape(run_id), _sql_escape(result.workspace_name),
                _sql_escape(cat), str(score), _sql_escape(grade), _sql_escape(grade_def),
            ))
        all_statements += _upsert("category_scores", ["run_id", "category"], _cs_cols, cs_rows)

    # 4. api_endpoints (MERGE on run_id + endpoint)
    ep_summary = getattr(result, "endpoint_summary", {})
    if ep_summary and ep_summary.get("endpoints"):
        _ep_cols = ["run_id", "workspace_name", "endpoint", "status", "items_count", "error_code"]
        ep_rows = []
        for e in ep_summary["endpoints"]:
            ep_rows.append((
                _sql_escape(run_id), _sql_escape(result.workspace_name),
                _sql_escape(e['endpoint']), _sql_escape(e['status']),
                str(e['items_count']), str(e.get('error_code', 0)),
            ))
        all_statements += _upsert("api_endpoints", ["run_id", "endpoint"], _ep_cols, ep_rows, batch_size)

    # 5. all_checks MERGE (batched)
    checks_rows = []
    for cid, ck in SAT_CHECKS.items():
        checks_rows.append(
            f"SELECT {_sql_escape(cid)} AS check_id, "
            f"{_sql_escape(ck.get('category', ''))} AS category, "
            f"{_sql_escape(ck.get('severity', ''))} AS severity, "
            f"{_sql_escape(ck.get('title', ''))} AS title, "
            f"{_sql_escape(ck.get('description', ''))} AS description, "
            f"{_sql_escape(ck.get('recommendation', ''))} AS recommendation, "
            f"{_sql_escape(_get_effort(cid))} AS effort, "
            f"{_sql_escape(ck.get('reference_url', ''))} AS reference_url"
        )
    for i in range(0, len(checks_rows), batch_size):
        batch = checks_rows[i : i + batch_size]
        source_query = " UNION ALL ".join(batch)
        all_statements.append(f"""MERGE INTO {fqn}.all_checks
USING ({source_query}) source
ON {fqn}.all_checks.check_id = source.check_id
WHEN MATCHED THEN UPDATE SET
    category = source.category, severity = source.severity, title = source.title,
    description = source.description, recommendation = source.recommendation,
    effort = source.effort, reference_url = source.reference_url
WHEN NOT MATCHED THEN INSERT (
    check_id, category, severity, title, description, recommendation, effort, reference_url
) VALUES (
    source.check_id, source.category, source.severity, source.title,
    source.description, source.recommendation, source.effort, source.reference_url
)""")

    # 6. prioritised_recommendations (MERGE on run_id + check_id)
    prio_items = _build_prioritised_recommendations(result.findings)
    if prio_items:
        _pr_cols = ["run_id", "workspace_name", "priority_label", "priority_score",
                    "check_id", "category", "severity", "status", "effort", "title",
                    "recommendation", "benefits", "portal_link", "cost_low", "cost_high", "cost_reason"]
        pr_rows = []
        for item in prio_items:
            pr_rows.append((
                _sql_escape(run_id), _sql_escape(result.workspace_name),
                _sql_escape(item['priority_label']), str(item['priority_score']),
                _sql_escape(item['check_id']), _sql_escape(item['category']),
                _sql_escape(item['severity']), _sql_escape(item['status']),
                _sql_escape(item['effort']), _sql_escape(item['title']),
                _sql_escape(item['recommendation']), _sql_escape(item['benefits']),
                _sql_escape(item['portal_link']), str(item['cost_low']),
                str(item['cost_high']), _sql_escape(item['cost_reason']),
            ))
        all_statements += _upsert("prioritised_recommendations", ["run_id", "check_id"], _pr_cols, pr_rows, batch_size)

    # ── Execute all statements in parallel ──
    _exec_sql_parallel(host, token, warehouse_id, all_statements)


def detect_and_store_changes(
    result: SATScanResult,
    run_id: str,
    catalog: str,
    schema: str,
    host: str,
    token: str,
    warehouse_id: str,
    quiet: bool = False,
) -> None:
    """Compare current scan against previous run for the same workspace.

    Detects: fixed checks, new failures, regressions (PASS->WARN),
    improvements (WARN->PASS), new API errors, score changes.
    Stores changes in scan_changes table and prints a summary.
    """
    fqn = f"`{catalog}`.`{schema}`"
    ws = result.workspace_name or result.workspace_url

    # Find the previous run for this workspace (exclude the current run)
    prev_sql = f"""
        SELECT run_id, scanned_at, overall_score
        FROM {fqn}.scan_runs
        WHERE workspace_name = {_sql_escape(ws)}
          AND run_id != {_sql_escape(run_id)}
        ORDER BY scanned_at DESC
        LIMIT 1
    """
    try:
        prev_rows = _query_sql_statement(host, token, warehouse_id, prev_sql)
    except Exception:
        prev_rows = []

    if not prev_rows:
        if not quiet:
            print("  First scan for this workspace — no previous run to compare.")
        return

    prev_run_id = prev_rows[0][0]
    prev_scanned_at = prev_rows[0][1]
    prev_score = int(prev_rows[0][2]) if prev_rows[0][2] else 0

    # Fetch previous findings
    prev_findings_sql = f"""
        SELECT check_id, status, is_api_error
        FROM {fqn}.findings
        WHERE run_id = {_sql_escape(prev_run_id)}
    """
    try:
        prev_finding_rows = _query_sql_statement(host, token, warehouse_id, prev_findings_sql)
    except Exception:
        prev_finding_rows = []

    if not prev_finding_rows:
        if not quiet:
            print(f"  Previous run found ({prev_scanned_at}) but no findings to compare.")
        return

    # Build lookup: check_id -> status
    prev_status = {r[0]: r[1] for r in prev_finding_rows}
    curr_status = {f.check_id: f.status for f in result.findings}
    curr_lookup = {f.check_id: f for f in result.findings}

    # Detect changes
    changes = []  # list of (change_type, check_id, status_before, status_after)
    all_checks = set(prev_status.keys()) | set(curr_status.keys())

    for cid in all_checks:
        before = prev_status.get(cid)
        after = curr_status.get(cid)
        if before is None or after is None:
            continue  # new check added or removed — skip
        if before == after:
            continue

        if before == "FAIL" and after == "PASS":
            changes.append(("FIXED", cid, before, after))
        elif before != "FAIL" and after == "FAIL":
            changes.append(("NEW_FAILURE", cid, before, after))
        elif before == "FAIL" and after == "WARN":
            changes.append(("IMPROVED", cid, before, after))
        elif before == "PASS" and after == "WARN":
            changes.append(("REGRESSION", cid, before, after))
        elif before == "WARN" and after == "PASS":
            changes.append(("FIXED", cid, before, after))
        else:
            changes.append(("CHANGED", cid, before, after))

    score_delta = result.overall_score - prev_score

    # Print summary
    if not quiet:
        print(f"\n  Changes from previous scan ({prev_scanned_at}):")
        arrow = "+" if score_delta > 0 else ""
        print(f"    Overall score: {prev_score} -> {result.overall_score} ({arrow}{score_delta})")
        if not changes:
            print("    No check status changes.")
        else:
            fixed = [c for c in changes if c[0] == "FIXED"]
            new_fail = [c for c in changes if c[0] == "NEW_FAILURE"]
            improved = [c for c in changes if c[0] == "IMPROVED"]
            regressed = [c for c in changes if c[0] == "REGRESSION"]
            other = [c for c in changes if c[0] == "CHANGED"]

            if fixed:
                ids = ", ".join(c[1] for c in fixed)
                print(f"    {len(fixed)} fixed ({ids})")
            if new_fail:
                ids = ", ".join(c[1] for c in new_fail)
                print(f"    {len(new_fail)} new failure(s) ({ids})")
            if improved:
                ids = ", ".join(c[1] for c in improved)
                print(f"    {len(improved)} improved ({ids})")
            if regressed:
                ids = ", ".join(c[1] for c in regressed)
                print(f"    {len(regressed)} regression(s) ({ids})")
            if other:
                ids = ", ".join(c[1] for c in other)
                print(f"    {len(other)} other change(s) ({ids})")
        print()

    # Store changes in scan_changes table (only if something actually changed)
    if not changes and score_delta == 0:
        if not quiet:
            print("    No changes to store.")
        return

    _sc_cols = ["run_id", "previous_run_id", "workspace_name", "scanned_at",
                "previous_scanned_at", "score_before", "score_after", "score_delta",
                "change_type", "check_id", "category", "severity", "title",
                "status_before", "status_after"]
    sc_rows = []
    scanned_at = result.scanned_at

    # Store a score-change row only if the score actually changed
    if score_delta != 0:
        sc_rows.append((
            _sql_escape(run_id), _sql_escape(prev_run_id),
            _sql_escape(ws), _sql_escape(scanned_at), _sql_escape(prev_scanned_at),
            str(prev_score), str(result.overall_score), str(score_delta),
            "'SCORE_CHANGE'", "''", "''", "''", "''", "''", "''",
        ))

    for change_type, cid, before, after in changes:
        f = curr_lookup.get(cid)
        cat = f.category if f else ""
        sev = f.severity if f else ""
        title = f.title if f else ""
        sc_rows.append((
            _sql_escape(run_id), _sql_escape(prev_run_id),
            _sql_escape(ws), _sql_escape(scanned_at), _sql_escape(prev_scanned_at),
            str(prev_score), str(result.overall_score), str(score_delta),
            _sql_escape(change_type), _sql_escape(cid),
            _sql_escape(cat), _sql_escape(sev), _sql_escape(title),
            _sql_escape(before), _sql_escape(after),
        ))

    merge_stmts = _build_merge(fqn, "scan_changes", ["run_id", "check_id"], _sc_cols, sc_rows)
    for sql in merge_stmts:
        _exec_sql_statement(host, token, warehouse_id, sql)


def export_delta_report(
    run_id: str,
    workspace_name: str,
    report_type: str,
    file_path: str,
    catalog: str,
    schema: str,
    host: str,
    token: str,
    warehouse_id: str,
) -> None:
    """Store a generated report file (Excel, HTML, etc.) as binary in the reports Delta table."""
    import base64

    fqn = f"`{catalog}`.`{schema}`"
    report_bytes = Path(file_path).read_bytes()
    file_name = Path(file_path).name
    b64 = base64.b64encode(report_bytes).decode("ascii")

    # MERGE on run_id + file_name for idempotent re-export
    sql = (
        f"MERGE INTO {fqn}.reports USING ("
        f"SELECT {_sql_escape(run_id)} AS run_id, "
        f"{_sql_escape(workspace_name)} AS workspace_name, "
        f"{_sql_escape(report_type)} AS report_type, "
        f"{_sql_escape(file_name)} AS file_name, "
        f"unbase64('{b64}') AS report_bytes, "
        f"{_sql_escape(datetime.now().isoformat())} AS created_at"
        f") AS source ON {fqn}.reports.run_id = source.run_id "
        f"AND {fqn}.reports.file_name = source.file_name "
        f"WHEN MATCHED THEN UPDATE SET "
        f"workspace_name = source.workspace_name, report_type = source.report_type, "
        f"report_bytes = source.report_bytes, created_at = source.created_at "
        f"WHEN NOT MATCHED THEN INSERT "
        f"(run_id, workspace_name, report_type, file_name, report_bytes, created_at) VALUES "
        f"(source.run_id, source.workspace_name, source.report_type, source.file_name, "
        f"source.report_bytes, source.created_at)"
    )
    _exec_sql_statement(host, token, warehouse_id, sql)


def cleanup_old_runs(
    catalog: str, schema: str, host: str, token: str, warehouse_id: str,
    key_column: str = "workspace_name",
    retain: int = 10,
    quiet: bool = False,
) -> int:
    """Delete old scan runs, keeping only the last `retain` per workspace."""
    fqn = f"`{catalog}`.`{schema}`"
    sql = f"""
        SELECT run_id FROM (
            SELECT run_id, {key_column},
                   ROW_NUMBER() OVER (PARTITION BY {key_column} ORDER BY scanned_at DESC) AS rn
            FROM {fqn}.scan_runs
        ) WHERE rn > {retain}
    """
    try:
        rows = _query_sql_statement(host, token, warehouse_id, sql)
    except Exception:
        return 0
    if not rows:
        if not quiet:
            print(f"  No old runs to clean up (retaining last {retain}).")
        return 0

    stale_ids = [r[0] for r in rows]
    if not quiet:
        print(f"  Cleaning up {len(stale_ids)} old scan run(s)...")

    batch_size = 50
    for i in range(0, len(stale_ids), batch_size):
        batch = stale_ids[i : i + batch_size]
        id_list = ", ".join(_sql_escape(rid) for rid in batch)
        delete_stmts = [
            f"DELETE FROM {fqn}.{table} WHERE run_id IN ({id_list})"
            for table in _TABLES_WITH_RUN_ID
        ]
        _exec_sql_parallel(host, token, warehouse_id, delete_stmts)

    if not quiet:
        print(f"  Cleaned up {len(stale_ids)} old scan run(s), keeping last {retain}.")
    return len(stale_ids)


# ─────────────────────────────────────────────────────────────────────────────
# Unity Catalog Inventory → Delta tables
# ─────────────────────────────────────────────────────────────────────────────

_INVENTORY_TABLES = {
    "uc_inventory_runs", "uc_catalogs", "uc_schemas", "uc_tables", "uc_columns",
    "uc_volumes", "uc_functions", "uc_models", "uc_model_versions", "uc_grants",
    "uc_constraints", "uc_column_masks", "uc_row_filters", "uc_catalog_bindings",
    "uc_monitors", "uc_providers", "uc_service_credentials",
    "uc_external_locations", "uc_storage_credentials",
    "uc_azure_storage_accounts", "uc_azure_role_assignments", "uc_azure_mapping",
}

# Column lists (all STRING — snapshot inventory). Every table leads with
# (run_id, workspace): run_id ties a whole fleet run together, workspace
# distinguishes each workspace so same-named catalogs don't collide.
_INV_COLUMNS: dict[str, list[str]] = {
    "uc_inventory_runs": ["run_id", "workspace", "workspace_url", "workspace_name",
                          "scanned_at", "azure_available", "stats"],
    "uc_catalogs": ["run_id", "workspace", "name", "catalog_type", "owner", "comment",
                    "storage_root", "isolation_mode"],
    "uc_schemas": ["run_id", "workspace", "full_name", "catalog", "name", "owner", "comment"],
    "uc_tables": ["run_id", "workspace", "full_name", "catalog", "schema", "name", "table_type",
                  "data_source_format", "storage_location", "owner", "comment", "num_columns"],
    "uc_columns": ["run_id", "workspace", "table_full_name", "name", "position", "type_text",
                   "nullable", "comment"],
    "uc_volumes": ["run_id", "workspace", "full_name", "catalog", "schema", "name", "volume_type",
                   "storage_location", "owner"],
    "uc_functions": ["run_id", "workspace", "full_name", "catalog", "schema", "name", "data_type", "owner"],
    "uc_models": ["run_id", "workspace", "full_name", "catalog", "schema", "name", "owner"],
    "uc_model_versions": ["run_id", "workspace", "model_full_name", "version", "status",
                          "created_at", "mlflow_run_id"],
    "uc_constraints": ["run_id", "workspace", "table_full_name", "name", "type", "columns"],
    "uc_column_masks": ["run_id", "workspace", "table_full_name", "column_name", "mask_function"],
    "uc_row_filters": ["run_id", "workspace", "table_full_name", "filter_function", "input_columns"],
    "uc_catalog_bindings": ["run_id", "workspace", "catalog", "workspace_id", "binding_type"],
    "uc_monitors": ["run_id", "workspace", "table_full_name", "status", "output_schema", "assets_dir"],
    "uc_providers": ["run_id", "workspace", "name", "authentication_type", "owner", "comment"],
    "uc_service_credentials": ["run_id", "workspace", "name", "purpose", "owner", "comment"],
    "uc_grants": ["run_id", "workspace", "securable_type", "full_name", "principal", "privileges",
                  "inherited_from"],
    "uc_external_locations": ["run_id", "workspace", "name", "url", "credential_name", "read_only",
                              "storage_account", "container"],
    "uc_storage_credentials": ["run_id", "workspace", "name", "owner", "comment"],
    "uc_azure_storage_accounts": ["run_id", "workspace", "name", "resource_group", "subscription_id",
                                  "location", "hns_enabled", "public_network_access",
                                  "network_default_action"],
    "uc_azure_role_assignments": ["run_id", "workspace", "storage_account", "role_name", "principal_id",
                                  "principal_type"],
    "uc_azure_mapping": ["run_id", "workspace", "uc_object_type", "uc_name", "url", "storage_account",
                         "container", "resolved", "credential_name", "identity_name",
                         "granting_roles", "notes"],
}

_INV_KEYS: dict[str, list[str]] = {
    "uc_inventory_runs": ["run_id", "workspace"],
    "uc_catalogs": ["run_id", "workspace", "name"],
    "uc_schemas": ["run_id", "workspace", "full_name"],
    "uc_tables": ["run_id", "workspace", "full_name"],
    "uc_columns": ["run_id", "workspace", "table_full_name", "name"],
    "uc_volumes": ["run_id", "workspace", "full_name"],
    "uc_functions": ["run_id", "workspace", "full_name"],
    "uc_models": ["run_id", "workspace", "full_name"],
    "uc_model_versions": ["run_id", "workspace", "model_full_name", "version"],
    "uc_constraints": ["run_id", "workspace", "table_full_name", "name"],
    "uc_column_masks": ["run_id", "workspace", "table_full_name", "column_name"],
    "uc_row_filters": ["run_id", "workspace", "table_full_name"],
    "uc_catalog_bindings": ["run_id", "workspace", "catalog", "workspace_id"],
    "uc_monitors": ["run_id", "workspace", "table_full_name"],
    "uc_providers": ["run_id", "workspace", "name"],
    "uc_service_credentials": ["run_id", "workspace", "name"],
    "uc_grants": ["run_id", "workspace", "securable_type", "full_name", "principal"],
    "uc_external_locations": ["run_id", "workspace", "name"],
    "uc_storage_credentials": ["run_id", "workspace", "name"],
    "uc_azure_storage_accounts": ["run_id", "workspace", "name"],
    "uc_azure_role_assignments": ["run_id", "workspace", "storage_account", "role_name", "principal_id"],
    "uc_azure_mapping": ["run_id", "workspace", "uc_object_type", "uc_name"],
}


def ensure_inventory_tables(catalog: str, schema: str, host: str, token: str,
                            warehouse_id: str) -> tuple[str, str]:
    """Create the UC-inventory Delta tables if missing. Returns (catalog, schema)."""
    catalog = _select_catalog(host, token, warehouse_id, catalog)
    schema = _select_schema(host, token, warehouse_id, catalog, schema)
    fqn = f"`{catalog}`.`{schema}`"
    _exec_sql_statement(host, token, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {fqn}")

    try:
        rows = _query_sql_statement(host, token, warehouse_id, f"SHOW TABLES IN {fqn}")
        existing = {r[1] for r in rows}
    except Exception:
        existing = set()

    create_stmts = []
    for table, cols in _INV_COLUMNS.items():
        if table in existing:
            continue
        col_ddl = ", ".join(f"{c} STRING" for c in cols)
        create_stmts.append(f"CREATE TABLE IF NOT EXISTS {fqn}.{table} ({col_ddl}) USING DELTA")
    if create_stmts:
        _exec_sql_parallel(host, token, warehouse_id, create_stmts)

    # Migrate pre-existing tables created before the `workspace` column was added.
    _migrate_inventory_tables(fqn, existing, host, token, warehouse_id)
    return catalog, schema


def _migrate_inventory_tables(fqn: str, existing: set, host: str, token: str,
                              warehouse_id: str) -> None:
    """Add any missing columns (e.g. `workspace`) to existing uc_* tables."""
    alter_stmts: list[str] = []
    for table, cols in _INV_COLUMNS.items():
        if table not in existing:
            continue  # freshly created with the full schema
        try:
            rows = _query_sql_statement(host, token, warehouse_id, f"DESCRIBE {fqn}.{table}")
            current = {r[0].lower() for r in rows if r and r[0] and not r[0].startswith("#")}
        except Exception:
            continue
        missing = [c for c in cols if c.lower() not in current]
        if missing:
            cols_sql = ", ".join(f"{c} STRING" for c in missing)
            alter_stmts.append(f"ALTER TABLE {fqn}.{table} ADD COLUMNS ({cols_sql})")
    if alter_stmts:
        _exec_sql_parallel(host, token, warehouse_id, alter_stmts)


def _v(x: Any) -> str:
    """Render a Python value as an escaped SQL string literal."""
    if x is None:
        return _sql_escape("")
    if isinstance(x, bool):
        return _sql_escape("true" if x else "false")
    if isinstance(x, (dict, list)):
        return _sql_escape(json.dumps(x, default=str, ensure_ascii=False))
    return _sql_escape(str(x))


def export_inventory_delta(inv, run_id: str, workspace: str, catalog: str, schema: str,
                           host: str, token: str, warehouse_id: str,
                           mode: str = "merge") -> None:
    """Write a UCInventoryResult to the UC-inventory Delta tables via the SQL API.

    Every row leads with (run_id, workspace): a shared ``run_id`` ties a whole
    fleet run together while ``workspace`` distinguishes each workspace.
    """
    fqn = f"`{catalog}`.`{schema}`"
    table_rows: dict[str, list[tuple]] = {t: [] for t in _INV_COLUMNS}
    rid, wsv = _v(run_id), _v(workspace)

    az = inv.azure or {}
    table_rows["uc_inventory_runs"].append((
        rid, wsv, _v(inv.workspace_url), _v(inv.workspace_name), _v(inv.scanned_at),
        _v(az.get("available", False)), _v(inv.stats)))

    def _grant_rows(grants):
        for g in grants:
            table_rows["uc_grants"].append((
                rid, wsv, _v(g.securable_type), _v(g.full_name), _v(g.principal),
                _v(", ".join(g.privileges)), _v(g.inherited_from)))

    _grant_rows(inv.metastore_grants)

    for c in inv.catalogs:
        table_rows["uc_catalogs"].append((
            rid, wsv, _v(c.name), _v(c.catalog_type), _v(c.owner), _v(c.comment),
            _v(c.storage_root), _v(c.isolation_mode)))
        _grant_rows(c.grants)
        for b in c.bindings:
            table_rows["uc_catalog_bindings"].append((
                rid, wsv, _v(c.name), _v(b.get("workspace_id", "")), _v(b.get("binding_type", ""))))
        for s in c.schemas:
            table_rows["uc_schemas"].append((
                rid, wsv, _v(s.full_name), _v(s.catalog), _v(s.name), _v(s.owner), _v(s.comment)))
            _grant_rows(s.grants)
            for t in s.tables:
                table_rows["uc_tables"].append((
                    rid, wsv, _v(t.full_name), _v(t.catalog), _v(t.schema), _v(t.name),
                    _v(t.table_type), _v(t.data_source_format), _v(t.storage_location),
                    _v(t.owner), _v(t.comment), _v(len(t.columns))))
                _grant_rows(t.grants)
                for col in t.columns:
                    table_rows["uc_columns"].append((
                        rid, wsv, _v(t.full_name), _v(col.name), _v(col.position),
                        _v(col.type_text), _v(col.nullable), _v(col.comment)))
                    if col.mask:
                        table_rows["uc_column_masks"].append((
                            rid, wsv, _v(t.full_name), _v(col.name),
                            _v((col.mask or {}).get("function_name", ""))))
                for con in t.constraints:
                    table_rows["uc_constraints"].append((
                        rid, wsv, _v(t.full_name), _v(con.get("name", "")), _v(con.get("type", "")),
                        _v(", ".join(con.get("columns", [])))))
                if t.row_filter:
                    table_rows["uc_row_filters"].append((
                        rid, wsv, _v(t.full_name), _v((t.row_filter or {}).get("function_name", "")),
                        _v(", ".join((t.row_filter or {}).get("input_column_names", []) or []))))
                if t.monitor:
                    table_rows["uc_monitors"].append((
                        rid, wsv, _v(t.full_name), _v(t.monitor.get("status", "")),
                        _v(t.monitor.get("output_schema_name", "")), _v(t.monitor.get("assets_dir", ""))))
            for v in s.volumes:
                table_rows["uc_volumes"].append((
                    rid, wsv, _v(v.full_name), _v(v.catalog), _v(v.schema), _v(v.name),
                    _v(v.volume_type), _v(v.storage_location), _v(v.owner)))
                _grant_rows(v.grants)
            for fn in s.functions:
                table_rows["uc_functions"].append((
                    rid, wsv, _v(fn.full_name), _v(fn.catalog), _v(fn.schema), _v(fn.name),
                    _v(fn.data_type), _v(fn.owner)))
                _grant_rows(fn.grants)
            for md in s.models:
                table_rows["uc_models"].append((
                    rid, wsv, _v(md.full_name), _v(md.catalog), _v(md.schema),
                    _v(md.name), _v(md.owner)))
                _grant_rows(md.grants)
                for mv in md.versions:
                    table_rows["uc_model_versions"].append((
                        rid, wsv, _v(md.full_name), _v(mv.get("version", "")), _v(mv.get("status", "")),
                        _v(mv.get("created_at", "")), _v(mv.get("run_id", ""))))

    for p in inv.providers:
        table_rows["uc_providers"].append((
            rid, wsv, _v(p.get("name", "")), _v(p.get("authentication_type", "")),
            _v(p.get("owner", "")), _v(p.get("comment", ""))))
    for sc in inv.service_credentials:
        table_rows["uc_service_credentials"].append((
            rid, wsv, _v(sc.get("name", "")), _v(sc.get("purpose", "")),
            _v(sc.get("owner", "")), _v(sc.get("comment", ""))))

    for l in inv.external_locations:
        amap = l.get("azure", {}) or {}
        table_rows["uc_external_locations"].append((
            rid, wsv, _v(l.get("name", "")), _v(l.get("url", "")), _v(l.get("credential_name", "")),
            _v(l.get("read_only", "")), _v(amap.get("storage_account", "")), _v(amap.get("container", ""))))
    for sc in inv.storage_credentials:
        table_rows["uc_storage_credentials"].append((
            rid, wsv, _v(sc.get("name", "")), _v(sc.get("owner", "")), _v(sc.get("comment", ""))))

    for a in az.get("storage_accounts", []):
        table_rows["uc_azure_storage_accounts"].append((
            rid, wsv, _v(a.get("name", "")), _v(a.get("resource_group", "")),
            _v(a.get("subscription_id", "")), _v(a.get("location", "")), _v(a.get("hns_enabled", "")),
            _v(a.get("public_network_access", "")), _v(a.get("network_default_action", ""))))
        for ra in a.get("role_assignments", []):
            table_rows["uc_azure_role_assignments"].append((
                rid, wsv, _v(a.get("name", "")), _v(ra.get("role_name", "")),
                _v(ra.get("principal_id", "")), _v(ra.get("principal_type", ""))))
    for m in az.get("mappings", []):
        ident = m.get("identity") or {}
        table_rows["uc_azure_mapping"].append((
            rid, wsv, _v(m.get("uc_object_type", "")), _v(m.get("uc_name", "")), _v(m.get("url", "")),
            _v(m.get("storage_account", "")), _v(m.get("container", "")),
            _v(m.get("storage_account_resolved", "")), _v(m.get("credential_name", "")),
            _v(ident.get("name", "")), _v(", ".join(m.get("granting_roles", []))),
            _v("; ".join(m.get("notes", [])))))

    if mode == "overwrite":
        # Scope the delete to this workspace so other workspaces in the same fleet run survive.
        del_stmts = [f"DELETE FROM {fqn}.{t} WHERE run_id = {_sql_escape(run_id)} "
                     f"AND workspace = {_sql_escape(workspace)}" for t in _INV_COLUMNS]
        try:
            _exec_sql_parallel(host, token, warehouse_id, del_stmts)
        except Exception:
            pass

    all_stmts: list[str] = []
    for table, rows in table_rows.items():
        if not rows:
            continue
        all_stmts.extend(_build_merge(fqn, table, _INV_KEYS[table], _INV_COLUMNS[table], rows))
    if all_stmts:
        _exec_sql_parallel(host, token, warehouse_id, all_stmts)
