"""OpenAI-compatible tool definitions and executor for the AI Assistant agentic loop.

TOOLS is the list passed to the model's `tools` parameter.
execute_tool() dispatches a parsed tool_call to the appropriate ai_tools function.
All ai_tools.* functions are synchronous — callers must use asyncio.to_thread().
"""

from __future__ import annotations

from api.routers import ai_tools

TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": (
                "Return column names, data types, row count, and 3 sample rows for a "
                "Unity Catalog table. Always call this before writing SQL that references "
                "specific column names."
            ),
            "parameters": {
                "type": "object",
                "required": ["catalog", "schema", "table"],
                "properties": {
                    "catalog": {"type": "string", "description": "Unity Catalog name"},
                    "schema":  {"type": "string", "description": "Schema (database) name"},
                    "table":   {"type": "string", "description": "Table name"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Execute a read-only SELECT SQL query on a Databricks SQL Warehouse "
                "and return the results as a formatted table. Use to validate queries "
                "or answer specific data questions."
            ),
            "parameters": {
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string", "description": "A read-only SELECT statement"},
                    "limit": {"type": "integer", "description": "Max rows to return (default 50)", "default": 50},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List all tables and their types in a Unity Catalog schema.",
            "parameters": {
                "type": "object",
                "required": ["catalog", "schema"],
                "properties": {
                    "catalog": {"type": "string", "description": "Unity Catalog name"},
                    "schema":  {"type": "string", "description": "Schema name"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_schemas",
            "description": "List all schemas in a Unity Catalog catalog.",
            "parameters": {
                "type": "object",
                "required": ["catalog"],
                "properties": {
                    "catalog": {"type": "string", "description": "Unity Catalog name"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_catalogs",
            "description": "List all Unity Catalog catalogs accessible in this workspace.",
            "parameters": {
                "type": "object",
                "required": [],
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_workspace_info",
            "description": (
                "Return the current user identity, workspace host URL, and available "
                "SQL warehouses. Use when the user asks who they are, what workspace "
                "they're in, or what compute resources they have."
            ),
            "parameters": {
                "type": "object",
                "required": [],
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_tables",
            "description": (
                "Search across Unity Catalog for tables (and columns) whose name matches "
                "a term. Use when the user doesn't know the exact location of a table or "
                "asks 'which tables have a customer_id column'."
            ),
            "parameters": {
                "type": "object",
                "required": ["term"],
                "properties": {
                    "term":    {"type": "string", "description": "Substring to match in table or column names"},
                    "catalog": {"type": "string", "description": "Optional catalog to restrict the search to"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_lineage",
            "description": (
                "Return the upstream (sources) and downstream (consumers) tables for a "
                "given table. Use to answer 'what feeds this table' or 'what depends on it'."
            ),
            "parameters": {
                "type": "object",
                "required": ["table"],
                "properties": {
                    "table": {"type": "string", "description": "Fully-qualified table name catalog.schema.table"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "profile_column",
            "description": (
                "Profile a single column: null percentage, distinct count, min/max, and "
                "sample distinct values. Use for data-quality questions about a column."
            ),
            "parameters": {
                "type": "object",
                "required": ["catalog", "schema", "table", "column"],
                "properties": {
                    "catalog": {"type": "string"},
                    "schema":  {"type": "string"},
                    "table":   {"type": "string"},
                    "column":  {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_query",
            "description": (
                "Return the physical execution plan (EXPLAIN FORMATTED) for a SQL query. "
                "Use to diagnose slow queries, shuffles, scans, and join strategies."
            ),
            "parameters": {
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to explain"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_assessment_findings",
            "description": (
                "Return the latest workspace security/WAF assessment findings, optionally "
                "filtered by severity, category, or status. Use for security-audit questions."
            ),
            "parameters": {
                "type": "object",
                "required": [],
                "properties": {
                    "severity": {"type": "string", "description": "Comma-separated: critical,high,medium,low"},
                    "category": {"type": "string", "description": "Finding category, e.g. security or governance"},
                    "status":   {"type": "string", "description": "Comma-separated: PASS,FAIL"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_pii_columns",
            "description": (
                "List columns tagged as PII/sensitive in a catalog (from Unity Catalog "
                "column tags). Call this before running SQL that may expose sensitive data."
            ),
            "parameters": {
                "type": "object",
                "required": ["catalog"],
                "properties": {
                    "catalog": {"type": "string"},
                },
            },
        },
    },
]


def _get_workspace_info(client, warehouse_id: str) -> str:
    try:
        me = client.current_user.me()
        user = getattr(me, "user_name", None) or getattr(me, "display_name", None) or "unknown"
    except Exception:
        user = "unknown"

    host = (getattr(client.config, "host", None) or "").rstrip("/")

    try:
        warehouses = list(client.warehouses.list())
        wh_lines = [
            f"  - {w.name} (id={w.id}, state={getattr(w, 'state', '?')}, "
            f"type={getattr(w, 'warehouse_type', '?')})"
            for w in warehouses
        ]
        active_wh = next((w for w in warehouses if w.id == warehouse_id), None)
        active_name = active_wh.name if active_wh else f"id={warehouse_id}" if warehouse_id else "(not set)"
    except Exception as e:
        wh_lines = [f"  ERROR listing warehouses: {e}"]
        active_name = f"id={warehouse_id}" if warehouse_id else "(not set)"

    lines = [
        f"User: {user}",
        f"Workspace: {host}",
        f"Active SQL Warehouse: {active_name}",
        "All SQL Warehouses:",
    ] + (wh_lines or ["  (none found)"])
    return "\n".join(lines)


def execute_tool(name: str, args: dict, client, warehouse_id: str) -> str:
    """Dispatch a tool call to the appropriate ai_tools function.
    Returns a string result suitable for the tool role message content.
    This function is synchronous — wrap with asyncio.to_thread() in async contexts.
    """
    try:
        if name == "describe_table":
            return ai_tools.dbx_describe_table(
                args["catalog"], args["schema"], args["table"],
                client, warehouse_id, sample_rows=3,
            )
        elif name == "run_sql":
            limit = min(int(args.get("limit", 50)), 200)
            return ai_tools.dbx_sql(args["query"], client, warehouse_id, limit=limit)
        elif name == "list_tables":
            return ai_tools.dbx_list_tables(args["catalog"], args["schema"], client)
        elif name == "list_schemas":
            return ai_tools.dbx_list_schemas(args["catalog"], client)
        elif name == "list_catalogs":
            return ai_tools.dbx_list_catalogs(client)
        elif name == "get_workspace_info":
            return _get_workspace_info(client, warehouse_id)
        elif name == "search_tables":
            return ai_tools.dbx_search_tables(args["term"], client, args.get("catalog", ""))
        elif name == "get_table_lineage":
            return ai_tools.dbx_table_lineage(args["table"], client)
        elif name == "profile_column":
            return ai_tools.dbx_profile_column(
                args["catalog"], args["schema"], args["table"], args["column"],
                client, warehouse_id,
            )
        elif name == "explain_query":
            return ai_tools.dbx_explain_query(args["query"], client, warehouse_id)
        elif name == "get_assessment_findings":
            return ai_tools.dbx_assessment_findings(
                args.get("severity", ""), args.get("category", ""), args.get("status", ""),
            )
        elif name == "list_pii_columns":
            return ai_tools.dbx_pii_columns(args["catalog"], client, warehouse_id)
        else:
            return f"ERROR: Unknown tool '{name}'"
    except KeyError as e:
        return f"ERROR: Missing required argument {e} for tool '{name}'"
    except Exception as e:
        return f"ERROR executing '{name}': {e}"
