"""AI Assistant & Genie — natural language to SQL, AI-powered data exploration."""

import json
import logging
import requests as req
from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel
from typing import Optional

from api.dependencies import get_db_client, get_app_config, get_credentials

router = APIRouter()
logger = logging.getLogger(__name__)


def _build_sql_prompt():
    return (
        "You are a Databricks SQL expert. Convert the user's natural language question into a valid Databricks SQL query.\n"
        "Return ONLY the raw SQL query. No explanation, no markdown, no code blocks, no comments.\n\n"
        "RULES:\n"
        "- Use Unity Catalog three-level namespace: catalog.schema.table\n"
        "- Always add LIMIT 100 unless user specifies otherwise\n"
        "- If user specifies a catalog/schema, use those\n\n"
        "AVAILABLE INFORMATION_SCHEMA VIEWS (per catalog):\n"
        "- {catalog}.information_schema.tables — columns: table_catalog, table_schema, table_name, table_type, data_source_format, created, created_by, last_altered, comment\n"
        "- {catalog}.information_schema.columns — columns: table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable, comment\n"
        "- {catalog}.information_schema.schemata — columns: catalog_name, schema_name, schema_owner, created, last_altered\n\n"
        "FORBIDDEN — THESE DO NOT EXIST:\n"
        "- information_schema.table_storage — DOES NOT EXIST\n"
        "- information_schema.table_privileges — DOES NOT EXIST\n"
        "- system.information_schema.* — DOES NOT EXIST\n"
        "- bytes, num_rows, size columns on information_schema.tables — DO NOT EXIST\n\n"
        "COMMON QUERIES:\n"
        "- Tables per schema: SELECT table_schema, COUNT(*) as table_count FROM {catalog}.information_schema.tables GROUP BY table_schema ORDER BY table_count DESC\n"
        "- Columns per table: SELECT table_name, COUNT(*) as col_count FROM {catalog}.information_schema.columns GROUP BY table_name ORDER BY col_count DESC\n"
        "- List schemas: SELECT schema_name, schema_owner FROM {catalog}.information_schema.schemata\n"
        "- List tables in schema: SELECT table_name, table_type FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}'\n"
        "- Describe table: DESCRIBE TABLE {catalog}.{schema}.{table}\n"
        "- Row count for ONE table: SELECT COUNT(*) as row_count FROM {catalog}.{schema}.{table}\n"
        "- Row counts across tables is NOT possible in a single query. Instead count tables per schema.\n"
    )


class NLQueryRequest(BaseModel):
    question: str
    catalog: str = ""
    schema_name: str = ""


class GenieQueryRequest(BaseModel):
    question: str
    space_id: str


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    catalog: str = ""
    schema_name: str = ""


def _get_auth_headers(client):
    """Extract auth headers from WorkspaceClient."""
    config = client.config
    headers = {"Content-Type": "application/json"}
    try:
        auth_headers = {}
        config.authenticate(auth_headers)
        headers.update(auth_headers)
    except Exception:
        token = getattr(config, "token", None)
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers


@router.post("/nl-to-sql")
async def natural_language_to_sql(
    request: NLQueryRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
    client=Depends(get_db_client),
):
    """Convert natural language to SQL using the selected AI model."""
    from src.ai_service import get_ai_service
    svc = get_ai_service()

    if not svc.is_available(x_databricks_model):
        return {"error": "No AI backend configured", "sql": ""}

    system_prompt = _build_sql_prompt()

    # Build context with real metadata
    context = f"Question: {request.question}"
    if request.catalog:
        context += f"\nCatalog: {request.catalog}"
    if request.schema_name:
        context += f"\nSchema: {request.schema_name}"

    # Inject actual schema metadata for better SQL generation
    if request.catalog:
        try:
            from src.client import execute_sql
            config = await get_app_config()
            wid = config.get("sql_warehouse_id", "")
            schemas = execute_sql(client, wid, f"SELECT schema_name FROM {request.catalog}.information_schema.schemata LIMIT 50")
            schema_names = [s.get("schema_name", s.get("SCHEMA_NAME", "")) for s in schemas]
            context += f"\nAvailable schemas in {request.catalog}: {', '.join(schema_names)}"
            if request.schema_name:
                tables = execute_sql(client, wid, f"SELECT table_name FROM {request.catalog}.information_schema.tables WHERE table_schema = '{request.schema_name}' LIMIT 50")
                table_names = [t.get("table_name", t.get("TABLE_NAME", "")) for t in tables]
                context += f"\nTables in {request.catalog}.{request.schema_name}: {', '.join(table_names[:30])}"
        except Exception:
            pass  # metadata fetch failed, continue without it

    try:
        sql = svc._call_llm(system_prompt, context, max_tokens=512, endpoint_name=x_databricks_model, client=client)
        sql = sql.strip()
        if sql.startswith("```"):
            sql = sql.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return {"sql": sql, "question": request.question}
    except Exception as e:
        logger.exception("NL-to-SQL failed")
        return {"error": str(e), "sql": ""}


@router.post("/execute-nl")
async def execute_natural_language(
    request: NLQueryRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
    client=Depends(get_db_client),
):
    """Convert natural language to SQL, execute it, and return results with AI explanation."""
    from src.ai_service import get_ai_service
    from src.client import execute_sql

    svc = get_ai_service()
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    if not svc.is_available(x_databricks_model):
        return {"error": "No AI backend configured"}

    # Step 1: Generate SQL — reuse the same prompt as nl-to-sql
    system_prompt = _build_sql_prompt()
    context = f"Question: {request.question}"
    if request.catalog:
        context += f"\nCatalog: {request.catalog}"
    if request.schema_name:
        context += f"\nSchema: {request.schema_name}"
    # Inject metadata
    if request.catalog:
        try:
            schemas = execute_sql(client, wid, f"SELECT schema_name FROM {request.catalog}.information_schema.schemata LIMIT 50")
            context += f"\nAvailable schemas: {', '.join(s.get('schema_name', '') for s in schemas)}"
            if request.schema_name:
                tables = execute_sql(client, wid, f"SELECT table_name FROM {request.catalog}.information_schema.tables WHERE table_schema = '{request.schema_name}' LIMIT 50")
                context += f"\nTables: {', '.join(t.get('table_name', '') for t in tables[:30])}"
        except Exception:
            pass

    sql = ""
    try:
        # Step 1: Generate SQL (use authenticated client for Databricks model serving)
        sql = svc._call_llm(system_prompt, context, max_tokens=512, endpoint_name=x_databricks_model, client=client)
        sql = sql.strip()
        if sql.startswith("```"):
            sql = sql.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        # Step 2: Execute SQL
        rows = execute_sql(client, wid, sql)

        # Step 3: Generate explanation (skip if too slow — use lighter prompt)
        explanation = f"Query returned {len(rows)} rows."
        try:
            if len(rows) > 0:
                explain_prompt = "Summarize these SQL results in 1-2 sentences. Be specific with numbers."
                explain_context = f"Question: {request.question}\nResults ({len(rows)} rows): {json.dumps(rows[:5], default=str)}"
                explanation = svc._call_llm(explain_prompt, explain_context, max_tokens=150, endpoint_name=x_databricks_model, client=client)
        except Exception:
            pass

        return {"sql": sql, "results": rows, "row_count": len(rows), "explanation": explanation, "question": request.question}
    except Exception as e:
        logger.exception("Execute NL query failed")
        return {"error": str(e), "sql": sql}


@router.post("/genie-query")
async def genie_query(
    request: GenieQueryRequest,
    client=Depends(get_db_client),
):
    """Send a question to Databricks Genie and return the response."""
    try:
        host = (client.config.host or "").rstrip("/")
        headers = _get_auth_headers(client)

        # Start a Genie conversation
        r = req.post(
            f"{host}/api/2.0/genie/spaces/{request.space_id}/start-conversation",
            headers=headers,
            json={"content": request.question},
            timeout=30,
        )
        if r.status_code != 200:
            return {"error": f"Genie API error: {r.status_code} — {r.text[:200]}"}

        data = r.json()
        conversation_id = data.get("conversation_id", "")
        message_id = data.get("message_id", "")

        # Poll for result
        import time
        for _ in range(30):
            time.sleep(1)
            poll = req.get(
                f"{host}/api/2.0/genie/spaces/{request.space_id}/conversations/{conversation_id}/messages/{message_id}",
                headers=headers,
                timeout=15,
            )
            if poll.status_code != 200:
                continue
            msg = poll.json()
            status = msg.get("status", "")
            if status == "COMPLETED":
                # Extract SQL and results
                attachments = msg.get("attachments", [])
                result = {"question": request.question, "conversation_id": conversation_id, "status": "completed"}
                for att in attachments:
                    if att.get("type") == "QUERY":
                        result["sql"] = att.get("query", {}).get("query", "")
                        result["description"] = att.get("query", {}).get("description", "")
                    if att.get("type") == "TEXT":
                        result["explanation"] = att.get("text", {}).get("content", "")
                return result
            if status in ("FAILED", "CANCELLED"):
                return {"error": f"Genie query {status}", "question": request.question}

        return {"error": "Genie query timed out", "question": request.question}
    except Exception as e:
        logger.exception("Genie query failed")
        return {"error": str(e)}


@router.post("/chat")
async def ai_chat(
    request: ChatRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    client=Depends(get_db_client),
):
    """Multi-turn chat with the AI model about data."""
    from src.ai_service import get_ai_service
    svc = get_ai_service()

    if not svc.is_available(x_databricks_model):
        return {"error": "No AI backend configured", "response": ""}

    system_prompt = (
        "You are a helpful Databricks data assistant for Clone-Xs. "
        "Help users explore their data, write SQL queries, understand schemas, and analyze results. "
        "When writing SQL, use Unity Catalog three-level namespace (catalog.schema.table). "
        "Be concise and specific."
    )

    history = "\n\n".join([f"{'User' if m.role == 'user' else 'Assistant'}: {m.content}" for m in request.messages])
    if request.catalog:
        history = f"Context: catalog={request.catalog}, schema={request.schema_name}\n\n{history}"

    try:
        response = svc._call_llm(system_prompt, history, max_tokens=1024, endpoint_name=x_databricks_model, client=client)
        return {"response": response}
    except Exception as e:
        return {"error": str(e), "response": ""}
