"""AI Assistant & Genie — natural language to SQL, streaming chat, and UC exploration."""

import json
import logging
import uuid
from pathlib import Path
from typing import AsyncGenerator, Optional

import httpx
import requests as req
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.dependencies import get_db_client, get_app_config, get_credentials
from api.routers import ai_sessions, ai_tools, ai_tool_definitions

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
            schemas = execute_sql(
                client,
                wid,
                f"SELECT schema_name FROM {request.catalog}.information_schema.schemata LIMIT 50",
            )
            schema_names = [s.get("schema_name", s.get("SCHEMA_NAME", "")) for s in schemas]
            context += f"\nAvailable schemas in {request.catalog}: {', '.join(schema_names)}"
            if request.schema_name:
                tables = execute_sql(
                    client,
                    wid,
                    f"SELECT table_name FROM {request.catalog}.information_schema.tables WHERE table_schema = '{request.schema_name}' LIMIT 50",
                )
                table_names = [t.get("table_name", t.get("TABLE_NAME", "")) for t in tables]
                context += f"\nTables in {request.catalog}.{request.schema_name}: {', '.join(table_names[:30])}"
        except Exception:
            pass  # metadata fetch failed, continue without it

    try:
        sql = svc._call_llm(
            system_prompt, context, max_tokens=512, endpoint_name=x_databricks_model, client=client
        )
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
        raise HTTPException(status_code=400, detail="No AI backend configured")

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
            schemas = execute_sql(
                client,
                wid,
                f"SELECT schema_name FROM {request.catalog}.information_schema.schemata LIMIT 50",
            )
            context += (
                f"\nAvailable schemas: {', '.join(s.get('schema_name', '') for s in schemas)}"
            )
            if request.schema_name:
                tables = execute_sql(
                    client,
                    wid,
                    f"SELECT table_name FROM {request.catalog}.information_schema.tables WHERE table_schema = '{request.schema_name}' LIMIT 50",
                )
                context += f"\nTables: {', '.join(t.get('table_name', '') for t in tables[:30])}"
        except Exception:
            pass

    sql = ""
    try:
        # Step 1: Generate SQL (use authenticated client for Databricks model serving)
        sql = svc._call_llm(
            system_prompt, context, max_tokens=512, endpoint_name=x_databricks_model, client=client
        )
        sql = sql.strip()
        if sql.startswith("```"):
            sql = sql.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        # Step 2: Execute SQL
        rows = execute_sql(client, wid, sql)

        # Step 3: Generate explanation (skip if too slow — use lighter prompt)
        explanation = f"Query returned {len(rows)} rows."
        try:
            if len(rows) > 0:
                explain_prompt = (
                    "Summarize these SQL results in 1-2 sentences. Be specific with numbers."
                )
                explain_context = f"Question: {request.question}\nResults ({len(rows)} rows): {json.dumps(rows[:5], default=str)}"
                explanation = svc._call_llm(
                    explain_prompt,
                    explain_context,
                    max_tokens=150,
                    endpoint_name=x_databricks_model,
                    client=client,
                )
        except Exception:
            pass

        return {
            "sql": sql,
            "results": rows,
            "row_count": len(rows),
            "explanation": explanation,
            "question": request.question,
        }
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
            raise HTTPException(
                status_code=502, detail=f"Genie API error: {r.status_code} — {r.text[:200]}"
            )

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
                result = {
                    "question": request.question,
                    "conversation_id": conversation_id,
                    "status": "completed",
                }
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
        raise HTTPException(status_code=500, detail=str(e))


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

    history = "\n\n".join(
        [f"{'User' if m.role == 'user' else 'Assistant'}: {m.content}" for m in request.messages]
    )
    if request.catalog:
        history = f"Context: catalog={request.catalog}, schema={request.schema_name}\n\n{history}"

    try:
        response = svc._call_llm(
            system_prompt, history, max_tokens=1024, endpoint_name=x_databricks_model, client=client
        )
        return {"response": response}
    except Exception as e:
        return {"error": str(e), "response": ""}


# ---------------------------------------------------------------------------
# Streaming chat (SSE)
# ---------------------------------------------------------------------------

_AGENTS_DIR = Path(__file__).parent.parent / "ai_agents"


def _parse_agent_frontmatter(path: Path) -> dict:
    """Return the parsed YAML frontmatter of an agent .md file."""
    import yaml  # pyyaml is in requirements.txt

    text = path.read_text()
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    try:
        return yaml.safe_load(parts[1]) or {}
    except yaml.YAMLError:
        return {}


@router.get("/agents")
async def list_agents():
    """Return available agent modes by scanning ai_agents/*.md frontmatter.

    Drop a new .md file into api/ai_agents/ — it appears here automatically.
    Frontmatter fields: name, label, description, subtitle, icon, color, order,
    prompts (list of {label, text} objects).
    """
    agents = []
    for path in _AGENTS_DIR.glob("*.md"):
        meta = _parse_agent_frontmatter(path)
        value = path.stem  # filename without .md — used as the mode key
        agents.append({
            "value":    value,
            "label":    meta.get("label") or value.replace("_", " ").title(),
            "subtitle": meta.get("subtitle", meta.get("description", "")),
            "icon":     meta.get("icon", "Bot"),
            "color":    meta.get("color", "text-muted-foreground"),
            "order":    int(meta.get("order", 99)),
            "prompts":  meta.get("prompts") or [],
        })
    agents.sort(key=lambda a: a["order"])
    return agents


def _load_agent_prompt(mode: str) -> str:
    """Load Markdown persona for the requested agent mode, stripping YAML frontmatter."""
    path = _AGENTS_DIR / f"{mode}.md"
    if path.exists():
        text = path.read_text()
        parts = text.split("---", 2)
        return parts[2].strip() if len(parts) >= 3 else text.strip()
    return "You are a helpful Databricks data assistant."


# ---------------------------------------------------------------------------
# Agentic loop helpers
# ---------------------------------------------------------------------------

async def _call_llm_non_streaming(
    messages: list,
    host: str,
    auth_headers: dict,
    model: str,
    tools: list | None = None,
) -> tuple[dict, dict]:
    """One-shot (non-streaming) LLM call. Returns (choices[0], usage) where usage
    holds prompt/completion/total token counts (or {} if absent).
    Used for tool-decision turns so tool_call JSON arrives complete, not split
    across streaming delta chunks (which requires complex stitching).
    Gracefully retries without tools on 400/422 (model doesn't support tool_calls)."""
    url = f"{host.rstrip('/')}/serving-endpoints/{model}/invocations"
    payload: dict = {
        "messages": messages,
        "stream": False,
        "max_tokens": 4096,
        "temperature": 0.7,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    headers = {**auth_headers, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=120) as c:
        resp = await c.post(url, headers=headers, json=payload)
        if resp.status_code in (400, 422) and tools:
            payload.pop("tools", None)
            payload.pop("tool_choice", None)
            resp = await c.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            body_text = resp.text[:400]
            raise RuntimeError(f"Model endpoint returned {resp.status_code}: {body_text}")
        data = resp.json()
        return data["choices"][0], (data.get("usage") or {})


def _estimate_tokens(messages: list) -> int:
    """Rough token estimate: ~4 chars per token + 4 overhead per message."""
    total = 0
    for m in messages:
        content = m.get("content") or ""
        if isinstance(content, list):
            content = " ".join(str(b.get("text", "")) for b in content if isinstance(b, dict))
        total += len(content) // 4 + 4
    return total


def _prune_history(
    history: list,
    system_prompt: str,
    current_message: str,
    token_budget: int = 6000,
) -> tuple[list, bool]:
    """Trim history to fit within the token budget.
    Always keeps at least the last 4 messages (2 turns).
    Returns (pruned_history, was_pruned)."""
    overhead = len(system_prompt) // 4 + len(current_message) // 4 + 200
    available = max(token_budget - overhead, 500)

    MIN_KEEP = 4
    if len(history) <= MIN_KEEP:
        return history, False

    for keep in range(len(history), MIN_KEEP - 1, -1):
        window = history[-keep:]
        if _estimate_tokens(window) <= available:
            return window, keep < len(history)

    return history[-MIN_KEEP:], True


async def _build_workspace_context(
    client,
    catalog: str | None,
    schema_name: str | None,
    warehouse_id: str,
) -> str:
    """Build a rich workspace context string appended to the system prompt.
    Replaces the old synchronous catalog/schema injection block and additionally
    adds user identity, workspace URL, and column-level table metadata.
    All SDK calls use asyncio.to_thread() to avoid blocking the event loop."""
    import asyncio

    parts: list[str] = []

    # User identity + workspace URL
    try:
        me = await asyncio.to_thread(client.current_user.me)
        user = getattr(me, "user_name", None) or getattr(me, "display_name", None) or "unknown"
        host = (getattr(client.config, "host", None) or "").rstrip("/")
        parts.append(f"## Workspace Context\n- User: {user}\n- Workspace: {host}")
    except Exception:
        pass

    # Active warehouse name
    if warehouse_id:
        try:
            wh = await asyncio.to_thread(client.warehouses.get, warehouse_id)
            parts.append(f"- Active SQL Warehouse: {wh.name} (id={warehouse_id})")
        except Exception:
            parts.append(f"- Active SQL Warehouse id: {warehouse_id}")

    # Schema list
    if catalog:
        try:
            schemas_text = await asyncio.to_thread(ai_tools.dbx_list_schemas, catalog, client)
            parts.append(f"\n## Available schemas in `{catalog}`\n{schemas_text}")
        except Exception:
            pass

    # Table list + column metadata for first 3 tables
    if catalog and schema_name:
        try:
            tables_text = await asyncio.to_thread(ai_tools.dbx_list_tables, catalog, schema_name, client)
            parts.append(f"\n## Tables in `{catalog}`.`{schema_name}`\n{tables_text}")
            # Describe up to 3 tables inline (0 sample rows for speed)
            table_names = [
                line.split("\t")[0].strip()
                for line in tables_text.splitlines()
                if line.strip() and not line.startswith("(") and not line.startswith("ERROR")
            ]
            for tbl in table_names[:3]:
                try:
                    desc = await asyncio.to_thread(
                        ai_tools.dbx_describe_table,
                        catalog, schema_name, tbl, client, warehouse_id, 0,
                    )
                    parts.append(f"\n### Columns: `{catalog}`.`{schema_name}`.`{tbl}`\n{desc}")
                except Exception:
                    pass
        except Exception:
            pass

    return "\n".join(parts)


async def _stream_llm(
    messages: list, host: str, auth_headers: dict, model: str
) -> AsyncGenerator[str, None]:
    """Yield text chunks from a Databricks serving endpoint (OpenAI-compatible SSE)."""
    url = f"{host.rstrip('/')}/serving-endpoints/{model}/invocations"
    payload = {
        "messages": messages,
        "stream": True,
        "max_tokens": 2048,
        "temperature": 0.7,
    }
    headers = {**auth_headers, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=120) as client:
        async with client.stream("POST", url, headers=headers, json=payload) as resp:
            if resp.status_code != 200:
                body_bytes = await resp.aread()
                raise RuntimeError(
                    f"Model endpoint returned {resp.status_code}: {body_bytes.decode()[:400]}"
                )
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data = line[6:]
                if data.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                    # Support both streaming (delta) and non-streaming (message) shapes
                    choice = chunk["choices"][0]
                    delta = choice.get("delta") or choice.get("message") or {}
                    text = delta.get("content") or ""
                    if text:
                        yield text
                except (KeyError, IndexError, json.JSONDecodeError):
                    continue


class ChatStreamRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    mode: str = "assistant"
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    regenerate: bool = False


@router.post("/stream")
async def chat_stream(
    body: ChatStreamRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    x_databricks_warehouse: Optional[str] = Header(None, alias="X-Databricks-Warehouse"),
    client=Depends(get_db_client),
):
    """Agentic SSE chat endpoint with OpenAI tool-calling loop.
    The model can call describe_table, run_sql, list_tables, list_schemas,
    list_catalogs, and get_workspace_info before producing a final answer."""
    import asyncio

    if not x_databricks_model:
        raise HTTPException(
            status_code=400,
            detail="No AI model configured. Go to Settings and select a serving endpoint.",
        )

    host = (client.config.host or "").rstrip("/")
    auth_headers = _get_auth_headers(client)
    if "Authorization" not in auth_headers:
        token = getattr(client.config, "token", None)
        if token:
            auth_headers["Authorization"] = f"Bearer {token}"

    sid     = body.session_id or f"chat-{uuid.uuid4()}"
    record  = ai_sessions.load(sid)
    history = record["messages"] if record else []

    # On regenerate, drop the previous answer turn so the model doesn't see its
    # own prior response (body.message re-sends the same user prompt).
    if body.regenerate and history:
        if history and history[-1].get("role") == "assistant":
            history = history[:-1]
        if history and history[-1].get("role") == "user":
            history = history[:-1]

    warehouse_id = x_databricks_warehouse or ""

    # Build system prompt with rich workspace context (async, non-blocking)
    system_prompt = _load_agent_prompt(body.mode)
    workspace_ctx = await _build_workspace_context(
        client, body.catalog, body.schema_name, warehouse_id
    )
    if workspace_ctx:
        system_prompt += f"\n\n{workspace_ctx}"

    # Behavioural rules applied to every agent.
    system_prompt += (
        "\n\n---\n"
        "**Working rules — apply to every response without exception:**\n\n"
        "1. When a `run_sql`, `explain_query`, `profile_column`, or `describe_table` tool "
        "returns a result starting with `ERROR:`, do NOT surface the raw error. Read the "
        "error, fix the SQL (wrong column, missing backticks, bad function), and retry the "
        "tool — up to 2 times. Only if it still fails, explain the problem plainly.\n\n"
        "2. Before running `run_sql` that returns raw rows from a table you haven't seen, "
        "consider whether it may expose sensitive data. If the question touches a table that "
        "likely holds personal data, call `list_pii_columns(catalog)` first and warn the user "
        "(one short line) if PII-tagged columns are present, then proceed with aggregated or "
        "masked output where possible.\n\n"
        "3. After your answer, append a fenced code block with language `next-steps` "
        "containing exactly 2–3 short follow-up actions or questions the user could ask next "
        "(one per line, ≤ 12 words each, no bullets or numbering). These render as clickable "
        "buttons — keep them specific and actionable.\n"
        "Example:\n"
        "```next-steps\n"
        "Describe the orders table to see its column structure\n"
        "Write a query to count rows grouped by status\n"
        "Check for null values in the customer_id column\n"
        "```\n"
        "Never omit the next-steps block."
    )

    pruned_history, was_pruned = _prune_history(history, system_prompt, body.message)
    working_messages = (
        [{"role": "system", "content": system_prompt}]
        + pruned_history
        + [{"role": "user", "content": body.message}]
    )

    async def event_stream():
        full_response = ""
        yield f"data: {json.dumps({'type': 'session_id', 'session_id': sid})}\n\n"
        if was_pruned:
            yield f"data: {json.dumps({'type': 'context_pruned'})}\n\n"

        loop_messages = list(working_messages)
        MAX_TOOL_TURNS = 5
        total_tokens = 0
        tool_count = 0

        try:
            for turn in range(MAX_TOOL_TURNS + 1):
                use_tools = turn < MAX_TOOL_TURNS
                choice, usage = await _call_llm_non_streaming(
                    loop_messages, host, auth_headers, x_databricks_model,
                    tools=ai_tool_definitions.TOOLS if use_tools else None,
                )
                total_tokens += int(usage.get("total_tokens") or 0)
                message = choice.get("message", {})
                finish_reason = choice.get("finish_reason", "")
                tool_calls = message.get("tool_calls") or []

                # Model wants to call one or more tools
                if tool_calls or finish_reason == "tool_calls":
                    loop_messages.append({
                        "role": "assistant",
                        "content": message.get("content") or "",
                        "tool_calls": tool_calls,
                    })

                    for tc in tool_calls:
                        fn      = tc.get("function", {})
                        name    = fn.get("name", "")
                        call_id = tc.get("id") or f"call_{name}"
                        try:
                            args = json.loads(fn.get("arguments") or "{}")
                        except json.JSONDecodeError:
                            args = {}

                        yield f"data: {json.dumps({'type': 'tool_start', 'tool': name, 'args': args, 'call_id': call_id})}\n\n"
                        tool_count += 1

                        result = await asyncio.to_thread(
                            ai_tool_definitions.execute_tool,
                            name, args, client, warehouse_id,
                        )

                        yield f"data: {json.dumps({'type': 'tool_done', 'tool': name, 'call_id': call_id, 'result_preview': result[:1500]})}\n\n"

                        loop_messages.append({
                            "role": "tool",
                            "tool_call_id": call_id,
                            "content": result,
                        })
                    continue  # next turn — model now synthesises the answer

                # Model produced a final text answer — stream in chunks for typewriter feel
                text_content = message.get("content") or ""
                CHUNK = 80
                for i in range(0, len(text_content), CHUNK):
                    chunk = text_content[i : i + CHUNK]
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'text', 'delta': chunk})}\n\n"
                break

            if not full_response:
                yield f"data: {json.dumps({'type': 'error', 'message': 'The model returned an empty response. Check that the serving endpoint is healthy and supports chat format.'})}\n\n"
            else:
                # Persist only user+assistant text turns (strip tool scaffolding)
                clean_history = [
                    m for m in loop_messages
                    if m.get("role") in ("user", "assistant") and not m.get("tool_calls")
                ]
                clean_history.append({"role": "assistant", "content": full_response})
                ai_sessions.save(sid, clean_history, body.message)

            if total_tokens or tool_count:
                yield f"data: {json.dumps({'type': 'usage', 'total_tokens': total_tokens, 'tool_count': tool_count})}\n\n"

        except Exception as exc:
            logger.exception("Agentic loop failed")
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


@router.get("/sessions")
async def list_sessions():
    """List all saved chat sessions."""
    return ai_sessions.list_all()


@router.get("/sessions/{sid}")
async def get_session(sid: str):
    """Load a saved session by ID."""
    record = ai_sessions.load(sid)
    if not record:
        raise HTTPException(status_code=404, detail="Session not found")
    return record


@router.delete("/sessions/{sid}")
async def delete_session(sid: str):
    """Delete a saved session."""
    deleted = ai_sessions.delete(sid)
    return {"deleted": deleted}


class RenameRequest(BaseModel):
    title: str


@router.post("/sessions/{sid}/rename")
async def rename_session(sid: str, body: RenameRequest):
    """Rename a saved session."""
    ok = ai_sessions.rename(sid, body.title)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"ok": True}


class PinRequest(BaseModel):
    pinned: bool


@router.post("/sessions/{sid}/pin")
async def pin_session(sid: str, body: PinRequest):
    """Pin or unpin a saved session."""
    ok = ai_sessions.set_pinned(sid, body.pinned)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Databricks context endpoint (UC metadata for the chat UI)
# ---------------------------------------------------------------------------


@router.get("/context/databricks")
async def get_databricks_context(
    catalog: Optional[str] = Query(None),
    client=Depends(get_db_client),
):
    """Return catalogs (and optionally schemas) for UC context injection."""
    catalogs_text = ai_tools.dbx_list_catalogs(client)
    schemas_text  = ai_tools.dbx_list_schemas(catalog, client) if catalog else ""
    return {
        "catalogs": [c for c in catalogs_text.splitlines() if c and not c.startswith("ERROR")],
        "schemas":  [s for s in schemas_text.splitlines()  if s and not s.startswith("ERROR")],
    }
