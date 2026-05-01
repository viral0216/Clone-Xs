"""Natural Language DQ Rule Builder.

Translates plain-English rule descriptions into executable DQ rule configs
using the Databricks Foundation Model API.

Storage: {audit_catalog}.governance.nl_rule_audit
"""

import logging
import json
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)

_AUDIT_DDL = """
    id STRING,
    input_text STRING,
    parsed_rule STRING,
    confidence DOUBLE,
    accepted BOOLEAN,
    modified BOOLEAN,
    created_by STRING,
    created_at TIMESTAMP
"""


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    try:
        _run_sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}.nl_rule_audit ({_AUDIT_DDL})
            USING DELTA COMMENT 'Clone-Xs: NL rule builder audit trail'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create nl_rule_audit: {e}")


def _get_table_schema(table_fqn: str, client, warehouse_id) -> list[dict]:
    """Fetch column info for a table."""
    try:
        return _query_sql(f"DESCRIBE TABLE {table_fqn}", limit=200, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def _build_prompt(nl_text: str, table_fqn: str, columns: list[dict]) -> str:
    col_info = "\n".join(f"  - {c.get('col_name', c.get('name', ''))}: {c.get('data_type', c.get('type', ''))}"
                         for c in columns[:30])

    return f"""You are a data quality rule generator. Convert the natural language description into a JSON rule config.

Available rule types: not_null, unique, range, regex, freshness, row_count, referential, custom_sql

Table: {table_fqn}
Columns:
{col_info}

User request: "{nl_text}"

Respond with ONLY valid JSON:
{{
  "name": "descriptive rule name",
  "table_fqn": "{table_fqn}",
  "column_name": "column or empty string",
  "rule_type": "one of the available types",
  "expression": "SQL expression if custom_sql, regex pattern if regex, else empty",
  "params": {{}},
  "threshold": 0.0,
  "severity": "warning or critical",
  "confidence": 0.0 to 1.0
}}

For range type, include min/max in params. For row_count, include expected_min/expected_max.
Set confidence based on how well you understood the request (1.0 = perfect match)."""


def parse_nl_rule(
    nl_text: str,
    table_fqn: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    created_by: str = "user",
) -> dict:
    """Parse a natural language rule description into a DQ rule config."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)

    columns = _get_table_schema(table_fqn, client, warehouse_id)
    prompt = _build_prompt(nl_text, table_fqn, columns)

    # Try Databricks Foundation Model API
    parsed_rule = None
    try:
        from databricks.sdk import WorkspaceClient
        w = client if client else WorkspaceClient()
        model = config.get("ai_model", "databricks-meta-llama-3-1-70b-instruct")

        response = w.serving_endpoints.query(
            name=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.1,
        )
        content = response.choices[0].message.content.strip()
        # Extract JSON from response
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        parsed_rule = json.loads(content)
    except Exception as e:
        logger.warning(f"AI rule parsing failed: {e}")
        # Fallback: simple keyword-based parsing
        parsed_rule = _fallback_parse(nl_text, table_fqn, columns)

    if not parsed_rule:
        parsed_rule = _fallback_parse(nl_text, table_fqn, columns)

    # Store audit record
    schema = _get_schema(config)
    rid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(f"""
            INSERT INTO {schema}.nl_rule_audit VALUES (
                '{rid}', '{_esc(nl_text)}', '{_esc(json.dumps(parsed_rule))}',
                {parsed_rule.get('confidence', 0.5)}, false, false, '{_esc(created_by)}', '{now}'
            )
        """, client, warehouse_id)
    except Exception:
        pass

    return parsed_rule


def _fallback_parse(nl_text: str, table_fqn: str, columns: list[dict]) -> dict:
    """Simple keyword-based rule parsing fallback."""
    text = nl_text.lower()
    col_names = [c.get("col_name", c.get("name", "")).lower() for c in columns]

    # Detect column reference
    found_col = ""
    for cn in col_names:
        if cn and cn in text:
            found_col = cn
            break

    rule = {
        "name": f"Rule from: {nl_text[:60]}",
        "table_fqn": table_fqn,
        "column_name": found_col,
        "rule_type": "custom_sql",
        "expression": "",
        "params": {},
        "threshold": 0.0,
        "severity": "warning",
        "confidence": 0.3,
    }

    if any(w in text for w in ["not null", "never null", "required", "mandatory"]):
        rule["rule_type"] = "not_null"
        rule["confidence"] = 0.8
    elif any(w in text for w in ["unique", "distinct", "no duplicates"]):
        rule["rule_type"] = "unique"
        rule["confidence"] = 0.8
    elif any(w in text for w in ["positive", "greater than", "between", "less than", "range"]):
        rule["rule_type"] = "range"
        rule["confidence"] = 0.6
        # Try to extract numbers
        import re
        nums = re.findall(r'[\d.]+', text)
        if len(nums) >= 2:
            rule["params"] = {"min": float(nums[0]), "max": float(nums[1])}
        elif "positive" in text:
            rule["params"] = {"min": 0}
    elif any(w in text for w in ["fresh", "updated", "stale"]):
        rule["rule_type"] = "freshness"
        rule["confidence"] = 0.7
    elif any(w in text for w in ["row count", "at least", "rows"]):
        rule["rule_type"] = "row_count"
        rule["confidence"] = 0.6
    elif any(w in text for w in ["pattern", "format", "match", "email", "phone"]):
        rule["rule_type"] = "regex"
        rule["confidence"] = 0.5

    return rule


def batch_parse(
    rules_text: list[str],
    table_fqn: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Parse multiple NL rule descriptions."""
    return [parse_nl_rule(text, table_fqn, client, warehouse_id, config) for text in rules_text]


def explain_rule(
    rule: dict,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> str:
    """Generate a plain-English explanation of an existing rule."""
    rt = rule.get("rule_type", "")
    col = rule.get("column_name", "")
    tbl = rule.get("table_fqn", "")
    expr = rule.get("expression", "")
    params = rule.get("params", {})
    threshold = rule.get("threshold", 0)

    if rt == "not_null":
        return f"Column '{col}' in {tbl} must never contain NULL values. Failure threshold: {threshold * 100}%."
    elif rt == "unique":
        return f"Column '{col}' in {tbl} must have all unique values with no duplicates."
    elif rt == "range":
        return f"Column '{col}' in {tbl} must have values between {params.get('min', '?')} and {params.get('max', '?')}."
    elif rt == "regex":
        return f"Column '{col}' in {tbl} must match the pattern: {expr}."
    elif rt == "freshness":
        return f"Table {tbl} must be updated within the configured freshness threshold."
    elif rt == "row_count":
        return f"Table {tbl} must have a row count within expected bounds."
    elif rt == "referential":
        return f"Column '{col}' in {tbl} must reference valid values in the referenced table."
    elif rt == "custom_sql":
        return f"Custom SQL check on {tbl}: {expr}"
    return f"Rule '{rule.get('name', '')}' of type '{rt}' on {tbl}."
