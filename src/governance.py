"""Governance module — Data Dictionary, Certifications, Change History.

Stores governance metadata in Delta tables within the configured audit catalog.
"""

import json
import logging
import uuid
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def _get_governance_schema(config: dict) -> str:
    """Get the governance schema FQN from config."""
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.governance"


def ensure_governance_tables(client, warehouse_id, config):
    """Create governance Delta tables if they don't exist."""
    schema = _get_governance_schema(config)
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception as e:
        logger.warning(f"Could not create governance schema: {e}")
        return

    tables = {
        "business_glossary": """
            term_id STRING, name STRING, abbreviation STRING, definition STRING,
            domain STRING, owner STRING, tags STRING, status STRING,
            created_by STRING, created_at TIMESTAMP, updated_at TIMESTAMP
        """,
        "glossary_links": """
            link_id STRING, term_id STRING, column_fqn STRING,
            created_by STRING, created_at TIMESTAMP
        """,
        "certifications": """
            cert_id STRING, table_fqn STRING, status STRING,
            certified_by STRING, certified_at TIMESTAMP,
            expiry_date DATE, notes STRING, review_frequency STRING,
            created_at TIMESTAMP, updated_at TIMESTAMP
        """,
        "change_history": """
            change_id STRING, entity_type STRING, entity_id STRING,
            change_type STRING, changed_by STRING, changed_at TIMESTAMP,
            details STRING
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs Governance: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# Business Glossary
# ---------------------------------------------------------------------------

def create_glossary_term(client, warehouse_id, config, term: dict, user: str = "") -> dict:
    """Create a new business glossary term."""
    schema = _get_governance_schema(config)
    term_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    tags_json = json.dumps(term.get("tags", []))

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.business_glossary
        VALUES ('{term_id}', '{_esc(term["name"])}', '{_esc(term.get("abbreviation", ""))}',
                '{_esc(term["definition"])}', '{_esc(term.get("domain", "General"))}',
                '{_esc(term.get("owner", ""))}', '{_esc(tags_json)}', '{term.get("status", "draft")}',
                '{_esc(user)}', '{now}', '{now}')
    """)

    _track_change(client, warehouse_id, config, "glossary", term_id, "created", {"name": term["name"]}, user)
    return {"term_id": term_id, "name": term["name"], "status": "created"}


def list_glossary_terms(client, warehouse_id, config) -> list[dict]:
    """List all business glossary terms."""
    schema = _get_governance_schema(config)
    try:
        rows = execute_sql(client, warehouse_id, f"SELECT * FROM {schema}.business_glossary ORDER BY name")
        return [_parse_glossary_row(r) for r in rows]
    except Exception:
        return []


def get_glossary_term(client, warehouse_id, config, term_id: str) -> dict | None:
    """Get a single glossary term with its linked columns."""
    schema = _get_governance_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.business_glossary WHERE term_id = '{_esc(term_id)}'")
        if not rows:
            return None
        term = _parse_glossary_row(rows[0])
        # Get linked columns
        links = execute_sql(client, warehouse_id,
            f"SELECT column_fqn FROM {schema}.glossary_links WHERE term_id = '{_esc(term_id)}'")
        term["linked_columns"] = [l["column_fqn"] for l in links]
        return term
    except Exception:
        return None


def delete_glossary_term(client, warehouse_id, config, term_id: str, user: str = ""):
    """Delete a glossary term and its links."""
    schema = _get_governance_schema(config)
    execute_sql(client, warehouse_id, f"DELETE FROM {schema}.glossary_links WHERE term_id = '{_esc(term_id)}'")
    execute_sql(client, warehouse_id, f"DELETE FROM {schema}.business_glossary WHERE term_id = '{_esc(term_id)}'")
    _track_change(client, warehouse_id, config, "glossary", term_id, "deleted", {}, user)


def link_term_to_columns(client, warehouse_id, config, term_id: str, column_fqns: list[str], user: str = ""):
    """Link a glossary term to one or more columns."""
    schema = _get_governance_schema(config)
    for fqn in column_fqns:
        link_id = str(uuid.uuid4())[:8]
        now = datetime.utcnow().isoformat()
        try:
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.glossary_links
                VALUES ('{link_id}', '{_esc(term_id)}', '{_esc(fqn)}', '{_esc(user)}', '{now}')
            """)
        except Exception:
            pass  # May already exist
    _track_change(client, warehouse_id, config, "glossary", term_id, "updated",
                  {"linked_columns": column_fqns}, user)


# ---------------------------------------------------------------------------
# Certifications
# ---------------------------------------------------------------------------

def certify_table(client, warehouse_id, config, cert: dict, user: str = "") -> dict:
    """Create or update a table certification."""
    schema = _get_governance_schema(config)
    cert_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    expiry = cert.get("expiry_date") or "NULL"
    expiry_clause = f"'{expiry}'" if expiry != "NULL" else "NULL"

    # Upsert: delete existing cert for this table, then insert
    execute_sql(client, warehouse_id,
        f"DELETE FROM {schema}.certifications WHERE table_fqn = '{_esc(cert['table_fqn'])}'")

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.certifications
        VALUES ('{cert_id}', '{_esc(cert["table_fqn"])}', '{cert["status"]}',
                '{_esc(user)}', '{now}', {expiry_clause},
                '{_esc(cert.get("notes", ""))}', '{cert.get("review_frequency", "quarterly")}',
                '{now}', '{now}')
    """)

    _track_change(client, warehouse_id, config, "certification", cert_id, "created",
                  {"table": cert["table_fqn"], "status": cert["status"]}, user)
    return {"cert_id": cert_id, "table_fqn": cert["table_fqn"], "status": cert["status"]}


def list_certifications(client, warehouse_id, config) -> list[dict]:
    """List all table certifications."""
    schema = _get_governance_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.certifications ORDER BY table_fqn")
        return [{k: str(v) if v is not None else "" for k, v in r.items()} for r in rows]
    except Exception:
        return []


def approve_certification(client, warehouse_id, config, cert_id: str, action: str, reviewer_notes: str = "", user: str = ""):
    """Approve or reject a certification."""
    schema = _get_governance_schema(config)
    new_status = "certified" if action == "approve" else "deprecated"
    now = datetime.utcnow().isoformat()
    execute_sql(client, warehouse_id, f"""
        UPDATE {schema}.certifications
        SET status = '{new_status}', notes = concat(notes, ' | {action}: {_esc(reviewer_notes)}'),
            updated_at = '{now}'
        WHERE cert_id = '{_esc(cert_id)}'
    """)
    _track_change(client, warehouse_id, config, "certification", cert_id, action,
                  {"reviewer_notes": reviewer_notes}, user)


# ---------------------------------------------------------------------------
# Global Metadata Search
# ---------------------------------------------------------------------------

def search_metadata(client, warehouse_id, config, query: str, catalogs: list[str] = None, search_type: str = "all", limit: int = 50) -> dict:
    """Search across catalogs, schemas, tables, columns, glossary terms, and tags."""
    results = {"tables": [], "columns": [], "terms": [], "tags": []}
    schema = _get_governance_schema(config)
    safe_query = _esc(query)

    # Get catalogs to search
    if not catalogs:
        try:
            cat_rows = execute_sql(client, warehouse_id, "SHOW CATALOGS")
            catalogs = [r.get("catalog", r.get("databaseName", "")) for r in cat_rows if r]
            catalogs = [c for c in catalogs if c and c not in ("system", "hive_metastore", "__databricks_internal")]
        except Exception:
            catalogs = []

    # Search tables
    if search_type in ("all", "tables"):
        for cat in catalogs[:5]:  # Limit to 5 catalogs
            try:
                rows = execute_sql(client, warehouse_id, f"""
                    SELECT table_catalog, table_schema, table_name, table_type, comment
                    FROM {cat}.information_schema.tables
                    WHERE (lower(table_name) LIKE '%{safe_query.lower()}%'
                           OR lower(comment) LIKE '%{safe_query.lower()}%')
                    AND table_schema != 'information_schema'
                    LIMIT {limit}
                """)
                results["tables"].extend([{
                    "catalog": r.get("table_catalog", cat),
                    "schema": r["table_schema"],
                    "table": r["table_name"],
                    "type": r.get("table_type", ""),
                    "comment": r.get("comment", ""),
                    "fqn": f"{r.get('table_catalog', cat)}.{r['table_schema']}.{r['table_name']}",
                } for r in rows])
            except Exception:
                pass

    # Search columns
    if search_type in ("all", "columns"):
        for cat in catalogs[:5]:
            try:
                rows = execute_sql(client, warehouse_id, f"""
                    SELECT table_catalog, table_schema, table_name, column_name, data_type, comment
                    FROM {cat}.information_schema.columns
                    WHERE (lower(column_name) LIKE '%{safe_query.lower()}%'
                           OR lower(comment) LIKE '%{safe_query.lower()}%')
                    AND table_schema != 'information_schema'
                    LIMIT {limit}
                """)
                results["columns"].extend([{
                    "catalog": r.get("table_catalog", cat),
                    "schema": r["table_schema"],
                    "table": r["table_name"],
                    "column": r["column_name"],
                    "type": r.get("data_type", ""),
                    "comment": r.get("comment", ""),
                    "fqn": f"{r.get('table_catalog', cat)}.{r['table_schema']}.{r['table_name']}.{r['column_name']}",
                } for r in rows])
            except Exception:
                pass

    # Search glossary terms
    if search_type in ("all", "terms"):
        try:
            rows = execute_sql(client, warehouse_id, f"""
                SELECT * FROM {schema}.business_glossary
                WHERE lower(name) LIKE '%{safe_query.lower()}%'
                   OR lower(definition) LIKE '%{safe_query.lower()}%'
                   OR lower(abbreviation) LIKE '%{safe_query.lower()}%'
                LIMIT {limit}
            """)
            results["terms"] = [_parse_glossary_row(r) for r in rows]
        except Exception:
            pass

    results["total"] = sum(len(v) for v in results.values())
    return results


# ---------------------------------------------------------------------------
# Change History
# ---------------------------------------------------------------------------

def _track_change(client, warehouse_id, config, entity_type, entity_id, change_type, details, user=""):
    """Record a metadata change in the change history table."""
    schema = _get_governance_schema(config)
    change_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    details_json = json.dumps(details) if details else "{}"
    try:
        execute_sql(client, warehouse_id, f"""
            INSERT INTO {schema}.change_history
            VALUES ('{change_id}', '{entity_type}', '{_esc(entity_id)}',
                    '{change_type}', '{_esc(user)}', '{now}', '{_esc(details_json)}')
        """)
    except Exception as e:
        logger.warning(f"Could not track change: {e}")


def get_change_history(client, warehouse_id, config, entity_type: str = "", limit: int = 100) -> list[dict]:
    """Get metadata change history."""
    schema = _get_governance_schema(config)
    where = f"WHERE entity_type = '{_esc(entity_type)}'" if entity_type else ""
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.change_history {where} ORDER BY changed_at DESC LIMIT {limit}")
        return [{k: str(v) if v is not None else "" for k, v in r.items()} for r in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _esc(s: str) -> str:
    """Escape single quotes for SQL."""
    if not s:
        return ""
    return str(s).replace("'", "\\'").replace("\\", "\\\\")


def _parse_glossary_row(r: dict) -> dict:
    """Parse a glossary row into a clean dict."""
    tags = []
    try:
        tags = json.loads(r.get("tags", "[]"))
    except Exception:
        pass
    return {
        "term_id": r.get("term_id", ""),
        "name": r.get("name", ""),
        "abbreviation": r.get("abbreviation", ""),
        "definition": r.get("definition", ""),
        "domain": r.get("domain", ""),
        "owner": r.get("owner", ""),
        "tags": tags,
        "status": r.get("status", ""),
        "created_by": r.get("created_by", ""),
        "created_at": str(r.get("created_at", "")),
        "updated_at": str(r.get("updated_at", "")),
    }
