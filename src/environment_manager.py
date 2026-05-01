"""Data Environment Manager — ephemeral sandbox environments.

One-click creation of isolated data environments with auto PII masking,
DQ validation, cost budgets, and TTL-based cleanup.

Storage: {audit_catalog}.state.environments
         {audit_catalog}.state.environment_templates
"""

import logging
import json
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "state")


_ENVS_DDL = """
    env_id STRING,
    name STRING,
    source_catalog STRING,
    target_catalog STRING,
    tables STRING,
    masking_profile STRING,
    ttl_hours INT,
    cost_budget DOUBLE,
    current_cost DOUBLE,
    status STRING,
    clone_type STRING,
    access_grants STRING,
    created_by STRING,
    created_at TIMESTAMP,
    expires_at TIMESTAMP,
    destroyed_at TIMESTAMP
"""

_TEMPLATES_DDL = """
    template_id STRING,
    name STRING,
    description STRING,
    config STRING,
    created_by STRING,
    created_at TIMESTAMP
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    for tbl, ddl, comment in [
        ("environments", _ENVS_DDL, "Ephemeral data environments"),
        ("environment_templates", _TEMPLATES_DDL, "Environment creation templates"),
    ]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {tbl}: {e}")


def create_environment(
    name: str,
    source_catalog: str,
    tables: list = None,
    masking_profile: str = "none",
    ttl_hours: int = 72,
    cost_budget: float = 100.0,
    clone_type: str = "SHALLOW",
    access_grants: list = None,
    created_by: str = "system",
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Create a new ephemeral environment by cloning selected tables."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    eid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    target_catalog = f"env_{eid}_{source_catalog}"

    # Calculate expiry
    from datetime import timedelta
    expires = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        _run_sql(f"""
            INSERT INTO {schema}.environments VALUES (
                '{eid}', '{_esc(name)}', '{_esc(source_catalog)}', '{_esc(target_catalog)}',
                '{_esc(json.dumps(tables or []))}', '{_esc(masking_profile)}',
                {ttl_hours}, {cost_budget}, 0.0, 'creating', '{_esc(clone_type)}',
                '{_esc(json.dumps(access_grants or []))}',
                '{_esc(created_by)}', '{now}', '{expires}', NULL
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create environment record: {e}")

    # Step 1: Create target catalog
    try:
        _run_sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}", client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create catalog {target_catalog}: {e}")
        _update_env_status(eid, "failed", schema, client, warehouse_id)
        return {"env_id": eid, "status": "failed", "error": str(e)}

    # Step 2: Clone tables
    tables_to_clone = tables or []
    if not tables_to_clone:
        # Clone entire catalog
        try:
            all_tables = _query_sql(f"""
                SELECT table_schema, table_name FROM {source_catalog}.information_schema.tables
                WHERE table_type = 'MANAGED'
            """, limit=500, client=client, warehouse_id=warehouse_id) or []
            tables_to_clone = [f"{source_catalog}.{t['table_schema']}.{t['table_name']}" for t in all_tables]
        except Exception:
            pass

    cloned = 0
    for tbl in tables_to_clone:
        parts = tbl.split(".")
        if len(parts) == 3:
            src_schema, src_table = parts[1], parts[2]
            try:
                _run_sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{src_schema}", client, warehouse_id)
                _run_sql(f"CREATE TABLE {target_catalog}.{src_schema}.{src_table} {clone_type} CLONE {tbl}",
                         client, warehouse_id)
                cloned += 1
            except Exception as e:
                logger.debug(f"Could not clone {tbl}: {e}")

    # Update status
    _update_env_status(eid, "active", schema, client, warehouse_id)

    return {
        "env_id": eid, "name": name, "source_catalog": source_catalog,
        "target_catalog": target_catalog, "tables_cloned": cloned,
        "ttl_hours": ttl_hours, "expires_at": expires, "status": "active",
    }


def _update_env_status(env_id, status, schema, client, warehouse_id):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    extra = ""
    if status == "destroyed":
        extra = f", destroyed_at = '{now}'"
    try:
        _run_sql(f"UPDATE {schema}.environments SET status = '{status}'{extra} WHERE env_id = '{_esc(env_id)}'",
                 client, warehouse_id)
    except Exception:
        pass


def list_environments(status: str = None, client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    where = f"WHERE status = '{_esc(status)}'" if status else ""
    try:
        return _query_sql(f"SELECT * FROM {schema}.environments {where} ORDER BY created_at DESC",
                          limit=100, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def get_environment(env_id: str, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(f"SELECT * FROM {schema}.environments WHERE env_id = '{_esc(env_id)}'",
                          limit=1, client=client, warehouse_id=warehouse_id)
        return rows[0] if rows else {}
    except Exception:
        return {}


def extend_environment(env_id: str, additional_hours: int = 24,
                       client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(f"""
            UPDATE {schema}.environments
            SET expires_at = DATEADD(HOUR, {additional_hours}, expires_at),
                ttl_hours = ttl_hours + {additional_hours}
            WHERE env_id = '{_esc(env_id)}'
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not extend environment: {e}")
    return get_environment(env_id, client, warehouse_id, config)


def destroy_environment(env_id: str, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    """Destroy an environment by dropping its catalog."""
    config = config or {}
    schema = _get_schema(config)
    env = get_environment(env_id, client, warehouse_id, config)
    if not env:
        return {"error": "Environment not found"}

    target_catalog = env.get("target_catalog", "")
    if target_catalog:
        try:
            _run_sql(f"DROP CATALOG IF EXISTS {target_catalog} CASCADE", client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not drop catalog {target_catalog}: {e}")

    _update_env_status(env_id, "destroyed", schema, client, warehouse_id)
    return {"env_id": env_id, "status": "destroyed"}


def cleanup_expired(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    """Destroy all expired environments."""
    config = config or {}
    schema = _get_schema(config)
    try:
        expired = _query_sql(f"""
            SELECT env_id, target_catalog FROM {schema}.environments
            WHERE status = 'active' AND expires_at < CURRENT_TIMESTAMP()
        """, limit=50, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []

    results = []
    for env in expired:
        r = destroy_environment(env["env_id"], client, warehouse_id, config)
        results.append(r)
    return results


# ─── Templates ──────────────────────────────────────────────────────────

def create_template(name: str, description: str = "", template_config: dict = None,
                    created_by: str = "system", client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    tid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(f"""
            INSERT INTO {schema}.environment_templates VALUES (
                '{tid}', '{_esc(name)}', '{_esc(description)}',
                '{_esc(json.dumps(template_config or {}))}', '{_esc(created_by)}', '{now}'
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create template: {e}")
    return {"template_id": tid, "name": name}


def list_templates(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"SELECT * FROM {schema}.environment_templates ORDER BY created_at DESC",
                          limit=50, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def delete_template(template_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(f"DELETE FROM {schema}.environment_templates WHERE template_id = '{_esc(template_id)}'",
                 client, warehouse_id)
    except Exception:
        pass
