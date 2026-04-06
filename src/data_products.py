"""Data Product Catalog & Marketplace.

Internal marketplace for publishing and subscribing to curated data products.
Each product bundles tables with docs, quality guarantees, and SLAs.

Storage: {audit_catalog}.governance.data_products
         {audit_catalog}.governance.data_product_subscriptions
"""

import logging
import json
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


_PRODUCTS_DDL = """
    product_id STRING,
    name STRING,
    description STRING,
    domain STRING,
    owner_team STRING,
    owner_email STRING,
    tables STRING,
    sla_guarantees STRING,
    quality_requirements STRING,
    tags STRING,
    status STRING,
    version STRING,
    created_by STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    published_at TIMESTAMP
"""

_SUBSCRIPTIONS_DDL = """
    subscription_id STRING,
    product_id STRING,
    subscriber_team STRING,
    subscriber_email STRING,
    notification_prefs STRING,
    use_case STRING,
    status STRING,
    subscribed_at TIMESTAMP
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
        ("data_products", _PRODUCTS_DDL, "Data product catalog"),
        ("data_product_subscriptions", _SUBSCRIPTIONS_DDL, "Data product subscriptions"),
    ]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {tbl}: {e}")


def create_product(
    name: str, description: str = "", domain: str = "", owner_team: str = "",
    owner_email: str = "", tables: list = None, sla_guarantees: dict = None,
    quality_requirements: dict = None, tags: list = None, created_by: str = "system",
    client=None, warehouse_id: str = "", config: dict = None,
) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    pid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        _run_sql(f"""
            INSERT INTO {schema}.data_products VALUES (
                '{pid}', '{_esc(name)}', '{_esc(description)}', '{_esc(domain)}',
                '{_esc(owner_team)}', '{_esc(owner_email)}',
                '{_esc(json.dumps(tables or []))}', '{_esc(json.dumps(sla_guarantees or {}))}',
                '{_esc(json.dumps(quality_requirements or {}))}', '{_esc(json.dumps(tags or []))}',
                'draft', '1.0', '{_esc(created_by)}', '{now}', '{now}', NULL
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create data product: {e}")

    return {"product_id": pid, "name": name, "status": "draft", "created_at": now}


def list_products(status: str = None, domain: str = None,
                  client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    where_parts = []
    if status:
        where_parts.append(f"status = '{_esc(status)}'")
    if domain:
        where_parts.append(f"domain = '{_esc(domain)}'")
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    try:
        return _query_sql(f"SELECT * FROM {schema}.data_products {where} ORDER BY updated_at DESC",
                          limit=200, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def get_product(product_id: str, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(f"SELECT * FROM {schema}.data_products WHERE product_id = '{_esc(product_id)}'",
                          limit=1, client=client, warehouse_id=warehouse_id)
        return rows[0] if rows else {}
    except Exception:
        return {}


def update_product(product_id: str, updates: dict, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    sets = [f"updated_at = '{now}'"]
    for k, v in updates.items():
        if k in ("name", "description", "domain", "owner_team", "owner_email", "status", "version"):
            sets.append(f"{k} = '{_esc(str(v))}'")
        elif k in ("tables", "sla_guarantees", "quality_requirements", "tags"):
            sets.append(f"{k} = '{_esc(json.dumps(v))}'")
    try:
        _run_sql(f"UPDATE {schema}.data_products SET {', '.join(sets)} WHERE product_id = '{_esc(product_id)}'",
                 client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not update product: {e}")
    return get_product(product_id, client, warehouse_id, config)


def publish_product(product_id: str, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(f"""
            UPDATE {schema}.data_products
            SET status = 'published', published_at = '{now}', updated_at = '{now}'
            WHERE product_id = '{_esc(product_id)}'
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not publish: {e}")
    return get_product(product_id, client, warehouse_id, config)


def delete_product(product_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(f"DELETE FROM {schema}.data_products WHERE product_id = '{_esc(product_id)}'", client, warehouse_id)
        _run_sql(f"DELETE FROM {schema}.data_product_subscriptions WHERE product_id = '{_esc(product_id)}'", client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not delete product: {e}")


def subscribe(product_id: str, subscriber_team: str, subscriber_email: str,
              use_case: str = "", notification_prefs: dict = None,
              client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    sid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(f"""
            INSERT INTO {schema}.data_product_subscriptions VALUES (
                '{sid}', '{_esc(product_id)}', '{_esc(subscriber_team)}', '{_esc(subscriber_email)}',
                '{_esc(json.dumps(notification_prefs or {}))}', '{_esc(use_case)}', 'active', '{now}'
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not subscribe: {e}")
    return {"subscription_id": sid, "product_id": product_id, "status": "active"}


def get_subscribers(product_id: str, client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT * FROM {schema}.data_product_subscriptions
            WHERE product_id = '{_esc(product_id)}' AND status = 'active'
        """, limit=200, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def unsubscribe(subscription_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(f"""
            UPDATE {schema}.data_product_subscriptions
            SET status = 'cancelled' WHERE subscription_id = '{_esc(subscription_id)}'
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not unsubscribe: {e}")
