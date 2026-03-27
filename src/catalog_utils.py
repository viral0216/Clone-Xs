"""Shared utilities for safe catalog and schema creation.

Avoids noisy Spark JVM errors by checking existence before CREATE.
Supports MANAGED LOCATION for environments without default storage root.
"""

import logging

logger = logging.getLogger(__name__)

# Cache of catalogs we've verified exist in this process
_verified_catalogs: set[str] = set()
_verified_schemas: set[str] = set()


def ensure_catalog(client, warehouse_id: str, catalog: str, storage_location: str = ""):
    """Ensure a catalog exists. Skip CREATE if it already exists.

    Args:
        storage_location: Optional MANAGED LOCATION for catalog creation.
    """
    if catalog in _verified_catalogs:
        return

    from src.client import execute_sql

    # Check if catalog exists first
    try:
        rows = execute_sql(client, warehouse_id, "SHOW CATALOGS")
        existing = {r.get("catalog", r.get("catalog_name", "")).lower() for r in (rows or [])}
        if catalog.lower() in existing:
            _verified_catalogs.add(catalog)
            return
    except Exception:
        pass

    # Catalog doesn't exist — try to create it
    try:
        location_clause = f" MANAGED LOCATION '{storage_location}'" if storage_location else ""
        execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`{location_clause}")
        _verified_catalogs.add(catalog)
    except Exception as e:
        # Check if it's actually usable despite the error
        try:
            execute_sql(client, warehouse_id, f"USE CATALOG `{catalog}`")
            _verified_catalogs.add(catalog)
        except Exception:
            logger.warning(f"Cannot create or access catalog '{catalog}': {e}")
            raise


def ensure_schema(client, warehouse_id: str, catalog: str, schema: str, storage_location: str = ""):
    """Ensure a schema exists within a catalog. Skip CREATE if it already exists.

    Args:
        storage_location: Optional base path. Schema location will be '{storage_location}/{schema}'.
    """
    fqn = f"{catalog}.{schema}"
    if fqn in _verified_schemas:
        return

    from src.client import execute_sql

    # Check if schema exists first
    try:
        rows = execute_sql(client, warehouse_id, f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
        if rows:
            _verified_schemas.add(fqn)
            return
    except Exception:
        pass

    # Schema doesn't exist — try to create it
    try:
        location_clause = ""
        if storage_location:
            schema_location = f"{storage_location.rstrip('/')}/{schema}"
            location_clause = f" MANAGED LOCATION '{schema_location}'"
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`{location_clause}")
        _verified_schemas.add(fqn)
    except Exception as e:
        logger.warning(f"Cannot create schema '{fqn}': {e}")
        raise


def ensure_catalog_and_schema(client, warehouse_id: str, catalog: str, schema: str,
                               storage_location: str = ""):
    """Ensure both catalog and schema exist."""
    ensure_catalog(client, warehouse_id, catalog, storage_location)
    ensure_schema(client, warehouse_id, catalog, schema, storage_location)


def safe_ensure_schema_from_fqn(fqn: str, client, warehouse_id: str, config: dict = None):
    """Ensure schema exists given a 'catalog.schema' FQN string.

    Extracts catalog and schema from the FQN, uses catalog_utils to check
    before creating. Reads catalog_location from config if available.

    Usage in ensure_*_tables functions:
        safe_ensure_schema_from_fqn(schema_fqn, client, warehouse_id, config)
    """
    config = config or {}
    parts = fqn.split(".", 1) if "." in fqn else (fqn, "default")
    cat, sch = parts[0], parts[1] if len(parts) > 1 else "default"
    loc = config.get("catalog_location", "")
    ensure_catalog_and_schema(client, warehouse_id, cat, sch, loc)
