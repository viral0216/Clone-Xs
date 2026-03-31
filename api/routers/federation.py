"""Lakehouse Federation endpoints: foreign catalogs, connections, migration."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.federation import ConnectionCloneRequest, ForeignTablesRequest, MigrateRequest

router = APIRouter()


@router.get("/catalogs", summary="List foreign catalogs")
async def get_foreign_catalogs(client=Depends(get_db_client)):
    """List all foreign (federated) catalogs in the metastore."""
    from src.federation import list_foreign_catalogs
    return list_foreign_catalogs(client)


@router.get("/connections", summary="List connections")
async def get_connections(client=Depends(get_db_client)):
    """List all connections (MySQL, PostgreSQL, Snowflake, etc.)."""
    from src.federation import list_connections
    return list_connections(client)


@router.get("/connections/{name}", summary="Export connection config")
async def get_connection_detail(name: str, client=Depends(get_db_client)):
    """Export a connection's configuration (sensitive fields redacted)."""
    from src.federation import export_connection
    config = export_connection(client, name)
    if config is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Connection '{name}' not found")
    return config


@router.post("/connections/clone", summary="Clone a connection")
async def clone_connection_endpoint(req: ConnectionCloneRequest, client=Depends(get_db_client)):
    """Create a new connection from an exported definition.

    Credentials must be supplied since they are redacted in exports.
    """
    from src.federation import export_connection, clone_connection
    defn = export_connection(client, req.connection_name)
    if defn is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Connection '{req.connection_name}' not found")
    return clone_connection(client, defn, req.new_name, req.credentials, req.dry_run)


@router.post("/tables", summary="List tables in a foreign catalog")
async def get_foreign_tables(req: ForeignTablesRequest, client=Depends(get_db_client)):
    """List tables available in a foreign (federated) catalog."""
    from src.federation import list_foreign_tables
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return list_foreign_tables(client, wid, req.catalog, req.schema_filter)


@router.post("/migrate", summary="Migrate foreign table to managed Delta")
async def migrate_table(req: MigrateRequest, client=Depends(get_db_client)):
    """Materialize a foreign table into a managed Delta table (CTAS)."""
    from src.federation import migrate_foreign_to_managed
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return migrate_foreign_to_managed(client, wid, req.foreign_fqn, req.dest_fqn, req.dry_run)
