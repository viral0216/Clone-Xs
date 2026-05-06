"""Lakehouse Federation endpoints: foreign catalogs, connections, migration."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_app_config
from api.models.federation import (
    ConnectionCloneRequest,
    ForeignTablesRequest,
    MigrateRequest,
    RegisterIcebergRestCatalogRequest,
    RegisterIcebergRestCatalogResponse,
)

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


@router.post(
    "/iceberg-rest/register",
    response_model=RegisterIcebergRestCatalogResponse,
    summary="Register an external Iceberg REST catalog as a UC Foreign Catalog",
)
async def register_iceberg_rest(
    req: RegisterIcebergRestCatalogRequest,
    client=Depends(get_db_client),
):
    """Register an external Apache Iceberg REST catalog (Polaris,
    Snowflake Open Catalog, Apache Iceberg REST) as a UC Foreign
    Catalog so the existing convert-format dispatch can read its
    tables via ``CONVERT TO DELTA`` without any new strategies.

    Idempotency: if a catalog with the requested name already exists,
    returns ``created=False`` with an ``error`` message rather than
    overwriting the existing binding.

    Surfaces a 400 with the underlying error if the warehouse rejects
    the ``CREATE FOREIGN CATALOG`` (most commonly: missing
    ``CREATE FOREIGN CATALOG`` privilege, missing secret scope, or
    the REST endpoint isn't reachable).
    """
    from src.federation import register_iceberg_rest_catalog

    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    if not wid:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "CREATE FOREIGN CATALOG needs a SQL warehouse to execute the DDL."
            ),
        )
    result = register_iceberg_rest_catalog(
        client,
        wid,
        req.name,
        uri=req.uri,
        warehouse=req.warehouse,
        credential=req.credential,
        comment=req.comment,
    )
    next_step = None
    if result.get("created"):
        next_step = (
            f"Open /convert and pick `{req.name}` from the catalog dropdown — "
            f"its Iceberg tables can now be converted to Delta via the existing flow."
        )
    return RegisterIcebergRestCatalogResponse(
        name=result["name"],
        created=bool(result.get("created")),
        error=result.get("error"),
        next_step=next_step,
    )
