"""ML Assets endpoints: registered models, feature tables, vector search, serving endpoints."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.ml_assets import MLAssetCloneRequest, MLAssetListRequest, ServingEndpointImportRequest

router = APIRouter()


@router.post("/list", summary="List ML assets in a catalog")
async def list_ml_assets(req: MLAssetListRequest, client=Depends(get_db_client)):
    """List registered models, feature tables, and vector search indexes in a catalog."""
    from src.clone_models import list_registered_models
    from src.clone_feature_tables import list_feature_tables
    from src.clone_vector_search import list_vector_indexes
    from src.clone_serving_endpoints import list_serving_endpoints

    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    schema = req.schemas[0] if req.schemas else None
    errors = []

    # Each SDK call is independent — catch failures individually
    try:
        models = list_registered_models(client, req.source_catalog, schema)
    except Exception as e:
        models = []
        errors.append(f"Models: {e}")

    try:
        feature_tables = list_feature_tables(client, wid, req.source_catalog, schema)
    except Exception as e:
        feature_tables = []
        errors.append(f"Feature tables: {e}")

    try:
        vector_indexes = list_vector_indexes(client, req.source_catalog, schema)
    except Exception as e:
        vector_indexes = []
        errors.append(f"Vector indexes: {e}")

    try:
        endpoints = list_serving_endpoints(client)
    except Exception as e:
        endpoints = []
        errors.append(f"Serving endpoints: {e}")

    return {
        "catalog": req.source_catalog,
        "models": models,
        "feature_tables": feature_tables,
        "vector_indexes": vector_indexes,
        "serving_endpoints": endpoints,
        "totals": {
            "models": len(models),
            "feature_tables": len(feature_tables),
            "vector_indexes": len(vector_indexes),
            "serving_endpoints": len(endpoints),
        },
        "errors": errors,
    }


@router.post("/clone", summary="Clone ML assets between catalogs")
async def clone_ml_assets(req: MLAssetCloneRequest, client=Depends(get_db_client)):
    """Clone registered models, feature tables, and vector search indexes
    from source to destination catalog."""
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    results = {"models": None, "feature_tables": None, "vector_indexes": None, "errors": []}

    # Clone registered models
    if req.include_models:
        from src.clone_models import clone_all_models
        results["models"] = clone_all_models(
            client, req.source_catalog, req.destination_catalog,
            schemas=req.schemas or None,
            copy_versions=req.copy_versions,
            dry_run=req.dry_run,
            max_workers=req.max_workers,
        )

    # Clone feature tables
    if req.include_feature_tables:
        from src.clone_feature_tables import list_feature_tables, clone_feature_table
        schema = req.schemas[0] if req.schemas else None
        ft_list = list_feature_tables(client, wid, req.source_catalog, schema)
        ft_results = []
        for ft in ft_list:
            r = clone_feature_table(
                client, wid, req.source_catalog, req.destination_catalog,
                ft["table_schema"], ft["table_name"],
                clone_type=req.clone_type, dry_run=req.dry_run,
            )
            ft_results.append(r)
        results["feature_tables"] = {
            "total": len(ft_list),
            "cloned": sum(1 for r in ft_results if r.get("success")),
            "results": ft_results,
        }

    # Clone vector search indexes
    if req.include_vector_indexes:
        from src.clone_vector_search import list_vector_indexes, export_index_definition, clone_vector_index
        schema = req.schemas[0] if req.schemas else None
        vi_list = list_vector_indexes(client, req.source_catalog, schema)
        vi_results = []
        for vi in vi_list:
            defn = export_index_definition(client, vi["name"])
            if defn:
                r = clone_vector_index(
                    client, defn, req.destination_catalog, req.source_catalog,
                    dry_run=req.dry_run,
                )
                vi_results.append(r)
        results["vector_indexes"] = {
            "total": len(vi_list),
            "cloned": sum(1 for r in vi_results if r.get("success")),
            "results": vi_results,
        }

    # Export serving endpoints (no clone — just export configs)
    if req.include_serving_endpoints:
        from src.clone_serving_endpoints import list_serving_endpoints, export_endpoint_config
        eps = list_serving_endpoints(client)
        exported = []
        for ep in eps:
            cfg = export_endpoint_config(client, ep["name"])
            if cfg:
                exported.append(cfg)
        results["serving_endpoints"] = {
            "total": len(eps),
            "exported": len(exported),
            "configs": exported,
        }

    return results


@router.post("/models/list", summary="List registered models")
async def list_models(req: MLAssetListRequest, client=Depends(get_db_client)):
    """List registered models in a catalog."""
    from src.clone_models import list_registered_models
    schema = req.schemas[0] if req.schemas else None
    return list_registered_models(client, req.source_catalog, schema)


@router.post("/vector-indexes/list", summary="List vector search indexes")
async def list_indexes(req: MLAssetListRequest, client=Depends(get_db_client)):
    """List vector search indexes in a catalog."""
    from src.clone_vector_search import list_vector_indexes
    schema = req.schemas[0] if req.schemas else None
    return list_vector_indexes(client, req.source_catalog, schema)


@router.get("/serving-endpoints", summary="List serving endpoints")
async def get_serving_endpoints(client=Depends(get_db_client)):
    """List all model serving endpoints."""
    from src.clone_serving_endpoints import list_serving_endpoints
    return list_serving_endpoints(client)


@router.post("/serving-endpoints/export", summary="Export serving endpoint config")
async def export_endpoint(req: dict, client=Depends(get_db_client)):
    """Export a serving endpoint configuration."""
    from src.clone_serving_endpoints import export_endpoint_config
    name = req.get("name", "")
    config = export_endpoint_config(client, name)
    if config is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Endpoint '{name}' not found")
    return config


@router.post("/serving-endpoints/import", summary="Import serving endpoint config")
async def import_endpoint(req: ServingEndpointImportRequest, client=Depends(get_db_client)):
    """Create a serving endpoint from an exported configuration."""
    from src.clone_serving_endpoints import import_endpoint_config
    return import_endpoint_config(
        client, req.config,
        dest_catalog=req.dest_catalog,
        source_catalog=req.source_catalog,
        name_suffix=req.name_suffix,
        dry_run=req.dry_run,
    )
