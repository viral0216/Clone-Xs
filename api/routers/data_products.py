"""Data Product Catalog & Marketplace API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class CreateProductRequest(BaseModel):
    name: str
    description: str = ""
    domain: str = ""
    owner_team: str = ""
    owner_email: str = ""
    tables: list = []
    sla_guarantees: dict = {}
    quality_requirements: dict = {}
    tags: list = []


class UpdateProductRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    domain: Optional[str] = None
    owner_team: Optional[str] = None
    owner_email: Optional[str] = None
    tables: Optional[list] = None
    sla_guarantees: Optional[dict] = None
    quality_requirements: Optional[dict] = None
    tags: Optional[list] = None
    status: Optional[str] = None
    version: Optional[str] = None


class SubscribeRequest(BaseModel):
    subscriber_team: str
    subscriber_email: str
    use_case: str = ""
    notification_prefs: dict = {}


@router.get("/", summary="List data products")
async def list_products(
    status: Optional[str] = None,
    domain: Optional[str] = None,
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import list_products
    return list_products(status, domain, client, wid, config)


@router.post("/", summary="Create a data product")
async def create_product(req: CreateProductRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import create_product
    return create_product(**req.model_dump(), client=client, warehouse_id=wid, config=config)


@router.get("/{product_id}", summary="Get data product details")
async def get_product(product_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import get_product
    return get_product(product_id, client, wid, config)


@router.put("/{product_id}", summary="Update a data product")
async def update_product(product_id: str, req: UpdateProductRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import update_product
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    return update_product(product_id, updates, client, wid, config)


@router.delete("/{product_id}", summary="Delete a data product")
async def delete_product(product_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import delete_product
    delete_product(product_id, client, wid, config)
    return {"status": "deleted"}


@router.post("/{product_id}/publish", summary="Publish a data product")
async def publish_product(product_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import publish_product
    return publish_product(product_id, client, wid, config)


@router.post("/{product_id}/subscribe", summary="Subscribe to a data product")
async def subscribe(product_id: str, req: SubscribeRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import subscribe
    return subscribe(product_id, req.subscriber_team, req.subscriber_email,
                     req.use_case, req.notification_prefs, client, wid, config)


@router.get("/{product_id}/subscribers", summary="List subscribers")
async def get_subscribers(product_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_products import get_subscribers
    return get_subscribers(product_id, client, wid, config)
