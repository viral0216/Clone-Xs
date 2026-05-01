"""Intelligent Alert Routing & Digest API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class CreateRoutingRuleRequest(BaseModel):
    name: str
    table_pattern: str = "*"
    severity_filter: str = "*"
    event_type_filter: str = "*"
    route_to_team: str = ""
    channel: str = "slack"
    channel_config: dict = {}


class RouteAlertRequest(BaseModel):
    event_type: str
    table_fqn: str
    severity: str
    title: str
    message: str


class CreateDigestRequest(BaseModel):
    recipient: str
    frequency: str = "daily"
    filters: dict = {}


# ─── Routing Rules ──────────────────────────────────────────────────────


@router.get("/routing-rules", summary="List routing rules")
async def list_routing_rules(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import list_routing_rules

    return list_routing_rules(client, wid, config)


@router.post("/routing-rules", summary="Create routing rule")
async def create_routing_rule(req: CreateRoutingRuleRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import create_routing_rule

    return create_routing_rule(**req.model_dump(), client=client, warehouse_id=wid, config=config)


@router.put("/routing-rules/{rule_id}", summary="Update routing rule")
async def update_routing_rule(
    rule_id: str, req: CreateRoutingRuleRequest, client=Depends(get_db_client)
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import update_routing_rule

    return update_routing_rule(rule_id, req.model_dump(), client, wid, config)


@router.delete("/routing-rules/{rule_id}", summary="Delete routing rule")
async def delete_routing_rule(rule_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import delete_routing_rule

    delete_routing_rule(rule_id, client, wid, config)
    return {"status": "deleted"}


# ─── Alert Inbox ────────────────────────────────────────────────────────


@router.get("/inbox", summary="Get alert inbox")
async def get_inbox(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import get_inbox

    return get_inbox(status, severity, client, wid, config)


@router.post("/route", summary="Route a new alert")
async def route_alert(req: RouteAlertRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import route_alert

    return route_alert(
        req.event_type, req.table_fqn, req.severity, req.title, req.message, client, wid, config
    )


@router.post("/inbox/{alert_id}/acknowledge", summary="Acknowledge alert")
async def acknowledge(alert_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import acknowledge_alert

    acknowledge_alert(alert_id, client, wid, config)
    return {"status": "acknowledged"}


@router.post("/inbox/{alert_id}/resolve", summary="Resolve alert")
async def resolve(alert_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import resolve_alert

    resolve_alert(alert_id, client, wid, config)
    return {"status": "resolved"}


@router.post("/inbox/{alert_id}/snooze", summary="Snooze alert")
async def snooze(alert_id: str, hours: int = Query(default=4, ge=1), client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import snooze_alert

    snooze_alert(alert_id, hours, client, wid, config)
    return {"status": "snoozed", "hours": hours}


@router.get("/analytics", summary="Get alert analytics")
async def get_analytics(days: int = Query(default=30, ge=1), client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import get_alert_analytics

    return get_alert_analytics(days, client, wid, config)


# ─── Digests ────────────────────────────────────────────────────────────


@router.get("/digests", summary="List digest configurations")
async def list_digests(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import list_digests

    return list_digests(client, wid, config)


@router.post("/digests", summary="Create digest configuration")
async def create_digest(req: CreateDigestRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import create_digest

    return create_digest(req.recipient, req.frequency, req.filters, client, wid, config)


@router.delete("/digests/{digest_id}", summary="Delete digest")
async def delete_digest(digest_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.alert_routing import delete_digest

    delete_digest(digest_id, client, wid, config)
    return {"status": "deleted"}
