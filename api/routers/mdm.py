"""MDM (Master Data Management) API router — 20 endpoints for entity resolution, stewardship, and hierarchies."""

from fastapi import APIRouter, Depends
from api.dependencies import get_db_client, get_app_config
from api.models.mdm import (
    GoldenRecordRequest, GoldenRecordUpdateRequest,
    IngestRequest, DetectDuplicatesRequest, MergeRequest, SplitRequest,
    MatchingRuleRequest, StewardshipActionRequest,
    HierarchyCreateRequest, HierarchyNodeRequest,
)

router = APIRouter()


def _manager(client, config):
    from src.mdm import MDMManager
    wid = config.get("sql_warehouse_id", "")
    return MDMManager(client, wid, config)


# ---- Init ----

@router.post("/init")
async def init_tables(client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).init_tables()


# ---- Dashboard ----

@router.get("/dashboard")
async def get_dashboard(client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).get_dashboard()
    except Exception:
        return {"entities": [], "pairs": [], "stewardship": {"total": 0, "open": 0, "high_priority": 0, "resolved": 0}}


# ---- Entities (Golden Records) ----

@router.get("/entities")
async def list_entities(entity_type: str = None, status: str = None, limit: int = 100, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).get_entities(entity_type, status, limit)
    except Exception:
        return []


@router.get("/entities/{entity_id}")
async def get_entity(entity_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).get_entity_detail(entity_id)
    except Exception:
        return {"entity": None, "source_records": []}


@router.post("/entities")
async def create_entity(req: GoldenRecordRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).create_entity(req.entity_type, req.display_name, req.attributes)


@router.put("/entities/{entity_id}")
async def update_entity(entity_id: str, req: GoldenRecordUpdateRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).update_entity(entity_id, req.display_name, req.attributes)


@router.delete("/entities/{entity_id}")
async def delete_entity(entity_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).delete_entity(entity_id)


# ---- Source Record Ingestion ----

@router.post("/ingest")
async def ingest_source_records(req: IngestRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).ingest_source_records(req.catalog, req.schema_name, req.table, req.entity_type, req.key_column, req.trust_score)


# ---- Match & Merge ----

@router.post("/detect")
async def detect_duplicates(req: DetectDuplicatesRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).detect_duplicates(req.entity_type, req.auto_merge_threshold, req.review_threshold)


@router.get("/pairs")
async def list_match_pairs(entity_type: str = None, status: str = None, limit: int = 100, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).store.get_match_pairs(entity_type, status, limit)
    except Exception:
        return []


@router.post("/merge")
async def merge_records(req: MergeRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).merge_records(req.pair_id, req.strategy)


@router.post("/split")
async def split_record(req: SplitRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).split_record(req.entity_id)


# ---- Matching Rules ----

@router.get("/rules")
async def list_rules(entity_type: str = None, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).store.get_rules(entity_type)
    except Exception:
        return []


@router.post("/rules")
async def create_rule(req: MatchingRuleRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).create_rule(req.entity_type, req.name, req.field, req.match_type, req.weight, req.threshold, req.enabled)


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).delete_rule(rule_id)


# ---- Stewardship Queue ----

@router.get("/stewardship")
async def list_stewardship(status: str = None, priority: str = None, limit: int = 50, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).store.get_tasks(status, priority, limit)
    except Exception:
        return []


@router.post("/stewardship/{task_id}/approve")
async def approve_task(task_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).approve_task(task_id, "user")


@router.post("/stewardship/{task_id}/reject")
async def reject_task(task_id: str, req: StewardshipActionRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).reject_task(task_id, "user", req.reason)


# ---- Hierarchies ----

@router.get("/hierarchies")
async def list_hierarchies(client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).store.get_all_hierarchies()
    except Exception:
        return []


@router.post("/hierarchies")
async def create_hierarchy(req: HierarchyCreateRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).create_hierarchy(req.name, req.entity_type)


@router.get("/hierarchies/{hierarchy_id}")
async def get_hierarchy(hierarchy_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _manager(client, config).store.get_hierarchy(hierarchy_id)
    except Exception:
        return []


@router.post("/hierarchies/{hierarchy_id}/nodes")
async def add_hierarchy_node(hierarchy_id: str, req: HierarchyNodeRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    return _manager(client, config).add_node(hierarchy_id, req.entity_id, req.label, req.parent_node_id, req.level)
