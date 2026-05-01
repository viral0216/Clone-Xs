"""Natural Language DQ Rule Builder API endpoints."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class NLRuleRequest(BaseModel):
    text: str
    table_fqn: str


class BatchNLRequest(BaseModel):
    rules: list[str]
    table_fqn: str


class ExplainRequest(BaseModel):
    rule: dict


@router.post("/from-natural-language", summary="Parse NL into DQ rule config")
async def parse_nl_rule(req: NLRuleRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.nl_rule_builder import parse_nl_rule

    return parse_nl_rule(req.text, req.table_fqn, client, wid, config)


@router.post("/batch-parse", summary="Parse multiple NL rules")
async def batch_parse(req: BatchNLRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.nl_rule_builder import batch_parse

    return batch_parse(req.rules, req.table_fqn, client, wid, config)


@router.post("/explain", summary="Generate English explanation of a rule")
async def explain_rule(req: ExplainRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.nl_rule_builder import explain_rule

    return {"explanation": explain_rule(req.rule, client, wid, config)}
