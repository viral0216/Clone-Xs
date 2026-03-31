"""Pydantic models for MDM API endpoints."""

from pydantic import BaseModel
from typing import Optional


class GoldenRecordRequest(BaseModel):
    entity_type: str
    display_name: str
    attributes: dict = {}


class GoldenRecordUpdateRequest(BaseModel):
    display_name: str
    attributes: dict = {}


class IngestRequest(BaseModel):
    catalog: str
    schema_name: str
    table: str
    entity_type: str
    key_column: str
    trust_score: float = 1.0


class DetectDuplicatesRequest(BaseModel):
    entity_type: str
    auto_merge_threshold: float = 95.0
    review_threshold: float = 80.0


class MergeRequest(BaseModel):
    pair_id: str
    strategy: str = "most_trusted"


class SplitRequest(BaseModel):
    entity_id: str


class MatchingRuleRequest(BaseModel):
    entity_type: str
    name: str
    field: str
    match_type: str
    weight: float = 1.0
    threshold: float = 0.8
    enabled: bool = True


class StewardshipActionRequest(BaseModel):
    reason: str = ""


class HierarchyCreateRequest(BaseModel):
    name: str
    entity_type: str


class HierarchyNodeRequest(BaseModel):
    entity_id: Optional[str] = None
    label: str
    parent_node_id: str
    level: int = 1
