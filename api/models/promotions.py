"""Promotion request/response models."""

from typing import Any

from pydantic import BaseModel, Field


class PromotionStepView(BaseModel):
    name: str
    source_catalog: str
    dest_catalog: str
    auto_mask_pii: bool = False
    require_approval: bool = False
    overrides: dict[str, Any] = Field(default_factory=dict)


class PromotionPlanView(BaseModel):
    key: str
    name: str
    description: str
    steps: list[PromotionStepView]


class RunPromotionRequest(BaseModel):
    """Inputs the user supplies to materialise a built-in plan.

    `prefix` substitutes into each step's `{prefix}` placeholder — e.g.
    `prefix="supplier_portal"` and step `source_catalog="{prefix}_prod"`
    yields `supplier_portal_prod`. `warehouse_id` and `max_workers` are
    inherited by every hop unless the hop overrides them.
    """

    prefix: str = Field(..., description="Catalog name prefix used in {prefix} substitution")
    warehouse_id: str = Field(..., description="SQL warehouse executing every hop")
    max_workers: int = 4


class HopJobRef(BaseModel):
    """One hop's slot in the run plan, with the JobManager job_id assigned."""

    name: str
    source_catalog: str
    dest_catalog: str
    job_id: str | None = None
    status: str = "queued"


class RunPromotionResponse(BaseModel):
    plan_key: str
    prefix: str
    hops: list[HopJobRef]
