"""REST surface for multi-hop promotion plans.

A "run" submits the first hop to JobManager immediately and stages the
rest as `queued` placeholders. The JobManager itself does not understand
hop dependencies — instead we mark all hops in the response with the
sequential JobManager job_ids so the UI / a follow-up call can advance
through them as each completes. This keeps the orchestration decision
client-side and avoids growing a new long-running orchestrator inside
the API process.

For a fully server-side sequential runner (advance hop N+1 only after
hop N completes), wire a small worker that polls `jm.get_job(prev)` —
out of scope here but easy to layer on top because each hop is just a
regular clone-job submission.
"""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_app_config, get_db_client, get_job_manager
from api.models.promotions import (
    HopJobRef,
    PromotionPlanView,
    RunPromotionRequest,
    RunPromotionResponse,
)
from api.queue.job_manager import JobManager

router = APIRouter()


@router.get("/plans", response_model=list[PromotionPlanView])
async def list_plans() -> list[PromotionPlanView]:
    """List built-in promotion plans."""
    from src.promotions import list_promotion_plans

    return [PromotionPlanView(**p) for p in list_promotion_plans()]


@router.get("/plans/{plan_key}", response_model=PromotionPlanView)
async def get_plan(plan_key: str) -> PromotionPlanView:
    from src.promotions import list_promotion_plans

    for p in list_promotion_plans():
        if p["key"] == plan_key:
            return PromotionPlanView(**p)
    raise HTTPException(status_code=404, detail=f"Plan not found: {plan_key}")


@router.post("/plans/{plan_key}/run", response_model=RunPromotionResponse)
async def run_plan(
    plan_key: str,
    req: RunPromotionRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm: JobManager = Depends(get_job_manager),
) -> RunPromotionResponse:
    """Submit the first hop; return all hops with placeholder job_ids."""
    from src.promotions import build_clone_request_for_step, get_promotion_plan

    plan = get_promotion_plan(plan_key)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"Plan not found: {plan_key}")

    base_config = {
        **dict(app_config),
        "sql_warehouse_id": req.warehouse_id,
        "max_workers": req.max_workers,
    }

    hops: list[HopJobRef] = []
    # Submit only the first hop — subsequent hops can be kicked off by the
    # client (or a future server-side waiter) once each prior hop reports
    # status=completed via GET /clone/{job_id}.
    for idx, step in enumerate(plan.steps):
        cfg = build_clone_request_for_step(step, prefix=req.prefix, base_config=base_config)
        hop = HopJobRef(
            name=step.name,
            source_catalog=cfg["source_catalog"],
            dest_catalog=cfg["destination_catalog"],
        )
        if idx == 0:
            job_id = await jm.submit_job("clone", cfg, client)
            hop.job_id = job_id
            hop.status = "submitted"
        else:
            hop.status = "pending_prior_hop"
        hops.append(hop)

    return RunPromotionResponse(plan_key=plan_key, prefix=req.prefix, hops=hops)
