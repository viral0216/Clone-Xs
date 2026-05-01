"""Multi-hop promotion plans (prod → staging → dev).

A "plan" is an ordered list of clone hops. Each hop has a source/dest
naming convention plus per-hop overrides — masking on lower envs,
approvals on prod-touching hops, etc. The runner executes each hop
sequentially: a hop is allowed to start only after the previous hop has
status ``completed``. Failure or denial of any hop halts the chain and
records why.

This composes the existing Tier 1/2 primitives — masking ([masking.py],
the auto_mask_pii flag), approvals ([approval.py]), DQ comparison
([clone_dq_compare.py]) — rather than re-implementing them. Each hop
becomes a regular clone job in the JobManager, so progress, audit, and
cost reconciliation just work.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class PromotionStep:
    """One hop in a promotion plan."""

    name: str  # human-readable, e.g. "prod -> staging"
    source_catalog: str  # template; supports {prefix} substitution
    dest_catalog: str  # ditto
    auto_mask_pii: bool = False  # default: only mask non-prod targets
    require_approval: bool = False  # default: approve hops that touch prod
    overrides: dict[str, Any] = field(default_factory=dict)


@dataclass
class PromotionPlan:
    """Ordered list of hops + plan-level metadata."""

    name: str
    description: str
    steps: list[PromotionStep]


# Built-in plans. The plan defines the *shape* (which catalogs and
# guardrails per hop); concrete catalog names come from the user's
# request via {prefix} substitution at run time. Example: prefix=
# "supplier_portal" yields source=supplier_portal_prod, dest=
# supplier_portal_staging on the first hop.
BUILTIN_PLANS: dict[str, PromotionPlan] = {
    "prod-to-staging": PromotionPlan(
        name="Prod → Staging",
        description=(
            "Promote production data into staging with PII masked. "
            "Approval required on the source-side read of prod."
        ),
        steps=[
            PromotionStep(
                name="prod -> staging",
                source_catalog="{prefix}_prod",
                dest_catalog="{prefix}_staging",
                auto_mask_pii=True,
                require_approval=True,
            ),
        ],
    ),
    "prod-to-staging-to-dev": PromotionPlan(
        name="Prod → Staging → Dev",
        description=(
            "Two-hop fan-down. Prod -> staging copies real shape with PII "
            "masked and steward approval; staging -> dev is a fast shallow "
            "refresh with no further approval."
        ),
        steps=[
            PromotionStep(
                name="prod -> staging",
                source_catalog="{prefix}_prod",
                dest_catalog="{prefix}_staging",
                auto_mask_pii=True,
                require_approval=True,
            ),
            PromotionStep(
                name="staging -> dev",
                source_catalog="{prefix}_staging",
                dest_catalog="{prefix}_dev",
                auto_mask_pii=False,  # staging is already masked
                require_approval=False,
                overrides={
                    "clone_type": "SHALLOW",
                    "enable_rollback": False,
                    "copy_permissions": False,
                },
            ),
        ],
    ),
}


def list_promotion_plans() -> list[dict[str, Any]]:
    """Return every built-in plan as a serialisable dict for the API."""
    return [
        {
            "key": key,
            "name": plan.name,
            "description": plan.description,
            "steps": [asdict(s) for s in plan.steps],
        }
        for key, plan in BUILTIN_PLANS.items()
    ]


def get_promotion_plan(key: str) -> PromotionPlan | None:
    return BUILTIN_PLANS.get(key)


def build_clone_request_for_step(
    step: PromotionStep,
    prefix: str,
    base_config: dict[str, Any],
) -> dict[str, Any]:
    """Turn a `PromotionStep` into a clone-request payload for JobManager.

    `base_config` carries shared values (warehouse_id, max_workers, etc.)
    that every hop in the plan inherits. Per-step `overrides` win against
    `base_config`. The mask + approval flags are set explicitly on top of
    everything else so they can't accidentally be turned off by a base
    config that pre-defines them.
    """
    cfg: dict[str, Any] = {**base_config}
    cfg.update(step.overrides)
    cfg["source_catalog"] = step.source_catalog.format(prefix=prefix)
    cfg["destination_catalog"] = step.dest_catalog.format(prefix=prefix)
    cfg["auto_mask_pii"] = bool(step.auto_mask_pii)
    cfg["approval_required"] = bool(step.require_approval)
    return cfg
