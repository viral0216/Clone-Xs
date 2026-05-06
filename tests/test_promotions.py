"""Tests for src/promotions.py — multi-hop promotion plan helpers."""

from src.promotions import (
    BUILTIN_PLANS,
    PromotionPlan,
    PromotionStep,
    build_clone_request_for_step,
    get_promotion_plan,
    list_promotion_plans,
)


class TestListPlans:
    def test_built_in_plans_present(self):
        plans = list_promotion_plans()
        keys = {p["key"] for p in plans}
        assert "prod-to-staging" in keys
        assert "prod-to-staging-to-dev" in keys

    def test_each_plan_has_at_least_one_step(self):
        for p in list_promotion_plans():
            assert len(p["steps"]) >= 1
            for step in p["steps"]:
                assert "{prefix}" in step["source_catalog"] or step["source_catalog"]
                assert "{prefix}" in step["dest_catalog"] or step["dest_catalog"]


class TestGetPlan:
    def test_returns_plan(self):
        plan = get_promotion_plan("prod-to-staging")
        assert isinstance(plan, PromotionPlan)
        assert len(plan.steps) == 1

    def test_unknown_returns_none(self):
        assert get_promotion_plan("nope") is None


class TestBuildCloneRequest:
    def test_substitutes_prefix(self):
        step = PromotionStep(
            name="x",
            source_catalog="{prefix}_prod",
            dest_catalog="{prefix}_staging",
        )
        cfg = build_clone_request_for_step(step, "supplier", base_config={})
        assert cfg["source_catalog"] == "supplier_prod"
        assert cfg["destination_catalog"] == "supplier_staging"

    def test_overrides_win_against_base_config(self):
        step = PromotionStep(
            name="x",
            source_catalog="{prefix}_a",
            dest_catalog="{prefix}_b",
            overrides={"clone_type": "SHALLOW"},
        )
        cfg = build_clone_request_for_step(
            step,
            "p",
            base_config={"clone_type": "DEEP", "max_workers": 4},
        )
        assert cfg["clone_type"] == "SHALLOW"  # override wins
        assert cfg["max_workers"] == 4  # base preserved

    def test_mask_and_approval_flags_set_explicitly(self):
        step = PromotionStep(
            name="x",
            source_catalog="a",
            dest_catalog="b",
            auto_mask_pii=True,
            require_approval=True,
        )
        cfg = build_clone_request_for_step(step, "p", base_config={})
        assert cfg["auto_mask_pii"] is True
        assert cfg["approval_required"] is True

    def test_step_overrides_cannot_disable_mask_and_approval(self):
        """Per-step overrides shouldn't accidentally turn off mask/approval —
        the flags are applied last so they always reflect the step intent."""
        step = PromotionStep(
            name="x",
            source_catalog="a",
            dest_catalog="b",
            auto_mask_pii=True,
            require_approval=True,
            overrides={"auto_mask_pii": False, "approval_required": False},
        )
        cfg = build_clone_request_for_step(step, "p", base_config={})
        assert cfg["auto_mask_pii"] is True
        assert cfg["approval_required"] is True


class TestPlanShape:
    def test_prod_to_staging_marks_approval_and_masking(self):
        plan = BUILTIN_PLANS["prod-to-staging"]
        step = plan.steps[0]
        assert step.require_approval is True
        assert step.auto_mask_pii is True

    def test_prod_to_staging_to_dev_has_two_hops_with_correct_chain(self):
        plan = BUILTIN_PLANS["prod-to-staging-to-dev"]
        assert len(plan.steps) == 2
        # Second hop reads from the first hop's destination
        assert plan.steps[1].source_catalog == plan.steps[0].dest_catalog

    def test_dev_hop_has_no_extra_approval(self):
        """Dev hop should NOT require approval — staging is already an
        approved & masked artefact, and forcing another reviewer wait
        defeats the point of a 'fast' dev refresh."""
        plan = BUILTIN_PLANS["prod-to-staging-to-dev"]
        assert plan.steps[1].require_approval is False
