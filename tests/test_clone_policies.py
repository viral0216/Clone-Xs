"""Tests for the clone policies (guardrails) module."""

from unittest.mock import MagicMock

from src.clone_policies import PolicyViolation, evaluate_policies, load_policies


class TestPolicyViolation:
    def test_creation(self):
        v = PolicyViolation("max_size", "Too large", "error")
        assert v.policy_name == "max_size"
        assert v.message == "Too large"
        assert v.severity == "error"

    def test_default_severity(self):
        v = PolicyViolation("test", "msg")
        assert v.severity == "error"

    def test_repr(self):
        v = PolicyViolation("test_policy", "test message")
        assert "test_policy" in repr(v)
        assert "test message" in repr(v)


class TestLoadPolicies:
    def test_from_config(self):
        config = {
            "clone_policies": [
                {"name": "deny_shallow_clone", "value": True, "severity": "error"},
            ]
        }
        policies = load_policies(config=config)
        assert len(policies) == 1
        assert policies[0]["name"] == "deny_shallow_clone"

    def test_empty_config(self):
        policies = load_policies(config={})
        assert policies == []

    def test_no_args(self):
        policies = load_policies()
        assert policies == []


class TestEvaluatePolicies:
    def test_deny_shallow_clone_violated(self):
        config = {"clone_type": "SHALLOW"}
        policies = [{"name": "deny_shallow_clone", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)

        assert len(violations) == 1
        assert violations[0].policy_name == "deny_shallow_clone"

    def test_deny_shallow_clone_passes_for_deep(self):
        config = {"clone_type": "DEEP"}
        policies = [{"name": "deny_shallow_clone", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 0

    def test_require_rollback_violated(self):
        config = {"enable_rollback": False}
        policies = [{"name": "require_rollback", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)

        assert len(violations) == 1
        assert violations[0].policy_name == "require_rollback"

    def test_require_rollback_passes(self):
        config = {"enable_rollback": True}
        policies = [{"name": "require_rollback", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 0

    def test_require_validation_violated(self):
        config = {"validate_after_clone": False}
        policies = [{"name": "require_validation", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 1

    def test_deny_cross_workspace_violated(self):
        config = {"dest_workspace": {"host": "https://other.databricks.com"}}
        policies = [{"name": "deny_cross_workspace", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 1

    def test_deny_cross_workspace_passes(self):
        config = {}
        policies = [{"name": "deny_cross_workspace", "value": True, "severity": "error"}]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 0

    def test_multiple_policies(self):
        config = {"clone_type": "SHALLOW", "enable_rollback": False}
        policies = [
            {"name": "deny_shallow_clone", "value": True, "severity": "error"},
            {"name": "require_rollback", "value": True, "severity": "warning"},
        ]

        violations = evaluate_policies(MagicMock(), "wh-123", config, policies)
        assert len(violations) == 2
        errors = [v for v in violations if v.severity == "error"]
        warnings = [v for v in violations if v.severity == "warning"]
        assert len(errors) == 1
        assert len(warnings) == 1

    def test_no_policies(self):
        violations = evaluate_policies(MagicMock(), "wh-123", {}, [])
        assert len(violations) == 0
