"""Tests for RBAC."""

import os
import tempfile

import yaml

from src.rbac import (
    RbacRule,
    check_permission,
    load_rbac_policy,
    _principal_matches,
    _matches_any,
)


class TestPrincipalMatches:
    def test_exact_match(self):
        assert _principal_matches("admin@test.com", ["admin@test.com"])

    def test_wildcard(self):
        assert _principal_matches("anyone@test.com", ["*"])

    def test_case_insensitive(self):
        assert _principal_matches("Admin@Test.com", ["admin@test.com"])

    def test_no_match(self):
        assert not _principal_matches("user@test.com", ["admin@test.com"])


class TestMatchesAny:
    def test_exact(self):
        assert _matches_any("prod_catalog", ["prod_catalog"])

    def test_regex(self):
        assert _matches_any("prod_catalog", ["prod_.*"])

    def test_wildcard(self):
        assert _matches_any("anything", [".*"])

    def test_no_match(self):
        assert not _matches_any("dev_catalog", ["prod_.*"])


class TestCheckPermission:
    def test_no_policy_allows(self):
        result = check_permission([], "user", "src", "dst")
        assert result["allowed"] is True

    def test_allow_rule(self):
        policy = [RbacRule(principals=["user@test.com"], allowed_sources=[".*"], allowed_destinations=[".*"])]
        result = check_permission(policy, "user@test.com", "src", "dst")
        assert result["allowed"] is True

    def test_deny_rule(self):
        policy = [RbacRule(principals=["*"], allowed_destinations=["prod_.*"], deny=True)]
        result = check_permission(policy, "user@test.com", "src", "prod_catalog")
        assert result["allowed"] is False

    def test_deny_overrides_allow(self):
        policy = [
            RbacRule(principals=["*"], allowed_destinations=["prod_.*"], deny=True),
            RbacRule(principals=["*"], allowed_sources=[".*"], allowed_destinations=[".*"]),
        ]
        result = check_permission(policy, "user@test.com", "src", "prod_catalog")
        assert result["allowed"] is False

    def test_no_matching_rule_denies(self):
        policy = [RbacRule(principals=["admin@test.com"], allowed_sources=[".*"], allowed_destinations=[".*"])]
        result = check_permission(policy, "other@test.com", "src", "dst")
        assert result["allowed"] is False


class TestLoadRbacPolicy:
    def test_load_from_file(self):
        policy_data = {
            "rules": [
                {"principals": ["admin@test.com"], "allowed_sources": [".*"], "allowed_destinations": [".*"]},
                {"principals": ["*"], "allowed_destinations": ["prod_.*"], "deny": True},
            ]
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(policy_data, f)
            path = f.name

        rules = load_rbac_policy(path)
        assert len(rules) == 2
        assert rules[0].principals == ["admin@test.com"]
        assert rules[1].deny is True
        os.remove(path)

    def test_missing_file_returns_empty(self):
        rules = load_rbac_policy("/nonexistent/path.yaml")
        assert rules == []
