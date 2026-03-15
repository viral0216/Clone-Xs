"""Tests for the clone templates module."""

import pytest

from src.clone_templates import (
    TEMPLATES,
    apply_template,
    get_template,
    list_templates,
)


class TestListTemplates:
    def test_returns_all_templates(self):
        result = list_templates()
        assert len(result) == len(TEMPLATES)

    def test_each_template_has_required_keys(self):
        result = list_templates()
        for t in result:
            assert "key" in t
            assert "name" in t
            assert "description" in t
            assert "clone_type" in t


class TestGetTemplate:
    def test_dev_copy(self):
        tmpl = get_template("dev-copy")
        assert tmpl is not None
        assert tmpl["name"] == "Development Copy"
        assert tmpl["config"]["clone_type"] == "SHALLOW"

    def test_dr_backup(self):
        tmpl = get_template("dr-backup")
        assert tmpl is not None
        assert tmpl["config"]["clone_type"] == "DEEP"
        assert tmpl["config"]["validate_checksum"] is True

    def test_nonexistent_template(self):
        assert get_template("does-not-exist") is None

    def test_all_templates_exist(self):
        expected = ["dev-copy", "dr-backup", "test-refresh", "staging-promote",
                     "incremental-sync", "schema-only", "cross-workspace"]
        for name in expected:
            assert get_template(name) is not None, f"Template '{name}' not found"


class TestApplyTemplate:
    def test_template_overrides_base_config(self):
        base = {
            "source_catalog": "prod",
            "destination_catalog": "dev",
            "clone_type": "DEEP",
            "sql_warehouse_id": "wh-123",
        }

        result = apply_template(base, "dev-copy")

        assert result["source_catalog"] == "prod"  # preserved from base
        assert result["clone_type"] == "SHALLOW"  # overridden by template
        assert result["sql_warehouse_id"] == "wh-123"  # preserved from base

    def test_overrides_take_precedence(self):
        base = {"source_catalog": "prod", "clone_type": "DEEP"}

        result = apply_template(base, "dev-copy", overrides={"clone_type": "DEEP"})

        assert result["clone_type"] == "DEEP"  # override beats template

    def test_invalid_template_raises(self):
        with pytest.raises(ValueError, match="Unknown template"):
            apply_template({}, "nonexistent-template")

    def test_dr_backup_enables_checksum(self):
        result = apply_template({}, "dr-backup")
        assert result.get("validate_checksum") is True
        assert result.get("copy_permissions") is True
        assert result.get("copy_security") is True
