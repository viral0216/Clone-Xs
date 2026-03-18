"""
Automated tests for every HOWTO.md example.

Validates that all 46 HOWTO sections work: CLI argument parsing,
config loading, and cmd_* function execution with mocked dependencies.

Run: make test-howto
  or: python3 -m pytest tests/test_howto_examples.py -v --tb=short
"""

from unittest.mock import MagicMock, patch
import argparse
import os
import pytest
import yaml

from src.main import (
    build_parser,
    cmd_audit,
    cmd_auth,
    cmd_clone,
    cmd_compare,
    cmd_completion,
    cmd_config_diff,
    cmd_diff,
    cmd_estimate,
    cmd_export,
    cmd_generate_workflow,
    cmd_lineage,
    cmd_monitor,
    cmd_preflight,
    cmd_profile,
    cmd_rollback,
    cmd_schema_drift,
    cmd_search,
    cmd_snapshot,
    cmd_stats,
    cmd_sync,
    cmd_terraform,
    cmd_validate,
    setup_logging,
)
from src.config import load_config


# ── Helpers ──────────────────────────────────────────────────────────


def _write_test_config(tmp_path, overrides=None):
    """Write a minimal valid config YAML and return its path."""
    base = {
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "DEEP",
        "sql_warehouse_id": "test-wh-id",
        "load_type": "FULL",
        "max_workers": 4,
        "exclude_schemas": ["information_schema", "default"],
    }
    if overrides:
        base.update(overrides)
    path = str(tmp_path / "test_config.yaml")
    with open(path, "w") as f:
        yaml.dump(base, f)
    return path


def _make_args(config, **kwargs):
    """Build an argparse.Namespace with sane defaults for cmd_* functions."""
    defaults = dict(
        config=config,
        profile=None,
        verbose=False,
        log_file=None,
        host=None,
        token=None,
        auth_profile=None,
        serverless=False,
        warehouse_id=None,
        max_parallel_queries=None,
        volume=None,
        verify_auth=False,
        login=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# Common patches used by most cmd_* tests
_PATCH_AUTH = patch("src.main._get_auth_client", return_value=MagicMock())
_PATCH_WH = patch("src.main._resolve_warehouse_id", return_value="test-wh-id")


# ═══════════════════════════════════════════════════════════════════
# PART 1: CLI Argument Parsing
# ═══════════════════════════════════════════════════════════════════


class TestCLIParsing:
    """Verify every HOWTO example command line parses without error."""

    @pytest.fixture(autouse=True)
    def _parser(self):
        self.parser = build_parser()

    @pytest.mark.parametrize("argv", [
        # Section 1: Clone a catalog
        ["clone", "--source", "prod", "--dest", "sandbox"],
        # Section 2: Deep vs shallow
        ["clone", "--source", "prod", "--dest", "qa", "--clone-type", "DEEP"],
        ["clone", "--source", "prod", "--dest", "dev", "--clone-type", "SHALLOW"],
        # Section 3: Full vs incremental
        ["clone", "--source", "prod", "--dest", "stg", "--load-type", "FULL"],
        ["clone", "--source", "prod", "--dest", "stg", "--load-type", "INCREMENTAL"],
        # Section 4: Time travel
        ["clone", "--source", "prod", "--dest", "recovery", "--as-of-timestamp", "2026-03-04T23:59:59"],
        ["clone", "--source", "prod", "--dest", "recovery", "--as-of-version", "42"],
        # Section 5: Dry run
        ["clone", "--dry-run", "-v"],
        # Section 7: Schema filtering
        ["clone", "--include-schemas", "sales", "marketing", "analytics"],
        # Section 8: Regex table filtering
        ["clone", "--include-tables-regex", "^fact_|^dim_"],
        ["clone", "--exclude-tables-regex", "_tmp$|_backup$"],
        # Section 10: Parallel processing
        ["clone", "--max-workers", "8", "--parallel-tables", "4"],
        ["clone", "--max-parallel-queries", "20"],
        # Section 11: Table size ordering
        ["clone", "--order-by-size", "asc"],
        ["clone", "--order-by-size", "desc"],
        # Section 12: Rate limiting
        ["clone", "--max-rps", "5"],
        # Section 13: Permissions
        ["clone", "--source", "prod", "--dest", "dev", "--no-permissions", "--no-ownership"],
        # Section 14: Tags & properties
        ["clone", "--no-tags", "--no-properties"],
        # Section 15: Security
        ["clone", "--no-security"],
        # Section 16: Constraints & comments
        ["clone", "--no-constraints", "--no-comments"],
        # Section 28: Rollback + resume
        ["clone", "--enable-rollback", "--validate", "--checksum", "--report", "--progress"],
        ["clone", "--resume", "rollback_logs/log.json"],
        # Section 37: Cross-workspace
        ["clone", "--source", "prod", "--dest", "dr", "--dest-host", "https://h", "--dest-token", "t"],
        # Section 0b: Serverless
        ["clone", "--source", "prod", "--dest", "stg", "--serverless"],
        # Non-clone commands
        ["diff", "--source", "prod", "--dest", "stg"],
        ["validate", "--source", "prod", "--dest", "stg", "--checksum"],
        ["preflight"],
        ["search", "--source", "prod", "--pattern", "email|phone", "--columns"],
        ["stats", "--source", "prod"],
        ["profile", "--source", "prod"],
        ["monitor", "--source", "prod", "--dest", "dr", "--once"],
        ["snapshot", "--source", "prod"],
        ["schema-drift", "--source", "prod", "--dest", "stg"],
        ["compare", "--source", "prod", "--dest", "stg"],
        ["sync", "--source", "prod", "--dest", "stg", "--drop-extra"],
        ["rollback", "--list"],
        ["estimate", "--source", "prod", "--price-per-gb", "0.03"],
        ["export", "--source", "prod", "--format", "csv"],
        ["export", "--source", "prod", "--format", "json"],
        ["export-iac", "--source", "prod", "--format", "pulumi"],
        ["export-iac", "--source", "prod", "--format", "terraform"],
        ["generate-workflow", "--format", "yaml"],
        ["generate-workflow", "--format", "json"],
        ["config-diff", "a.yaml", "b.yaml"],
        ["completion", "bash"],
        ["completion", "zsh"],
        ["completion", "fish"],
        ["init"],
        ["auth", "--list-profiles"],
        ["cost-estimate", "--source", "prod"],
        ["pii-scan", "--source", "prod"],
        ["audit", "--init"],
        ["lineage", "--init"],
    ], ids=lambda a: " ".join(a))
    def test_cli_parses(self, argv):
        args = self.parser.parse_args(argv)
        assert args.command == argv[0]
        assert hasattr(args, "func")


# ═══════════════════════════════════════════════════════════════════
# PART 2: Feature Tests by HOWTO Section
# ═══════════════════════════════════════════════════════════════════


class TestHowto00_Auth:
    """Section 0: Authentication & Login."""

    @patch("src.main.ensure_authenticated", return_value={"user": "u", "host": "h", "auth_method": "pat"})
    def test_auth_status(self, mock_auth):
        args = argparse.Namespace(
            host=None, token=None, auth_profile=None,
            list_profiles=False, login=False, verbose=False,
        )
        cmd_auth(args)
        mock_auth.assert_called_once()

    @patch("src.main.list_profiles", return_value=[{"name": "default", "host": "h", "auth_type": "pat"}])
    def test_auth_list_profiles(self, mock_lp):
        args = argparse.Namespace(
            host=None, token=None, auth_profile=None,
            list_profiles=True, login=False, verbose=False,
        )
        cmd_auth(args)
        mock_lp.assert_called_once()

    @patch("src.main.interactive_login")
    def test_auth_login_interactive(self, mock_login):
        args = argparse.Namespace(
            host=None, token=None, auth_profile=None,
            list_profiles=False, login=True, verbose=False,
        )
        cmd_auth(args)
        mock_login.assert_called_once()


class TestHowto00b_Serverless:
    """Section 0b: Serverless Compute."""

    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.serverless.submit_clone_job", return_value={
        "tables": {"success": 5, "failed": 0},
        "views": {"success": 0, "failed": 0},
        "functions": {"success": 0, "failed": 0},
        "volumes": {"success": 0, "failed": 0},
    })
    def test_clone_serverless(self, mock_submit, mock_auth, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="prod", dest="stg", serverless=True,
                          volume="/Volumes/cat/s/v",
                          clone_type=None, load_type=None, max_workers=None,
                          no_permissions=False, no_ownership=False, no_tags=False,
                          no_properties=False, no_security=False, no_constraints=False,
                          no_comments=False, dry_run=False, include_schemas=None,
                          report=False, enable_rollback=False, validate=False,
                          checksum=False, parallel_tables=None,
                          include_tables_regex=None, exclude_tables_regex=None,
                          resume=None, progress=False, no_progress=False,
                          order_by_size=None, max_rps=None,
                          dest_host=None, dest_token=None, dest_warehouse_id=None,
                          as_of_timestamp=None, as_of_version=None, location=None)
        cmd_clone(args)
        mock_submit.assert_called_once()


# Helper: common clone args namespace
def _clone_args(config, **overrides):
    defaults = dict(
        source="src_cat", dest="dst_cat",
        clone_type=None, load_type=None, max_workers=None,
        no_permissions=False, no_ownership=False, no_tags=False,
        no_properties=False, no_security=False, no_constraints=False,
        no_comments=False, dry_run=False, include_schemas=None,
        report=False, enable_rollback=False, validate=False,
        checksum=False, parallel_tables=None,
        include_tables_regex=None, exclude_tables_regex=None,
        resume=None, progress=False, no_progress=False,
        order_by_size=None, max_rps=None,
        dest_host=None, dest_token=None, dest_warehouse_id=None,
        as_of_timestamp=None, as_of_version=None, location=None,
    )
    defaults.update(overrides)
    return _make_args(config, **defaults)


# Shared mock return value for clone_catalog
_CLONE_OK = {
    "tables": {"success": 5, "failed": 0},
    "views": {"success": 2, "failed": 0},
    "functions": {"success": 1, "failed": 0},
    "volumes": {"success": 0, "failed": 0},
}


class TestHowto01_Clone:
    """Section 1: Clone a Catalog."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_clone_basic(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg)
        cmd_clone(args)
        mock_clone.assert_called_once()
        call_config = mock_clone.call_args[0][1]
        assert call_config["source_catalog"] == "src_cat"
        assert call_config["destination_catalog"] == "dst_cat"


class TestHowto02_CloneType:
    """Section 2: Deep vs Shallow Clone."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_deep_clone(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, clone_type="DEEP")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["clone_type"] == "DEEP"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_shallow_clone(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, clone_type="SHALLOW")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["clone_type"] == "SHALLOW"


class TestHowto03_LoadType:
    """Section 3: Full vs Incremental Load."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_full_load(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, load_type="FULL")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["load_type"] == "FULL"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_incremental_load(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, load_type="INCREMENTAL")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["load_type"] == "INCREMENTAL"


class TestHowto04_TimeTravel:
    """Section 4: Time Travel Clone."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_as_of_timestamp(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, as_of_timestamp="2026-03-04T23:59:59")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["as_of_timestamp"] == "2026-03-04T23:59:59"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_as_of_version(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, as_of_version=42)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["as_of_version"] == 42


class TestHowto05_DryRun:
    """Section 5: Dry Run Mode."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_dry_run(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, dry_run=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["dry_run"] is True


class TestHowto06_Preflight:
    """Section 6: Pre-Flight Checks."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.preflight.run_preflight", return_value={"ready": True, "checks": []})
    def test_preflight_pass(self, mock_pf, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", no_write_check=False)
        cmd_preflight(args)
        mock_pf.assert_called_once()

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.preflight.run_preflight", return_value={"ready": False, "checks": []})
    def test_preflight_fail_exits(self, mock_pf, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", no_write_check=False)
        with pytest.raises(SystemExit):
            cmd_preflight(args)


class TestHowto07_SchemaFilter:
    """Section 7: Schema Filtering."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_include_schemas_cli(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, include_schemas=["sales", "marketing"])
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["include_schemas"] == ["sales", "marketing"]

    def test_exclude_schemas_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"exclude_schemas": ["info", "tmp", "scratch"]})
        config = load_config(cfg)
        assert config["exclude_schemas"] == ["info", "tmp", "scratch"]


class TestHowto08_RegexFilter:
    """Section 8: Regex Table Filtering."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_include_tables_regex(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, include_tables_regex="^fact_|^dim_")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["include_tables_regex"] == "^fact_|^dim_"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_exclude_tables_regex(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, exclude_tables_regex="_tmp$|_backup$")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["exclude_tables_regex"] == "_tmp$|_backup$"


class TestHowto09_TagFilter:
    """Section 9: Tag-Based Filtering."""

    def test_filter_by_tags_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"filter_by_tags": {"pii_level": "none", "environment": "shareable"}})
        config = load_config(cfg)
        assert config["filter_by_tags"] == {"pii_level": "none", "environment": "shareable"}


class TestHowto10_Parallel:
    """Section 10: Parallel Processing."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_max_workers(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, max_workers=8)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["max_workers"] == 8

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_parallel_tables(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, parallel_tables=4)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["parallel_tables"] == 4


class TestHowto11_SizeOrder:
    """Section 11: Table Size Ordering."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_order_by_size(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, order_by_size="desc")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["order_by_size"] == "desc"


class TestHowto12_RateLimit:
    """Section 12: Rate Limiting."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_max_rps(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, max_rps=5.0)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["max_rps"] == 5.0


class TestHowto13_Permissions:
    """Section 13: Permissions & Ownership."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_permissions(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_permissions=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_permissions"] is False

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_ownership(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_ownership=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_ownership"] is False


class TestHowto14_TagsProperties:
    """Section 14: Tags & Properties."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_tags(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_tags=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_tags"] is False

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_properties(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_properties=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_properties"] is False


class TestHowto15_Security:
    """Section 15: Security Policies."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_security(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_security=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_security"] is False


class TestHowto16_Constraints:
    """Section 16: Constraints & Comments."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_constraints(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_constraints=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_constraints"] is False

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_comments(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_comments=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["copy_comments"] is False


class TestHowto17_Masking:
    """Section 17: Data Masking."""

    def test_masking_rules_config(self, tmp_path):
        rules = [
            {"column": "email", "strategy": "email_mask", "match_type": "exact"},
            {"column": "ssn|phone", "strategy": "redact", "match_type": "regex"},
        ]
        cfg = _write_test_config(tmp_path, {"masking_rules": rules})
        config = load_config(cfg)
        assert len(config["masking_rules"]) == 2
        assert config["masking_rules"][0]["strategy"] == "email_mask"


class TestHowto18_Hooks:
    """Section 18: Pre/Post Hooks."""

    def test_pre_clone_hooks_config(self, tmp_path):
        hooks = [{"sql": "SELECT 1", "description": "Health check", "on_error": "fail"}]
        cfg = _write_test_config(tmp_path, {"pre_clone_hooks": hooks})
        config = load_config(cfg)
        assert len(config["pre_clone_hooks"]) == 1
        assert config["pre_clone_hooks"][0]["on_error"] == "fail"

    def test_post_clone_hooks_config(self, tmp_path):
        hooks = [{"sql": "OPTIMIZE dst.sales.orders", "description": "Compact", "on_error": "warn"}]
        cfg = _write_test_config(tmp_path, {"post_clone_hooks": hooks})
        config = load_config(cfg)
        assert len(config["post_clone_hooks"]) == 1


class TestHowto19_Validation:
    """Section 19: Validation."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.validation.validate_catalog", return_value={"total_tables": 10, "matched": 10, "mismatched": 0, "errors": 0})
    def test_validate_pass(self, mock_val, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", checksum=False)
        cmd_validate(args)
        mock_val.assert_called_once()

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.validation.validate_catalog", return_value={"total_tables": 10, "matched": 8, "mismatched": 2, "errors": 0})
    def test_validate_mismatch_exits(self, mock_val, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", checksum=False)
        with pytest.raises(SystemExit):
            cmd_validate(args)

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.validation.validate_catalog", return_value={"total_tables": 10, "matched": 10, "mismatched": 0, "errors": 0})
    def test_validate_checksum(self, mock_val, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", checksum=True)
        cmd_validate(args)
        assert mock_val.call_args[1]["use_checksum"] is True


class TestHowto20_SchemaDrift:
    """Section 20: Schema Drift Detection."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.schema_drift.detect_schema_drift", return_value={"tables_checked": 10, "tables_with_drift": 0, "drifts": []})
    def test_schema_drift(self, mock_drift, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat")
        cmd_schema_drift(args)
        mock_drift.assert_called_once()


class TestHowto21_Profile:
    """Section 21: Data Profiling."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.profiling.profile_catalog", return_value={"tables_profiled": 5})
    def test_profile(self, mock_prof, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", output=None)
        cmd_profile(args)
        mock_prof.assert_called_once()


class TestHowto22_Search:
    """Section 22: Catalog Search."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.search.search_tables")
    def test_search(self, mock_search, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", pattern="email|phone", columns=True)
        cmd_search(args)
        mock_search.assert_called_once()
        assert mock_search.call_args[1]["search_columns"] is True


class TestHowto23_Stats:
    """Section 23: Catalog Statistics."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.stats.catalog_stats")
    def test_stats(self, mock_stats, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat")
        cmd_stats(args)
        mock_stats.assert_called_once()


class TestHowto24_Diff:
    """Section 24: Catalog Diff."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.diff.print_diff")
    @patch("src.diff.compare_catalogs", return_value={"schemas": {}, "tables": {}})
    def test_diff(self, mock_cmp, mock_print, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat")
        cmd_diff(args)
        mock_cmp.assert_called_once()
        mock_print.assert_called_once()


class TestHowto25_Compare:
    """Section 25: Deep Compare."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.compare.compare_catalogs_deep", return_value={"tables_compared": 10, "tables_with_issues": 0})
    def test_compare(self, mock_cmp, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat")
        cmd_compare(args)
        mock_cmp.assert_called_once()


class TestHowto26_Sync:
    """Section 26: Two-Way Sync."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.sync_catalog.sync_catalogs", return_value={"added": 3, "dropped": 0, "errors": []})
    def test_sync(self, mock_sync, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", dry_run=False, drop_extra=False)
        cmd_sync(args)
        mock_sync.assert_called_once()

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.sync_catalog.sync_catalogs", return_value={"added": 0, "dropped": 2, "errors": []})
    def test_sync_drop_extra(self, mock_sync, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat", dry_run=False, drop_extra=True)
        cmd_sync(args)
        assert mock_sync.call_args[1]["drop_extra"] is True


class TestHowto27_Monitor:
    """Section 27: Continuous Monitoring."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.monitor.monitor_once", return_value={"in_sync": True})
    def test_monitor_once(self, mock_mon, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", dest="dst_cat",
                          once=True, interval=30, max_checks=0,
                          check_drift=True, check_counts=False)
        cmd_monitor(args)
        mock_mon.assert_called_once()


class TestHowto28_Rollback:
    """Section 28: Rollback."""

    @patch("src.rollback.list_rollback_logs", return_value=[
        {"file": "log.json", "timestamp": "2026-03-10", "destination_catalog": "stg", "total_objects": 5},
    ])
    def test_rollback_list(self, mock_list):
        args = argparse.Namespace(list=True, rollback_log_file=None,
                                  config="x", profile=None, warehouse_id=None,
                                  serverless=False, verbose=False)
        cmd_rollback(args)
        mock_list.assert_called_once()

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.rollback.rollback", return_value={"success": 5, "failed": 0})
    def test_rollback_execute(self, mock_rb, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, list=False, rollback_log_file="log.json", drop_catalog=False)
        cmd_rollback(args)
        mock_rb.assert_called_once()


class TestHowto29_Resume:
    """Section 29: Resume from Failure."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_resume(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, resume="rollback_logs/rollback_20260310.json")
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["resume"] == "rollback_logs/rollback_20260310.json"


class TestHowto30_Snapshot:
    """Section 30: Catalog Snapshot."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.snapshot.create_snapshot", return_value="snapshot.json")
    def test_snapshot(self, mock_snap, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", output=None)
        cmd_snapshot(args)
        mock_snap.assert_called_once()


class TestHowto31_Export:
    """Section 31: Export Metadata."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.export.export_catalog_metadata", return_value="export.csv")
    def test_export_csv(self, mock_exp, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", format="csv", output=None)
        cmd_export(args)
        assert mock_exp.call_args[1]["output_format"] == "csv"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.export.export_catalog_metadata", return_value="export.json")
    def test_export_json(self, mock_exp, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", format="json", output=None)
        cmd_export(args)
        assert mock_exp.call_args[1]["output_format"] == "json"


class TestHowto32_CostEstimation:
    """Section 32: Cost Estimation."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.cost_estimation.estimate_clone_cost", return_value={"total_size_gb": 100, "monthly_cost": 2.3})
    def test_estimate(self, mock_est, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", price_per_gb=0.023)
        cmd_estimate(args)
        mock_est.assert_called_once()


class TestHowto33_Profiles:
    """Section 33: Config Profiles."""

    def test_profile_staging(self, tmp_path):
        cfg = _write_test_config(tmp_path, {
            "profiles": {
                "staging": {"destination_catalog": "staging_cat", "clone_type": "SHALLOW"},
                "dr": {"destination_catalog": "dr_cat", "enable_rollback": True},
            }
        })
        config = load_config(cfg, profile="staging")
        assert config["destination_catalog"] == "staging_cat"
        assert config["clone_type"] == "SHALLOW"

    def test_profile_unknown_raises(self, tmp_path):
        cfg = _write_test_config(tmp_path, {
            "profiles": {"staging": {"clone_type": "SHALLOW"}}
        })
        with pytest.raises(ValueError, match="Unknown config profile"):
            load_config(cfg, profile="nonexistent")


class TestHowto34_ConfigDiff:
    """Section 34: Config Diff."""

    @patch("src.config_diff.print_config_diff")
    def test_config_diff(self, mock_diff):
        args = argparse.Namespace(file_a="a.yaml", file_b="b.yaml", verbose=False, log_file=None)
        cmd_config_diff(args)
        mock_diff.assert_called_once_with("a.yaml", "b.yaml")


class TestHowto35_Workflow:
    """Section 35: Workflow Generation."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.workflow.generate_workflow")
    def test_generate_workflow_json(self, mock_gen, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, format="json", output=None, job_name=None,
                          cluster_id=None, schedule=None, notification_email=None)
        cmd_generate_workflow(args)
        mock_gen.assert_called_once()


class TestHowto36_Terraform:
    """Section 36: Terraform / Pulumi Export."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.terraform.generate_terraform")
    def test_export_terraform(self, mock_tf, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", format="terraform", output=None)
        cmd_terraform(args)
        mock_tf.assert_called_once()

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.terraform.generate_pulumi")
    def test_export_pulumi(self, mock_pu, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, source="src_cat", format="pulumi", output=None)
        cmd_terraform(args)
        mock_pu.assert_called_once()


class TestHowto37_CrossWorkspace:
    """Section 37: Cross-Workspace Cloning."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main.get_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_cross_workspace(self, mock_clone, mock_get_client, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, dest_host="https://dr.cloud.databricks.com",
                           dest_token="dapi_token", dest_warehouse_id="dr-wh-id")
        cmd_clone(args)
        config = mock_clone.call_args[0][1]
        assert config["dest_workspace"]["host"] == "https://dr.cloud.databricks.com"
        assert config["dest_workspace"]["token"] == "dapi_token"


class TestHowto38_Lineage:
    """Section 38: Lineage Tracking."""

    def test_lineage_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"lineage": {"catalog": "gov", "schema": "lineage"}})
        config = load_config(cfg)
        assert config["lineage"]["catalog"] == "gov"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.lineage_tracker.ensure_lineage_table")
    def test_lineage_init(self, mock_init, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, init=True, table=None, operation_id=None, limit=50)
        cmd_lineage(args)
        mock_init.assert_called_once()


class TestHowto39_Report:
    """Section 39: Reporting."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_report_flag(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, report=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["generate_report"] is True


class TestHowto40_Notifications:
    """Section 40: Notifications."""

    def test_slack_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"slack_webhook_url": "https://hooks.slack.com/xxx"})
        config = load_config(cfg)
        assert config["slack_webhook_url"] == "https://hooks.slack.com/xxx"

    def test_teams_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"teams_webhook_url": "https://outlook.office.com/xxx"})
        config = load_config(cfg)
        assert config["teams_webhook_url"] == "https://outlook.office.com/xxx"

    def test_email_config(self, tmp_path):
        email_cfg = {"smtp_host": "smtp.gmail.com", "smtp_port": 587, "sender": "bot@co.com",
                     "recipients": ["team@co.com"]}
        cfg = _write_test_config(tmp_path, {"email": email_cfg})
        config = load_config(cfg)
        assert config["email"]["smtp_host"] == "smtp.gmail.com"

    def test_webhook_config(self, tmp_path):
        webhook = {"url": "https://pagerduty.com/v2/enqueue", "headers": {"Authorization": "Bearer tok"}}
        cfg = _write_test_config(tmp_path, {"webhook": webhook})
        config = load_config(cfg)
        assert config["webhook"]["url"] == "https://pagerduty.com/v2/enqueue"


class TestHowto41_Audit:
    """Section 41: Audit Logging."""

    def test_audit_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"audit": {"catalog": "gov", "schema": "audit", "table": "clone_audit_log"}})
        config = load_config(cfg)
        assert config["audit"]["catalog"] == "gov"

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.audit_trail.ensure_audit_table")
    def test_audit_init(self, mock_init, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _make_args(cfg, init=True, source=None, status=None, limit=20)
        cmd_audit(args)
        mock_init.assert_called_once()


class TestHowto42_Retry:
    """Section 42: Retry Policy."""

    def test_retry_config(self, tmp_path):
        cfg = _write_test_config(tmp_path, {"max_retries": 5})
        config = load_config(cfg)
        assert config["max_retries"] == 5


class TestHowto43_Completions:
    """Section 43: Shell Completions."""

    @patch("src.completions.install_completions")
    def test_completion_bash(self, mock_comp):
        args = argparse.Namespace(shell="bash")
        cmd_completion(args)
        mock_comp.assert_called_once_with("bash")

    @patch("src.completions.install_completions")
    def test_completion_zsh(self, mock_comp):
        args = argparse.Namespace(shell="zsh")
        cmd_completion(args)
        mock_comp.assert_called_once_with("zsh")

    @patch("src.completions.install_completions")
    def test_completion_fish(self, mock_comp):
        args = argparse.Namespace(shell="fish")
        cmd_completion(args)
        mock_comp.assert_called_once_with("fish")


class TestHowto44_Wizard:
    """Section 44: Config Wizard."""

    @patch("src.wizard.run_wizard")
    def test_init(self, mock_wiz):
        args = argparse.Namespace(output="config/clone_config.yaml")
        from src.main import cmd_init
        cmd_init(args)
        mock_wiz.assert_called_once()


class TestHowto45_Progress:
    """Section 45: Progress Bar & Logging."""

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_progress_flag(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, progress=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["show_progress"] is True

    @patch("src.main._resolve_warehouse_id", return_value="test-wh-id")
    @patch("src.main._get_auth_client", return_value=MagicMock())
    @patch("src.main.clone_catalog", return_value=_CLONE_OK)
    def test_no_progress_flag(self, mock_clone, mock_auth, mock_wh, tmp_path):
        cfg = _write_test_config(tmp_path)
        args = _clone_args(cfg, no_progress=True)
        cmd_clone(args)
        assert mock_clone.call_args[0][1]["show_progress"] is False

    def test_setup_logging_verbose(self):
        """Verify setup_logging doesn't raise with verbose=True."""
        setup_logging(verbose=True)

    def test_setup_logging_with_file(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(verbose=False, log_file=log_file)
        assert os.path.exists(log_file)


class TestHowto46_NotebookAPI:
    """Section 46: Notebook API (Wheel & Repo)."""

    def test_python_api_imports(self):
        """Verify the public API can be imported."""
        from src.catalog_clone_api import (
            clone_full_catalog,
            clone_schema,
            clone_single_table,
            compare_catalogs,
            run_preflight_checks,
            validate_clone,
        )
        assert callable(clone_full_catalog)
        assert callable(clone_schema)
        assert callable(clone_single_table)
        assert callable(compare_catalogs)
        assert callable(run_preflight_checks)
        assert callable(validate_clone)
