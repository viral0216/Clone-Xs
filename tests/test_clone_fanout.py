"""Tests for src/clone_fanout.py — multi-target fanout orchestrator.

Roadmap called this the "highest leverage, biggest test surface" feature
because partial-failure isolation is the central correctness contract:
one target failing must NOT fail others. These tests exercise the four
failure-mode scenarios from the roadmap explicitly:

1. All N targets succeed → status=success, all bytes/tables aggregated.
2. One target fails to connect (auth/network) → others continue,
   aggregate=partial.
3. One target raises mid-clone (e.g. share creation OK but DEEP CLONE
   fails) → others continue, aggregate=partial.
4. Same-metastore preflight rejects ONE target only — others run.

Plus: 0 targets is a hard error, 1 target is the degenerate case (still
routes through fanout for shape consistency).
"""

from unittest.mock import MagicMock, patch

import pytest

from src.clone_fanout import run_cross_workspace_fanout


def _target(host: str) -> dict:
    """Minimal TargetWorkspace-shaped dict for fanout tests."""
    return {
        "host": host,
        "auth_method": "pat",
        "token": f"dapi-{host}",
        "warehouse_id": f"wh-{host}",
        "data_sync_mode": "snapshot_once",
    }


def _config(*hosts: str, max_parallel: int = 5) -> dict:
    return {
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "sql_warehouse_id": "wh-source",
        "target_workspaces": [_target(h) for h in hosts],
        "fanout_max_parallel": max_parallel,
    }


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_all_targets_succeed(mock_run):
    """3 targets, all succeed → aggregate status=success, every target's
    bytes/tables roll up into the totals."""
    def succeed(_client, sub_config):
        host = sub_config["target_workspace"]["host"]
        return {
            "status": "success",
            "bytes_copied": 1000,
            "files_copied": 10,
            "tables_total": 5, "tables_cloned": 5, "tables_failed": 0,
            "share_name": f"share-{host}",
        }
    mock_run.side_effect = succeed

    result = run_cross_workspace_fanout(
        MagicMock(), _config("eu-host", "us-host", "apac-host"),
    )

    assert result["mode"] == "fanout"
    assert result["status"] == "success"
    assert result["target_count"] == 3
    assert result["succeeded_targets"] == 3
    assert result["failed_targets"] == 0
    assert result["bytes_copied"] == 3000
    assert result["files_copied"] == 30
    assert result["tables_cloned"] == 15
    assert len(result["per_target"]) == 3
    # Each per-target entry carries its host label so the UI can render
    # per-target rows in the result card.
    assert {r["target_host"] for r in result["per_target"]} == {
        "eu-host", "us-host", "apac-host",
    }


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_single_target_routed_through_fanout(mock_run):
    """Degenerate case: 1 target. Still routes through fanout for shape
    consistency (caller can switch to multi-target without changing how
    they consume the result). Aggregate has target_count=1."""
    mock_run.return_value = {
        "status": "success",
        "bytes_copied": 500, "files_copied": 5,
        "tables_total": 3, "tables_cloned": 3, "tables_failed": 0,
    }
    result = run_cross_workspace_fanout(MagicMock(), _config("only-host"))

    assert result["target_count"] == 1
    assert result["succeeded_targets"] == 1
    assert result["status"] == "success"


# ---------------------------------------------------------------------------
# Failure isolation — the central contract
# ---------------------------------------------------------------------------


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_one_target_connection_failure_does_not_fail_others(mock_run):
    """Target B fails to connect (auth/network); targets A and C continue
    and complete. Aggregate marked `partial` rather than `failed`."""
    def per_target(_client, sub_config):
        host = sub_config["target_workspace"]["host"]
        if host == "broken-host":
            raise RuntimeError("AUTH: invalid PAT")
        return {
            "status": "success",
            "bytes_copied": 100, "files_copied": 1,
            "tables_total": 2, "tables_cloned": 2, "tables_failed": 0,
        }
    mock_run.side_effect = per_target

    result = run_cross_workspace_fanout(
        MagicMock(), _config("good-host", "broken-host", "another-good-host"),
    )

    assert result["status"] == "partial"
    assert result["succeeded_targets"] == 2
    assert result["failed_targets"] == 1
    # Bytes only from the 2 successful targets, NOT from the failed one.
    assert result["bytes_copied"] == 200
    # The failed target appears in per_target with status=failed and an error.
    failed_entry = next(r for r in result["per_target"] if r["target_host"] == "broken-host")
    assert failed_entry["target_status"] == "failed"
    assert "AUTH" in failed_entry["error"]


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_one_target_mid_clone_failure_does_not_fail_others(mock_run):
    """Target B raises after share creation (e.g. DEEP CLONE fails on a
    table with column type drift). Targets A and C are unaffected — their
    independent shares/recipients/shared catalogs are unrelated. Aggregate
    is `partial`."""
    def per_target(_client, sub_config):
        host = sub_config["target_workspace"]["host"]
        if host == "midfail-host":
            raise RuntimeError(
                "DEEP CLONE failed on table users: column type changed"
            )
        return {
            "status": "success",
            "bytes_copied": 50, "files_copied": 1,
            "tables_total": 1, "tables_cloned": 1, "tables_failed": 0,
        }
    mock_run.side_effect = per_target

    result = run_cross_workspace_fanout(
        MagicMock(), _config("a", "midfail-host", "c"),
    )

    assert result["status"] == "partial"
    assert result["succeeded_targets"] == 2
    assert result["failed_targets"] == 1


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_all_targets_fail_aggregate_is_failed(mock_run):
    """If every target fails, status=failed (not partial). UI / report
    generators distinguish "everything broken" from "some broken"."""
    mock_run.side_effect = RuntimeError("source warehouse offline")

    result = run_cross_workspace_fanout(MagicMock(), _config("a", "b"))

    assert result["status"] == "failed"
    assert result["succeeded_targets"] == 0
    assert result["failed_targets"] == 2
    # All per-target entries carry the error string for diagnostics.
    for r in result["per_target"]:
        assert "source warehouse offline" in r["error"]


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_same_metastore_rejection_isolated_to_offending_target(mock_run):
    """Roadmap edge case: one target is in the SAME metastore as source
    (preflight catches it inside `run_cross_workspace_clone` and raises).
    The other targets — which ARE in different metastores — still run."""
    def per_target(_client, sub_config):
        host = sub_config["target_workspace"]["host"]
        if host == "same-meta-host":
            raise RuntimeError(
                "Source and target workspaces are in the same Unity Catalog metastore"
            )
        return {
            "status": "success",
            "bytes_copied": 10, "files_copied": 1,
            "tables_total": 1, "tables_cloned": 1, "tables_failed": 0,
        }
    mock_run.side_effect = per_target

    result = run_cross_workspace_fanout(
        MagicMock(), _config("eu", "same-meta-host", "us"),
    )

    assert result["status"] == "partial"
    assert result["failed_targets"] == 1
    rejected = next(
        r for r in result["per_target"] if r["target_host"] == "same-meta-host"
    )
    assert "same Unity Catalog metastore" in rejected["error"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_zero_targets_raises():
    """Empty `target_workspaces` is invalid — fanout has nothing to do.
    Pydantic catches this at the API boundary; this guards programmatic
    callers (CLI / tests / scripted use)."""
    with pytest.raises(ValueError, match="target_workspaces is empty"):
        run_cross_workspace_fanout(MagicMock(), _config())


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_per_target_config_strips_plural_field(mock_run):
    """Each inner cross-workspace orchestrator must receive a config with
    `target_workspace` (singular) — NOT `target_workspaces` (plural). If
    we passed plural through, the inner orchestrator could loop back into
    fanout. Verify the per-target config the inner orchestrator sees."""
    captured_configs = []
    def capture(_client, sub_config):
        captured_configs.append(dict(sub_config))
        return {
            "status": "success",
            "bytes_copied": 0, "files_copied": 0,
            "tables_total": 0, "tables_cloned": 0, "tables_failed": 0,
        }
    mock_run.side_effect = capture

    run_cross_workspace_fanout(MagicMock(), _config("a", "b"))

    for cfg in captured_configs:
        # The plural field must be stripped — would cause infinite recursion.
        assert "target_workspaces" not in cfg
        # Singular field must be set to ONE target dict.
        assert cfg["target_workspace"]["host"] in ("a", "b")


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_max_parallel_is_capped_at_target_count(mock_run):
    """fanout_max_parallel=10 with only 3 targets shouldn't spawn 10
    threads. ThreadPoolExecutor max_workers should be min(parallel, count).
    Verified indirectly: 3 targets all complete with no error."""
    mock_run.return_value = {
        "status": "success",
        "bytes_copied": 0, "files_copied": 0,
        "tables_total": 0, "tables_cloned": 0, "tables_failed": 0,
    }
    result = run_cross_workspace_fanout(
        MagicMock(), _config("a", "b", "c", max_parallel=10),
    )
    assert result["target_count"] == 3
    assert result["succeeded_targets"] == 3


@patch("src.clone_cross_workspace.run_cross_workspace_clone")
def test_runs_in_parallel(mock_run):
    """Concurrency check: 3 targets each sleeping 100ms should complete
    in well under 300ms (sequential), confirming parallel execution.
    This is the real reason fanout exists — N-region DR can take hours
    sequentially but minutes in parallel."""
    import time

    def slow(_client, sub_config):
        time.sleep(0.1)
        return {
            "status": "success",
            "bytes_copied": 0, "files_copied": 0,
            "tables_total": 0, "tables_cloned": 0, "tables_failed": 0,
        }
    mock_run.side_effect = slow

    start = time.time()
    result = run_cross_workspace_fanout(
        MagicMock(), _config("a", "b", "c", max_parallel=3),
    )
    elapsed = time.time() - start

    # Sequential would be ~0.30s; parallel ≈ 0.10s. Generous bound to avoid
    # CI flakiness while still detecting an unintentional serialization.
    assert elapsed < 0.25, f"Expected parallel execution, took {elapsed:.2f}s"
    assert result["succeeded_targets"] == 3
