"""Tests for src/demo_code.py — code type registry, per-language
generators, preview math, and the orchestrator's destination dispatch.

Mirrors tests/test_demo_logs.py and tests/test_demo_knowledge.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.demo_code import (
    CODE_TYPES,
    _make_repo_name,
    _sql_str,
    is_available,
    preview_code,
)


# ── Registry shape ────────────────────────────────────────────────


def test_registry_contains_expected_code_types():
    """Three planned languages, each with the required keys."""
    assert set(CODE_TYPES.keys()) == {"python_repo", "js_repo", "java_repo"}
    for type_id, info in CODE_TYPES.items():
        for key in ("category", "label", "extension", "language", "gen_fn"):
            assert key in info, f"{type_id} missing {key}"


def test_is_available_always_true():
    available, reason = is_available()
    assert available is True
    assert reason is None


def test_make_repo_name_uses_industry_prefix_and_uuid_suffix():
    """Repo names follow `<industry-prefix>-<suffix>-<6-hex>` so the
    corpus has industry-coherent naming."""
    import random

    random.seed(42)
    name = _make_repo_name("financial")
    parts = name.split("-")
    assert len(parts) >= 3, f"unexpected name shape: {name}"
    # The hex suffix is 6 chars.
    assert len(parts[-1]) == 6
    # Industry prefix should come from the financial pool.
    assert parts[0] in {"payments", "ledger", "fraud", "kyc", "trading", "risk"}


def test_make_repo_name_falls_back_for_unknown_industry():
    name = _make_repo_name("not_a_real_industry")
    parts = name.split("-")
    # Fallback prefixes: app, service, platform.
    assert parts[0] in {"app", "service", "platform"}


# ── Per-language generators ───────────────────────────────────────


def test_gen_python_repo_produces_runnable_shape():
    """Python repo should have README, pyproject, src/, tests/."""
    from faker import Faker

    from src.demo_code import _gen_python_repo

    fkr = Faker()
    fkr.seed_instance(42)
    files, meta = _gen_python_repo("healthcare", fkr, None)
    assert meta["language"] == "python"
    assert meta["file_count"] == len(files)
    assert meta["src_count"] >= 15
    assert meta["test_count"] >= 5

    paths = {p for p, _ in files}
    assert "README.md" in paths
    assert "pyproject.toml" in paths
    assert any(p.startswith("src/") and p.endswith(".py") for p in paths)
    assert any(p.startswith("tests/test_") and p.endswith(".py") for p in paths)


def test_gen_js_repo_produces_node_shape():
    """JS repo should have README, package.json, src/, tests/."""
    from faker import Faker

    from src.demo_code import _gen_js_repo

    fkr = Faker()
    fkr.seed_instance(42)
    files, meta = _gen_js_repo("retail", fkr, None)
    assert meta["language"] == "javascript"
    paths = {p for p, _ in files}
    assert "README.md" in paths
    assert "package.json" in paths
    assert any(p.startswith("src/") and p.endswith(".js") for p in paths)
    assert any(p.startswith("tests/") and p.endswith(".test.js") for p in paths)
    # package.json should be valid JSON.
    import json

    pkg_json = next(c for p, c in files if p == "package.json")
    parsed = json.loads(pkg_json)
    assert parsed["name"] == meta["repo_name"]
    assert "scripts" in parsed


def test_gen_java_repo_produces_maven_shape():
    """Java repo should have README, pom.xml, src/main/java/, src/test/java/."""
    from faker import Faker

    from src.demo_code import _gen_java_repo

    fkr = Faker()
    fkr.seed_instance(42)
    files, meta = _gen_java_repo("financial", fkr, None)
    assert meta["language"] == "java"
    assert meta["package"].startswith("com.example.")
    paths = {p for p, _ in files}
    assert "README.md" in paths
    assert "pom.xml" in paths
    assert any(p.startswith("src/main/java/com/example/") and p.endswith(".java") for p in paths)
    assert any(
        p.startswith("src/test/java/com/example/") and p.endswith("Test.java") for p in paths
    )
    # pom.xml should reference the repo name.
    pom = next(c for p, c in files if p == "pom.xml")
    assert meta["repo_name"] in pom


def test_python_module_content_includes_class_and_functions():
    """Spot-check one generated Python module for class + function defs."""
    from faker import Faker

    from src.demo_code import _gen_python_module

    fkr = Faker()
    fkr.seed_instance(42)
    fname, content = _gen_python_module("financial", fkr, None)
    assert fname.endswith(".py")
    assert "class " in content
    assert "def " in content
    assert "from __future__ import annotations" in content


def test_java_class_content_uses_camelcase_methods():
    """Spot-check the Java class generator: PascalCase class +
    camelCase methods."""
    from faker import Faker

    from src.demo_code import _gen_java_class

    fkr = Faker()
    fkr.seed_instance(42)
    fname, content = _gen_java_class("financial", fkr, None, "com.example.fin")
    assert fname.endswith(".java")
    # Class name in the filename matches the class declaration.
    cls_name = fname[:-5]
    assert f"public class {cls_name}" in content
    assert "package com.example.fin;" in content


# ── SQL helper ────────────────────────────────────────────────────


def test_sql_str_escapes_single_quotes_and_handles_none():
    assert _sql_str("hello") == "'hello'"
    assert _sql_str("it's") == "'it''s'"
    assert _sql_str(None) == "NULL"
    assert _sql_str("") == "''"


# ── Preview math ──────────────────────────────────────────────────


def test_preview_returns_per_type_repo_and_file_totals():
    out = preview_code(
        {
            "types": ["python_repo", "js_repo"],
            "counts": {"python_repo": 3, "js_repo": 2},
        }
    )
    assert len(out["per_type"]) == 2
    assert out["total_repos"] == 5
    # Each repo ~30 files, so ~150 files total — exact value depends
    # on the per-type averages.
    assert out["total_files"] > 0
    assert out["total_bytes"] > 0
    assert out["unknown_types"] == []


def test_preview_handles_empty_input():
    out = preview_code({"types": [], "counts": {}})
    assert out["total_repos"] == 0
    assert out["total_files"] == 0
    assert out["total_bytes"] == 0


def test_preview_isolates_unknown_types():
    out = preview_code(
        {"types": ["python_repo", "go_repo"], "counts": {"python_repo": 2, "go_repo": 5}}
    )
    assert out["total_repos"] == 2  # only python counted
    assert "go_repo" in out["unknown_types"]


# ── Orchestrator ──────────────────────────────────────────────────


@pytest.fixture
def fake_client():
    client = MagicMock()
    client.files.upload = MagicMock()
    return client


def test_generate_code_rejects_unknown_destination(fake_client):
    from src.demo_code import generate_code

    with pytest.raises(ValueError, match="Unknown destination"):
        generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "not_a_dest",
                "types": ["python_repo"],
                "counts": {"python_repo": 1},
            },
        )


def test_generate_code_rejects_empty_types(fake_client):
    from src.demo_code import generate_code

    with pytest.raises(ValueError, match="must contain at least one"):
        generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "volume",
                "volume": "vol",
                "types": [],
                "counts": {},
            },
        )


def test_generate_code_rejects_unknown_type(fake_client):
    from src.demo_code import generate_code

    with pytest.raises(ValueError, match="Unknown code types"):
        generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["go_repo"],
                "counts": {"go_repo": 1},
            },
        )


def test_generate_code_volume_destination_uploads_repo_files(fake_client):
    """`volume` destination uploads each file in each repo."""
    from src.demo_code import generate_code

    with patch("src.demo_code.execute_sql"):
        result = generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["python_repo"],
                "counts": {"python_repo": 1},
                "industry": "healthcare",
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["repos_written"] == 1
    # 1 repo × ~30 files = many uploads. Must be > 20 to confirm the
    # whole repo was uploaded.
    assert fake_client.files.upload.call_count > 20
    assert result["table_fqn"] is None


def test_generate_code_direct_table_inserts_one_row_per_file(fake_client):
    """`direct_table` inserts one row per source file with content
    inline as STRING — natural shape for code-search."""
    from src.demo_code import generate_code

    with patch("src.demo_code.execute_sql") as mock_sql:
        result = generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "direct_table",
                "types": ["python_repo"],
                "counts": {"python_repo": 1},
                "industry": "healthcare",
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["files_written"] > 20
    assert result["table_fqn"] == "demo.iot.demo_code"
    # No Volume uploads on direct_table.
    fake_client.files.upload.assert_not_called()
    sql_texts = [c.args[2] for c in mock_sql.mock_calls]
    # CREATE table mentions content STRING (not BINARY).
    create_sqls = [s for s in sql_texts if "CREATE OR REPLACE TABLE" in s]
    assert any("content          STRING" in s or "content STRING" in s for s in create_sqls)
    # No unhex() in the inserts — content is text-shaped.
    insert_sqls = [s for s in sql_texts if s.strip().startswith("INSERT INTO")]
    for s in insert_sqls:
        assert "unhex(" not in s


def test_generate_code_volume_with_catalog_writes_per_file_rows(fake_client):
    """`volume_with_catalog` writes one row per FILE into the
    demo_code_catalog table."""
    from src.demo_code import generate_code

    with patch("src.demo_code.execute_sql") as mock_sql:
        result = generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume_with_catalog",
                "types": ["python_repo"],
                "counts": {"python_repo": 1},
                "industry": "healthcare",
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["table_fqn"] == "demo.iot.demo_code_catalog"
    # All files uploaded.
    assert fake_client.files.upload.call_count > 20
    sql_texts = [c.args[2] for c in mock_sql.mock_calls]
    insert_sqls = [s for s in sql_texts if s.strip().startswith("INSERT INTO")]
    # Catalog rows include language + repo_name.
    combined = " ".join(insert_sqls)
    assert "language" in combined or "repo_name" in combined


def test_generate_code_respects_stop_check(fake_client):
    """If stop_check returns True between repos, the orchestrator
    breaks out before writing the rest."""
    from src.demo_code import generate_code

    call_count = {"n": 0}

    def stop_after_one() -> bool:
        call_count["n"] += 1
        # Fire often enough to break out partway through.
        return call_count["n"] > 2

    with patch("src.demo_code.execute_sql"):
        result = generate_code(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["python_repo"],
                "counts": {"python_repo": 10},
                "industry": "healthcare",
                "faker_seed": 42,
            },
            stop_check=stop_after_one,
        )
    assert result["status"] == "completed"
    assert result["repos_written"] < 10, "stop_check should have cut the loop short"
