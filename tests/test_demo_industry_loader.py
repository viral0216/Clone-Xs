"""Tests for src/demo_industry_loader.py — YAML custom industry templates."""

from pathlib import Path

import pytest

from src.demo_industry_loader import (
    load_yaml_industries,
    merge_into_industries,
)


def _write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


VALID_YAML = """\
name: aerospace
description: Custom aerospace demo
tables:
  - name: flights
    rows: 1000
    ddl_cols: "flight_id BIGINT, carrier STRING, dep_date DATE"
    insert_expr: "id + {offset} AS flight_id, 'UA' AS carrier, current_date() AS dep_date"
"""


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestLoad:
    def test_loads_valid_yaml(self, tmp_path):
        p = _write(tmp_path, "aerospace.yaml", VALID_YAML)
        out = load_yaml_industries([p])
        assert "aerospace" in out
        assert out["aerospace"]["tables"][0]["name"] == "flights"
        assert out["aerospace"]["tables"][0]["rows"] == 1000

    def test_loads_multiple_files(self, tmp_path):
        p1 = _write(tmp_path, "aero.yaml", VALID_YAML)
        # second yaml: same shape, different industry name
        p2 = _write(tmp_path, "robotics.yaml", VALID_YAML.replace("aerospace", "robotics"))
        out = load_yaml_industries([p1, p2])
        assert set(out) == {"aerospace", "robotics"}


# ---------------------------------------------------------------------------
# Validation errors — each must include the offending file path
# ---------------------------------------------------------------------------


class TestValidation:
    def test_missing_file_raises_clearly(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            load_yaml_industries([tmp_path / "no.yaml"])

    def test_malformed_yaml_includes_path(self, tmp_path):
        p = _write(tmp_path, "bad.yaml", "name: aerospace\n  bad indent\n")
        with pytest.raises(ValueError, match="bad.yaml"):
            load_yaml_industries([p])

    def test_empty_file_raises(self, tmp_path):
        p = _write(tmp_path, "empty.yaml", "")
        with pytest.raises(ValueError, match="empty"):
            load_yaml_industries([p])

    def test_missing_top_level_keys(self, tmp_path):
        # `tables:` missing
        p = _write(tmp_path, "no_tables.yaml", "name: aerospace\n")
        with pytest.raises(ValueError, match="missing required keys"):
            load_yaml_industries([p])

    def test_reserved_industry_name_rejected(self, tmp_path):
        """`healthcare` is a built-in — refusing this prevents a custom file
        from silently shadowing the real industry."""
        bad = VALID_YAML.replace("aerospace", "healthcare")
        p = _write(tmp_path, "shadow.yaml", bad)
        with pytest.raises(ValueError, match="reserved|clashes"):
            load_yaml_industries([p])

    def test_table_missing_required_keys(self, tmp_path):
        # rows missing
        bad = """
name: aero
tables:
  - name: flights
    ddl_cols: "id BIGINT"
    insert_expr: "id"
"""
        p = _write(tmp_path, "missing_rows.yaml", bad)
        with pytest.raises(ValueError, match="missing keys"):
            load_yaml_industries([p])

    def test_negative_rows_rejected(self, tmp_path):
        bad = """
name: aero
tables:
  - name: flights
    rows: -1
    ddl_cols: "id BIGINT"
    insert_expr: "id"
"""
        p = _write(tmp_path, "neg.yaml", bad)
        with pytest.raises(ValueError, match="non-negative"):
            load_yaml_industries([p])

    def test_invalid_industry_name_rejected(self, tmp_path):
        bad = VALID_YAML.replace("name: aerospace", "name: with spaces")
        p = _write(tmp_path, "ws.yaml", bad)
        with pytest.raises(ValueError, match="snake_case"):
            load_yaml_industries([p])

    def test_duplicate_industry_across_files(self, tmp_path):
        p1 = _write(tmp_path, "a.yaml", VALID_YAML)
        p2 = _write(tmp_path, "b.yaml", VALID_YAML)  # same name twice
        with pytest.raises(ValueError, match="Duplicate"):
            load_yaml_industries([p1, p2])


# ---------------------------------------------------------------------------
# merge_into_industries — base must NOT be mutated
# ---------------------------------------------------------------------------


class TestMerge:
    def test_returns_new_dict_doesnt_mutate_base(self):
        base = {"healthcare": {"existing": True}}
        custom = {"aerospace": {"tables": []}}
        merged = merge_into_industries(base, custom)
        assert "aerospace" in merged
        assert "aerospace" not in base, "merge_into_industries must NOT mutate base"

    def test_clash_with_builtin_raises(self):
        base = {"healthcare": {"existing": True}}
        custom = {"healthcare": {"hijacked": True}}
        with pytest.raises(ValueError, match="clash|reserved"):
            merge_into_industries(base, custom)
