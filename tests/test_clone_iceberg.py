"""Tests for src/clone_iceberg.py — Phase B preflight + recovery helpers.

These cover the two responsibilities of the module: refusing source tables
that can't be cloned cleanly (hidden partitioning) and classifying CLONE
failures so the caller knows whether CTAS would succeed.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.clone_iceberg import (
    HIDDEN_PARTITION_TRANSFORMS,
    ICEBERG_TYPE_NOTES,
    IcebergPreflightError,
    detect_hidden_partitioning,
    is_recoverable_via_ctas,
    preflight_iceberg_source,
)


def _describe_rows(*entries: tuple[str, str]) -> list[dict]:
    """Build a fake DESCRIBE TABLE EXTENDED response.

    Each entry is (col_name, data_type). Caller is responsible for inserting
    the section header rows ("# Partition Information") at the right places.
    """
    return [{"col_name": c, "data_type": d} for c, d in entries]


@patch("src.clone_iceberg.execute_sql")
def test_detect_hidden_partitioning_finds_bucket(mock_sql):
    """A bucket(N, col) transform in the partition section is flagged.
    bucket() hashes the source column at write time — Delta has no equivalent
    and lossy substitutions would silently break partition pruning."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("user_id", "bigint"),
        ("# Partition Information", ""),
        ("# col_name", "data_type"),
        ("user_id", "bucket(16, user_id)"),
    )
    transforms = detect_hidden_partitioning(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert transforms == ["bucket(16, user_id)"]


@patch("src.clone_iceberg.execute_sql")
def test_detect_hidden_partitioning_finds_multiple_transforms(mock_sql):
    """Iceberg tables can have several hidden-partition columns. All are
    surfaced so the error message lists the full set, not just the first."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("# Partition Information", ""),
        ("ts", "days(ts)"),
        ("region", "truncate(2, region)"),
    )
    transforms = detect_hidden_partitioning(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert "days(ts)" in transforms
    assert "truncate(2, region)" in transforms


@patch("src.clone_iceberg.execute_sql")
def test_detect_hidden_partitioning_ignores_plain_column_partition(mock_sql):
    """Plain column-level partitioning (PARTITIONED BY (region)) maps cleanly
    to Delta — no transform, no refusal. The detector returns an empty list."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("region", "string"),
        ("# Partition Information", ""),
        ("region", "string"),
    )
    transforms = detect_hidden_partitioning(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert transforms == []


@patch("src.clone_iceberg.execute_sql")
def test_detect_hidden_partitioning_ignores_transforms_outside_partition_section(mock_sql):
    """A column named or typed like a transform in the regular schema section
    must not trigger detection — only rows inside `# Partition Information`
    count. Defends against a column comment containing the word `bucket()`."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("payload", "string COMMENT 'see bucket(16, user_id)'"),
    )
    transforms = detect_hidden_partitioning(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert transforms == []


@patch("src.clone_iceberg.execute_sql")
def test_detect_hidden_partitioning_swallows_describe_failure(mock_sql):
    """If DESCRIBE TABLE EXTENDED fails (permission, transient), the detector
    returns an empty list rather than blocking the clone. The post-failure
    handler in clone_cross_workspace catches anything that slips through."""
    mock_sql.side_effect = Exception("permission denied")
    transforms = detect_hidden_partitioning(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert transforms == []


@patch("src.clone_iceberg.execute_sql")
def test_preflight_raises_on_hidden_partitioning(mock_sql):
    """preflight_iceberg_source raises IcebergPreflightError listing the
    offending transform(s) and pointing at the documented workarounds."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("# Partition Information", ""),
        ("user_id", "bucket(16, user_id)"),
    )
    with pytest.raises(IcebergPreflightError) as exc:
        preflight_iceberg_source(MagicMock(), "wh-1", "`src`.`s`.`t`")
    assert "bucket(16, user_id)" in str(exc.value)
    # Workaround text should be in the message so users know what to do.
    assert "CONVERT TO DELTA" in str(exc.value) or "CTAS" in str(exc.value)


@patch("src.clone_iceberg.execute_sql")
def test_preflight_passes_on_clean_source(mock_sql):
    """No transforms → preflight is a no-op. clone_table() proceeds to CLONE
    as if the field weren't set."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint"),
        ("region", "string"),
        ("# Partition Information", ""),
        ("region", "string"),
    )
    # Should not raise.
    preflight_iceberg_source(MagicMock(), "wh-1", "`src`.`s`.`t`")


def test_is_recoverable_via_ctas_recognises_partition_evolution():
    """`partition evolution` errors from Databricks CLONE are CTAS-recoverable
    — CTAS reads rows directly and bypasses the metadata mismatch."""
    err = Exception("Cannot clone table due to partition evolution in the Iceberg source")
    assert is_recoverable_via_ctas(err) is True


def test_is_recoverable_via_ctas_recognises_truncated_decimal():
    """`truncated` partition decimal failure on DBR < 13.3 — CTAS sidesteps
    by re-deriving the value at write time."""
    err = Exception("Decimal partition column is truncated; not supported")
    assert is_recoverable_via_ctas(err) is True


def test_is_recoverable_via_ctas_rejects_permission_error():
    """Permission errors are NOT recoverable via CTAS — CTAS would just fail
    with the same permission error. Auto-retrying would mask the real issue."""
    err = Exception("Permission denied: USE CATALOG required")
    assert is_recoverable_via_ctas(err) is False


def test_is_recoverable_via_ctas_rejects_schema_mismatch():
    """Schema-mismatch errors mean the destination already has a different
    shape — CTAS would fail too, and re-trying could overwrite real data."""
    err = Exception("Schema mismatch: column `id` type changed from int to string")
    assert is_recoverable_via_ctas(err) is False


def test_hidden_partition_transforms_set_is_complete():
    """Sanity-check the keyword set. If Iceberg ever adds a new transform
    (e.g. `quarters`) we want this test to remind us to handle it."""
    # Snapshot of Iceberg transform spec circa the Phase B implementation.
    expected = {"bucket", "truncate", "years", "months", "days", "hours"}
    assert HIDDEN_PARTITION_TRANSFORMS == expected


def test_iceberg_type_notes_documents_known_painful_types():
    """ICEBERG_TYPE_NOTES is documentation that surfaces in error messages
    and the docs site. If the keys drift from the documented set, the docs
    will show stale information."""
    assert "time" in ICEBERG_TYPE_NOTES
    assert "uuid" in ICEBERG_TYPE_NOTES
    assert "fixed" in ICEBERG_TYPE_NOTES
