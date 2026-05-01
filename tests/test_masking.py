from unittest.mock import MagicMock, patch

from src.masking import apply_masking_rules, _get_mask_expression, build_pii_masking_rules


# ---------- build_pii_masking_rules (auto_mask_pii feature) ----------

def test_build_pii_rules_maps_strategies_from_suggested_masking():
    detections = [
        {"schema": "s1", "table": "customers", "column": "email_addr",
         "pii_type": "EMAIL", "suggested_masking": "email_mask"},
        {"schema": "s1", "table": "customers", "column": "ssn",
         "pii_type": "SSN", "suggested_masking": "hash"},
        {"schema": "s2", "table": "orders", "column": "customer_phone",
         "pii_type": "PHONE", "suggested_masking": "partial"},
    ]
    with patch("src.pii_detection.detect_pii_from_uc_tags", return_value=detections):
        rules = build_pii_masking_rules(MagicMock(), "wh", "cat")

    assert len(rules) == 3
    assert rules[0]["strategy"] == "email_mask"
    assert rules[0]["match_type"] == "exact"
    assert rules[0]["source"] == "auto_pii_tag"
    assert rules[1]["strategy"] == "hash"
    assert rules[2]["strategy"] == "partial"
    # Schema/table on rule lets clone_catalog filter per-table before applying
    assert rules[0]["schema"] == "s1"
    assert rules[0]["table"] == "customers"


def test_build_pii_rules_returns_empty_on_detection_failure():
    """Detection failure (no UC access, no column_tags table) shouldn't crash
    the clone — return empty so the caller falls back to no masking."""
    with patch("src.pii_detection.detect_pii_from_uc_tags",
               side_effect=Exception("UC unavailable")):
        rules = build_pii_masking_rules(MagicMock(), "wh", "cat")
    assert rules == []


def test_build_pii_rules_falls_back_to_redact_for_unknown_type():
    with patch("src.pii_detection.detect_pii_from_uc_tags", return_value=[
        {"schema": "s", "table": "t", "column": "c", "pii_type": "UNKNOWN_TYPE"},
    ]):
        rules = build_pii_masking_rules(MagicMock(), "wh", "cat")
    assert rules[0]["strategy"] == "redact"


def test_build_pii_rules_passes_exclude_schemas_through():
    """The exclude_schemas filter should reach the underlying detector so the
    same exclusion list used for the clone (e.g. information_schema) skips
    PII detection too."""
    captured = {}

    def fake_detect(*_args, **kwargs):
        captured.update(kwargs)
        return []

    with patch("src.pii_detection.detect_pii_from_uc_tags", side_effect=fake_detect):
        build_pii_masking_rules(
            MagicMock(), "wh", "cat",
            exclude_schemas=["information_schema", "default"],
        )
    assert captured.get("exclude_schemas") == ["information_schema", "default"]


# ---------- _get_mask_expression ----------

def test_mask_hash_string():
    expr = _get_mask_expression("col1", "hash", "STRING")
    assert "SHA2" in expr
    assert "col1" in expr


def test_mask_hash_non_string():
    expr = _get_mask_expression("col1", "hash", "INT")
    assert "SHA2" in expr
    assert "CAST" in expr


def test_mask_redact_string():
    expr = _get_mask_expression("col1", "redact", "STRING")
    assert expr == "'***REDACTED***'"


def test_mask_redact_numeric():
    expr = _get_mask_expression("col1", "redact", "INT")
    assert expr == "0"


def test_mask_redact_other():
    expr = _get_mask_expression("col1", "redact", "TIMESTAMP")
    assert expr == "NULL"


def test_mask_null():
    expr = _get_mask_expression("col1", "null", "STRING")
    assert expr == "NULL"


def test_mask_email():
    expr = _get_mask_expression("email", "email_mask", "STRING")
    assert "CONCAT" in expr
    assert "SUBSTRING_INDEX" in expr


def test_mask_partial():
    expr = _get_mask_expression("name", "partial", "STRING")
    assert "REPEAT" in expr


def test_mask_custom_expression():
    expr = _get_mask_expression("col1", "UPPER(`col1`)", "STRING")
    assert expr == "UPPER(`col1`)"


# ---------- apply_masking_rules ----------

@patch("src.masking.execute_sql")
def test_apply_masking_rules_happy(mock_sql):
    # First call returns columns, second call applies UPDATE
    mock_sql.side_effect = [
        [{"column_name": "email", "data_type": "STRING"},
         {"column_name": "name", "data_type": "STRING"},
         {"column_name": "id", "data_type": "INT"}],
        [],  # UPDATE result
    ]
    rules = [
        {"column": "email", "strategy": "hash"},
        {"column": "name", "strategy": "redact"},
    ]
    count = apply_masking_rules(
        MagicMock(), "wh-1", "dst", "schema1", "table1", rules,
    )
    assert count == 2
    update_sql = mock_sql.call_args_list[1][0][2]
    assert "UPDATE" in update_sql
    assert "SHA2" in update_sql
    assert "REDACTED" in update_sql


@patch("src.masking.execute_sql")
def test_apply_masking_rules_regex_match(mock_sql):
    mock_sql.side_effect = [
        [{"column_name": "user_email", "data_type": "STRING"},
         {"column_name": "user_phone", "data_type": "STRING"},
         {"column_name": "id", "data_type": "INT"}],
        [],
    ]
    rules = [{"column": "user_.*", "strategy": "redact", "match_type": "regex"}]
    count = apply_masking_rules(
        MagicMock(), "wh-1", "dst", "s1", "t1", rules,
    )
    assert count == 2


@patch("src.masking.execute_sql")
def test_apply_masking_rules_no_matching_columns(mock_sql):
    mock_sql.return_value = [
        {"column_name": "id", "data_type": "INT"},
    ]
    rules = [{"column": "nonexistent", "strategy": "redact"}]
    count = apply_masking_rules(
        MagicMock(), "wh-1", "dst", "s1", "t1", rules,
    )
    assert count == 0


@patch("src.masking.execute_sql")
def test_apply_masking_rules_dry_run(mock_sql):
    mock_sql.side_effect = [
        [{"column_name": "email", "data_type": "STRING"}],
        [],
    ]
    rules = [{"column": "email", "strategy": "null"}]
    count = apply_masking_rules(
        MagicMock(), "wh-1", "dst", "s1", "t1", rules, dry_run=True,
    )
    assert count == 1
    # The second call should pass dry_run=True
    assert mock_sql.call_args_list[1][1].get("dry_run") is True


@patch("src.masking.execute_sql")
def test_apply_masking_rules_update_failure(mock_sql):
    """UPDATE failure should be caught and not raise."""
    mock_sql.side_effect = [
        [{"column_name": "email", "data_type": "STRING"}],
        Exception("update failed"),
    ]
    rules = [{"column": "email", "strategy": "redact"}]
    count = apply_masking_rules(
        MagicMock(), "wh-1", "dst", "s1", "t1", rules,
    )
    # count is set before the update attempt
    assert count == 1
