from unittest.mock import MagicMock, patch

from src.compliance_api import (
    generate_compliance_report_api,
    _classify_owner,
    _compute_overall_score,
    _section,
)


def _config(**overrides):
    base = {
        "audit": {"catalog": "clone_audit", "schema": "logs", "table": "clone_operations"},
        "validate_after_clone": True,
        "validate_checksum": True,
        "copy_permissions": True,
        "copy_ownership": True,
        "copy_tags": True,
        "enable_rollback": True,
    }
    base.update(overrides)
    return base


# ---------- helper unit tests ----------

def test_classify_owner_user():
    assert _classify_owner("alice@company.com") == "user"


def test_classify_owner_service_principal():
    assert _classify_owner("spn-my-app@serviceprincipal") == "service_principal"


def test_classify_owner_group():
    assert _classify_owner("data-team") == "group"


def test_classify_owner_missing():
    assert _classify_owner("") == "missing"
    assert _classify_owner("(none)") == "missing"


def test_compute_overall_score_high():
    sections = [{"score": 90}, {"score": 100}]
    score, status = _compute_overall_score(sections)
    assert score == 95
    assert status == "COMPLIANT"


def test_compute_overall_score_warning():
    sections = [{"score": 50}, {"score": 60}]
    score, status = _compute_overall_score(sections)
    assert status == "WARNING"


def test_compute_overall_score_empty():
    score, status = _compute_overall_score([])
    assert score == 0
    assert status == "WARNING"


def test_section_compliant():
    s = _section("Title", "Desc", 85)
    assert s["status"] == "COMPLIANT"


def test_section_non_compliant():
    s = _section("Title", "Desc", 30)
    assert s["status"] == "NON_COMPLIANT"


# ---------- data_governance report ----------

@patch("src.compliance_api.execute_sql")
def test_generate_data_governance_report(mock_sql):
    mock_sql.return_value = [
        {"status": "SUCCESS", "started_at": "2025-01-01"},
    ]
    config = _config()
    result = generate_compliance_report_api(
        MagicMock(), "wh-1", config, "my_catalog", report_type="data_governance",
    )
    assert "status" in result
    assert "score" in result
    assert result["summary"]["report_type"] == "data_governance"
    assert result["summary"]["total_checks"] >= 1
    assert isinstance(result["sections"], list)


# ---------- tag_coverage report ----------

@patch("src.compliance_api.execute_sql")
def test_generate_tag_coverage_report(mock_sql):
    # First call: all tables, second: tagged tables, third: column tags
    mock_sql.side_effect = [
        [{"table_schema": "s1", "table_name": "t1"}],
        [{"schema_name": "s1", "table_name": "t1"}],
        [{"schema_name": "s1", "table_name": "t1", "column_name": "c1"}],
    ]
    result = generate_compliance_report_api(
        MagicMock(), "wh-1", _config(), "my_catalog", report_type="tag_coverage",
    )
    assert result["summary"]["report_type"] == "tag_coverage"
    assert len(result["sections"]) >= 1


# ---------- permission_audit report ----------

@patch("src.compliance_api.execute_sql")
def test_generate_permission_audit_report(mock_sql):
    mock_sql.side_effect = [
        # grants
        [{"Principal": "admin", "ActionType": "ALL PRIVILEGES", "ObjectType": "CATALOG", "ObjectKey": "cat"}],
        # table ownership
        [{"table_schema": "s1", "table_name": "t1", "table_owner": "alice@co.com"}],
    ]
    result = generate_compliance_report_api(
        MagicMock(), "wh-1", _config(), "my_catalog", report_type="permission_audit",
    )
    assert result["summary"]["report_type"] == "permission_audit"
    sections = result["sections"]
    assert any("Grants" in s["title"] for s in sections)


# ---------- unknown report_type falls back to data_governance ----------

@patch("src.compliance_api.execute_sql")
def test_unknown_report_type_falls_back(mock_sql):
    mock_sql.return_value = []
    config = _config()
    # Remove audit config so the data_governance section uses fallback path
    config.pop("audit", None)
    result = generate_compliance_report_api(
        MagicMock(), "wh-1", config, "my_catalog", report_type="nonexistent",
    )
    assert result["summary"]["report_type"] == "nonexistent"
    assert len(result["sections"]) >= 1


# ---------- sql failure produces NON_COMPLIANT section ----------

@patch("src.compliance_api.execute_sql")
def test_tag_coverage_sql_failure(mock_sql):
    mock_sql.side_effect = Exception("access denied")
    result = generate_compliance_report_api(
        MagicMock(), "wh-1", _config(), "my_catalog", report_type="tag_coverage",
    )
    # Should still return a result with error sections rather than raising
    assert result["score"] <= 50
    assert any(s["status"] == "WARNING" or s["status"] == "NON_COMPLIANT" for s in result["sections"])
