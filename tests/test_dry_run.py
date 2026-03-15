"""Tests for dry-run enhancement."""

from src.dry_run import classify_sql, SqlCapture, format_plan_console, format_plan_json


class TestClassifySql:
    def test_create_table(self):
        assert classify_sql("CREATE TABLE IF NOT EXISTS t") == "CREATE_TABLE"

    def test_clone(self):
        assert classify_sql("CREATE TABLE t DEEP CLONE s") == "CLONE"

    def test_create_view(self):
        assert classify_sql("CREATE OR REPLACE VIEW v AS SELECT 1") == "CREATE_VIEW"

    def test_grant(self):
        assert classify_sql("GRANT SELECT ON TABLE t TO user") == "GRANT"

    def test_select(self):
        assert classify_sql("SELECT * FROM t") == "READ"

    def test_show(self):
        assert classify_sql("SHOW GRANTS ON TABLE t") == "READ"

    def test_alter(self):
        assert classify_sql("ALTER TABLE t ADD COLUMN c INT") == "ALTER"

    def test_drop(self):
        assert classify_sql("DROP TABLE IF EXISTS t") == "DROP"


class TestSqlCapture:
    def test_captures_statements(self):
        cap = SqlCapture()
        cap.capture("CREATE TABLE t (id INT)")
        cap.capture("GRANT SELECT ON t TO user")
        assert len(cap.statements) == 2

    def test_summary(self):
        cap = SqlCapture()
        cap.capture("CREATE TABLE t (id INT)")
        cap.capture("CREATE TABLE t2 (id INT)")
        cap.capture("GRANT SELECT ON t TO user")
        summary = cap.get_summary()
        assert summary["total_statements"] == 3
        assert summary["by_category"]["CREATE_TABLE"] == 2
        assert summary["by_category"]["GRANT"] == 1


class TestFormatPlan:
    def test_console_format(self):
        plan = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "clone_type": "DEEP",
            "load_type": "FULL",
            "sql_summary": {"total_statements": 1, "by_category": {"CLONE": 1}},
            "sql_statements": [{"sql": "CREATE TABLE t DEEP CLONE s", "category": "CLONE", "timestamp": 0.1}],
            "clone_summary": {"schemas_processed": 1},
            "cost_estimate": None,
            "generated_at": "2024-01-01",
        }
        output = format_plan_console(plan)
        assert "CLONE EXECUTION PLAN" in output
        assert "src" in output

    def test_json_format(self):
        plan = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "sql_summary": {"total_statements": 0, "by_category": {}},
            "sql_statements": [],
        }
        output = format_plan_json(plan)
        import json
        parsed = json.loads(output)
        assert parsed["source_catalog"] == "src"
