"""Tests for config linting."""

from src.config_lint import (
    Severity,
    check_conflicts,
    check_optimizations,
    check_required_fields,
    check_value_ranges,
    format_lint_results,
    lint_config,
    lint_has_errors,
)


class TestCheckRequiredFields:
    def test_missing_source_catalog(self):
        results = check_required_fields({"destination_catalog": "dst", "clone_type": "DEEP", "sql_warehouse_id": "wh"})
        assert any(r.field == "source_catalog" for r in results)

    def test_all_present(self):
        results = check_required_fields({
            "source_catalog": "src", "destination_catalog": "dst",
            "clone_type": "DEEP", "sql_warehouse_id": "wh",
        })
        assert len(results) == 0


class TestCheckConflicts:
    def test_shallow_with_masking(self):
        results = check_conflicts({"clone_type": "SHALLOW", "masking_rules": [{"column": "x"}]})
        assert any(r.field == "masking_rules" for r in results)

    def test_auto_rollback_without_rollback(self):
        results = check_conflicts({"auto_rollback_on_failure": True, "enable_rollback": False})
        assert any(r.severity == Severity.ERROR for r in results)

    def test_same_source_dest(self):
        results = check_conflicts({"source_catalog": "cat", "destination_catalog": "cat"})
        assert any(r.severity == Severity.ERROR for r in results)

    def test_no_conflicts(self):
        results = check_conflicts({"clone_type": "DEEP", "enable_rollback": True})
        assert len(results) == 0


class TestCheckOptimizations:
    def test_parallel_tables_suggestion(self):
        results = check_optimizations({"parallel_tables": 1})
        assert any(r.field == "parallel_tables" for r in results)

    def test_rollback_suggestion(self):
        results = check_optimizations({"enable_rollback": False})
        assert any(r.field == "enable_rollback" for r in results)


class TestCheckValueRanges:
    def test_max_workers_out_of_range(self):
        results = check_value_ranges({"max_workers": 100})
        assert any(r.field == "max_workers" for r in results)

    def test_max_workers_in_range(self):
        results = check_value_ranges({"max_workers": 4})
        assert len(results) == 0


class TestFormatLintResults:
    def test_empty_results(self):
        output = format_lint_results([])
        assert "no issues" in output

    def test_with_errors(self):
        from src.config_lint import LintResult
        results = [LintResult(Severity.ERROR, "field", "msg")]
        output = format_lint_results(results)
        assert "[ERROR]" in output


class TestLintConfig:
    def test_valid_config(self):
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "clone_type": "DEEP", "sql_warehouse_id": "wh",
            "max_workers": 4, "parallel_tables": 4,
            "enable_rollback": True, "validate_after_clone": True,
        }
        results = lint_config(config)
        assert not lint_has_errors(results)
