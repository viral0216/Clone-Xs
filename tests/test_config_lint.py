"""Tests for config linting."""

from src.config_lint import (
    LintResult,
    Severity,
    check_conflicts,
    check_deprecated,
    check_optimizations,
    check_required_fields,
    check_types,
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

    def test_empty_value_treated_as_missing(self):
        results = check_required_fields({
            "source_catalog": "", "destination_catalog": "dst",
            "clone_type": "DEEP", "sql_warehouse_id": "wh",
        })
        assert any(r.field == "source_catalog" for r in results)

    def test_all_missing(self):
        results = check_required_fields({})
        assert len(results) == 4


class TestCheckTypes:
    def test_wrong_type(self):
        results = check_types({"max_workers": "not_an_int"})
        assert any(r.field == "max_workers" and r.severity == Severity.ERROR for r in results)

    def test_correct_type(self):
        results = check_types({"max_workers": 4})
        assert len(results) == 0

    def test_invalid_clone_type_value(self):
        results = check_types({"clone_type": "INVALID"})
        assert any(r.field == "clone_type" for r in results)

    def test_valid_clone_type_value(self):
        results = check_types({"clone_type": "DEEP"})
        assert len(results) == 0

    def test_none_value_skipped(self):
        results = check_types({"max_workers": None})
        assert len(results) == 0

    def test_unknown_field_ignored(self):
        results = check_types({"unknown_field": "whatever"})
        assert len(results) == 0


class TestCheckConflicts:
    def test_shallow_with_masking(self):
        results = check_conflicts({"clone_type": "SHALLOW", "masking_rules": [{"column": "x"}]})
        assert any(r.field == "masking_rules" for r in results)

    def test_auto_rollback_without_rollback(self):
        results = check_conflicts({"auto_rollback_on_failure": True, "enable_rollback": False})
        assert any(r.severity == Severity.ERROR for r in results)

    def test_auto_rollback_without_validation(self):
        results = check_conflicts({"auto_rollback_on_failure": True, "enable_rollback": True, "validate_after_clone": False})
        assert any(r.field == "auto_rollback_on_failure" and "validate_after_clone" in r.message for r in results)

    def test_same_source_dest(self):
        results = check_conflicts({"source_catalog": "cat", "destination_catalog": "cat"})
        assert any(r.severity == Severity.ERROR for r in results)

    def test_where_clauses_with_shallow(self):
        results = check_conflicts({"where_clauses": {"t": "x > 1"}, "clone_type": "SHALLOW"})
        assert any(r.field == "where_clauses" for r in results)

    def test_checksum_with_dry_run(self):
        results = check_conflicts({"validate_checksum": True, "dry_run": True})
        assert any(r.field == "validate_checksum" for r in results)

    def test_no_conflicts(self):
        results = check_conflicts({"clone_type": "DEEP", "enable_rollback": True})
        assert len(results) == 0


class TestCheckValueRanges:
    def test_max_workers_out_of_range(self):
        results = check_value_ranges({"max_workers": 100})
        assert any(r.field == "max_workers" for r in results)

    def test_max_workers_in_range(self):
        results = check_value_ranges({"max_workers": 4})
        assert len(results) == 0

    def test_max_rps_below_range(self):
        results = check_value_ranges({"max_rps": -1.0})
        assert any(r.field == "max_rps" for r in results)

    def test_none_value_skipped(self):
        results = check_value_ranges({"max_workers": None})
        assert len(results) == 0


class TestCheckDeprecated:
    def test_no_deprecated_keys(self):
        results = check_deprecated({"source_catalog": "src"})
        assert len(results) == 0


class TestCheckOptimizations:
    def test_parallel_tables_suggestion(self):
        results = check_optimizations({"parallel_tables": 1})
        assert any(r.field == "parallel_tables" for r in results)

    def test_rollback_suggestion(self):
        results = check_optimizations({"enable_rollback": False})
        assert any(r.field == "enable_rollback" for r in results)

    def test_validate_after_clone_suggestion(self):
        results = check_optimizations({"validate_after_clone": False})
        assert any(r.field == "validate_after_clone" for r in results)

    def test_high_max_workers_warning(self):
        results = check_optimizations({"max_workers": 32})
        assert any(r.field == "max_workers" and r.severity == Severity.WARNING for r in results)

    def test_no_suggestions_when_optimal(self):
        results = check_optimizations({
            "parallel_tables": 4, "enable_rollback": True,
            "validate_after_clone": True, "max_workers": 8,
        })
        assert len(results) == 0


class TestFormatLintResults:
    def test_empty_results(self):
        output = format_lint_results([])
        assert "no issues" in output

    def test_with_errors(self):
        results = [LintResult(Severity.ERROR, "field", "msg")]
        output = format_lint_results(results)
        assert "[ERROR]" in output

    def test_with_warnings(self):
        results = [LintResult(Severity.WARNING, "field", "warn msg")]
        output = format_lint_results(results)
        assert "[WARN]" in output

    def test_with_suggestions(self):
        results = [LintResult(Severity.SUGGESTION, "field", "info msg", "try this")]
        output = format_lint_results(results)
        assert "[INFO]" in output
        assert "try this" in output

    def test_summary_line(self):
        results = [
            LintResult(Severity.ERROR, "f1", "err"),
            LintResult(Severity.WARNING, "f2", "warn"),
            LintResult(Severity.SUGGESTION, "f3", "info"),
        ]
        output = format_lint_results(results)
        assert "1 errors" in output
        assert "1 warnings" in output
        assert "1 suggestions" in output


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

    def test_invalid_config_has_errors(self):
        config = {}  # missing all required fields
        results = lint_config(config)
        assert lint_has_errors(results)

    def test_lint_has_errors_false_for_warnings_only(self):
        results = [LintResult(Severity.WARNING, "f", "msg")]
        assert not lint_has_errors(results)

    def test_lint_has_errors_true_for_errors(self):
        results = [LintResult(Severity.ERROR, "f", "msg")]
        assert lint_has_errors(results)

    def test_lint_has_errors_empty(self):
        assert not lint_has_errors([])
