"""Configuration validation and linting for clxs."""

import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"
    SUGGESTION = "suggestion"


@dataclass
class LintResult:
    severity: Severity
    field: str
    message: str
    suggestion: str = ""


# Schema definition for config validation
CONFIG_SCHEMA = {
    "source_catalog": {"type": str, "required": True},
    "destination_catalog": {"type": str, "required": True},
    "clone_type": {"type": str, "required": True, "values": ["DEEP", "SHALLOW"]},
    "sql_warehouse_id": {"type": str, "required": True},
    "load_type": {"type": str, "values": ["FULL", "INCREMENTAL"]},
    "max_workers": {"type": int, "range": (1, 64)},
    "parallel_tables": {"type": int, "range": (1, 32)},
    "max_rps": {"type": (int, float), "range": (0.0, 100.0)},
    "max_retries": {"type": int, "range": (0, 20)},
    "max_parallel_queries": {"type": int, "range": (1, 200)},
    "rollback_threshold": {"type": (int, float), "range": (0.0, 100.0)},
}

DEPRECATED_OPTIONS = {
    # Add deprecated options here as they arise
    # "old_key": "Use 'new_key' instead.",
}


def lint_config(config: dict) -> list[LintResult]:
    """Validate a config dict. Returns list of issues found."""
    results = []
    results.extend(check_required_fields(config))
    results.extend(check_types(config))
    results.extend(check_value_ranges(config))
    results.extend(check_deprecated(config))
    results.extend(check_conflicts(config))
    results.extend(check_optimizations(config))
    return results


def check_required_fields(config: dict) -> list[LintResult]:
    """Check that required fields are present and non-empty."""
    results = []
    for field_name, schema in CONFIG_SCHEMA.items():
        if schema.get("required"):
            val = config.get(field_name)
            if not val:
                results.append(LintResult(
                    severity=Severity.ERROR,
                    field=field_name,
                    message=f"Required field '{field_name}' is missing or empty",
                ))
    return results


def check_types(config: dict) -> list[LintResult]:
    """Check that field values match expected types."""
    results = []
    for field_name, schema in CONFIG_SCHEMA.items():
        if field_name not in config:
            continue
        val = config[field_name]
        if val is None:
            continue
        expected = schema.get("type")
        if expected and not isinstance(val, expected):
            results.append(LintResult(
                severity=Severity.ERROR,
                field=field_name,
                message=f"'{field_name}' should be {expected} but got {type(val).__name__}",
            ))
        # Check allowed values
        allowed = schema.get("values")
        if allowed and val not in allowed:
            results.append(LintResult(
                severity=Severity.ERROR,
                field=field_name,
                message=f"'{field_name}' must be one of {allowed}, got '{val}'",
            ))
    return results


def check_value_ranges(config: dict) -> list[LintResult]:
    """Check that numeric values are within valid ranges."""
    results = []
    for field_name, schema in CONFIG_SCHEMA.items():
        if field_name not in config:
            continue
        val = config[field_name]
        if val is None:
            continue
        value_range = schema.get("range")
        if value_range and isinstance(val, (int, float)):
            min_val, max_val = value_range
            if val < min_val or val > max_val:
                results.append(LintResult(
                    severity=Severity.WARNING,
                    field=field_name,
                    message=f"'{field_name}' value {val} is outside recommended range ({min_val}-{max_val})",
                    suggestion=f"Consider a value between {min_val} and {max_val}",
                ))
    return results


def check_deprecated(config: dict) -> list[LintResult]:
    """Warn about deprecated config options."""
    results = []
    for key, message in DEPRECATED_OPTIONS.items():
        if key in config:
            results.append(LintResult(
                severity=Severity.WARNING,
                field=key,
                message=f"'{key}' is deprecated. {message}",
            ))
    return results


def check_conflicts(config: dict) -> list[LintResult]:
    """Detect conflicting options."""
    results = []

    # Shallow clone + masking rules
    if config.get("clone_type") == "SHALLOW" and config.get("masking_rules"):
        results.append(LintResult(
            severity=Severity.WARNING,
            field="masking_rules",
            message="Masking rules with SHALLOW clone may not work as expected — "
                    "shallow clones reference source data, masking modifies destination data",
            suggestion="Use DEEP clone when applying masking rules",
        ))

    # Validate checksum + dry run
    if config.get("validate_checksum") and config.get("dry_run"):
        results.append(LintResult(
            severity=Severity.WARNING,
            field="validate_checksum",
            message="Checksum validation has no effect in dry-run mode — nothing to validate",
        ))

    # Auto-rollback without rollback enabled
    if config.get("auto_rollback_on_failure") and not config.get("enable_rollback"):
        results.append(LintResult(
            severity=Severity.ERROR,
            field="auto_rollback_on_failure",
            message="auto_rollback_on_failure requires enable_rollback to be True",
            suggestion="Set enable_rollback: true or use --enable-rollback flag",
        ))

    # Auto-rollback without validation
    if config.get("auto_rollback_on_failure") and not config.get("validate_after_clone"):
        results.append(LintResult(
            severity=Severity.ERROR,
            field="auto_rollback_on_failure",
            message="auto_rollback_on_failure requires validate_after_clone to be True",
            suggestion="Set validate_after_clone: true or use --validate flag",
        ))

    # Source == destination
    if (config.get("source_catalog") and config.get("destination_catalog")
            and config["source_catalog"] == config["destination_catalog"]):
        results.append(LintResult(
            severity=Severity.ERROR,
            field="destination_catalog",
            message="source_catalog and destination_catalog cannot be the same",
        ))

    # Where clauses with shallow clone
    if config.get("where_clauses") and config.get("clone_type") == "SHALLOW":
        results.append(LintResult(
            severity=Severity.ERROR,
            field="where_clauses",
            message="WHERE clause filtering is only supported with DEEP clone",
            suggestion="Use clone_type: DEEP when using where_clauses",
        ))

    return results


def check_optimizations(config: dict) -> list[LintResult]:
    """Suggest optimizations."""
    results = []

    # Parallel tables
    if config.get("parallel_tables", 1) == 1:
        results.append(LintResult(
            severity=Severity.SUGGESTION,
            field="parallel_tables",
            message="parallel_tables is 1 — tables within each schema are cloned sequentially",
            suggestion="Consider parallel_tables: 4 or higher for faster cloning",
        ))

    # Rollback not enabled
    if not config.get("enable_rollback"):
        results.append(LintResult(
            severity=Severity.SUGGESTION,
            field="enable_rollback",
            message="Rollback logging is disabled — you won't be able to undo this clone",
            suggestion="Set enable_rollback: true for production clones",
        ))

    # Validation not enabled
    if not config.get("validate_after_clone"):
        results.append(LintResult(
            severity=Severity.SUGGESTION,
            field="validate_after_clone",
            message="Post-clone validation is disabled — data integrity won't be verified",
            suggestion="Set validate_after_clone: true for production clones",
        ))

    # Max workers very high
    if config.get("max_workers", 4) > 16:
        results.append(LintResult(
            severity=Severity.WARNING,
            field="max_workers",
            message=f"max_workers={config['max_workers']} is very high and may cause rate limiting",
            suggestion="Consider max_workers: 8-16 for optimal throughput",
        ))

    return results


def format_lint_results(results: list[LintResult]) -> str:
    """Format lint results for console display."""
    if not results:
        return "Config validation passed — no issues found."

    lines = []
    errors = [r for r in results if r.severity == Severity.ERROR]
    warnings = [r for r in results if r.severity == Severity.WARNING]
    suggestions = [r for r in results if r.severity == Severity.SUGGESTION]

    if errors:
        lines.append(f"\nErrors ({len(errors)}):")
        for r in errors:
            lines.append(f"  [ERROR] {r.field}: {r.message}")
            if r.suggestion:
                lines.append(f"          Suggestion: {r.suggestion}")

    if warnings:
        lines.append(f"\nWarnings ({len(warnings)}):")
        for r in warnings:
            lines.append(f"  [WARN]  {r.field}: {r.message}")
            if r.suggestion:
                lines.append(f"          Suggestion: {r.suggestion}")

    if suggestions:
        lines.append(f"\nSuggestions ({len(suggestions)}):")
        for r in suggestions:
            lines.append(f"  [INFO]  {r.field}: {r.message}")
            if r.suggestion:
                lines.append(f"          Suggestion: {r.suggestion}")

    summary = f"\nSummary: {len(errors)} errors, {len(warnings)} warnings, {len(suggestions)} suggestions"
    lines.append(summary)

    return "\n".join(lines)


def lint_has_errors(results: list[LintResult]) -> bool:
    """Return True if any ERROR-severity issues found."""
    return any(r.severity == Severity.ERROR for r in results)
