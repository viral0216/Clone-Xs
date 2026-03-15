"""Clone policies (guardrails) — enforce rules before and during clone operations."""

import logging
import re

import yaml

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Built-in policy types
POLICY_TYPES = {
    "max_table_size_gb": "Reject tables larger than N GB",
    "max_total_size_gb": "Reject clone if total size exceeds N GB",
    "require_masking_for_pii": "Require masking rules for detected PII columns",
    "blocked_schemas": "Prevent cloning specific schemas",
    "blocked_tables_regex": "Prevent cloning tables matching a regex",
    "require_approval_above_gb": "Require approval for tables above N GB",
    "max_tables": "Reject clone if table count exceeds N",
    "require_dry_run_first": "Require a successful dry run before real clone",
    "deny_shallow_clone": "Block shallow clones (require deep clone only)",
    "deny_cross_workspace": "Block cross-workspace clones",
    "require_rollback": "Require rollback to be enabled",
    "require_validation": "Require post-clone validation",
    "allowed_clone_hours": "Only allow cloning during specific hours (UTC)",
}


class PolicyViolation:
    """Represents a policy violation."""

    def __init__(self, policy_name: str, message: str, severity: str = "error"):
        self.policy_name = policy_name
        self.message = message
        self.severity = severity  # error, warning

    def __repr__(self):
        return f"PolicyViolation({self.policy_name}: {self.message})"


def load_policies(policy_path: str | None = None, config: dict | None = None) -> list[dict]:
    """Load clone policies from a YAML file or config dict.

    Policy format:
        policies:
          - name: max_table_size_gb
            value: 500
            severity: error
          - name: require_masking_for_pii
            value: true
            severity: error

    Returns:
        List of policy dicts.
    """
    if policy_path:
        with open(policy_path) as f:
            data = yaml.safe_load(f)
        return data.get("policies", [])

    if config:
        return config.get("clone_policies", [])

    return []


def evaluate_policies(
    client,
    warehouse_id: str,
    config: dict,
    policies: list[dict],
    table_sizes: dict[str, float] | None = None,
) -> list[PolicyViolation]:
    """Evaluate all policies against the current clone configuration.

    Args:
        client: WorkspaceClient
        warehouse_id: SQL warehouse ID
        config: Clone configuration
        policies: List of policy dicts
        table_sizes: Optional pre-computed table sizes {fqn: size_gb}

    Returns:
        List of PolicyViolation objects (empty = all policies passed).
    """
    violations = []
    source = config.get("source_catalog", "")

    for policy in policies:
        name = policy.get("name", "")
        value = policy.get("value")
        severity = policy.get("severity", "error")

        if name == "max_table_size_gb" and value is not None:
            sizes = table_sizes or _get_table_sizes(client, warehouse_id, source, config)
            for table_fqn, size_gb in sizes.items():
                if size_gb > float(value):
                    violations.append(PolicyViolation(
                        name,
                        f"Table {table_fqn} is {size_gb:.1f} GB (max: {value} GB)",
                        severity,
                    ))

        elif name == "max_total_size_gb" and value is not None:
            sizes = table_sizes or _get_table_sizes(client, warehouse_id, source, config)
            total = sum(sizes.values())
            if total > float(value):
                violations.append(PolicyViolation(
                    name,
                    f"Total catalog size is {total:.1f} GB (max: {value} GB)",
                    severity,
                ))

        elif name == "max_tables" and value is not None:
            sizes = table_sizes or _get_table_sizes(client, warehouse_id, source, config)
            if len(sizes) > int(value):
                violations.append(PolicyViolation(
                    name,
                    f"Catalog has {len(sizes)} tables (max: {value})",
                    severity,
                ))

        elif name == "blocked_schemas" and value:
            include = config.get("include_schemas", [])
            for blocked in value:
                if not include or blocked in include:
                    violations.append(PolicyViolation(
                        name,
                        f"Schema '{blocked}' is blocked by policy",
                        severity,
                    ))

        elif name == "blocked_tables_regex" and value:
            sizes = table_sizes or _get_table_sizes(client, warehouse_id, source, config)
            pattern = re.compile(value)
            for table_fqn in sizes:
                if pattern.search(table_fqn):
                    violations.append(PolicyViolation(
                        name,
                        f"Table {table_fqn} matches blocked pattern: {value}",
                        severity,
                    ))

        elif name == "deny_shallow_clone" and value:
            if config.get("clone_type", "DEEP").upper() == "SHALLOW":
                violations.append(PolicyViolation(
                    name,
                    "Shallow clones are not allowed by policy",
                    severity,
                ))

        elif name == "deny_cross_workspace" and value:
            if config.get("dest_workspace"):
                violations.append(PolicyViolation(
                    name,
                    "Cross-workspace clones are not allowed by policy",
                    severity,
                ))

        elif name == "require_rollback" and value:
            if not config.get("enable_rollback"):
                violations.append(PolicyViolation(
                    name,
                    "Rollback must be enabled (--enable-rollback)",
                    severity,
                ))

        elif name == "require_validation" and value:
            if not config.get("validate_after_clone"):
                violations.append(PolicyViolation(
                    name,
                    "Post-clone validation must be enabled (--validate)",
                    severity,
                ))

        elif name == "require_dry_run_first" and value:
            if not config.get("dry_run") and not config.get("dry_run_completed"):
                violations.append(PolicyViolation(
                    name,
                    "A dry run must be completed before performing a real clone",
                    severity,
                ))

        elif name == "allowed_clone_hours" and value:
            from datetime import datetime, timezone
            now_utc = datetime.now(timezone.utc).hour
            start, end = value.get("start", 0), value.get("end", 24)
            if not (start <= now_utc < end):
                violations.append(PolicyViolation(
                    name,
                    f"Cloning is only allowed between {start}:00-{end}:00 UTC (current: {now_utc}:00)",
                    severity,
                ))

    return violations


def enforce_policies(
    client,
    warehouse_id: str,
    config: dict,
    policy_path: str | None = None,
) -> bool:
    """Load and enforce policies. Returns True if all pass, exits on error violations.

    Returns:
        True if all policies pass (or only warnings).
    """
    policies = load_policies(policy_path=policy_path, config=config)
    if not policies:
        logger.debug("No clone policies configured")
        return True

    logger.info(f"Evaluating {len(policies)} clone policies...")
    violations = evaluate_policies(client, warehouse_id, config, policies)

    errors = [v for v in violations if v.severity == "error"]
    warnings = [v for v in violations if v.severity == "warning"]

    if warnings:
        for w in warnings:
            logger.warning(f"Policy warning [{w.policy_name}]: {w.message}")

    if errors:
        logger.error(f"Clone blocked by {len(errors)} policy violation(s):")
        for e in errors:
            logger.error(f"  ❌ [{e.policy_name}]: {e.message}")
        return False

    logger.info(f"All {len(policies)} policies passed ✓")
    return True


def _get_table_sizes(client, warehouse_id: str, catalog: str, config: dict) -> dict[str, float]:
    """Get table sizes in GB. Returns {fqn: size_gb}."""
    exclude = config.get("exclude_schemas", [])
    include = config.get("include_schemas")

    sql = f"""
    SELECT t.table_schema, t.table_name,
        COALESCE(
            (SELECT CAST(p.value AS BIGINT)
             FROM {catalog}.information_schema.table_properties p
             WHERE p.table_schema = t.table_schema
               AND p.table_name = t.table_name
               AND p.property_key = 'spark.sql.statistics.totalSize'),
            0
        ) AS size_bytes
    FROM {catalog}.information_schema.tables t
    WHERE t.table_schema NOT IN ('information_schema')
      AND t.table_type != 'VIEW'
    """
    rows = execute_sql(client, warehouse_id, sql)

    sizes = {}
    for row in rows:
        schema = row["table_schema"]
        if schema in exclude:
            continue
        if include and schema not in include:
            continue
        fqn = f"{catalog}.{schema}.{row['table_name']}"
        sizes[fqn] = int(row.get("size_bytes") or 0) / (1024 ** 3)

    return sizes
