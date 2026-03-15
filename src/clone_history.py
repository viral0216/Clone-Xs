"""Git-style clone operation history and diffing."""

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)


class CloneHistory:
    """Query and display clone operation history."""

    def __init__(self, client, warehouse_id: str, config: dict):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config

    def list_operations(
        self, limit: int = 20, source_catalog: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        """Query historical clone operations from audit trail."""
        audit_config = self.config.get("audit")
        if not audit_config:
            logger.warning("Audit trail not configured — cannot show history")
            return []

        catalog = audit_config.get("catalog", "clone_audit")
        schema = audit_config.get("schema", "logs")
        table = audit_config.get("table", "clone_audit_log")
        fqn = f"`{catalog}`.`{schema}`.`{table}`"

        conditions = []
        if source_catalog:
            conditions.append(f"source_catalog = '{source_catalog}'")
        if status:
            conditions.append(f"status = '{status}'")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT *
            FROM {fqn}
            {where}
            ORDER BY started_at DESC
            LIMIT {limit}
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.error(f"Failed to query history: {e}")
            return []

    def show_operation(self, operation_id: str) -> dict | None:
        """Get full details of a single operation."""
        audit_config = self.config.get("audit")
        if not audit_config:
            return None

        catalog = audit_config.get("catalog", "clone_audit")
        schema = audit_config.get("schema", "logs")
        table = audit_config.get("table", "clone_audit_log")
        fqn = f"`{catalog}`.`{schema}`.`{table}`"

        sql = f"SELECT * FROM {fqn} WHERE operation_id = '{operation_id}'"
        try:
            rows = execute_sql(self.client, self.warehouse_id, sql)
            return rows[0] if rows else None
        except Exception as e:
            logger.error(f"Failed to show operation {operation_id}: {e}")
            return None

    def diff_operations(self, op_id_1: str, op_id_2: str) -> dict:
        """Compare two operations to find what changed between runs."""
        op1 = self.show_operation(op_id_1)
        op2 = self.show_operation(op_id_2)

        if not op1 or not op2:
            return {"error": "One or both operations not found"}

        result = {
            "operation_1": {"id": op_id_1, "started_at": op1.get("started_at")},
            "operation_2": {"id": op_id_2, "started_at": op2.get("started_at")},
            "changes": [],
        }

        # Compare key metrics
        compare_fields = [
            ("tables_cloned", "Tables cloned"),
            ("tables_failed", "Tables failed"),
            ("views_cloned", "Views cloned"),
            ("functions_cloned", "Functions cloned"),
            ("volumes_cloned", "Volumes cloned"),
            ("duration_seconds", "Duration"),
            ("total_size_bytes", "Total size"),
        ]

        for field, label in compare_fields:
            val1 = op1.get(field)
            val2 = op2.get(field)
            if val1 != val2:
                result["changes"].append({
                    "field": label,
                    "old_value": val1,
                    "new_value": val2,
                })

        return result

    def format_log(self, operations: list[dict]) -> str:
        """Format operations in git-log style."""
        if not operations:
            return "No clone operations found."

        lines = []
        for op in operations:
            op_id = op.get("operation_id", "unknown")
            user = op.get("user_name", "unknown")
            started = op.get("started_at", "")
            source = op.get("source_catalog", "")
            dest = op.get("destination_catalog", "")
            clone_type = op.get("clone_type", "")
            status = op.get("status", "")
            tables = op.get("tables_cloned", 0)
            failed = op.get("tables_failed", 0)
            duration = op.get("duration_seconds", 0)

            status_marker = "+" if status == "SUCCESS" else "-" if status == "FAILED" else "~"

            lines.append(f"operation {op_id}")
            lines.append(f"Author: {user}")
            lines.append(f"Date:   {started}")
            lines.append("")
            lines.append(f"    {status_marker} Clone {source} -> {dest} ({clone_type})")
            lines.append(f"    Tables: {tables} cloned, {failed} failed")
            if duration:
                m, s = divmod(int(duration), 60)
                lines.append(f"    Duration: {m}m{s}s")
            lines.append("")

        return "\n".join(lines)

    def format_diff(self, diff: dict) -> str:
        """Format diff between two operations."""
        if "error" in diff:
            return f"Error: {diff['error']}"

        lines = []
        lines.append("Comparing operations:")
        lines.append(f"  {diff['operation_1']['id']} ({diff['operation_1'].get('started_at', '')})")
        lines.append(f"  {diff['operation_2']['id']} ({diff['operation_2'].get('started_at', '')})")
        lines.append("")

        if not diff["changes"]:
            lines.append("  No differences found.")
        else:
            for change in diff["changes"]:
                lines.append(f"  ~ {change['field']}: {change['old_value']} -> {change['new_value']}")

        return "\n".join(lines)
