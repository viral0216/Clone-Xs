"""Data retention / TTL policy management for cloned catalogs."""

import logging
import re
from datetime import datetime, timedelta, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)


def parse_ttl_string(ttl_str: str) -> int:
    """Parse TTL string to integer days.

    Supported formats: 7d (days), 2w (weeks), 6m (months), 1y (years)
    """
    match = re.match(r"^(\d+)\s*([dwmy])$", ttl_str.strip().lower())
    if not match:
        raise ValueError(f"Invalid TTL format: '{ttl_str}'. Use format like '7d', '2w', '6m', '1y'")

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {"d": 1, "w": 7, "m": 30, "y": 365}
    return value * multipliers[unit]


class TTLManager:
    """Manages TTL policies for cloned catalogs."""

    def __init__(
        self, client, warehouse_id: str,
        state_catalog: str = "clone_audit",
        state_schema: str = "state",
    ):
        self.client = client
        self.warehouse_id = warehouse_id
        self.table_fqn = f"`{state_catalog}`.`{state_schema}`.`ttl_policies`"
        self.state_catalog = state_catalog
        self.state_schema = state_schema

    def init_ttl_table(self) -> None:
        """Create the TTL policies Delta table if it doesn't exist."""
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE CATALOG IF NOT EXISTS `{self.state_catalog}`")
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE SCHEMA IF NOT EXISTS `{self.state_catalog}`.`{self.state_schema}`")
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_fqn} (
                dest_catalog STRING,
                dest_schema STRING,
                ttl_days INT,
                created_at TIMESTAMP,
                expires_at TIMESTAMP,
                created_by STRING,
                operation_id STRING,
                status STRING DEFAULT 'active'
            )
        """
        execute_sql(self.client, self.warehouse_id, sql)
        logger.info(f"TTL table initialized: {self.table_fqn}")

    def set_ttl(
        self, dest_catalog: str, ttl_days: int,
        operation_id: str | None = None, created_by: str | None = None,
    ) -> None:
        """Set TTL on a destination catalog."""
        expires_at = (datetime.now(timezone.utc) + timedelta(days=ttl_days)).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
            MERGE INTO {self.table_fqn} AS target
            USING (SELECT '{dest_catalog}' AS dest_catalog) AS source
            ON target.dest_catalog = source.dest_catalog AND target.dest_schema IS NULL
            WHEN MATCHED THEN UPDATE SET
                ttl_days = {ttl_days},
                expires_at = '{expires_at}',
                status = 'active'
            WHEN NOT MATCHED THEN INSERT (
                dest_catalog, ttl_days, created_at, expires_at, created_by, operation_id, status
            ) VALUES (
                '{dest_catalog}', {ttl_days}, current_timestamp(),
                '{expires_at}', '{created_by or ""}', '{operation_id or ""}', 'active'
            )
        """
        execute_sql(self.client, self.warehouse_id, sql)
        logger.info(f"TTL set: {dest_catalog} expires in {ttl_days} days ({expires_at})")

    def check_expired(self) -> list[dict]:
        """Find all expired TTL policies."""
        sql = f"""
            SELECT dest_catalog, dest_schema, ttl_days, created_at, expires_at, created_by, operation_id
            FROM {self.table_fqn}
            WHERE status = 'active' AND expires_at < current_timestamp()
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def check_expiring_soon(self, warn_days: int = 3) -> list[dict]:
        """Find TTL policies expiring within warn_days."""
        sql = f"""
            SELECT dest_catalog, dest_schema, ttl_days, expires_at,
                   DATEDIFF(expires_at, current_timestamp()) AS days_remaining
            FROM {self.table_fqn}
            WHERE status = 'active'
              AND expires_at >= current_timestamp()
              AND DATEDIFF(expires_at, current_timestamp()) <= {warn_days}
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def list_all(self) -> list[dict]:
        """List all TTL policies."""
        sql = f"""
            SELECT dest_catalog, dest_schema, ttl_days, created_at, expires_at, status,
                   CASE WHEN expires_at < current_timestamp() THEN 'EXPIRED'
                        ELSE CONCAT(CAST(DATEDIFF(expires_at, current_timestamp()) AS STRING), ' days remaining')
                   END AS ttl_status
            FROM {self.table_fqn}
            ORDER BY expires_at
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def extend_ttl(self, dest_catalog: str, additional_days: int) -> None:
        """Extend TTL for a catalog by additional days."""
        sql = f"""
            UPDATE {self.table_fqn}
            SET expires_at = DATEADD(DAY, {additional_days}, expires_at),
                ttl_days = ttl_days + {additional_days}
            WHERE dest_catalog = '{dest_catalog}' AND status = 'active'
        """
        execute_sql(self.client, self.warehouse_id, sql)
        logger.info(f"TTL extended by {additional_days} days for {dest_catalog}")

    def remove_ttl(self, dest_catalog: str) -> None:
        """Remove TTL policy for a catalog."""
        sql = f"""
            UPDATE {self.table_fqn}
            SET status = 'removed'
            WHERE dest_catalog = '{dest_catalog}' AND status = 'active'
        """
        execute_sql(self.client, self.warehouse_id, sql)
        logger.info(f"TTL removed for {dest_catalog}")

    def cleanup_expired(self, confirm: bool = False, dry_run: bool = True) -> dict:
        """Drop expired catalogs. Requires confirm=True for actual drops."""
        expired = self.check_expired()
        result = {"expired_count": len(expired), "dropped": [], "failed": [], "skipped": []}

        if not expired:
            logger.info("No expired catalogs found")
            return result

        for entry in expired:
            cat = entry["dest_catalog"]
            if dry_run or not confirm:
                result["skipped"].append(cat)
                logger.info(f"[DRY RUN] Would drop catalog: {cat}")
                continue

            try:
                execute_sql(self.client, self.warehouse_id,
                            f"DROP CATALOG IF EXISTS `{cat}` CASCADE")
                # Mark as cleaned up
                execute_sql(self.client, self.warehouse_id,
                            f"UPDATE {self.table_fqn} SET status = 'cleaned' "
                            f"WHERE dest_catalog = '{cat}'")
                result["dropped"].append(cat)
                logger.info(f"Dropped expired catalog: {cat}")
            except Exception as e:
                result["failed"].append({"catalog": cat, "error": str(e)})
                logger.error(f"Failed to drop {cat}: {e}")

        return result


def format_ttl_report(policies: list[dict]) -> str:
    """Format TTL policies for console display."""
    if not policies:
        return "No TTL policies found."

    lines = []
    lines.append("=" * 70)
    lines.append("TTL POLICIES")
    lines.append("=" * 70)
    lines.append(f"{'Catalog':30s} {'TTL Days':>10s} {'Expires':20s} {'Status':15s}")
    lines.append("-" * 70)

    for p in policies:
        lines.append(
            f"{p.get('dest_catalog', ''):30s} "
            f"{str(p.get('ttl_days', '')):>10s} "
            f"{str(p.get('expires_at', '')):20s} "
            f"{p.get('ttl_status', p.get('status', '')):15s}"
        )

    lines.append("=" * 70)
    return "\n".join(lines)
