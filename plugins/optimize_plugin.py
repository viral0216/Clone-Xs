"""Optimize plugin — runs OPTIMIZE on cloned tables."""

import logging

from src.plugin_system import ClonePlugin

logger = logging.getLogger(__name__)


class OptimizePlugin(ClonePlugin):
    """Plugin that runs OPTIMIZE on each table after cloning."""

    name = "optimize-after-clone"
    description = "Runs OPTIMIZE on each cloned table to compact small files"
    version = "1.0.0"

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        if status != "success":
            return
        from src.client import execute_sql
        try:
            execute_sql(client, warehouse_id, f"OPTIMIZE {table_fqn}")
            logger.info(f"[OptimizePlugin] Optimized {table_fqn}")
        except Exception as e:
            logger.warning(f"[OptimizePlugin] Failed to optimize {table_fqn}: {e}")

    def on_clone_complete(self, config, summary, client, warehouse_id):
        tables = summary.get("tables", {})
        logger.info(
            f"[OptimizePlugin] Clone complete — "
            f"{tables.get('success', 0)} tables optimized"
        )
