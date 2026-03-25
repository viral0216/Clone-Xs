"""Logging plugin — logs all clone lifecycle events."""

import logging

from src.plugin_system import ClonePlugin

logger = logging.getLogger(__name__)


class LoggingPlugin(ClonePlugin):
    """Plugin that logs all clone events to the Python logger."""

    name = "logging"
    description = "Logs all clone lifecycle events"
    version = "1.0.0"

    def on_clone_start(self, config, client, warehouse_id):
        logger.info(
            f"[LoggingPlugin] Clone starting: "
            f"{config.get('source_catalog')} -> {config.get('destination_catalog')}"
        )
        return config

    def on_clone_complete(self, config, summary, client, warehouse_id):
        tables = summary.get("tables", {})
        logger.info(
            f"[LoggingPlugin] Clone completed: "
            f"{tables.get('success', 0)} tables succeeded, "
            f"{tables.get('failed', 0)} failed"
        )

    def on_clone_error(self, config, error, client, warehouse_id):
        logger.error(f"[LoggingPlugin] Clone failed: {error}")

    def on_schema_start(self, schema_name, config, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Processing schema: {schema_name}")

    def on_schema_complete(self, schema_name, results, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Schema {schema_name} done: {results}")

    def on_table_start(self, table_fqn, config, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Cloning table: {table_fqn}")

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Table {table_fqn}: {status}")
