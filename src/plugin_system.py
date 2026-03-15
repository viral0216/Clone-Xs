"""Plugin system — extensible pre/post hooks and custom transformations."""

import importlib
import importlib.util
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ClonePlugin(ABC):
    """Base class for clone plugins.

    Subclass this and implement the hooks you need.
    """

    name: str = "unnamed_plugin"
    description: str = ""
    version: str = "1.0.0"

    def on_clone_start(self, config: dict, client, warehouse_id: str) -> dict:
        """Called before the clone operation starts.

        Can modify config. Return the (possibly modified) config.
        """
        return config

    def on_clone_complete(self, config: dict, summary: dict, client, warehouse_id: str) -> None:
        """Called after the clone operation completes."""
        pass

    def on_clone_error(self, config: dict, error: Exception, client, warehouse_id: str) -> None:
        """Called when the clone operation fails."""
        pass

    def on_schema_start(self, schema_name: str, config: dict, client, warehouse_id: str) -> None:
        """Called before cloning a schema."""
        pass

    def on_schema_complete(self, schema_name: str, results: dict, client, warehouse_id: str) -> None:
        """Called after a schema is cloned."""
        pass

    def on_table_start(self, table_fqn: str, config: dict, client, warehouse_id: str) -> None:
        """Called before cloning a table."""
        pass

    def on_table_complete(self, table_fqn: str, status: str, client, warehouse_id: str) -> None:
        """Called after a table is cloned."""
        pass

    def transform_sql(self, sql: str, table_fqn: str, config: dict) -> str:
        """Transform the clone SQL before execution. Return modified SQL."""
        return sql

    def validate_table(self, table_fqn: str, client, warehouse_id: str) -> dict:
        """Custom validation for a cloned table. Return {valid: bool, message: str}."""
        return {"valid": True, "message": ""}


class PluginManager:
    """Manages loading and executing plugins."""

    def __init__(self):
        self.plugins: list[ClonePlugin] = []

    def load_plugin_from_file(self, filepath: str) -> None:
        """Load a plugin from a Python file."""
        if not os.path.exists(filepath):
            logger.error(f"Plugin file not found: {filepath}")
            return

        module_name = os.path.splitext(os.path.basename(filepath))[0]
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find ClonePlugin subclasses in the module
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, ClonePlugin)
                and attr is not ClonePlugin
            ):
                plugin = attr()
                self.plugins.append(plugin)
                logger.info(f"Loaded plugin: {plugin.name} v{plugin.version}")

    def load_plugin_from_class(self, plugin_class: type) -> None:
        """Load a plugin from a class."""
        if issubclass(plugin_class, ClonePlugin):
            plugin = plugin_class()
            self.plugins.append(plugin)
            logger.info(f"Loaded plugin: {plugin.name} v{plugin.version}")

    def load_plugins_from_directory(self, directory: str) -> None:
        """Load all plugins from a directory."""
        if not os.path.isdir(directory):
            logger.debug(f"Plugin directory not found: {directory}")
            return

        for filename in sorted(os.listdir(directory)):
            if filename.endswith(".py") and not filename.startswith("_"):
                filepath = os.path.join(directory, filename)
                try:
                    self.load_plugin_from_file(filepath)
                except Exception as e:
                    logger.error(f"Failed to load plugin {filename}: {e}")

    def load_plugins_from_config(self, config: dict) -> None:
        """Load plugins specified in the config.

        Config format:
            plugins:
              - path: plugins/my_plugin.py
              - path: plugins/another_plugin.py
              - directory: plugins/
        """
        plugin_configs = config.get("plugins", [])
        for pc in plugin_configs:
            if "path" in pc:
                self.load_plugin_from_file(pc["path"])
            elif "directory" in pc:
                self.load_plugins_from_directory(pc["directory"])

    def run_on_clone_start(self, config: dict, client, warehouse_id: str) -> dict:
        """Run all on_clone_start hooks. Returns potentially modified config."""
        for plugin in self.plugins:
            try:
                config = plugin.on_clone_start(config, client, warehouse_id)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} on_clone_start failed: {e}")
        return config

    def run_on_clone_complete(self, config: dict, summary: dict, client, warehouse_id: str) -> None:
        """Run all on_clone_complete hooks."""
        for plugin in self.plugins:
            try:
                plugin.on_clone_complete(config, summary, client, warehouse_id)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} on_clone_complete failed: {e}")

    def run_on_clone_error(self, config: dict, error: Exception, client, warehouse_id: str) -> None:
        """Run all on_clone_error hooks."""
        for plugin in self.plugins:
            try:
                plugin.on_clone_error(config, error, client, warehouse_id)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} on_clone_error failed: {e}")

    def run_on_table_start(self, table_fqn: str, config: dict, client, warehouse_id: str) -> None:
        """Run all on_table_start hooks."""
        for plugin in self.plugins:
            try:
                plugin.on_table_start(table_fqn, config, client, warehouse_id)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} on_table_start failed: {e}")

    def run_on_table_complete(self, table_fqn: str, status: str, client, warehouse_id: str) -> None:
        """Run all on_table_complete hooks."""
        for plugin in self.plugins:
            try:
                plugin.on_table_complete(table_fqn, status, client, warehouse_id)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} on_table_complete failed: {e}")

    def run_transform_sql(self, sql: str, table_fqn: str, config: dict) -> str:
        """Run all transform_sql hooks. Each plugin can modify the SQL."""
        for plugin in self.plugins:
            try:
                sql = plugin.transform_sql(sql, table_fqn, config)
            except Exception as e:
                logger.error(f"Plugin {plugin.name} transform_sql failed: {e}")
        return sql

    def run_validate_table(self, table_fqn: str, client, warehouse_id: str) -> list[dict]:
        """Run all validate_table hooks. Returns list of validation results."""
        results = []
        for plugin in self.plugins:
            try:
                result = plugin.validate_table(table_fqn, client, warehouse_id)
                result["plugin"] = plugin.name
                results.append(result)
            except Exception as e:
                results.append({"plugin": plugin.name, "valid": False, "message": str(e)})
        return results


# --- Built-in example plugins ---

class LoggingPlugin(ClonePlugin):
    """Example plugin that logs all clone events."""

    name = "logging_plugin"
    description = "Logs all clone lifecycle events"

    def on_clone_start(self, config, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Clone starting: {config.get('source_catalog')} -> {config.get('destination_catalog')}")
        return config

    def on_clone_complete(self, config, summary, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Clone completed: {summary}")

    def on_table_start(self, table_fqn, config, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Cloning table: {table_fqn}")

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        logger.info(f"[LoggingPlugin] Table {table_fqn}: {status}")


class OptimizeAfterClonePlugin(ClonePlugin):
    """Plugin that runs OPTIMIZE on each table after cloning."""

    name = "optimize_after_clone"
    description = "Runs OPTIMIZE on each cloned table"

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        if status == "success":
            from src.client import execute_sql
            try:
                execute_sql(client, warehouse_id, f"OPTIMIZE {table_fqn}")
                logger.info(f"[OptimizePlugin] Optimized {table_fqn}")
            except Exception as e:
                logger.warning(f"[OptimizePlugin] Failed to optimize {table_fqn}: {e}")


class AnalyzeAfterClonePlugin(ClonePlugin):
    """Plugin that runs ANALYZE TABLE on each table after cloning."""

    name = "analyze_after_clone"
    description = "Runs ANALYZE TABLE on each cloned table to update statistics"

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        if status == "success":
            from src.client import execute_sql
            try:
                execute_sql(client, warehouse_id, f"ANALYZE TABLE {table_fqn} COMPUTE STATISTICS")
                logger.info(f"[AnalyzePlugin] Analyzed {table_fqn}")
            except Exception as e:
                logger.warning(f"[AnalyzePlugin] Failed to analyze {table_fqn}: {e}")
