import logging
import re
from collections import defaultdict

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_view_dependencies(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> dict[str, list[str]]:
    """Build a dependency map for views in a schema.

    Returns {view_name: [list of tables/views it depends on]}.
    """
    sql = f"""
        SELECT table_name, view_definition
        FROM {catalog}.information_schema.views
        WHERE table_schema = '{schema}'
    """
    views = execute_sql(client, warehouse_id, sql)

    # Get all table names in the schema for matching
    table_sql = f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """
    tables = execute_sql(client, warehouse_id, table_sql)
    all_objects = {r["table_name"] for r in tables}

    deps = {}
    for view in views:
        view_name = view["table_name"]
        definition = view.get("view_definition", "") or ""

        # Find references to other objects in the view definition
        referenced = set()
        for obj_name in all_objects:
            if obj_name == view_name:
                continue
            # Check for backtick-quoted or unquoted references
            patterns = [
                rf"`{re.escape(obj_name)}`",
                rf"\b{re.escape(obj_name)}\b",
            ]
            for pattern in patterns:
                if re.search(pattern, definition):
                    referenced.add(obj_name)
                    break

        deps[view_name] = list(referenced)

    return deps


def topological_sort(dependency_map: dict[str, list[str]]) -> list[str]:
    """Sort objects in dependency order (dependencies first).

    Uses Kahn's algorithm. Objects with no dependencies come first.
    Falls back to appending remaining objects if cycles are detected.
    """
    # Build in-degree map
    in_degree = defaultdict(int)
    graph = defaultdict(list)

    all_nodes = set(dependency_map.keys())
    for node, deps in dependency_map.items():
        for dep in deps:
            if dep in all_nodes:
                graph[dep].append(node)
                in_degree[node] += 1

    # Start with nodes that have no dependencies
    queue = [n for n in all_nodes if in_degree[n] == 0]
    result = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Handle cycles: append remaining nodes
    remaining = [n for n in all_nodes if n not in result]
    if remaining:
        logger.warning(f"Dependency cycle detected among: {remaining}")
        result.extend(remaining)

    return result


def get_ordered_views(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> list[str]:
    """Get views in dependency-safe creation order."""
    deps = get_view_dependencies(client, warehouse_id, catalog, schema)
    if not deps:
        return []

    ordered = topological_sort(deps)
    logger.debug(f"View creation order for {schema}: {ordered}")
    return ordered


def get_function_dependencies(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> dict[str, list[str]]:
    """Build a dependency map for functions.

    Functions can reference other functions or tables.
    """
    sql = f"""
        SELECT function_name, routine_definition
        FROM {catalog}.information_schema.routines
        WHERE routine_schema = '{schema}'
        AND routine_type = 'FUNCTION'
    """
    functions = execute_sql(client, warehouse_id, sql)

    func_names = {r["function_name"] for r in functions}
    deps = {}

    for func in functions:
        func_name = func["function_name"]
        definition = func.get("routine_definition", "") or ""

        referenced = set()
        for other_func in func_names:
            if other_func == func_name:
                continue
            if re.search(rf"\b{re.escape(other_func)}\b", definition):
                referenced.add(other_func)

        deps[func_name] = list(referenced)

    return deps
