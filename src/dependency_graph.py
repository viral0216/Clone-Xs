"""Table dependency graph — visualize view→table and function→table dependencies."""

import json
import logging
import re

from src.client import execute_sql

logger = logging.getLogger(__name__)


def build_dependency_graph(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
) -> dict:
    """Build a dependency graph for all views and functions in a catalog.

    Parses view definitions and function bodies to find table/view references.

    Returns:
        Dict with nodes, edges, and adjacency list.
    """
    exclude = exclude_schemas or []
    nodes = {}  # fqn -> {type, schema, name}
    edges = []  # [{source, target, edge_type}]

    # Get all tables
    tables_sql = f"""
    SELECT table_schema, table_name, table_type
    FROM {catalog}.information_schema.tables
    WHERE table_schema NOT IN ('information_schema')
    """
    tables = execute_sql(client, warehouse_id, tables_sql)

    for t in tables:
        if t["table_schema"] in exclude:
            continue
        fqn = f"{catalog}.{t['table_schema']}.{t['table_name']}"
        obj_type = "VIEW" if "VIEW" in t.get("table_type", "").upper() else "TABLE"
        nodes[fqn] = {
            "type": obj_type,
            "schema": t["table_schema"],
            "name": t["table_name"],
        }

    # Get view definitions and parse dependencies
    for fqn, info in list(nodes.items()):
        if info["type"] != "VIEW":
            continue

        try:
            view_sql = f"SHOW CREATE TABLE {fqn}"
            result = execute_sql(client, warehouse_id, view_sql)
            if result:
                definition = result[0].get("createtab_stmt", "")
                deps = _extract_table_references(definition, catalog)
                for dep in deps:
                    if dep != fqn:  # Skip self-references
                        edges.append({
                            "source": fqn,
                            "target": dep,
                            "edge_type": "depends_on",
                        })
                        # Ensure target node exists
                        if dep not in nodes:
                            nodes[dep] = {
                                "type": "TABLE",
                                "schema": dep.split(".")[1] if "." in dep else "unknown",
                                "name": dep.split(".")[-1],
                            }
        except Exception as e:
            logger.debug(f"Could not get definition for {fqn}: {e}")

    # Build adjacency list
    adjacency = {}
    reverse_adjacency = {}  # "what depends on me"
    for edge in edges:
        adjacency.setdefault(edge["source"], []).append(edge["target"])
        reverse_adjacency.setdefault(edge["target"], []).append(edge["source"])

    # Find root tables (no dependencies)
    root_tables = [fqn for fqn, info in nodes.items()
                   if fqn not in adjacency and info["type"] == "TABLE"]

    # Find leaf views (nothing depends on them)
    leaf_views = [fqn for fqn, info in nodes.items()
                  if fqn not in reverse_adjacency and info["type"] == "VIEW"]

    graph = {
        "catalog": catalog,
        "nodes": nodes,
        "edges": edges,
        "adjacency": adjacency,
        "reverse_adjacency": reverse_adjacency,
        "root_tables": root_tables,
        "leaf_views": leaf_views,
        "total_nodes": len(nodes),
        "total_edges": len(edges),
    }

    return graph


def _extract_table_references(sql_text: str, default_catalog: str) -> list[str]:
    """Extract table/view references from SQL text.

    Handles patterns like:
      - catalog.schema.table
      - schema.table (prepends default catalog)
      - `backtick`.`quoted`.`names`
    """
    refs = set()

    # Pattern: three-part name (catalog.schema.table)
    three_part = re.findall(
        r'(?:`([^`]+)`|(\w+))\.(?:`([^`]+)`|(\w+))\.(?:`([^`]+)`|(\w+))',
        sql_text
    )
    for match in three_part:
        cat = match[0] or match[1]
        schema = match[2] or match[3]
        table = match[4] or match[5]
        refs.add(f"{cat}.{schema}.{table}")

    # Pattern: two-part name (schema.table) — prepend default catalog
    two_part = re.findall(
        r'(?:FROM|JOIN|TABLE)\s+(?:`([^`]+)`|(\w+))\.(?:`([^`]+)`|(\w+))',
        sql_text,
        re.IGNORECASE,
    )
    for match in two_part:
        schema = match[0] or match[1]
        table = match[2] or match[3]
        fqn = f"{default_catalog}.{schema}.{table}"
        if fqn not in refs:
            refs.add(fqn)

    return list(refs)


def get_clone_order(graph: dict) -> list[str]:
    """Determine the order to clone objects based on dependencies (topological sort).

    Tables first, then views in dependency order.

    Returns:
        Ordered list of fully qualified names.
    """
    nodes = graph["nodes"]
    adjacency = graph["adjacency"]

    # Separate tables and views
    tables = [fqn for fqn, info in nodes.items() if info["type"] == "TABLE"]
    views = [fqn for fqn, info in nodes.items() if info["type"] == "VIEW"]

    # Topological sort for views
    visited = set()
    order = []

    def _visit(node):
        if node in visited:
            return
        visited.add(node)
        for dep in adjacency.get(node, []):
            if dep in nodes:
                _visit(dep)
        order.append(node)

    for view in views:
        _visit(view)

    # Tables first, then views in dependency order
    return tables + order


def print_dependency_graph(graph: dict) -> None:
    """Print the dependency graph as ASCII art."""
    nodes = graph["nodes"]
    adjacency = graph["adjacency"]
    reverse_adj = graph["reverse_adjacency"]

    logger.info("=" * 60)
    logger.info(f"DEPENDENCY GRAPH: {graph['catalog']}")
    logger.info("=" * 60)
    logger.info(f"Total objects: {graph['total_nodes']} ({len(graph['root_tables'])} root tables)")
    logger.info(f"Total edges:   {graph['total_edges']}")
    logger.info("")

    # Print by schema
    schemas = {}
    for fqn, info in nodes.items():
        schemas.setdefault(info["schema"], []).append((fqn, info))

    for schema_name, items in sorted(schemas.items()):
        logger.info(f"Schema: {schema_name}")
        for fqn, info in sorted(items, key=lambda x: x[1]["type"]):
            icon = "📋" if info["type"] == "TABLE" else "👁 "
            deps = adjacency.get(fqn, [])
            dependents = reverse_adj.get(fqn, [])

            dep_str = ""
            if deps:
                dep_names = [d.split(".")[-1] for d in deps]
                dep_str = f" → depends on: {', '.join(dep_names)}"
            if dependents:
                dep_names = [d.split(".")[-1] for d in dependents]
                dep_str += f" ← used by: {', '.join(dep_names)}"

            logger.info(f"  {icon} {info['name']}{dep_str}")
        logger.info("")

    # Print clone order
    clone_order = get_clone_order(graph)
    logger.info("Recommended clone order:")
    for i, fqn in enumerate(clone_order, 1):
        info = nodes.get(fqn, {})
        logger.info(f"  {i}. {fqn} ({info.get('type', 'UNKNOWN')})")


def export_dependency_graph(graph: dict, output_path: str = "dependency_graph.json") -> str:
    """Export the dependency graph to a JSON file."""
    export_data = {
        "catalog": graph["catalog"],
        "total_nodes": graph["total_nodes"],
        "total_edges": graph["total_edges"],
        "nodes": {fqn: info for fqn, info in graph["nodes"].items()},
        "edges": graph["edges"],
        "clone_order": get_clone_order(graph),
        "root_tables": graph["root_tables"],
        "leaf_views": graph["leaf_views"],
    }

    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2)

    logger.info(f"Dependency graph exported to: {output_path}")
    return output_path
