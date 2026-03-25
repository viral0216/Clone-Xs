import json
import logging
import os

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def generate_terraform(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    output_path: str = "terraform_catalog.tf.json",
) -> str:
    """Generate Terraform JSON config for a catalog's resources.

    Generates databricks_catalog, databricks_schema, and databricks_sql_table resources.
    """
    logger.info(f"Generating Terraform config for catalog: {catalog}")

    resources = {}

    # Catalog resource
    catalog_resource_name = _tf_name(catalog)
    resources[f"databricks_catalog.{catalog_resource_name}"] = {
        "name": catalog,
        "comment": "Managed by Terraform - cloned catalog",
    }

    # Get schemas
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    schema_sql = f"""
        SELECT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({exclude_clause})
    """
    schemas = execute_sql(client, warehouse_id, schema_sql)

    for schema_row in schemas:
        schema_name = schema_row["schema_name"]
        schema_tf_name = _tf_name(f"{catalog}_{schema_name}")

        resources[f"databricks_schema.{schema_tf_name}"] = {
            "catalog_name": f"${{databricks_catalog.{catalog_resource_name}.name}}",
            "name": schema_name,
        }

        # Get tables
        table_sql = f"""
            SELECT table_name, table_type
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, table_sql)

        for table_row in tables:
            table_name = table_row["table_name"]
            table_tf_name = _tf_name(f"{catalog}_{schema_name}_{table_name}")

            # Get columns
            col_sql = f"""
                SELECT column_name, data_type, is_nullable, comment
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            columns = execute_sql(client, warehouse_id, col_sql)

            column_defs = []
            for col in columns:
                col_def = {
                    "name": col["column_name"],
                    "type": col["data_type"],
                }
                if col.get("is_nullable") == "NO":
                    col_def["nullable"] = False
                if col.get("comment"):
                    col_def["comment"] = col["comment"]
                column_defs.append(col_def)

            resources[f"databricks_sql_table.{table_tf_name}"] = {
                "catalog_name": f"${{databricks_catalog.{catalog_resource_name}.name}}",
                "schema_name": f"${{databricks_schema.{schema_tf_name}.name}}",
                "name": table_name,
                "table_type": table_row["table_type"],
                "column": column_defs,
            }

    # Build Terraform JSON
    tf_config = {
        "resource": {},
    }

    for resource_key, resource_config in resources.items():
        resource_type, resource_name = resource_key.split(".", 1)
        if resource_type not in tf_config["resource"]:
            tf_config["resource"][resource_type] = {}
        tf_config["resource"][resource_type][resource_name] = resource_config

    # Write output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(tf_config, f, indent=2)

    total = len(resources)
    logger.info(f"Terraform config generated: {output_path} ({total} resources)")
    return output_path


def generate_pulumi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    output_path: str = "pulumi_catalog.py",
) -> str:
    """Generate a Pulumi Python program for a catalog's resources."""
    logger.info(f"Generating Pulumi config for catalog: {catalog}")

    lines = [
        '"""Pulumi program for Databricks Unity Catalog resources."""',
        "import pulumi",
        "import pulumi_databricks as databricks",
        "",
        f'# Catalog: {catalog}',
        f'catalog = databricks.Catalog("{_tf_name(catalog)}",',
        f'    name="{catalog}",',
        '    comment="Managed by Pulumi",',
        ")",
        "",
    ]

    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    schema_sql = f"""
        SELECT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({exclude_clause})
    """
    schemas = execute_sql(client, warehouse_id, schema_sql)

    for schema_row in schemas:
        schema_name = schema_row["schema_name"]
        var_name = _tf_name(f"schema_{schema_name}")

        lines.extend([
            f'{var_name} = databricks.Schema("{_tf_name(catalog)}_{schema_name}",',
            '    catalog_name=catalog.name,',
            f'    name="{schema_name}",',
            ")",
            "",
        ])

    # Write output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    logger.info(f"Pulumi program generated: {output_path}")
    return output_path


def _tf_name(name: str) -> str:
    """Convert a name to a Terraform-safe resource name."""
    return name.replace("-", "_").replace(".", "_").replace(" ", "_").lower()
