"""Clone Unity Catalog vector search index definitions."""

import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def list_vector_indexes(
    client: WorkspaceClient, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List vector search indexes in a catalog."""
    results = []
    try:
        indexes = client.vector_search_indexes.list_indexes()
        for idx in indexes:
            name = idx.name or ""
            # Filter by catalog
            if not name.startswith(f"{catalog}."):
                continue
            if schema:
                parts = name.split(".")
                if len(parts) >= 2 and parts[1] != schema:
                    continue
            results.append({
                "name": idx.name,
                "index_type": str(idx.index_type) if idx.index_type else None,
                "primary_key": idx.primary_key,
                "endpoint_name": idx.endpoint_name,
                "status": str(idx.status.ready) if idx.status else None,
            })
    except Exception as e:
        logger.error(f"Failed to list vector search indexes: {e}")
    return results


def export_index_definition(
    client: WorkspaceClient, index_name: str,
) -> dict | None:
    """Export a vector search index definition for cloning."""
    try:
        idx = client.vector_search_indexes.get_index(index_name=index_name)
        definition = {
            "name": idx.name,
            "endpoint_name": idx.endpoint_name,
            "primary_key": idx.primary_key,
            "index_type": str(idx.index_type) if idx.index_type else None,
        }

        # Capture delta sync config if present
        if idx.delta_sync_index_spec:
            spec = idx.delta_sync_index_spec
            definition["delta_sync_spec"] = {
                "source_table": spec.source_table,
                "embedding_source_columns": [
                    {"name": c.name, "embedding_model_endpoint_name": c.embedding_model_endpoint_name}
                    for c in (spec.embedding_source_columns or [])
                ],
                "embedding_vector_columns": [
                    {"name": c.name, "embedding_dimension": c.embedding_dimension}
                    for c in (spec.embedding_vector_columns or [])
                ],
                "pipeline_type": str(spec.pipeline_type) if spec.pipeline_type else None,
            }

        # Capture direct access config if present
        if idx.direct_access_index_spec:
            spec = idx.direct_access_index_spec
            definition["direct_access_spec"] = {
                "embedding_source_columns": [
                    {"name": c.name, "embedding_model_endpoint_name": c.embedding_model_endpoint_name}
                    for c in (spec.embedding_source_columns or [])
                ],
                "embedding_vector_columns": [
                    {"name": c.name, "embedding_dimension": c.embedding_dimension}
                    for c in (spec.embedding_vector_columns or [])
                ],
                "schema_json": spec.schema_json,
            }

        return definition
    except Exception as e:
        logger.error(f"Failed to export index {index_name}: {e}")
        return None


def clone_vector_index(
    client: WorkspaceClient,
    source_definition: dict,
    dest_catalog: str,
    source_catalog: str,
    dry_run: bool = False,
) -> dict:
    """Create a vector search index on the destination catalog based on source definition.

    Rewrites table references from source_catalog to dest_catalog.
    """
    source_name = source_definition["name"]
    dest_name = source_name.replace(f"{source_catalog}.", f"{dest_catalog}.", 1)

    result = {
        "source": source_name,
        "destination": dest_name,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        # Rewrite source table reference for delta sync indexes
        if "delta_sync_spec" in source_definition:
            spec = source_definition["delta_sync_spec"]
            source_table = spec["source_table"].replace(
                f"{source_catalog}.", f"{dest_catalog}.", 1
            )

            from databricks.sdk.service.vectorsearch import (
                DeltaSyncVectorIndexSpecRequest,
                EmbeddingSourceColumn,
                EmbeddingVectorColumn,
            )

            embedding_source = [
                EmbeddingSourceColumn(
                    name=c["name"],
                    embedding_model_endpoint_name=c.get("embedding_model_endpoint_name"),
                )
                for c in spec.get("embedding_source_columns", [])
            ] or None

            embedding_vector = [
                EmbeddingVectorColumn(
                    name=c["name"],
                    embedding_dimension=c.get("embedding_dimension"),
                )
                for c in spec.get("embedding_vector_columns", [])
            ] or None

            client.vector_search_indexes.create_index(
                name=dest_name,
                endpoint_name=source_definition["endpoint_name"],
                primary_key=source_definition["primary_key"],
                delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                    source_table=source_table,
                    embedding_source_columns=embedding_source,
                    embedding_vector_columns=embedding_vector,
                    pipeline_type=spec.get("pipeline_type"),
                ),
            )
        else:
            # Direct access index — no source table rewriting needed
            from databricks.sdk.service.vectorsearch import (
                DirectAccessVectorIndexSpec,
                EmbeddingSourceColumn,
                EmbeddingVectorColumn,
            )

            da_spec = source_definition.get("direct_access_spec", {})
            client.vector_search_indexes.create_index(
                name=dest_name,
                endpoint_name=source_definition["endpoint_name"],
                primary_key=source_definition["primary_key"],
                direct_access_index_spec=DirectAccessVectorIndexSpec(
                    embedding_source_columns=[
                        EmbeddingSourceColumn(
                            name=c["name"],
                            embedding_model_endpoint_name=c.get("embedding_model_endpoint_name"),
                        )
                        for c in da_spec.get("embedding_source_columns", [])
                    ] or None,
                    embedding_vector_columns=[
                        EmbeddingVectorColumn(
                            name=c["name"],
                            embedding_dimension=c.get("embedding_dimension"),
                        )
                        for c in da_spec.get("embedding_vector_columns", [])
                    ] or None,
                    schema_json=da_spec.get("schema_json"),
                ),
            )

        result["success"] = True
        logger.info(f"Created vector index: {dest_name}")

    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
            logger.info(f"Vector index already exists: {dest_name}")
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create vector index {dest_name}: {e}")

    return result
