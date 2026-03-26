"""Clone Unity Catalog registered models and model versions."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def list_registered_models(
    client: WorkspaceClient, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List all registered models in a catalog, optionally filtered by schema."""
    results = []
    try:
        models = client.registered_models.list(
            catalog_name=catalog,
            schema_name=schema,
        )
        for m in models:
            results.append({
                "full_name": m.full_name,
                "name": m.name,
                "catalog_name": m.catalog_name,
                "schema_name": m.schema_name,
                "comment": m.comment,
                "owner": m.owner,
                "created_at": str(m.created_at) if m.created_at else None,
                "updated_at": str(m.updated_at) if m.updated_at else None,
            })
    except Exception as e:
        logger.error(f"Failed to list models in {catalog}: {e}")
    return results


def list_model_versions(
    client: WorkspaceClient, model_full_name: str,
) -> list[dict]:
    """List all versions of a registered model."""
    results = []
    try:
        versions = client.model_versions.list(full_name=model_full_name)
        for v in versions:
            results.append({
                "version": v.version,
                "model_name": v.model_name,
                "catalog_name": v.catalog_name,
                "schema_name": v.schema_name,
                "source": v.source,
                "run_id": v.run_id,
                "status": str(v.status) if v.status else None,
                "comment": v.comment,
                "created_at": str(v.created_at) if v.created_at else None,
            })
    except Exception as e:
        logger.error(f"Failed to list versions for {model_full_name}: {e}")
    return results


def clone_model(
    client: WorkspaceClient,
    source_catalog: str, dest_catalog: str,
    schema: str, model_name: str,
    copy_versions: bool = True,
    dry_run: bool = False,
) -> dict:
    """Clone a registered model from source to destination catalog.

    Creates the model in the destination and optionally copies all versions.
    """
    source_fqn = f"{source_catalog}.{schema}.{model_name}"
    dest_fqn = f"{dest_catalog}.{schema}.{model_name}"

    result = {
        "source": source_fqn,
        "destination": dest_fqn,
        "versions_copied": 0,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        # Get source model details
        source_model = client.registered_models.get(full_name=source_fqn)

        # Create model in destination
        try:
            client.registered_models.create(
                name=model_name,
                catalog_name=dest_catalog,
                schema_name=schema,
                comment=source_model.comment or f"Cloned from {source_fqn}",
            )
            logger.info(f"Created model: {dest_fqn}")
        except Exception as e:
            if "ALREADY_EXISTS" in str(e):
                logger.info(f"Model already exists: {dest_fqn}")
            else:
                raise

        # Copy versions
        if copy_versions:
            versions = client.model_versions.list(full_name=source_fqn)
            for v in versions:
                try:
                    client.model_versions.create(
                        model_name=dest_fqn,
                        source=v.source,
                        comment=v.comment or f"Cloned from {source_fqn} v{v.version}",
                    )
                    result["versions_copied"] += 1
                except Exception as ve:
                    logger.warning(f"Could not copy version {v.version} of {source_fqn}: {ve}")

        result["success"] = True
        logger.info(f"Cloned model {source_fqn} -> {dest_fqn} ({result['versions_copied']} versions)")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to clone model {source_fqn}: {e}")

    return result


def clone_all_models(
    client: WorkspaceClient,
    source_catalog: str, dest_catalog: str,
    schemas: list[str] | None = None,
    copy_versions: bool = True,
    dry_run: bool = False,
    max_workers: int = 4,
) -> dict:
    """Clone all registered models from source to destination catalog."""
    models = list_registered_models(client, source_catalog)

    if schemas:
        models = [m for m in models if m["schema_name"] in schemas]

    results = []
    errors = []

    def _clone(m):
        return clone_model(
            client, source_catalog, dest_catalog,
            m["schema_name"], m["name"],
            copy_versions=copy_versions, dry_run=dry_run,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_clone, m): m for m in models}
        for future in as_completed(futures):
            try:
                r = future.result()
                results.append(r)
                if not r.get("success"):
                    errors.append(r)
            except Exception as e:
                m = futures[future]
                errors.append({"source": m["full_name"], "error": str(e)})

    return {
        "total": len(models),
        "cloned": sum(1 for r in results if r.get("success")),
        "failed": len(errors),
        "results": results,
        "errors": errors,
    }
