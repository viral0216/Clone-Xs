"""Export and import Unity Catalog model serving endpoint configurations."""

import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def list_serving_endpoints(client: WorkspaceClient) -> list[dict]:
    """List all model serving endpoints."""
    results = []
    try:
        endpoints = client.serving_endpoints.list()
        for ep in endpoints:
            results.append({
                "name": ep.name,
                "state": str(ep.state.ready) if ep.state else None,
                "creator": ep.creator,
                "creation_timestamp": ep.creation_timestamp,
                "last_updated_timestamp": ep.last_updated_timestamp,
            })
    except Exception as e:
        logger.error(f"Failed to list serving endpoints: {e}")
    return results


def export_endpoint_config(
    client: WorkspaceClient, endpoint_name: str,
) -> dict | None:
    """Export a serving endpoint's full configuration."""
    try:
        ep = client.serving_endpoints.get(name=endpoint_name)

        config = {
            "name": ep.name,
            "config": None,
            "tags": [],
            "route_optimized": getattr(ep, "route_optimized", None),
        }

        if ep.config:
            served_entities = []
            for se in (ep.config.served_entities or []):
                served_entities.append({
                    "name": se.name,
                    "entity_name": se.entity_name,
                    "entity_version": se.entity_version,
                    "workload_size": se.workload_size,
                    "scale_to_zero_enabled": se.scale_to_zero_enabled,
                    "workload_type": str(se.workload_type) if se.workload_type else None,
                    "environment_vars": se.environment_vars,
                })

            traffic_config = None
            if ep.config.traffic_config and ep.config.traffic_config.routes:
                traffic_config = {
                    "routes": [
                        {
                            "served_model_name": r.served_model_name,
                            "traffic_percentage": r.traffic_percentage,
                        }
                        for r in ep.config.traffic_config.routes
                    ]
                }

            auto_capture = None
            if ep.config.auto_capture_config:
                acc = ep.config.auto_capture_config
                auto_capture = {
                    "catalog_name": acc.catalog_name,
                    "schema_name": acc.schema_name,
                    "table_name_prefix": getattr(acc, "table_name_prefix", None),
                    "enabled": acc.enabled,
                }

            config["config"] = {
                "served_entities": served_entities,
                "traffic_config": traffic_config,
                "auto_capture_config": auto_capture,
            }

        if ep.tags:
            config["tags"] = [{"key": t.key, "value": t.value} for t in ep.tags]

        return config

    except Exception as e:
        logger.error(f"Failed to export endpoint {endpoint_name}: {e}")
        return None


def import_endpoint_config(
    client: WorkspaceClient,
    config: dict,
    dest_catalog: str | None = None,
    source_catalog: str | None = None,
    name_suffix: str = "",
    dry_run: bool = False,
) -> dict:
    """Create a serving endpoint from an exported configuration.

    Optionally rewrites model references from source_catalog to dest_catalog.
    """
    endpoint_name = config["name"] + name_suffix

    result = {
        "name": endpoint_name,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        from databricks.sdk.service.serving import (
            EndpointCoreConfigInput,
            ServedEntityInput,
            TrafficConfig,
            Route,
            AutoCaptureConfigInput,
        )

        ep_config = config.get("config", {})
        if not ep_config:
            result["error"] = "No config found in exported definition"
            return result

        # Build served entities with optional catalog rewrite
        served_entities = []
        for se in ep_config.get("served_entities", []):
            entity_name = se.get("entity_name", "")
            if source_catalog and dest_catalog and entity_name.startswith(f"{source_catalog}."):
                entity_name = entity_name.replace(f"{source_catalog}.", f"{dest_catalog}.", 1)

            served_entities.append(
                ServedEntityInput(
                    name=se.get("name"),
                    entity_name=entity_name,
                    entity_version=se.get("entity_version"),
                    workload_size=se.get("workload_size"),
                    scale_to_zero_enabled=se.get("scale_to_zero_enabled", True),
                )
            )

        # Build traffic config
        traffic_config = None
        tc = ep_config.get("traffic_config")
        if tc and tc.get("routes"):
            traffic_config = TrafficConfig(
                routes=[
                    Route(
                        served_model_name=r["served_model_name"],
                        traffic_percentage=r["traffic_percentage"],
                    )
                    for r in tc["routes"]
                ]
            )

        # Build auto capture config
        auto_capture = None
        acc = ep_config.get("auto_capture_config")
        if acc and acc.get("enabled"):
            catalog_name = acc.get("catalog_name", "")
            if source_catalog and dest_catalog and catalog_name == source_catalog:
                catalog_name = dest_catalog
            auto_capture = AutoCaptureConfigInput(
                catalog_name=catalog_name,
                schema_name=acc.get("schema_name"),
                enabled=True,
            )

        client.serving_endpoints.create(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=served_entities,
                traffic_config=traffic_config,
                auto_capture_config=auto_capture,
            ),
        )

        result["success"] = True
        logger.info(f"Created serving endpoint: {endpoint_name}")

    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create endpoint {endpoint_name}: {e}")

    return result
