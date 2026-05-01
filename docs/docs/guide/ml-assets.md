---
sidebar_position: 40
title: ML Assets
---

# ML Assets

> Clone Models, Feature Tables, Vector Search Indexes, and Serving Endpoints alongside (or independently from) catalog clones.

## Overview

A "catalog clone" used to mean tables + views + functions + volumes. With ML workloads, the asset set widens to include Feature Tables (engineered ML inputs), MLflow Models (registered models + versions), Vector Search Indexes (RAG / semantic search), and Serving Endpoints (deployed inference). Clone-Xs treats each as a first-class asset type with its own clone path.

Source modules:
- [`src/clone_feature_tables.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_feature_tables.py) — Feature Store tables (offline)
- [`src/clone_models.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_models.py) — Registered models + versions
- [`src/clone_vector_search.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_vector_search.py) — Vector Search index definitions
- [`src/clone_serving_endpoints.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_serving_endpoints.py) — Model Serving endpoint configs
- [`api/routers/ml_assets.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/ml_assets.py) — `/api/ml-assets/*`
- UI at `/ml-assets` (tab-based)

## What gets cloned

| Asset | Clone behaviour | Notes |
|---|---|---|
| **Feature Tables** | Underlying Delta table is DEEP CLONED; Feature Store metadata (primary keys, timestamp keys, tags) is replayed via `FeatureStoreClient` | Online tables are recreated as references; backing offline data is what's actually copied |
| **MLflow Models** | Each registered model + version is exported and re-registered in destination's MLflow registry | Run history isn't cloned by default — opt in via `include_run_history: true` |
| **Vector Indexes** | Index definition (source table + embeddings model) is recreated; the index rebuilds at the destination | Embedding generation cost is paid again; for large indexes consider `recreate_in_place: true` to point at the existing source table |
| **Serving Endpoints** | Config + entity references; the endpoint is deployed in the destination workspace | Traffic split, auto-capture, rate limits are preserved |

## Discover & clone

```bash
# Discover ML assets in the source catalog
curl $CLXS_HOST/api/ml-assets/discover?source_catalog=prod
```

Returns:

```json
{
  "feature_tables": [...],
  "models": [...],
  "vector_indexes": [...],
  "serving_endpoints": [...]
}
```

```bash
# Clone all ML assets
curl -X POST $CLXS_HOST/api/ml-assets/clone \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "dev_ml_replica",
    "include_feature_tables": true,
    "include_models": true,
    "include_vector_indexes": true,
    "include_serving_endpoints": true
  }'
```

The UI at `/ml-assets` shows discovery + clone in a four-tab layout (one tab per asset class).

## Cross-workspace ML clone

For migrating ML assets across workspaces (e.g. dev → prod with model promotion), include a target workspace connection:

```bash
curl -X POST $CLXS_HOST/api/ml-assets/clone \
  -d '{
    "source_catalog": "dev",
    "destination_catalog": "prod_ml",
    "target_workspace": "prod-workspace-connection",
    "include_models": true,
    "promote_to_stage": "Production"
  }'
```

`promote_to_stage` automatically transitions the cloned model version to the named stage on the target workspace's MLflow registry.

## Vector index nuances

Vector indexes are expensive to rebuild. Three migration strategies:

1. **Rebuild in place** (`recreate_in_place: false`, default) — copy the source table, point a new index at the cloned table, regenerate embeddings. Most expensive but fully independent.
2. **Reference source table** (`recreate_in_place: true`) — the cloned index points at the source-catalog table. Cheap but creates a cross-catalog dependency; not suitable for production cutover.
3. **Pre-computed embeddings copy** (`copy_embeddings: true`) — copy the index's underlying embedding table along with the vector index, skip embedding generation. Best when embeddings are stable.

## Serving endpoints — cutover pattern

For zero-downtime cutover of a serving endpoint:

1. Clone the endpoint to the destination workspace with `traffic_split: {model_a: 0, model_b: 100}` (cloned, but receives no traffic yet).
2. Run shadow traffic from prod to the cloned endpoint and compare predictions for N hours.
3. Flip the source endpoint's traffic split to route to the cloned endpoint.
4. Once traffic is fully on the new endpoint, deprecate the old one.

Clone-Xs handles step 1; steps 2–4 are still manual in v0.7. A planned `/ml-assets/cutover` workflow in v0.8 will orchestrate the shadow comparison + auto-promotion.

## Related

- [Advanced Cloning](advanced-clone.md) — Materialized Views, Streaming Tables, Online Tables (which feature stores often depend on)
- [Lineage](analytics.md) — auto-discovers model → feature-table → source-table chains
