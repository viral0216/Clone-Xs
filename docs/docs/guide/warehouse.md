---
title: SQL Warehouse
sidebar_label: Warehouse
---

# SQL Warehouse

The Warehouse page at `/infrastructure/warehouse` (or `/warehouse`) manages the Databricks SQL warehouses Clone-Xs uses for clone, sync, validation, and DQ rule execution. Where [Warehouse Efficiency](finops-warehouses.md) is the cost-tuning view, this page is the operational view: status, scaling, lifecycle.

## What you see

For each warehouse:

- Name + cluster size (`Small` … `4X-Large`)
- Type (Pro / Serverless / Classic)
- State (running / stopped / starting / stopping)
- Min / max scaling cluster count
- Auto-stop minutes
- Photon enabled
- Channel (current / preview)
- Tags
- Connection string (JDBC URL)

## Actions

Per-warehouse:

- **Start** / **Stop**
- **Resize** — change size or min/max clusters
- **Edit** — rename, change auto-stop, toggle Photon, set tags
- **Delete** — confirm dialog required

The toolbar adds:

- **Create warehouse** — wizard with sensible defaults for Clone-Xs workloads
- **Set default** — which warehouse the cloner uses by default

## Default warehouse

Clone-Xs uses one warehouse as default for all operations unless overridden in `clxs.yaml`:

```yaml
warehouse_id: 1234abcd
```

Override per profile or per command via `--warehouse` on the CLI. The Settings page also lets you pick the default for the active profile.

## Health & utilisation

Each card shows current concurrency, query queue depth, and a 24-hour load sparkline. Hover for tooltip with peak / avg.

For deeper trend analysis use [Warehouse Efficiency](finops-warehouses.md).

## Connection

Connection strings are read-only in the UI; copy with the clipboard icon. They include the JDBC URL, HTTP path, and a secret-reference placeholder for the access token (managed via [Authentication](authentication.md)).

## API

```bash
GET    /infrastructure/warehouses
POST   /infrastructure/warehouses           # create
PATCH  /infrastructure/warehouses/{id}      # edit
POST   /infrastructure/warehouses/{id}/start
POST   /infrastructure/warehouses/{id}/stop
DELETE /infrastructure/warehouses/{id}
```

These wrap the Databricks SQL Warehouses API.

## Related

- [Warehouse Efficiency](finops-warehouses.md) — cost & utilisation analysis
- [Authentication](authentication.md) — token / OAuth used for warehouse access
- [Infrastructure Overview](#) — portal landing page
