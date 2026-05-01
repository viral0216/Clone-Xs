---
sidebar_position: 39
title: Lakehouse Federation
---

# Lakehouse Federation

> Browse foreign catalogs (Postgres, MySQL, Snowflake, Redshift, BigQuery), manage connections, migrate to managed Delta.

## Overview

[Lakehouse Federation](https://docs.databricks.com/en/query-federation/index.html) lets Unity Catalog query external databases as if they were UC catalogs. Clone-Xs adds a UI on top of the federation primitives plus a one-click "promote to managed Delta" path so a federated catalog can graduate to a real cloned catalog.

Source: [`src/federation.py`](https://github.com/viral0216/clone-xs/blob/main/src/federation.py) · `/api/federation` · UI at `/federation`.

## Connections

A **connection** is a UC connection object pointing to an external database. The Federation page CRUD-s connections, validates them, and surfaces the list of foreign catalogs each connection exposes.

`GET /api/federation/connections` lists existing connections; `POST /api/federation/connections` creates one (Postgres example):

```yaml
name: prod_postgres
connection_type: postgresql
options:
  host: prod-db.example.com
  port: 5432
  user: clxs_reader
  password: ${secret:postgres/clxs_reader}      # UC secret reference
  database: prod
```

Once created, the connection appears as a foreign catalog in `SHOW CATALOGS` and is browsable from `/federation` like any UC catalog.

## Browse foreign tables

`GET /api/federation/tables?catalog={foreign_catalog}` returns the foreign catalog's schemas and tables. The UI renders them in a tree with table-level previews:

- Schema list (auto-fetched on connection)
- Table list per schema (foreign-table count + size estimate where the source provides it)
- Sample rows (uses `LIMIT 10` against the foreign table)

## Migrate foreign → managed Delta

When you're ready to promote a federated catalog to a managed copy in your lakehouse, kick off a federated migration:

```bash
curl -X POST $CLXS_HOST/api/federation/migrate \
  -d '{
    "source_catalog": "prod_postgres",
    "destination_catalog": "prod_warehouse",
    "options": {
      "include_schemas": ["public", "ecommerce"],
      "incremental_strategy": "watermark",
      "watermark_column": "updated_at"
    }
  }'
```

Returns `{ job_id }` which you can track on the standard `/clone` page. Under the hood the migration:

1. Issues `CREATE TABLE … AS SELECT * FROM <foreign_catalog>.<schema>.<table>` per table
2. Lands the result as managed Delta in the destination
3. Applies optional row sampling, column masking, schema filtering
4. Records lineage entries pointing the new managed table back at the foreign source

For incremental migrations, supply a `watermark_column` so subsequent runs only pull rows where `updated_at > last_run_watermark`.

## Supported sources

Lakehouse Federation supports (as of Databricks 2026-04):

- PostgreSQL · MySQL · SQL Server · Oracle · Snowflake · Redshift · BigQuery · Databricks
- Salesforce · Workday · ServiceNow · NetSuite (via Databricks-managed connectors)

Clone-Xs's UI tests connection-types it knows about; for new types not in the dropdown you can still use the raw "custom" connection type and supply options directly.

## Cost & latency

Federated queries are **not free** — they hit the foreign DB. The Federation page surfaces a "last-query-cost" estimate for each browse / preview interaction and exposes a per-connection rate-limit knob to avoid overwhelming the source.

For bulk analysis or anything beyond ad-hoc browsing, prefer the **migration** path — it pays the round-trip once and amortises over all subsequent reads against the managed Delta copy.

## Related

- [Clone](clone.md) — the underlying engine the migration path uses
- [Cross-Workspace Migration](advanced-clone.md) — for UC-to-UC across workspaces
- [Delta Sharing](analytics.md) — the recommended path for UC-to-UC across organisations
