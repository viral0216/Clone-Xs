---
sidebar_position: 3
title: FAQ
---

# FAQ

## General

### What Databricks features does this require?

- **Unity Catalog** — your workspace must have Unity Catalog enabled
- **SQL Warehouse** — serverless or pro (classic SQL warehouses work too)
- **Databricks SDK** — v0.20.0 or later

### Does it work with Hive Metastore?

No. Clone Catalog only works with Unity Catalog. Hive metastore tables are not supported.

### Can I clone across workspaces?

Yes. Use the `--dest-host` and `--dest-token` flags to specify the destination workspace. Both workspaces must share the same Unity Catalog metastore, or you must have cross-metastore access configured.

### Does it clone the data or just metadata?

- **Deep clone** copies all data (creates independent Delta files)
- **Shallow clone** copies only metadata (references source data files)

---

## Authentication

### I'm getting "permission denied" errors

Ensure your user or service principal has:
- `USE CATALOG` on the source catalog
- `CREATE CATALOG` permission (if destination doesn't exist)
- `USE CATALOG` + `CREATE SCHEMA` + `CREATE TABLE` on the destination

### How do I authenticate in CI/CD?

Use environment variables (`DATABRICKS_HOST` + `DATABRICKS_TOKEN`) or a service principal (`DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`). See [CI/CD](../guide/cicd).

### Can I use browser login in a headless environment?

Not directly. Use `--auth-profile` with a pre-configured profile, or set environment variables instead.

---

## Cloning

### The clone failed halfway. Can I resume?

Yes. Re-run the same command with `--load-type INCREMENTAL`. It will skip objects that already exist in the destination and only clone what's missing.

### Why is my clone slow?

- Increase `--max-workers` for more parallelism
- Check if your SQL warehouse is auto-scaling (more clusters = faster)
- Large tables take time for deep clone — consider shallow clone for dev/test

### What happens if the destination catalog already exists?

The tool uses `CREATE CATALOG IF NOT EXISTS` — it won't overwrite an existing catalog. Schemas, tables, views, etc. within the catalog follow the same pattern.

### Can I clone only specific tables?

Yes. Use `--include-tables-regex` with a regex:

```bash
clxs clone --source prod --dest staging --include-tables-regex "^dim_.*"
```

### What about views that reference the source catalog?

View definitions are automatically rewritten to reference the destination catalog. For example, `SELECT * FROM production.sales.orders` becomes `SELECT * FROM staging.sales.orders`.

---

## Troubleshooting

### "Metastore storage root URL does not exist"

Your workspace uses Default Storage and requires a managed location when creating catalogs. Use the `--location` flag:

```bash
clxs clone --source prod --dest staging \
  --location "abfss://catalog@storage.dfs.core.windows.net/staging"
```

### "System owned catalog can't be modified"

This happens when a catalog was created without an explicit owner. Drop the catalog and re-run (the tool now sets proper ownership at creation time). If you can't drop it, use the Databricks Account Console.

### "The wait_timeout field must be 0 seconds, or between 5 and 50 seconds"

This was a bug in earlier versions. Update to the latest version:

```bash
pip install clone-xs
```

### Pre-flight shows failures but I want to proceed anyway

Pre-flight is advisory. The clone command doesn't require pre-flight to pass. Common "expected" failures:
- **Destination doesn't exist** — the clone will create it
- **Warehouse is STOPPED** — start it before cloning

### How do I see what SQL is being executed?

Use verbose mode:

```bash
clxs clone --source prod --dest staging -v
```

Or use dry run to see all SQL without executing:

```bash
clxs clone --source prod --dest staging --dry-run
```

---

## New features

### How does auto-rollback work?

When `--auto-rollback` is enabled, the clone pipeline runs post-clone validation (row counts + optional checksums). If the mismatch percentage exceeds the threshold (default 5%), the tool automatically rolls back all cloned objects and sends a notification. Both `--enable-rollback` and `--validate` are force-enabled when using `--auto-rollback`.

### What are clone templates?

Templates are predefined configuration profiles for common scenarios. Use `clxs templates list` to see all available templates, then apply one with `clxs clone --template dev-refresh`. Built-in templates include `dev-refresh` (shallow, no permissions), `dr-replica` (deep with checksums), `pii-safe` (deep with masking), and more.

### How do I schedule recurring clones?

Use `clxs schedule --interval 6h` for interval-based scheduling, or `--cron "0 */6 * * *"` for cron expressions. The scheduler includes drift detection — if no changes are found between source and destination, the clone is skipped. Use `--no-drift-check` to force cloning every run.

### What is the API server mode?

`clxs serve` starts a REST API server that exposes clone operations as HTTP endpoints. You can submit clone jobs via `POST /api/clone`, check status via `GET /api/clone/{id}`, and run diffs or validations. Secure it with `--api-key`.

### How does RBAC work?

RBAC policies are defined in a YAML file (default: `~/.clone-xs/rbac_policy.yaml`). Rules specify which principals (users/groups) can clone which source/destination catalogs. Deny rules are evaluated first. Enable with `rbac_enabled: true` in config.

### What are TTL policies?

TTL (Time-To-Live) policies automatically track expiration dates for cloned catalogs. Set a TTL during clone with `--ttl 7d`, then use `clxs ttl cleanup --confirm` to drop expired catalogs. Useful for ephemeral dev/test environments.

### How do I use the WHERE clause filter?

Use `--where "year >= 2024"` to filter all tables globally, or `--table-filter "sales.orders:region = 'US'"` for per-table filters. Note: filtered clones use `CREATE TABLE AS SELECT` instead of `DEEP CLONE`, which means Delta history and versioning are not preserved.

### What metrics does Clone Catalog track?

When `metrics_enabled: true`, the tool tracks clone duration, throughput (tables/min), failure rate, row counts, and data sizes. Metrics can be exported to Delta tables, JSON files, Prometheus format, or webhooks.
