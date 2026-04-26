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

### My cross-workspace clone fails with "table has row level security or column masks, which is not supported by Delta Sharing" — what now?

Set `auto_handle_masks: true` on the `target_workspace` config block. Clone-Xs will then:
1. Detect masks/filters on each table via `DESCRIBE EXTENDED`,
2. Drop them on source so the table can join the share,
3. Run the clone (DEEP CLONE → views → functions),
4. Re-apply the masks/filters on the target,
5. Restore them on source if `data_sync_mode` is `snapshot_once` or `force_full`.

For `incremental` mode the source masks stay dropped while the sync is active — re-applying them mid-sync invalidates the share. Drop and re-apply manually after you stop syncing if you need source-side protection back. See the [Cross-workspace clone guide](../guide/clone#column-masks-and-row-filters) for the full flow.

### How do I keep the target catalog in sync with source after the initial clone?

Set `data_sync_mode: incremental` in the `target_workspace` config block. On the first run Clone-Xs creates the share/recipient/shared-catalog and full-clones every table. On every subsequent run for the same source → target pair, the deterministic Delta Sharing object names mean the handshake is skipped, and `CREATE OR REPLACE TABLE … DEEP CLONE` reads both Delta logs and copies only the files that changed since the last run.

⚠ DEEP CLONE is a one-way mirror. Any rows or columns added on the target side after a previous clone are lost on the next `incremental` run — Databricks doesn't expose `MERGE` semantics for the clone operation. If you need bidirectional or merge semantics, you'll need to write a `MERGE INTO` job that reads from the shared catalog Clone-Xs provisions on target.

The default `data_sync_mode: snapshot_once` is non-destructive: re-runs only catch newly-added tables and never touch already-cloned data.

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

## Cross-workspace troubleshooting

### Cross-workspace clone fails with "Source and target workspaces are in the same Unity Catalog metastore"

You're trying to clone via Delta Sharing between two workspaces that happen to share the same UC metastore. Delta Sharing requires distinct source and target metastores — you cannot share to yourself. The fix:

1. On `/clone`, **untick** "Clone to a different workspace"
2. Run a normal in-metastore clone (`source_catalog → destination_catalog`)

Same metastore = same UC = no Delta Sharing required. The same-metastore preflight check fails fast in 1–2 seconds before any orphan recipients/shares are created. To verify the metastores match: in either workspace's SQL editor, run `SELECT current_metastore()` — if both return the same UUID, this is your situation.

### Why does my second cross-workspace clone reuse the recipient from the first one?

You'll see a log line like:

```
Reusing existing recipient 'clone_xs_recipient_6dd41a34' that already points at
target metastore 'azure:westeurope:a649b7f5-...'. (Clone-Xs originally derived
'clone_xs_recipient_143c66be' from the deterministic hash, but Databricks
allows only one recipient per target metastore.)
```

This is correct behaviour, not a bug. **Databricks Unity Catalog enforces one recipient per `(source_metastore, target_metastore_sharing_id)` tuple.** After your first cross-workspace clone created `clone_xs_recipient_<suffix>` pointing at the target metastore, that one recipient occupies the only "slot" the source metastore has for that target. Subsequent clones from the same source workspace to the same target workspace — regardless of `dest_catalog` name, regardless of which deterministic suffix Clone-Xs computes — must share that single recipient.

**Recipients are pure auth identifiers**, so this is fine: one recipient can be GRANTed to many shares, and Clone-Xs creates a fresh share per `(source_catalog, dest_catalog)` pair. Sharing the recipient across multiple clone pairs is semantically correct.

The pre-fix symptom was that `CREATE RECIPIENT … USING ID …` issued via the SQL Statement Execution API silently no-oped against the in-use target metastore — the SDK exposes it as the real "already exists with same sharing identifier" error, which Clone-Xs now catches by scanning existing recipients first and reusing instead of creating.

If you want a fresh recipient (e.g. you DROPed the old one for compliance reasons), Clone-Xs's next clone will detect there's no existing recipient for the target and create one. No special config needed.

### "GRANT failed because recipient ... is not visible" / "phantom recipient"

Less common after the recipient-reuse fix above, but can still happen if `CREATE RECIPIENT` fails for an unrelated reason (cross-region/account constraint, missing entitlement, name collision with a soft-deleted recipient in another metastore). Clone-Xs now uses an **SDK-based `recipients.create()`** path (not SQL DDL), which surfaces the real error rather than silently no-oping. If the SDK call fails, you'll see the underlying Databricks error wrapped with these likely causes:

- Cross-account / cross-region D2D Delta Sharing isn't enabled on the source metastore (check Databricks Account Console → Delta Sharing settings)
- Your identity lacks `CREATE RECIPIENT` privilege on the source metastore (metastore-admin needed)
- Target metastore is unreachable from this account

To diagnose manually, run this in the source workspace SQL editor (without `IF NOT EXISTS`, so the real error surfaces):

```sql
CREATE RECIPIENT clone_xs_diag USING ID 'azure:westeurope:<target-metastore-uuid>';
```

### A table fails with "row level security or column masks, which is not supported by Delta Sharing" even with `auto_handle_masks: true`

The upfront `DESCRIBE EXTENDED` parser doesn't reliably catch every mask/filter format. Clone-Xs now catches this specific Delta Sharing error in the ADD TABLE loop and runs **inventory + drop + retry once**. If inventory still misses it, falls back to a blind `ALTER TABLE ... DROP ROW FILTER`. You should see lines like this in the log:

```
ADD failed for healthcare.facilities due to mask/row-filter; inventorying and dropping protections, then retrying
fallback DROP ROW FILTER on `demo_quick`.`healthcare`.`facilities`
retry succeeded for healthcare.facilities
```

The source-side mask/filter is still restored at the end of the run (via the existing `_apply_table_protections` finally block) for `snapshot_once` / `force_full` modes.

### DEEP CLONE fails with "TABLE_OR_VIEW_NOT_FOUND" on a table I just added to the share

`CREATE CATALOG ... USING SHARE` snapshots the share's table list at mount time. If you re-run a clone and a previous run already created the shared catalog on the target, subsequent additions to the share aren't visible to the existing mounted catalog. Clone-Xs fixes this automatically by **dropping and recreating the shared catalog** on the target whenever `to_add` is non-empty (i.e. tables were added to the share this run). If the issue persists, manually drop the shared catalog on the target and re-run:

```sql
-- Run on TARGET workspace SQL editor:
DROP CATALOG IF EXISTS clone_xs_shared_<suffix>;
```

The deterministic suffix is shown in the Clone-Xs log (`Shared cat: clone_xs_shared_xxxxxxxx`).

### Where are my saved target workspaces stored?

In **browser localStorage**, key `clxs_target_connections`. They never persist on the server. PATs and client_secrets are sent inline with each clone request; nothing touches `clone_config.yaml`. To clear all saved targets, open browser devtools → Application → Local Storage → delete the `clxs_target_connections` key. To export/import (e.g., onboard a teammate), copy the JSON value.

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
