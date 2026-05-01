---
sidebar_position: 3
title: Use Cases
---

# Use Cases

A catalog of common scenarios mapped to the right combination of Clone-Xs features. Start here if you know **what you want to do** but aren't sure which flags, flows, or guides apply.

Each use case lists:
- **The problem** — what situation you're in
- **Recommended approach** — which features to combine
- **Starting point** — a minimal config or command to adapt

---

## Disaster recovery

### DR replica across regions (same metastore)

**Problem:** Your production catalog lives in `eastus` and you need a hot standby in `westus` that stays close to real-time. Both workspaces share the same Unity Catalog metastore.

**Approach:** Same-workspace [DEEP CLONE](./clone#deep-vs-shallow-clone) for the initial snapshot, then [Incremental Sync](./sync#incremental-sync) on a cron for ongoing catch-up. Delta time-travel on the destination gives you point-in-time recovery.

```yaml
source_catalog: "prod"
destination_catalog: "prod_dr"
clone_type: "DEEP"
enable_rollback: true
validate_after_clone: true
```

Then schedule `clxs incremental-sync --source prod --dest prod_dr` every 15 minutes.

### Cross-cloud DR (AWS → Azure)

**Problem:** Compliance requires your DR site on a different cloud from production. The two workspaces are on different Unity Catalog metastores.

**Approach:** [Cross-workspace migration](./clone#cross-workspace--cross-cloud-migration) via Delta Sharing + DEEP CLONE. Source creates a share, target consumes it and materializes data into target cloud storage. Run on a schedule for a weekly warm replica.

```json
{
  "source_catalog": "retail_prod",
  "destination_catalog": "retail_prod_dr",
  "target_workspace": {
    "host": "https://adb-target.azuredatabricks.net",
    "auth_method": "pat",
    "token": "dapi...",
    "warehouse_id": "abc123"
  }
}
```

---

## Development & testing

### Dev sandbox from production snapshot

**Problem:** Developers need a production-like catalog to experiment in, but you don't want to pay storage for a full copy and you don't need persistence beyond a sprint.

**Approach:** [SHALLOW CLONE](./clone#deep-vs-shallow-clone) + [TTL](./advanced-clone#ttl-policies). Metadata-only pointer (near-zero storage cost), auto-expires after N days.

```bash
clxs clone \
  --source production \
  --dest dev_sandbox \
  --clone-type SHALLOW \
  --ttl 14d
```

### CI test catalog per pull request

**Problem:** Each PR spins up an isolated test catalog for integration tests, then tears it down. Keeping a SQL warehouse running 24/7 for this is wasteful.

**Approach:** [Serverless clone](./clone#serverless-execution) + SHALLOW + auto-rollback. No warehouse needed; the serverless job starts on demand. Pair with `schema_only: true` if the tests don't need data.

```bash
clxs clone \
  --source production \
  --dest pr_${PR_NUMBER} \
  --serverless \
  --volume /Volumes/ops/libs/clone_xs \
  --clone-type SHALLOW \
  --ttl 3d
```

### Schema migration dry run

**Problem:** You're about to rename columns and refactor tables in production. You want to validate the migration against real structure before touching production.

**Approach:** `schema_only: true` to create the destination catalog + schemas + empty tables (no data), run the migration scripts against it, diff the result.

```yaml
source_catalog: "production"
destination_catalog: "migration_test"
schema_only: true
copy_constraints: true
copy_comments: true
```

### QA environment for destructive tests

**Problem:** Your QA team runs destructive stress tests (deletes, updates, schema drops). They can't risk touching production files even through a shallow clone.

**Approach:** DEEP clone — fully independent data. Optional `validate_after_clone: true` to confirm parity before QA starts.

```yaml
source_catalog: "production"
destination_catalog: "qa_full"
clone_type: "DEEP"
validate_after_clone: true
generate_report: true
```

---

## Compliance & governance

### PII-masked dev copy

**Problem:** Developers need realistic production data but compliance requires PII masking. Raw prod data can't leave the locked-down workspace.

**Approach:** [Data masking](./clone#data-masking) during clone — PII columns pass through a masking function before write. Pair with [Scope Picker](./clone#scope-picker--partial-catalog-clones) to only copy the schemas developers need.

```yaml
source_catalog: "prod"
destination_catalog: "dev_masked"
clone_type: "DEEP"
include_schemas: ["orders", "customers"]
masking:
  "customers.email": "hash_email"
  "customers.phone": "redact_phone"
```

### Audit-ready snapshot

**Problem:** Regulator asks for the exact state of a catalog at month-end. You need a tamper-resistant frozen copy with a full manifest.

**Approach:** DEEP clone + `generate_report: true` + TTL set to the retention period. The report is the audit manifest (what was copied, counts, checksums, timestamps).

```yaml
source_catalog: "sales"
destination_catalog: "sales_2026_01_close"
clone_type: "DEEP"
as_of_timestamp: "2026-01-31T23:59:59"
validate_after_clone: true
validate_checksum: true
generate_report: true
ttl: "2555d"   # 7 years
```

### GDPR right-to-be-forgotten verification

**Problem:** A customer files a GDPR erasure request. Before running the delete, you want an isolated verification copy of their data to confirm the delete was complete.

**Approach:** [Scope Picker](./clone#scope-picker--partial-catalog-clones) to pick only the tables with the customer's data + [RTBF workflow](./rtbf) on the copy.

---

## Data migration

### Cross-cloud workload migration

**Problem:** Business decision to move production workloads from AWS to Azure. Every catalog, schema, table, view, function, volume, and permission has to land on the new side.

**Approach:** [Cross-workspace migration](./clone#cross-workspace--cross-cloud-migration) — full-fidelity Delta Sharing → DEEP CLONE pipeline handles all object types + metadata replay. Run per catalog; validate; cut over DNS / clients.

### Workspace consolidation

**Problem:** Merger brought 5 team workspaces under one org. You want all their catalogs under one consolidated workspace.

**Approach:** Five cross-workspace migrations, one per source → same target workspace. Namespace collisions handled by renaming `destination_catalog` per job.

```bash
for src in team_a team_b team_c team_d team_e; do
  clxs clone \
    --source "$src" \
    --dest "${src}_migrated" \
    --target-host https://adb-target.azuredatabricks.net \
    --target-token "$DAPI" \
    --target-warehouse "$WH_ID"
done
```

### Pre-deletion audit copy

**Problem:** About to drop a 5-year-old catalog that hasn't been touched in months. Legal wants a frozen copy retained before the drop.

**Approach:** DEEP CLONE to a `legal_hold_*` catalog with long TTL + `auto_rollback_on_failure: false` (don't revert if validation trips — keep whatever landed).

---

## Ongoing operations

### Weekly staging refresh

**Problem:** Staging drifts from prod as developers create throwaway tables and leave them. Every Monday you want staging to match prod structurally.

**Approach:** [Two-way Sync](./sync) with `--drop-extra`. Reconciles structure, removes tables that no longer exist in source.

```bash
clxs sync --source production --dest staging --drop-extra
```

Schedule via [Scheduling](./scheduling) or a Databricks workflow.

### Near-real-time prod → staging sync

**Problem:** You want staging to trail production by minutes, not days — but full re-clones take hours.

**Approach:** Enable Change Data Feed on source tables, run [Incremental Sync in CDF mode](./sync#how-it-works) every 5 minutes. Only the changed rows flow through.

```bash
# One-time: enable CDF on source tables
ALTER TABLE prod.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# Then cron: clxs incremental-sync --source prod --dest staging --sync-mode cdf
```

### Roll back a bad clone

**Problem:** A clone job ran but the destination looks wrong. You want to undo the clone non-destructively.

**Approach:** [Rollback](./rollback) using the rollback log. Uses Delta `RESTORE TABLE` to put pre-existing tables back to their pre-clone version; drops tables that were newly created.

```bash
clxs rollback --log-file rollback_logs/rollback_staging_20260419_103000.json
```

### Auto-recover from failed validation

**Problem:** You run scheduled clones unattended overnight. If a clone completes but validation finds > 5% row-count drift, you want to automatically undo it rather than leave a half-broken destination.

**Approach:** [Auto-rollback on fail](./rollback#auto-rollback-on-fail) with a threshold.

```yaml
validate_after_clone: true
auto_rollback_on_failure: true
rollback_threshold: 5.0
```

---

## Delta Live Tables

### Clone a DLT pipeline to a DR workspace

**Problem:** Your production DLT pipeline runs in one workspace. You need a DR copy ready to switch on in another workspace.

**Approach:** [DLT Pipeline clone](./dlt#pipeline-clone) (cross-workspace). Copies pipeline definition + libraries; lands in development mode for safe review.

### Duplicate a pipeline for A/B testing

**Problem:** You want to test a logic change in your Silver DLT pipeline without touching the live one.

**Approach:** Same-workspace DLT clone with a new name, modify the cloned libraries, run side-by-side against the same Bronze source.

---

## Advanced scenarios

### Time-travel clone from a known-good state

**Problem:** Bad data landed in production at 3 AM; you need a clean snapshot from 2:59 AM.

**Approach:** [Time-travel clone](./clone#time-travel) — DEEP CLONE with `as_of_timestamp` set to before the bad data landed.

```yaml
clone_type: "DEEP"
as_of_timestamp: "2026-04-19T02:59:00"
```

### Partial-region copy (only 2024 data)

**Problem:** Analytics team only needs the 2024 partition for their model; the full history is 8 TB.

**Approach:** [WHERE-clause clone](./clone) (DEEP only — loses Delta history).

```yaml
clone_type: "DEEP"
where_clauses:
  "*": "year = 2024"
  "orders.events": "year = 2024 AND region = 'US'"
```

### Clone only selected schemas/objects

**Problem:** Engineering team owns 3 of the 30 schemas in `prod`. They want a dev copy with just their 3 schemas.

**Approach:** [Scope Picker](./clone#scope-picker--partial-catalog-clones) (UI) or `include_schemas` + `include_objects` (API/YAML) for granular selection.

```yaml
source_catalog: "prod"
destination_catalog: "prod_dev"
include_objects:
  - { schema: "orders",    name: "line_items",  type: "table" }
  - { schema: "orders",    name: "customers",   type: "table" }
  - { schema: "marketing", name: "v_campaigns", type: "view" }
```

---

## Choosing the right clone type — quick reference

| If you want… | Use |
|---|---|
| Full independent copy (destructive-safe) | DEEP clone |
| Fast metadata-only reference | SHALLOW clone |
| Copy only changed tables since last run | Incremental sync (version mode) |
| Copy only changed rows since last run | Incremental sync (CDF mode — requires PK + CDF on source) |
| Keep dest structure aligned with source | Two-way Sync |
| Move catalog across workspaces / clouds | Cross-workspace migration |
| Spin up a catalog without a SQL warehouse | Serverless clone |
| Undo a previous clone | Rollback |
| Copy a DLT pipeline (definition only) | DLT pipeline clone |
| Restore at an earlier point in time | Time-travel clone (`as_of_timestamp` / `as_of_version`) |
| Subset of data by predicate | WHERE-clause clone (DEEP only) |
| Subset of schemas / specific objects | Scope Picker + `include_objects` |

See [configuration reference](../reference/configuration#clone-options-reference) for every config field that appears in these scenarios.
