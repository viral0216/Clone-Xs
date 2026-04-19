---
sidebar_position: 12
title: Advanced Features
---

# Advanced Features

> **Status note:** Two of the features below (Continuous Sync, Streaming/MV Data Clone) are **preview only** in v0.11.0 — the endpoints return a runnable plan but Clone-Xs does not auto-execute it. Full execution ships in v0.12.0.

Four production-ready features + two scaffolds for capabilities that sit one release away. Each has a dedicated endpoint; most have UI surfacing.

---

## Schema evolution

> **Source:** [`src/schema_evolution.py`](https://github.com/viral0216/clone-xs/blob/main/src/schema_evolution.py) · [`api/routers/schema_evolution.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/schema_evolution.py)

**When to use:** the source catalog picked up new columns since your last clone and you want to update the destination in place rather than re-clone the whole table. Additive changes (`ADD COLUMN`, compatible type widening) apply cleanly; destructive changes (`DROP COLUMN`) require explicit opt-in.

### How it works

1. `POST /api/schema-evolution/detect` — runs a column-level diff via `information_schema.columns` for one table. Returns `added_columns`, `removed_columns`, `changed_columns`, and an `is_compatible` flag.
2. `POST /api/schema-evolution/apply` — generates `ALTER TABLE … ADD COLUMN`, `DROP COLUMN` (opt-in), `ALTER COLUMN … SET DATA TYPE` statements from the detect response. Pass `dry_run: true` to preview the SQL without executing.
3. `POST /api/schema-evolution/evolve-catalog` — detects + applies across every table in the catalog in parallel. Use this after an incremental sync to bring the destination structure back into alignment.

### Usage

```bash
# Detect drift on a single table
curl -X POST $CLXS_HOST/api/schema-evolution/detect \
  -d '{"source_catalog":"prod","destination_catalog":"staging","schema_name":"orders","table_name":"line_items"}'

# Apply the returned changes (dry run)
curl -X POST $CLXS_HOST/api/schema-evolution/apply \
  -d '{
    "destination_catalog":"staging",
    "schema_name":"orders",
    "table_name":"line_items",
    "changes": { ... from detect ... },
    "dry_run": true
  }'

# Or fire-and-forget across the whole catalog
curl -X POST $CLXS_HOST/api/schema-evolution/evolve-catalog \
  -d '{"source_catalog":"prod","destination_catalog":"staging","dry_run":false}'
```

### Limits

- `ALTER COLUMN … SET DATA TYPE` is limited by Delta — only certain widenings are supported (e.g. `int → bigint`, not `string → int`). Failures are logged and reported in the `errors` array of the response; the table is skipped, not corrupted.
- Column drops are off by default. Pass `drop_removed: true` only when you're certain the destination shouldn't carry the old column.
- Not a replacement for [Incremental Sync](./sync#incremental-sync) — schema evolution aligns structure, Incremental Sync aligns data.

---

## Cross-metastore reconciliation

> **Source:** [`src/cross_metastore_recon.py`](https://github.com/viral0216/clone-xs/blob/main/src/cross_metastore_recon.py) · [`api/routers/cross_metastore_recon.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/cross_metastore_recon.py)

**When to use:** after a [cross-workspace migration](./clone#cross-workspace--cross-cloud-migration) you want to verify every table on the destination matches the source. The built-in validation endpoint is same-workspace only — this one spans two metastores via two `WorkspaceClient`s.

### How it works

Two layers of check, both running in parallel across tables:

1. **Row counts** — `SELECT COUNT(*)` on each side. Cheap, catches the common "some tables didn't clone" failure mode.
2. **Optional SHA-256 checksums** — `sha2(cast(sum(xxhash64(concat_ws('|', columns...))) as string), 256)` summed over hashable columns (excludes ARRAY / MAP / STRUCT). Catches silent data drift. Slower — reads the full table.

Response shape:

```json
{
  "status": "match | partial | mismatch | failed",
  "table_count": 611,
  "matched": 609,
  "mismatched": 2,
  "errors": 0,
  "use_checksum": false,
  "details": [
    {"schema":"bronze","table":"orders","source_count":123456,"target_count":123456,"match":true}
  ]
}
```

### Usage

```bash
curl -X POST $CLXS_HOST/api/reconciliation/cross-metastore \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_dr",
    "target_workspace": {
      "host": "https://adb-target.azuredatabricks.net",
      "auth_method": "pat",
      "token": "dapi...",
      "warehouse_id": "abc123"
    },
    "use_checksum": false,
    "max_workers": 4
  }'
```

Run checksum mode (`use_checksum: true`) on spot-check runs, not scheduled jobs — it reads every row. For scheduled jobs, row counts only.

---

## Clone signing / provenance

> **Source:** [`src/clone_provenance.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_provenance.py) · [`api/routers/clone_provenance.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/clone_provenance.py)

**When to use:** compliance workflows where you need to later prove "this catalog `prod_audit_2026_04` is the tamper-evident result of Clone-Xs run X with config Y at time T". HMAC-SHA256 signs a canonical manifest of the clone's config + result summary; tampering with any field breaks the signature.

### Enabling

```bash
export CLONE_XS_SIGNING_SECRET="<base64-encoded-random-bytes>"
```

Without the env var set, sign endpoints return `{"signed": false, "reason": "..."}` — no crypto failure, just a clear message.

### How it works

1. **Canonicalize** — drop sensitive fields (`token`, `client_secret`, `target_workspace`, etc.) + runtime-nondeterministic fields (logs, run_url), then `json.dumps` with `sort_keys=True, separators=(',',':')`. Two independent signings of the same logical clone agree on a byte-identical canonical form.
2. **Sign** — `HMAC-SHA256(secret, canonical).hexdigest()`. Envelope returned: `{signed, algorithm, signature, manifest}`.
3. **Verify** — re-canonicalize the manifest, recompute HMAC, compare in constant time (`hmac.compare_digest`).

### Usage

```bash
# Sign a completed job
curl -X POST $CLXS_HOST/api/provenance/sign/<job_id>

# Or sign an externally-constructed manifest
curl -X POST $CLXS_HOST/api/provenance/sign \
  -d '{"source_catalog":"prod","destination_catalog":"prod_audit","config":{...},"result":{...}}'

# Verify later
curl -X POST $CLXS_HOST/api/provenance/verify \
  -d '<the signed envelope from above>'
```

:::caution
This is tamper-evidence, not proof against an attacker with the secret. The HMAC secret is as sensitive as a database password — rotate if exposed, and don't check it into the repo. For stronger guarantees, sign the envelope with an external KMS and put its key ID in the manifest.
:::

---

## AI-suggested config (Clone Builder)

> **Source:** [`src/ai_service.py`](https://github.com/viral0216/clone-xs/blob/main/src/ai_service.py) · [`api/routers/ai.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/ai.py) · [`ui/src/components/CloneBuilder.tsx`](https://github.com/viral0216/clone-xs/blob/main/ui/src/components/CloneBuilder.tsx)

**When to use:** you know what you want in English but don't remember which combination of flags matches. The Clone Builder translates "I need a 14-day dev copy of prod without PII" into a ready-to-run `CloneRequest` config.

### How it works

The existing `/ai/clone-builder` endpoint forwards your natural-language query plus the list of available catalogs to the configured AI backend (Databricks Model Serving or the Anthropic API — see [AI Assistant](./ai-assistant) for backend setup). The service returns a structured config JSON + a short explanation of which flags it picked and why.

### Usage

**UI:** the Clone Builder modal in the header bar — ⌘K → "Clone Builder", or the sparkles icon.

**API:**

```bash
curl -X POST $CLXS_HOST/api/ai/clone-builder \
  -H "X-Databricks-Model: databricks-dbrx-instruct" \
  -d '{
    "query": "14-day dev copy of retail_prod, exclude PII columns, skip unused tables",
    "available_catalogs": ["retail_prod","retail_dev","retail_staging"]
  }'
```

Response:

```json
{
  "config": {
    "source_catalog": "retail_prod",
    "destination_catalog": "retail_dev",
    "clone_type": "SHALLOW",
    "ttl": "14d",
    "skip_unused": true,
    "masking": {...}
  },
  "explanation": "Using SHALLOW for low-cost dev; 14d TTL auto-expires; skip_unused cuts scope; masking applied to columns matching pii_patterns."
}
```

The AI can't know your column semantics — always review the suggested `masking` rules and filter config before executing.

---

## Continuous sync (preview)

> **Source:** [`src/continuous_sync.py`](https://github.com/viral0216/clone-xs/blob/main/src/continuous_sync.py) · [`api/routers/continuous_sync.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/continuous_sync.py)

:::caution Preview only in v0.11.0
This endpoint returns a **runnable streaming job plan** but Clone-Xs does not auto-submit it. Paste the plan into your scheduler, or wait for the v0.12.0 execution engine.
:::

**When it'll be useful:** for DR setups where minutes-fresh replicas are required. Beyond the scheduled [Incremental Sync](./sync#incremental-sync) which is polling-based, continuous sync uses Structured Streaming against the source's Change Data Feed — seconds-level lag, checkpoint-recoverable.

### What the plan contains

`POST /api/continuous-sync/plan` returns:

- **Declarative job spec** — `run_name`, task list, a placeholder `new_cluster` for the caller to fill in
- **Inlined Python body** — a `readStream … writeStream` template with CDF options preset + a clear `TODO` block where the v0.12.0 engine will inject the per-table MERGE logic
- **Prerequisites list** — e.g. "CDF enabled on every source table in scope"
- **Checkpoint root** — defaults to `/Volumes/<dest>/_sys/continuous_sync` if not provided

### Usage

```bash
curl -X POST $CLXS_HOST/api/continuous-sync/plan \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_dr",
    "schema_name": "orders",
    "trigger_ms": 30000
  }'
```

Save the response JSON, submit to a scheduler of your choice (Databricks Jobs CLI, Terraform, Airflow). The generated Python is valid and runs; it just doesn't have Clone-Xs's full MERGE-with-PK logic yet — that lands in v0.12.0.

---

## Streaming / MV data clone (preview)

> **Source:** [`src/streaming_clone_generator.py`](https://github.com/viral0216/clone-xs/blob/main/src/streaming_clone_generator.py) · [`api/routers/streaming_clone_generator.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/streaming_clone_generator.py)

:::caution Preview only in v0.11.0
Generates the DLT pipeline spec + notebook SQL. Does not auto-create the pipeline — caller POSTs to `/api/2.0/pipelines` themselves, or waits for v0.12.0.
:::

**When it'll be useful:** the existing [Advanced Tables Clone](./advanced-clone) flow migrates MV + streaming-table **definitions** but does not repopulate the data — because data in MVs and streaming tables can only be built by running a DLT pipeline. This module closes the loop by generating that pipeline.

### What the plan contains

`POST /api/streaming-clone/generate` returns:

- **`pipeline_spec`** — consumable by `client.pipelines.create()` as-is. Lands in development mode for safety.
- **`notebook_sql`** — one `CREATE OR REFRESH MATERIALIZED VIEW …` or `CREATE OR REFRESH STREAMING TABLE …` statement per advanced table, with source SQL bodies rewritten from the input
- **`notebook_path`** — where to paste the SQL before creating the pipeline
- **`next_steps`** — 3-step caller instructions

### Usage

```bash
curl -X POST $CLXS_HOST/api/streaming-clone/generate \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_dr",
    "schema_name": "analytics",
    "advanced_tables": [
      {"name": "daily_sales", "table_type": "MATERIALIZED_VIEW", "source_sql": "SELECT ... FROM prod.bronze.sales ..."},
      {"name": "live_orders", "table_type": "STREAMING_TABLE", "source_sql": "SELECT ... FROM STREAM(prod.bronze.orders)"}
    ]
  }'
```

Response includes the full notebook SQL — copy-paste into a notebook at the `notebook_path`, then POST the `pipeline_spec` to `/api/2.0/pipelines` and trigger a full-refresh update. The new pipeline populates both MVs + streaming tables on the destination.
