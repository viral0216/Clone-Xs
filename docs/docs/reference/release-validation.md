---
sidebar_position: 6
title: Release validation
---

# Release validation runbook

End-to-end validation procedure for a Clone-Xs release candidate. Three layers:
**(1) automated regression** (10s, run anytime), **(2) per-feature smoke** against
real Databricks (~30 min), **(3) one-shot end-to-end** that exercises multiple
features in a single clone.

The unit suite covers wiring + state-machine correctness via mocks; the smoke
runs prove the features actually behave correctly against real Databricks.
Both are required for a release.

---

## 1. Automated regression baseline

```bash
# Full suite — must be green before anything else
python3 -m pytest tests/ -q

# Just the recent feature-set (faster to iterate during fixes)
python3 -m pytest \
  tests/test_clone_tables.py \
  tests/test_clone_cross_workspace.py \
  tests/test_selective_reclone.py \
  tests/test_cost_estimation.py \
  tests/test_quiesce.py \
  tests/test_clone_fanout.py \
  tests/test_continuous_sync_runner.py \
  tests/test_router_clone.py \
  tests/test_router_continuous_sync.py -v
```

**Pass criteria:** zero failures; `1567 passed` (or higher with new tests).

```bash
# Lint — must be clean on src/ and tests/
python3 -m ruff check src/ tests/
```

---

## 2. Per-feature smoke

Each feature has a small, scripted smoke procedure. Use a sandbox catalog
(`demo_quick → demo_quick_*`) — these are designed to be cheap to recreate.

Set up once:

```bash
export CLXS_HOST=https://your-clone-xs.example.com
export SOURCE_CATALOG=demo_quick
```

### Feature 1 — Parquet / Iceberg source support

**Setup**

```sql
-- On source workspace, register a non-Delta table:
CREATE TABLE demo_quick.bronze.parquet_test (id INT, name STRING)
USING PARQUET LOCATION 's3://your-bucket/parquet_test/';
```

**Run**

```bash
clxs clone --source $SOURCE_CATALOG --dest demo_quick_p1
```

**Pass criteria**

- Run-summary JSON contains `summary.formats` with `{"DELTA": N, "PARQUET": 1}`.
- Step 4 result card renders the per-format Badge row (visible only when ≥ 2 formats).
- `DESCRIBE FORMATTED demo_quick_p1.bronze.parquet_test` shows `Provider: delta` (Databricks materialises CLONE as Delta regardless of source format).
- No `_format_clone_error` warnings in the logs (those only fire on Iceberg/Parquet edge cases).

### Feature 2 — Selective re-clone (`load_type: SELECTIVE`)

**Setup**

```bash
# Initial full clone establishes the target.
clxs clone --source $SOURCE_CATALOG --dest demo_quick_p2 --load-type FULL
```

**Drift one source table**

```sql
INSERT INTO demo_quick.bronze.events VALUES (...);
```

**Run**

```bash
clxs clone --source $SOURCE_CATALOG --dest demo_quick_p2 --load-type SELECTIVE
```

**Pass criteria**

- Run-summary JSON has `mode: "selective"` and `total_drifted_tables: 1`.
- Logs show `Schema bronze: 1 drifted (1 version_drift)` followed by exactly **one** `CREATE TABLE … DEEP CLONE` statement (not all of them).
- Other schemas log `Schema X in sync — 0 drifted tables`.
- Wall-clock time is much shorter than the FULL run.

### Feature 3 — Pre-clone source quiesce (`quiesce_source: true`)

**Setup**

In the UI, tick "Pre-clone source quiesce" on Step 2 (Options). Or via API:

```bash
curl -X POST $CLXS_HOST/api/clone -H "Content-Type: application/json" -d '{
  "source_catalog": "'$SOURCE_CATALOG'",
  "destination_catalog": "demo_quick_p3",
  "quiesce_source": true
}'
```

**While the clone is running, in another tab on the source workspace:**

```sql
-- Should fail with PERMISSION_DENIED
INSERT INTO demo_quick.bronze.events VALUES (1, 'mid-clone-write');
```

**After the clone completes:**

```sql
-- Should succeed — restore ran
INSERT INTO demo_quick.bronze.events VALUES (2, 'post-clone-write');
```

**Pass criteria**

- Mid-clone INSERT was denied (proves revoke fired).
- Post-clone INSERT succeeded (proves restore fired).
- Logs contain `Quiesce: revoked` followed at the end by `Quiesce restore complete: N principal/schema grant(s) re-applied`.
- No `Restore: could not re-grant ...` warnings.

**Failure-path validation:** kill the clone job mid-run (e.g. via the UI cancel button). Confirm the restore still ran (look for the "restore complete" log line) — the finally block must always execute.

### Feature 4 — Dry-run cost-vs-selective comparison

**Setup**

`demo_quick_p2` already exists from Feature 2. Hit `/estimate` against it:

```bash
curl -X POST $CLXS_HOST/api/estimate -H "Content-Type: application/json" -d '{
  "source_catalog": "'$SOURCE_CATALOG'",
  "destination_catalog": "demo_quick_p2"
}' | jq '.selective'
```

**Pass criteria**

- Response contains a `selective` block with `target_exists: true`, `tables_to_clone`, `tables_in_sync`, `savings_pct`, `recommended`.
- In the UI Preview panel, the "Full clone vs selective re-clone" tile renders with the appropriate "Recommended: SELECTIVE" or "Recommended: FULL" badge.

**Hide-on-fresh-target:**

```bash
# Point at a non-existent dest catalog — selective block must be ABSENT
curl -X POST $CLXS_HOST/api/estimate -d '{
  "source_catalog": "'$SOURCE_CATALOG'",
  "destination_catalog": "this_does_not_exist_yet"
}' | jq '.selective'
# → should print null
```

**Accuracy:** then run an actual SELECTIVE clone. Confirm `bytes_copied` from the run summary is within ~10% of `selective.size_bytes` from the estimate (the roadmap acceptance criterion).

### Feature 5 — Multi-target fanout (`target_workspaces`)

**Setup**

You need 2+ saved target connections in `/settings`. If you only have one
workspace pair, register the same target twice with different names but
different `dest` catalogs (the deterministic suffix differs by dest catalog).

**Run via UI**

`/clone` → tick "Clone to a different workspace" → tick "Fan out to multiple targets" → pick 2 targets → set parallel=5 → submit.

**Pass criteria**

- Submit message: `Multi-target fanout clone job submitted (N targets, max_parallel=5)`.
- Step 4 result card shows per-target rollup rows (✓/✗ icon, host, tables/bytes/duration).
- Aggregate badge shows SUCCESS / PARTIAL / FAILED matching the per-target outcomes.
- Run-summary `mode: "fanout"`, `target_count: 2`, `succeeded_targets`/`failed_targets` add up.

**Failure-isolation smoke** — point one target at a deliberately broken `warehouse_id`:

- Aggregate goes `partial`.
- Broken target shows ✗ with the SDK error string in `error`.
- Other target completes with `target_status: success` and real bytes/tables.

### Feature 6 — Continuous sync executor

**Prerequisites on source**

```sql
-- Enable CDF on the table you want to stream
ALTER TABLE demo_quick.bronze.events
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

**Start a stream**

```bash
RESPONSE=$(curl -X POST $CLXS_HOST/api/continuous-sync/start -H "Content-Type: application/json" -d '{
  "source_catalog": "'$SOURCE_CATALOG'",
  "destination_catalog": "demo_quick_streaming",
  "tables": ["bronze.events"],
  "trigger_ms": 30000
}')
echo $RESPONSE | jq
STREAM_ID=$(echo $RESPONSE | jq -r '.stream_id')
```

**Verify it's running**

```bash
# Should show status=running after ~30s
curl "$CLXS_HOST/api/continuous-sync/streams?refresh=true" | jq

# Or check Databricks Jobs UI:
#   filter "Run name" by clxs-continuous-sync-
```

**Insert and observe propagation**

```sql
-- On source:
INSERT INTO demo_quick.bronze.events VALUES (...);

-- After ~30-60s (one trigger cycle), on target:
SELECT count(*) FROM demo_quick_streaming.bronze.events;
-- count must reflect the insert
```

**Restart smoke**

```bash
# Should cancel the existing run + submit a new one with a NEW run_id
# but the SAME stream_id
curl -X POST $CLXS_HOST/api/continuous-sync/streams/$STREAM_ID/restart | jq
```

**Stop**

```bash
curl -X POST $CLXS_HOST/api/continuous-sync/streams/$STREAM_ID/stop | jq
# response status: stopped
```

**Pass criteria**

- Stream status transitions: starting → running → (insert visible on target) → stopped.
- Run-id changes after restart, stream_id is preserved.
- After API server restart (`docker restart` or equivalent), the stream is re-discovered: `GET /streams` lists it as `running` (status came from `discover_existing_streams`).

**24-hour smoke** — *operations exercise, not part of unit suite.* Run a stream against a low-volume source for 24h+, assert delta is visible on target within minute-level latency throughout. Document any restart events and root-cause them before tagging the release.

---

## 3. End-to-end "kitchen sink"

A single clone that exercises 4 features at once. Validates they compose
correctly (no surprising interactions):

```bash
curl -X POST $CLXS_HOST/api/clone -H "Content-Type: application/json" -d '{
  "source_catalog": "'$SOURCE_CATALOG'",
  "destination_catalog": "demo_quick_kitchen_sink",
  "load_type": "SELECTIVE",
  "quiesce_source": true,
  "clone_tbl_properties": {"delta.logRetentionDuration": "30 days"}
}'
```

**Pass criteria**

- Run-summary `mode: "selective"` and `total_drifted_tables` populated (Feature 2).
- Logs show `Quiesce: revoked` and `Quiesce restore complete` (Feature 3).
- `summary.formats` populated, possibly mixed (Feature 1).
- `summary.bytes_copied`, `files_copied` populated (Tier 1 work).
- `SHOW TBLPROPERTIES demo_quick_kitchen_sink.bronze.events` contains `delta.logRetentionDuration = '30 days'` (Tier 1 work).

For a 5-feature kitchen sink, add `target_workspace` (or `target_workspaces`) and run the same payload — that exercises cross-workspace + recipient reuse on top.

---

## 4. Evidence locations

| Where | What it proves |
|---|---|
| Run summary JSON (Step 4 / `/api/clone/{id}` response) | `mode`, `formats`, `bytes_copied`, `total_drifted_tables`, `per_target`, `selective` |
| `edp_dev.logging.logging_01` | Per-job audit trail row: status, duration, error |
| `edp_dev.metrics.clone_metrics` | Per-table CLONE counters (`copied_files_size`, `num_copied_files`, etc.) |
| Databricks Jobs UI | Continuous sync runs — filter by `run_name LIKE 'clxs-continuous-sync-%'` |
| Application logs | `Quiesce: revoked`, `Reusing existing Delta Share`, `Schema X in sync — 0 drifted tables` |
| Source workspace `SHOW RECIPIENTS` | One recipient per target metastore (deterministic name `clone_xs_recipient_<sha1>`) |
| Source workspace `SHOW SHARES` | One share per `(source, dest, target_metastore)` tuple |

---

## 5. Pre-release checklist

Tick all before tagging a release:

- [ ] Full pytest suite green (`python3 -m pytest tests/ -q`)
- [ ] `ruff check src/ tests/` clean
- [ ] UI build clean (`cd ui && npm run build`)
- [ ] Each feature smoke (sections 2.1 - 2.6) passed against a real Databricks workspace
- [ ] Kitchen-sink end-to-end (section 3) passed
- [ ] Continuous sync 24h+ smoke documented (operations exercise — see Feature 6)
- [ ] Cross-workspace fanout to 2 distinct workspaces validated (section 2.5)
- [ ] Changelog `Unreleased` sections promoted to a dated release header
- [ ] `docs/docs/reference/changelog.md` entries reference the right files / fields / contracts

If any feature smoke regresses on a release candidate, hold the release and fix the underlying issue — don't ship "all green except 2.4". The smoke procedures are the only validation that proves the code does what the unit tests claim.
