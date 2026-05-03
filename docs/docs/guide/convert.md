---
sidebar_position: 12
---

# Convert table format — N×N in-place conversion

Distinct from clone: this rewrites the *source* table from one format to another in-place. The same FQN keeps pointing at the same logical data, but the underlying format changes. There is no destination — the operation has one identity and the source becomes the chosen target format.

This is the right tool when you've decided to migrate a table off one format and don't want a dual-table window where source and target both exist. It's the **wrong** tool when you want a copy in another catalog (use clone) or when downstream readers in the original format still need the table (they'll lose access).

## Supported format pairs

Six cells today, executed by four distinct physical strategies. Hudi is gated behind a Job-cluster runtime decision (D3, not yet shipped) — the UI accepts it but the request validator rejects every Hudi pair with a 422.

| Source ↓  Target → | DELTA | ICEBERG | PARQUET | HUDI |
|---|---|---|---|---|
| **DELTA** | identity (skipped) | UniForm metadata *(default)* / physical CTAS | CTAS + rename | gated |
| **ICEBERG** | `CONVERT TO DELTA` | identity | CTAS + rename | gated |
| **PARQUET** | `CONVERT TO DELTA` | CTAS + rename | identity | gated |
| **HUDI** | gated | gated | gated | identity |

Identity pairs (source already matches target) are short-circuited as `skipped` — the orchestrator emits no SQL.

## Strategies in detail

| Strategy | Used by pairs | What runs | Reversible? |
|---|---|---|---|
| `convert_to_delta` | `(PARQUET, DELTA)`, `(ICEBERG, DELTA)` | Single statement: `CONVERT TO DELTA <fqn>` | No |
| `uniform` | `(DELTA, ICEBERG)` *with `iceberg_physical=false` (default)* | 3-step ALTER chain: disable DV → REORG PURGE → SET UniForm props. Data files stay where they are; only metadata changes. | Yes — undo by removing the UniForm TBLPROPERTIES |
| `ctas_iceberg` | `(DELTA, ICEBERG)` *with `iceberg_physical=true`* and `(PARQUET, ICEBERG)` | 3-step CTAS+rename: create at `<fqn>_convert_tmp` → rename source aside → rename temp to original. Produces a *real* Iceberg table; UC reports `Data source: Iceberg`. Loses Delta history. | Yes if `keep_backup=true` (default) — operator can rename the backup back |
| `ctas_parquet` | `(DELTA, PARQUET)`, `(ICEBERG, PARQUET)` | 3-step CTAS+rename, only the `USING parquet` clause differs. Loses every Delta-only feature: history, deletion vectors, change feed, time travel. | Same as `ctas_iceberg` — `keep_backup=true` keeps a renamed source aside |

The audit row's `strategy_used` column records which path ran, so post-hoc you can tell whether a Delta→Iceberg run went through UniForm (no data movement) or physical CTAS (table replaced).

## When to use each strategy

Picking the right strategy comes down to three trade-offs: **data movement**, **history preservation**, and **what the downstream reader can consume**. Quick guide per strategy:

### `convert_to_delta` — best fit: lifting an open-format table into the Delta ecosystem

**Use when:**
- You have a Parquet or Iceberg table that you've decided should become Delta going forward.
- You want **time travel, change feed, deletion vectors, MERGE, OPTIMIZE** — all the Delta-only features.
- You're done with the source format and don't need it accessible by readers that only speak Parquet/Iceberg.

**Why it's a good fit:** runs as a single `CONVERT TO DELTA` statement, which is metadata-only on Parquet (no rewrite of the data files) and a metadata-rebuild on Iceberg. Fast and atomic — same FQN keeps working immediately after the statement returns.

**Avoid when:** downstream readers still need the table in its original format (they'll fail — same FQN, different format).

### `uniform` — best fit: dual-format access without copying data

**Use when:**
- You have a Delta table and you need an **Iceberg-compatible reader** (Snowflake, Trino, AWS Athena, BigQuery's Iceberg connector) to also read the same data.
- You don't want a second physical copy — storage cost, freshness lag, and operational complexity all go up with dual writes.
- The Iceberg reader supports UniForm metadata (most do as of 2025).

**Why it's a good fit:** zero data movement. UniForm writes Iceberg metadata alongside the existing Delta files; both readers see the same bytes. Reversible — drop the TBLPROPERTIES and the table is pure Delta again.

**Avoid when:** the downstream reader explicitly rejects UniForm-style Iceberg (rare, but some older engines insist on a "real" Iceberg table). In that case, switch to `ctas_iceberg`.

### `ctas_iceberg` — best fit: when an external engine demands a "real" Iceberg table

**Use when:**
- You picked Iceberg as your downstream format AND the consuming engine doesn't accept UniForm-emulated Iceberg.
- You're migrating off Delta entirely and want the table physically rewritten as Iceberg.
- You can tolerate a one-shot rewrite cost (data files get re-emitted) and the loss of Delta history.

**Why it's a good fit:** produces a table where Unity Catalog reports `Data source: Iceberg`. Every external engine treats it as native Iceberg — no UniForm-aware reader required.

**Trade-offs to accept:**
- **Loses Delta history** — time travel, change feed, MERGE log are gone after the rename.
- **3-step plan** (CTAS → rename source aside → rename temp to original) — atomic at each step, but the source table briefly exists under a backup name. `keep_backup=true` (default) keeps it accessible via the renamed FQN so you can roll back if validation finds drift.
- **Costs storage**: 2× until you delete the backup.

### `ctas_parquet` — best fit: feeding tools that only speak raw Parquet

**Use when:**
- You have a downstream consumer (legacy Spark on EMR, an older Athena workgroup, a custom reader) that **only** reads plain Parquet directories — no Delta log, no Iceberg metadata.
- The consumer can't be upgraded and you don't want a parallel pipeline maintaining a Parquet copy.

**Why it's a good fit:** simplest possible target — pure Parquet files in a UC-managed path. Maximum reader compatibility.

**Trade-offs to accept (significant):**
- **Loses every Delta-only feature** — history, deletion vectors, change feed, time travel, MERGE. This is a one-way door for everything except the data itself.
- Same 3-step backup-rename pattern as `ctas_iceberg`; same 2× storage during the migration.
- After this conversion, you're back to immutable-snapshot semantics — updates require a full table rewrite.

**Avoid unless** the legacy reader is truly the constraint. Most Parquet-only readers can be replaced or upgraded; `ctas_parquet` is the right answer when they can't.

### Quick decision matrix

| Goal | Strategy | Picked automatically when |
|---|---|---|
| "Make it Delta" | `convert_to_delta` | source is `PARQUET` or `ICEBERG`, target = `DELTA` |
| "Add Iceberg reader without copying" | `uniform` | source = `DELTA`, target = `ICEBERG`, `iceberg_physical=false` (default) |
| "Make it physically Iceberg" | `ctas_iceberg` | source = `DELTA` + `iceberg_physical=true`, OR source = `PARQUET`, target = `ICEBERG` |
| "Make it raw Parquet" | `ctas_parquet` | target = `PARQUET` (any source other than `PARQUET` itself) |

The orchestrator always picks the cheapest viable strategy by default — you only override (`iceberg_physical=true`) when the cheap path doesn't work for your downstream.

## Endpoint

```http
POST /api/convert-to-delta
```

The endpoint URL is unchanged for back-compat; only the UI page slug renamed (`/convert-to-delta` → `/convert`, with a redirect from the old URL).

### Request

```json
{
  "targets": [
    {"fqn": "edp_dev.bronze.events_iceberg",  "source_format": "ICEBERG", "target_format": "DELTA"},
    {"fqn": "edp_dev.bronze.legacy_parquet",  "source_format": "PARQUET", "target_format": "ICEBERG"},
    {"fqn": "edp_dev.silver.curated_delta",   "source_format": "DELTA",   "target_format": "PARQUET"}
  ],
  "warehouse_id": "abc123",
  "confirm_destructive": true,
  "iceberg_physical": false,
  "keep_backup": true,
  "dry_run": false
}
```

| Field | Required | Notes |
|---|---|---|
| `targets[]` | ✓ | At least one. Each target is a 3-part FQN, the source format you observed in UC, and the desired target. |
| `targets[].target_format` | optional, default `"DELTA"` | Old clients sending no field get the original D1 behaviour. |
| `warehouse_id` | If unset, falls back to the global config default. | Needed to execute the DDL. |
| `confirm_destructive` | ✓ unless `dry_run` | Explicit acknowledgement that the source table will be rewritten. The server returns `400` if missing. |
| `iceberg_physical` | optional, default `false` | Only meaningful for any `(DELTA, ICEBERG)` row. `false` picks the UniForm-update path; `true` picks the temp+rename CTAS path that produces a physical Iceberg table. |
| `keep_backup` | optional, default `true` | For temp+rename CTAS pairs (any → ICEBERG/PARQUET when not UniForm), `true` renames the source aside as `{fqn}_pre_convert_<utc>` so the conversion is reversible. `false` drops the source after rename — non-recoverable. |
| `dry_run` | optional, default `false` | Logs every step in the multi-step plan but doesn't execute. Bypasses the confirmation gate so wizard previews are safe. |

Unsupported pairs are rejected with a structured 422 that names the offending target — for example `(DELTA, HUDI)` returns `Some target pairs are not yet supported in this release. … Offending targets: edp_dev.bronze.x (DELTA→HUDI)`. Hudi pairs all fail this check until D3 ships.

### Response

```json
{
  "total": 3,
  "converted": 2,
  "failed": 0,
  "skipped": 1,
  "results": [
    {"fqn": "edp_dev.bronze.events_iceberg",
     "source_format": "ICEBERG", "destination_format": "DELTA",
     "strategy_used": "convert_to_delta",
     "status": "converted", "duration_ms": 14820, "error": null},
    {"fqn": "edp_dev.bronze.legacy_parquet",
     "source_format": "PARQUET", "destination_format": "ICEBERG",
     "strategy_used": "ctas_iceberg",
     "status": "converted", "duration_ms": 38110, "error": null},
    {"fqn": "edp_dev.silver.curated_delta",
     "source_format": "DELTA", "destination_format": "PARQUET",
     "strategy_used": "ctas_parquet",
     "status": "skipped", "duration_ms": 4,
     "error": "compat preflight refused: column `year` is GENERATED ALWAYS — Parquet has no equivalent"}
  ]
}
```

Per-table status is `converted` / `failed` / `skipped`. The endpoint returns **200 with partial results** when some tables in the batch fail or are refused — the response body has the per-table breakdown so you can re-submit just the offenders.

## Compatibility preflight

Before dispatching a strategy, the orchestrator runs a per-pair compatibility check (`src/format_compat.py`). Refusal returns `status="skipped"` with a structured `error` and **no SQL** runs against the warehouse. Today's checks:

| Source → Target | Refusal reason |
|---|---|
| `(ICEBERG, *)` | Hidden partitioning (`bucket(N, col)`, `truncate(N, col)`, `years(col)`, …) — Iceberg's hidden partitioning has no Delta/Parquet equivalent and would be silently dropped. Reuses `clone_iceberg.preflight_iceberg_source` so clone and convert refuse for the same reason. |
| `(DELTA, ICEBERG)`, `(DELTA, PARQUET)` | Source has a `GENERATED ALWAYS` or identity column — Iceberg/Parquet can't represent computed columns and a silent loss would surface as an incident later. |

The preflight is **skipped on dry-run** so operators can preview the plan even when the source has known incompatibilities (some users want the dry-run plan first to decide whether to refactor the source).

If `DESCRIBE TABLE EXTENDED` fails (perms, transient warehouse error), the preflight fails open — empty refusal list, conversion proceeds. The post-execution failure handler will surface the real problem in context, which is more useful than blocking on a transient.

## Multi-statement plans + dry-run

Every conversion is now built as a `Plan` of one or more `PlanStep`s (label + SQL). `Plan.execute()` runs each step in order; if a step fails, the exception is wrapped with the step's label so the operator sees `step 'disable deletion vectors' failed: …` rather than a bare SQL error.

The dry-run path renders every step:

```
[DRY RUN] [disable deletion vectors] ALTER TABLE `cat`.`schema`.`tbl` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
[DRY RUN] [purge deletion vector files] REORG TABLE `cat`.`schema`.`tbl` APPLY (PURGE)
[DRY RUN] [enable Iceberg compat metadata] ALTER TABLE `cat`.`schema`.`tbl` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
```

so the UI's preview shows the full sequence, not just the first statement.

## Permission preservation

The CTAS strategies (`ctas_iceberg`, `ctas_parquet`) replace the underlying table entirely — without intervention, the new table at the original FQN starts with no GRANTs and is owned by whoever ran the CONVERT. To keep the conversion permission-transparent, the orchestrator now:

1. Runs `SHOW GRANTS ON TABLE <fqn>` and reads `client.tables.get(fqn).owner` **before** executing the CTAS plan.
2. After the plan succeeds, replays each captured `(principal, privilege)` as a fresh `GRANT … TO …` and restores ownership via `ALTER TABLE … OWNER TO <principal>`.

The `convert_to_delta` and `uniform` strategies don't touch the underlying table identity (CONVERT TO DELTA mutates in place; UniForm only adds metadata), so they keep grants and ownership automatically — the capture/replay step is skipped for those.

The capture and replay are both **best-effort**:

- If `SHOW GRANTS` fails (perms, transient warehouse error), we log a warning and proceed without GRANT replay. The table converts; permissions reset.
- If a single `GRANT` replay fails (e.g. caller has ALTER but not GRANT on the new table), we log and continue with the rest. Partial replay is better than no replay.
- Ownership replay failures are logged identically.

This matches the clone path's behaviour (`copy_table_permissions` in `src/permissions.py`) so operator expectations stay consistent across the two surfaces.

## Safety gates

Three layers of "are you sure":

1. **Pydantic validator** at the request level — request without `confirm_destructive: true` (and without `dry_run: true`) returns `400` before any SQL touches the warehouse.
2. **Pair validator** — every `(source, target)` pair must be in `SUPPORTED_PAIRS`. Identity pairs are accepted (they short-circuit as `skipped`); Hudi and other unimplemented pairs return `422`.
3. **Module-level check** in `src/convert_to_delta.py:convert_tables_format` — defence in depth in case a future caller (CLI, scheduled job, etc.) bypasses the API model.

## Limitations

- **Source must be quiesced.** `CONVERT TO DELTA` rewrites data files; CTAS+rename rewrites the entire underlying storage. Concurrent writes during the conversion can corrupt the resulting log. Clone-Xs **does not** automatically quiesce the source for this endpoint — coordinate with upstream writers before submitting.
- **Iceberg requires DBR 13.3+** and the source must be UC-registered. Path-based references aren't supported.
- **History resets for CTAS strategies.** Delta time-travel starts at version 0 of the converted table. The UniForm path keeps history because the underlying Delta table is unchanged.
- **No reverse for `convert_to_delta`.** Once converted to Delta, there's no `CONVERT FROM DELTA`. Roll back via the `keep_backup` rename-aside (CTAS strategies only) or restore from a snapshot.
- **Hudi gated.** The four Hudi cells (any → HUDI, HUDI → any) are not yet executable. They surface in the UI as a disabled tooltip and the API returns 422 for any Hudi pair.

## History

Every batch generates one `operation_id` (UUID) and one row per target in `<audit_catalog>.logs.convert_operations`. The schema gained two columns this round (additive, idempotent migration on first call):

- `destination_format STRING` — pre-D1 rows backfilled to `"DELTA"`.
- `strategy_used STRING` — pre-D2 rows left empty.

The Convert page's **Recent runs** panel surfaces these in a `Source → Target` and `Strategy` column pair, so operators can tell at a glance which physical path each historical run took.

```http
GET /api/convert-to-delta/history?limit=50&status=failed&fqn_like=edp.bronze.%25&destination_format=ICEBERG
```

Filters: `limit` (capped at 1000 server-side), `status`, `fqn_like` (SQL LIKE), `dry_run`, `operation_id`, `destination_format`. Returns `{rows: [], count: 0}` rather than 404 when the audit table doesn't exist yet.

## When to use this vs. clone

| Situation | Use |
|---|---|
| Move data to a new catalog/schema, source unchanged | `POST /api/clone` |
| Source is Iceberg/Parquet, you want it to *be* Delta going forward | `POST /api/convert-to-delta` (target=DELTA) |
| Delta table that downstream Iceberg readers also need | `POST /api/convert-to-delta` (target=ICEBERG, `iceberg_physical=false` — UniForm metadata, no data movement) |
| Delta table you want as a *physical* Iceberg table for an external engine that doesn't read UniForm | `POST /api/convert-to-delta` (target=ICEBERG, `iceberg_physical=true`) |
| Same FQN, same data, but in raw Parquet for a downstream tool that insists | `POST /api/convert-to-delta` (target=PARQUET) — accept the history loss |
| You have downstream readers in the *original* format still in use | **Neither** — they'll break. Stand up a parallel target via clone, migrate readers, then convert the source. |
| Your Iceberg source has hidden partitioning that breaks clone | Not this — the convert preflight refuses hidden-partitioned Iceberg sources for the same reason clone does |
