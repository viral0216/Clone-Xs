# Convert Table Format In-Place: How Clone-Xs Handles N×N Lakehouse Format Conversion

How Clone-Xs v0.9.0 rewrites Unity Catalog tables between Delta, Iceberg, and Parquet at the same FQN — six format pairs, four physical strategies, one identity.

> **Status: shipped in v0.9.0.** The N×N converter described here is live in [Clone-Xs v0.9.0](https://github.com/viral0216/clone-xs/blob/main/docs/docs/reference/changelog.md). Hudi pairs are gated behind a future runtime decision (D3) — the UI accepts them but the API rejects them with a 422 until the work ships.

- - -

If you've ever owned a lakehouse table for more than two years, you've watched a format decision age. You picked Delta in 2022 because Databricks made it the path of least resistance, the warehouse was already provisioned, and the time-travel demo was genuinely impressive. Three years later, your downstream Snowflake users want Iceberg. Your data science team is on Trino and they want Iceberg. Your compliance org has standardised on a tool that reads only "real" Iceberg tables, not Iceberg-via-UniForm. And meanwhile, an old EMR job that still feeds the executive dashboard reads only raw Parquet directories — no log, no catalog metadata.

You have one logical dataset and four downstream readers, each with its own opinion about what shape the bytes should be in. The lakehouse promise was supposed to be one copy, many readers. The reality is that "many readers" turns out to mean "many formats," and every format gets there a different way.

The naive answer is to clone. Make a Delta copy here, a UniForm copy there, a physical Iceberg copy for Snowflake, a Parquet copy for the EMR job. Now you have four tables, four storage costs, four refresh schedules, and four ways the data can drift between copies — and a downstream user who reads "events" will need to know which "events" they're looking at.

The other answer is to convert in place. Pick the table, pick the new format, and rewrite the same FQN. The same name keeps pointing at the same logical data; the underlying format changes. There is no destination, no copy, no drift. This post is about how Clone-Xs v0.9.0 does that for six format pairs across four physical strategies, what each strategy actually runs against your warehouse, what it can't do, and how it stays honest about every trade-off along the way.

> [INSERT IMAGE: screenshots/convert-page-overview.png]
> Caption: The Convert page in Clone-Xs. Per-row target dropdown lets you mix conversions — one row to Delta, the next to physical Iceberg, the third to Parquet — in a single batch.

- - -

## Convert is not clone

The cardinal distinction up front, because it determines whether you should read the rest of this post: **convert rewrites the source in place**. Clone copies the source to a new destination. Two completely different operations.

Clone has two identities — source and destination — and the source is left untouched. After a clone you have two tables. Convert has one identity. The same fully-qualified name (FQN) keeps pointing at the same logical data, but the underlying format underneath that name changes. After a convert you still have one table; only its physical representation has changed.

This sounds pedantic until you think about who reads the table. If a downstream consumer holds a reference to `edp_prod.silver.events` and that table is Delta, and you convert it to physical Iceberg, the consumer's reference is still valid — they read `edp_prod.silver.events` and get the same logical rows back. If their reader speaks Iceberg, they're fine. If their reader only speaks Delta, they break.

That's the trade convert asks you to make. You're saying *no reader will need this in its current format anymore* — or you're committing to migrating every reader before you push the button. Clone is the right tool when you want a parallel target so you can migrate readers gradually. Convert is the right tool when you've already done that work and you want to reclaim the storage, retire the old format, and stop paying for two copies.

The other reason this matters: convert is destructive in a way clone isn't. Once `CONVERT TO DELTA` runs over an Iceberg table, there is no `CONVERT FROM DELTA` to undo it. Once a CTAS-and-rename rewrites a Delta table as Parquet, the Delta history is gone. You can roll back via a renamed-aside backup if you opted in (more on that later), but you cannot reverse the rewrite itself. Clone is reversible because the source still exists; convert is reversible only if you set up the safety net first.

If you're converting because you want a parallel copy, you want clone. If you're converting because the original table needs to *be* the new format going forward, you want this.

- - -

## The N×N problem

Four formats — Delta, Iceberg, Parquet, Hudi — give you a 4×4 matrix of possible conversions. The diagonals (Delta→Delta, Iceberg→Iceberg, etc.) are identity: there is nothing to do, and Clone-Xs short-circuits them as `skipped` without sending any SQL to the warehouse. The Hudi cells (any format → Hudi, or Hudi → any format) are gated until a Job-cluster runtime sponsorship lands; the API rejects every Hudi pair with a structured 422 today. That leaves six executable cells:

| Source ↓  Target → | DELTA | ICEBERG | PARQUET | HUDI |
|---|---|---|---|---|
| **DELTA** | identity (skipped) | UniForm metadata *(default)* / physical CTAS | CTAS + rename | gated |
| **ICEBERG** | `CONVERT TO DELTA` | identity | CTAS + rename | gated |
| **PARQUET** | `CONVERT TO DELTA` | CTAS + rename | identity | gated |
| **HUDI** | gated | gated | gated | identity |

Six cells. The naive design would write six conversion paths, one per cell, and hope nobody adds a fifth format. The Clone-Xs design groups by physics: cells that need the same kind of rewrite share a strategy. `(PARQUET, DELTA)` and `(ICEBERG, DELTA)` both run a single `CONVERT TO DELTA` statement and share a strategy. `(DELTA, ICEBERG)` with the cheap path runs a UniForm metadata rewrite. `(DELTA, ICEBERG)` with the physical path, plus `(PARQUET, ICEBERG)`, run a temp-and-rename CTAS into Iceberg. `(DELTA, PARQUET)` and `(ICEBERG, PARQUET)` run the same temp-and-rename CTAS but with `USING parquet`. Six cells collapse into four strategies.

The audit row's `strategy_used` column records which path actually ran for each conversion, so post-hoc you can ask "did this Delta-to-Iceberg run go through UniForm metadata or physical CTAS?" without re-reading the request body. New format pairs in future releases register one more entry in the dispatch table; the strategy primitives don't change.

That framing — six format pairs, four strategies — is the entire mental model for the converter. Everything else in this post is about how each strategy actually executes, what it costs, and what it refuses to do.

- - -

## Strategy 1: `convert_to_delta` — lifting an open-format table into the Delta ecosystem

The simplest of the four. Used for `(PARQUET, DELTA)` and `(ICEBERG, DELTA)`. The plan is exactly one step:

```sql
CONVERT TO DELTA `edp_prod`.`bronze`.`legacy_parquet`
```

That's it. The statement runs synchronously against the warehouse, the table comes back as Delta, and every Delta-only feature is now available — time travel, change feed, deletion vectors, MERGE, OPTIMIZE, the lot.

Two important details about what `CONVERT TO DELTA` actually does. On a Parquet source, it's metadata-only: Databricks reads the existing Parquet files in place, generates a Delta log over them, and the same data files now belong to a Delta table. No data movement, no rewrite, fast at any size. On an Iceberg source, it's a metadata rebuild: Databricks reads the Iceberg metadata to discover the data files, then writes a Delta log that references them. Slightly heavier than the Parquet path, but still well short of a full data rewrite.

This is the right strategy when you've decided that the table belongs in the Delta world going forward. You want time travel. You want MERGE statements. You want change-data-feed for downstream incremental consumers. You want OPTIMIZE to compact small files. None of those features exist on Parquet, and only some of them work on Iceberg. Pulling the table into Delta is the door to all of it.

The trade-off is that the conversion is a one-way door. There is no `CONVERT FROM DELTA`. If a downstream reader still needs to read the table as Parquet or Iceberg, the only recovery path is a fresh CTAS into the desired format — which is what the other three strategies do. The corollary: don't run `convert_to_delta` until you've migrated every downstream reader off the original format. They'll still find the table at the same FQN; they'll just fail to read it.

The other trade-off is identity-column compatibility. If the source has a `GENERATED ALWAYS` column, the target Delta table can hold it fine — but the inverse is the problem the next strategies have to deal with. We'll come back to this in the preflight section.

- - -

## Strategy 2: `uniform` — dual-format access without copying data

This is the unsexy hero of the four strategies. Used by default for `(DELTA, ICEBERG)` whenever `iceberg_physical=false` (which is the default). It's the only strategy that doesn't move a single byte of data.

UniForm — Universal Format — is a Databricks feature that writes Iceberg-compatible metadata alongside an existing Delta table. The data files stay where they are, in the same Delta storage layout. A separate set of metadata files describes the same data in Iceberg's terms, so an Iceberg reader (Snowflake, Trino, AWS Athena, BigQuery's Iceberg connector — most engines as of 2025) can read the same bytes through its native Iceberg path. Two readers, one storage footprint, zero copy lag.

The plan is three steps, in a strict order. The order matters: Databricks' IcebergCompatV2 validator rejects any other sequence with `DELTA_ICEBERG_COMPAT_VIOLATION.DELETION_VECTORS_SHOULD_BE_DISABLED`. Get it wrong and the third statement fails.

```sql
-- step 1: disable deletion vectors
ALTER TABLE `edp_prod`.`silver`.`curated_delta`
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

-- step 2: purge deletion vector files
REORG TABLE `edp_prod`.`silver`.`curated_delta` APPLY (PURGE);

-- step 3: enable Iceberg compat metadata
ALTER TABLE `edp_prod`.`silver`.`curated_delta` SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

Step one disables the deletion-vector feature on the Delta side — Iceberg has no equivalent and IcebergCompatV2 refuses to enable on a table that still has them turned on. Step two physically rewrites any data files that had associated deletion vectors so the rows-marked-deleted-via-DV become rows-actually-deleted-from-the-files (still without rewriting unaffected files). Step three flips the IcebergCompatV2 switch and tells UniForm to start emitting Iceberg metadata.

After the three steps, `edp_prod.silver.curated_delta` is still a Delta table — Unity Catalog reports `Data source: Delta` — but it now also has Iceberg metadata that an Iceberg-aware engine can read. Both worlds, same files, same FQN.

The properties of this strategy are unusual and worth dwelling on:

- **Zero data movement.** The plan is three ALTER/REORG statements, none of which copies the data. REORG only touches files that had deletion vectors associated; on most tables that's a small fraction of the total.
- **Reversible.** Drop the three TBLPROPERTIES (`delta.universalFormat.enabledFormats`, `delta.enableIcebergCompatV2`, `delta.columnMapping.mode`) and the table is pure Delta again. The Iceberg metadata is generated lazily and goes away when you stop asking for it.
- **Keeps Delta history.** Time travel and change feed are unaffected — the underlying Delta log is unchanged, only augmented.
- **Keeps grants and ownership.** UniForm doesn't replace the table identity, so Unity Catalog's permissions persist transparently. (The CTAS strategies need explicit capture-and-replay, covered later.)

The catch — there's always a catch — is that some downstream engines reject UniForm-style Iceberg. Most engines don't, but the ones that do typically insist on a native Iceberg table file layout, not a Delta layout with Iceberg metadata stapled on. For those engines, you need the next strategy.

- - -

## Strategy 3: `ctas_iceberg` — physical Iceberg via CTAS + rename

When the consuming engine demands a "real" Iceberg table — file layout, metadata, the works — UniForm isn't enough. You need the table to physically be Iceberg, with Unity Catalog reporting `Data source: Iceberg`. This strategy gets you there, at the cost of a one-shot rewrite and the loss of Delta history.

It's used for `(DELTA, ICEBERG)` when you set `iceberg_physical=true`, and for `(PARQUET, ICEBERG)` always (UniForm needs Delta as a base, so Parquet sources have to physically rewrite). The plan is three steps:

```sql
-- step 1: create iceberg table at temp fqn
CREATE TABLE `edp_prod`.`bronze`.`events_convert_tmp`
  USING iceberg
  AS SELECT * FROM `edp_prod`.`bronze`.`events`;

-- step 2: rename source to backup fqn
ALTER TABLE `edp_prod`.`bronze`.`events`
  RENAME TO `edp_prod`.`bronze`.`events_pre_convert_20260403T142510Z`;

-- step 3: rename temp to original fqn
ALTER TABLE `edp_prod`.`bronze`.`events_convert_tmp`
  RENAME TO `edp_prod`.`bronze`.`events`;
```

Each step is atomic in Unity Catalog. Step one writes a new Iceberg table at a deterministic temp FQN (`{fqn}_convert_tmp`) — the temp name is deterministic per operation so a retry or partial failure leaves a single artefact for the operator to inspect, not a cascade of timestamped tables. Step two renames the source aside under a timestamped backup FQN (`{fqn}_pre_convert_<utc>`). Step three renames the temp to the original, atomically replacing what the source used to be.

There's a window between step two and step three — a few hundred milliseconds — where the original FQN doesn't exist. Read queries against the FQN during that window will fail with "table not found." For most batch workloads this is invisible; for a high-QPS interactive surface it's a real consideration. Coordinate with downstream readers if the table is hot.

The trade-offs to accept:

- **Loses Delta history.** The new Iceberg table starts at version 0 of its own log. Time travel queries that referenced versions of the original Delta table will fail.
- **Costs 2× storage during the migration.** The backup table holds a copy of the data until you delete it. With `keep_backup=true` (the default), you can roll back by renaming the backup back to the original; with `keep_backup=false`, the source is dropped after the rename and only the converted table remains.
- **Breaks Delta-only features.** Deletion vectors, change feed, MERGE log — gone after the rename. The new Iceberg table has Iceberg's feature set, not Delta's.
- **Resets ownership and GRANTs.** A naive implementation would drop every privilege grant on the original table when the rename swaps tables. The orchestrator handles this automatically via capture-and-replay (covered below), but the mechanism is best-effort and worth understanding.

The reason the strategy exists despite all of this is that some downstream engines genuinely won't read UniForm-emulated Iceberg. When that's the constraint, this is the answer. When it's not, take the UniForm path.

- - -

## Strategy 4: `ctas_parquet` — the escape hatch for legacy readers

The fourth strategy serves a smaller but real category of need: a downstream consumer that reads only raw Parquet files — no Delta log, no Iceberg metadata, just a directory of Parquet. Legacy Spark on EMR. An older Athena workgroup. A custom in-house reader that nobody can find time to upgrade. If you can't change the reader, and you don't want to maintain a parallel pipeline producing a Parquet copy, this strategy converts the table at the original FQN to plain Parquet.

The plan is identical to `ctas_iceberg` in shape — three steps, temp-and-rename — with `USING parquet` instead of `USING iceberg`:

```sql
-- step 1: create parquet table at temp fqn
CREATE TABLE `edp_prod`.`silver`.`curated_convert_tmp`
  USING parquet
  AS SELECT * FROM `edp_prod`.`silver`.`curated`;

-- step 2: rename source to backup fqn
ALTER TABLE `edp_prod`.`silver`.`curated`
  RENAME TO `edp_prod`.`silver`.`curated_pre_convert_20260403T142510Z`;

-- step 3: rename temp to original fqn
ALTER TABLE `edp_prod`.`silver`.`curated_convert_tmp`
  RENAME TO `edp_prod`.`silver`.`curated`;
```

Same atomicity properties, same brief window where the FQN doesn't resolve, same 2× storage during the migration, same `keep_backup` semantics for rollback.

What's different is what you give up. Going to physical Iceberg loses Delta-only features but gains Iceberg's feature set in exchange. Going to Parquet loses Delta-only features and gains nothing back — Parquet has no log, no time travel, no transactions, no MERGE, no row-level deletes, no change feed. After this conversion you're back to immutable-snapshot semantics: any update means rewriting the whole table.

This is the most destructive of the four strategies in terms of what you lose, and the most narrowly useful. Don't reach for it because Parquet feels simpler. Reach for it when a specific downstream reader genuinely cannot handle anything else, and even then, double-check that you can't upgrade the reader instead. Most "Parquet-only" consumers can be migrated to a Delta-aware reader with a few days of work; `ctas_parquet` is the right answer when those few days don't exist.

The `_pre_convert_` backup matters more here than for `ctas_iceberg`, because rolling back is the only way to recover Delta history if you change your mind. Set `keep_backup=true` (the default). Drop the backup explicitly once you're confident the conversion is healthy.

- - -

## The decision matrix

The four strategies map to choices via a small table the orchestrator follows internally. Read it as a checklist when you're picking what to submit:

| Goal | Strategy | Picked automatically when |
|---|---|---|
| "Make it Delta" | `convert_to_delta` | source is `PARQUET` or `ICEBERG`, target = `DELTA` |
| "Add Iceberg reader without copying" | `uniform` | source = `DELTA`, target = `ICEBERG`, `iceberg_physical=false` (default) |
| "Make it physically Iceberg" | `ctas_iceberg` | source = `DELTA` + `iceberg_physical=true`, OR source = `PARQUET`, target = `ICEBERG` |
| "Make it raw Parquet" | `ctas_parquet` | target = `PARQUET` (any source other than `PARQUET` itself) |

The orchestrator picks the cheapest viable strategy by default, where "cheapest" means the least data movement. You only need to override (`iceberg_physical=true`) when the cheap path doesn't work for your downstream — which is a real but uncommon situation. If you're not sure whether your Iceberg consumer accepts UniForm, run a quick test query against a UniForm-converted table before committing every Delta source to physical Iceberg.

The other thing worth flagging: the matrix is uniform across the API and the UI. The `/convert` page lets you pick a per-row target format and surfaces the same `iceberg_physical` and `keep_backup` toggles the API accepts. So whatever decision tree you build for one operator scales to the other.

- - -

## Compatibility preflight: refusing the conversions that would silently lose data

Before any strategy runs, the orchestrator asks the source table a couple of questions and refuses the conversion outright if the answers say the target format can't faithfully represent what's in the source. The check lives in [`src/format_compat.py`](https://github.com/viral0216/clone-xs/blob/main/src/format_compat.py) and runs against the warehouse via `DESCRIBE TABLE EXTENDED` before any DDL fires. A refusal returns `status="skipped"` with a structured reason in the response — and crucially, no SQL touches the warehouse.

Today's checks are narrow and motivated by specific failure modes that would otherwise show up as silent data loss:

| Source → Target | Refusal reason |
|---|---|
| `(ICEBERG, *)` | Source uses hidden-partition transforms (`bucket(N, col)`, `truncate(N, col)`, `years(col)`, `months(col)`, `days(col)`, `hours(col)`). Iceberg's hidden partitioning has no Delta or Parquet equivalent, and silently dropping it would change partition pruning semantics on the target. |
| `(DELTA, ICEBERG)`, `(DELTA, PARQUET)` | Source has a `GENERATED ALWAYS` column or an identity column. Iceberg and Parquet can't represent computed columns; a CTAS would either fail at execution or emit a column populated with garbage. |

The hidden-partition check delegates to `src/clone_iceberg.py:preflight_iceberg_source` — the same preflight the clone path uses, refusing for the same reason. Convert and clone share the failure mode, share the check, share the error message. When the convert orchestrator skips a target, you get something like:

```
column 'year' is GENERATED — the target format has no equivalent.
Drop the column or change the target.
```

The message names the offending column and tells the operator what to do — drop the column on the source, or pick a target format that can hold it. This isn't a stack trace; it's a conversation with the human running the conversion.

Two design decisions worth highlighting:

**The preflight is skipped on dry-run.** Some operators want to preview the plan against a known-incompatible source — to confirm that's the failure they're seeing, or to decide whether to refactor the source before converting. A dry-run with the preflight enforced would block them from previewing anything. The dry-run path bypasses the check and renders the plan; if you then submit for real, the check runs and refuses.

**The preflight fails open.** If `DESCRIBE TABLE EXTENDED` itself errors — perms, transient warehouse issue, the warehouse is cold — the preflight returns "no incompatibilities found" rather than blocking the conversion. The reasoning: a flaky DESCRIBE call shouldn't be a worse outcome than a real incompatibility (which fails loudly during execution). Better to attempt the conversion and surface the real problem in context than to refuse on a transient. The trade-off is that a transient DESCRIBE failure right before a converting a table with a `GENERATED ALWAYS` column would not catch it via preflight; the CTAS itself will catch it during execution and surface the same column-by-column error from the warehouse. Either way, the operator hears about it before any data is silently lost.

- - -

## Permission preservation: capture and replay

The CTAS strategies (`ctas_iceberg`, `ctas_parquet`) physically replace the table at the original FQN. From Unity Catalog's perspective, after the rename dance the FQN points at a different table object — and that new object starts with no GRANTs and is owned by whoever ran the CONVERT call. Without intervention, every downstream consumer who had `SELECT` on the original table loses access the moment the conversion completes, and the metastore admin has to replay the grants by hand.

The orchestrator handles this automatically. Before executing the CTAS plan:

1. `SHOW GRANTS ON TABLE <fqn>` captures every `(principal, privilege)` pair currently on the table.
2. `client.tables.get(fqn).owner` captures the current owner.

After the plan succeeds, each captured grant is replayed as a fresh `GRANT … TO …` against the new table, and ownership is restored via `ALTER TABLE … OWNER TO <principal>`. From the consumer's perspective, the conversion is permission-transparent — they had `SELECT` before, they have `SELECT` after, no admin ticket required.

The mechanism is best-effort, deliberately so:

- If `SHOW GRANTS` fails before the conversion (perms, transient warehouse issue), the orchestrator logs a warning and proceeds without grant replay. The conversion still happens; the operator gets a "permissions reset, replay manually" line in the run log.
- If a single `GRANT` replay fails after the conversion (caller has `ALTER` on the new table but not `GRANT`), it's logged and the rest of the grants continue replaying. Partial replay is better than no replay.
- Ownership replay failures are logged identically.

The reasoning for best-effort over fail-fast: the conversion itself is the high-value, hard-to-reverse operation; the grant replay is a cleanup step. Failing the entire conversion because one of fifty grants couldn't be replayed would be worse than logging the gap and letting the operator finish the cleanup manually.

This matches the clone path's behaviour (`copy_table_permissions` in `src/permissions.py`) so operator expectations stay consistent across the two surfaces. If you're used to how clone preserves grants, convert preserves them the same way.

The two strategies that don't need this — `convert_to_delta` and `uniform` — both preserve grants and ownership automatically because they don't replace the table identity. `CONVERT TO DELTA` mutates the table in place; UniForm only adds metadata. The capture/replay step is skipped for those strategies entirely.

- - -

## API and dry-run: showing the plan before running it

The endpoint hasn't moved since the original D1 release — `POST /api/convert-to-delta` — for back-compat with existing callers. Only the UI page slug renamed (`/convert-to-delta` → `/convert`, with a redirect from the old URL).

A representative request, batching three different conversions:

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

Each target carries its own source format and desired target format — different rows can run different strategies. The `warehouse_id` is required to execute the DDL (or falls back to the global config default). `confirm_destructive: true` is required unless `dry_run: true` — the API returns a 400 if neither is set, before any SQL is dispatched. `iceberg_physical: false` (default) picks the UniForm path for any `(DELTA, ICEBERG)` row in the batch; the third row above (`DELTA → PARQUET`) ignores the flag because it doesn't apply. `keep_backup: true` (default) keeps the renamed-aside source for any temp+rename CTAS so the conversion is reversible.

The response, including a partial-results case where one target was refused by the preflight:

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
     "error": "column 'year' is GENERATED — the target format has no equivalent. Drop the column or change the target."}
  ]
}
```

The endpoint returns **200 with partial results** when some tables succeed and others are refused or failed. The HTTP layer doesn't conflate "the request was malformed" (400/422) with "some of the work refused" (200 with status breakdown). You re-submit only the offending targets after fixing the underlying issue — drop the `GENERATED ALWAYS` column on `curated_delta`, or change the target to `ICEBERG` instead of `PARQUET`.

The dry-run path renders every step in the multi-step plan, with the step labels intact, so the wizard preview shows the full sequence rather than just the first statement:

```
[DRY RUN] [disable deletion vectors] ALTER TABLE `cat`.`schema`.`tbl` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
[DRY RUN] [purge deletion vector files] REORG TABLE `cat`.`schema`.`tbl` APPLY (PURGE)
[DRY RUN] [enable Iceberg compat metadata] ALTER TABLE `cat`.`schema`.`tbl` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
```

This matters more than it sounds. Without per-step labels, an operator looking at a failed conversion only sees the SQL that blew up — which for the three-statement UniForm chain is unhelpful, because all three are similar `ALTER TABLE` calls. With labels, the failure message reads `step 'enable Iceberg compat metadata' failed: …` and the operator knows exactly which conceptual step broke. The implementation builds the full `Plan` of `PlanStep`s up front and runs them in order; on step failure, the exception is wrapped with the step's label so the failure path stays human-readable.

> [INSERT IMAGE: screenshots/convert-dry-run-preview.png]
> Caption: The Convert page's dry-run preview, showing the per-step plan for a (DELTA, ICEBERG) UniForm conversion. The labels match the ones in the audit log so failures correlate cleanly.

- - -

## Honest limitations

The convert path has real edges, and the docs say so up front rather than burying them. The same applies here.

**Source must be quiesced.** `CONVERT TO DELTA` rewrites the data files' metadata; CTAS+rename rewrites the entire underlying storage. Concurrent writes during the conversion can corrupt the resulting log or produce a target with rows that didn't exist in the source's atomic snapshot. Clone-Xs **does not** automatically quiesce the source — coordinate with upstream writers (turn off the streaming job, pause the scheduled load, hold the producer's pull) before submitting. This is the single most likely failure mode for first-time users.

**Iceberg requires DBR 13.3+ and the source must be UC-registered.** Path-based references (Iceberg tables defined by storage path rather than by UC catalog/schema/name) aren't supported. The `DESCRIBE TABLE EXTENDED` preflight path expects a UC FQN and the strategy primitives reference the table by FQN throughout.

**History resets for CTAS strategies.** Delta time travel on a `ctas_iceberg` or `ctas_parquet` target starts at version 0 of the new table. Pre-conversion versions are reachable only via the renamed-aside backup (when `keep_backup=true`) — they're physically still there, but at a different FQN. The UniForm path keeps history intact because the underlying Delta table is unchanged.

**No reverse for `convert_to_delta`.** Once a Parquet or Iceberg table has been converted to Delta, there's no Databricks-supported way to go back. The only rollback paths are restoring from a snapshot or, for the `(PARQUET, DELTA)` case specifically, dropping the Delta log and re-registering the underlying Parquet files as a Parquet table — which Clone-Xs doesn't automate. Treat `convert_to_delta` as a one-way door and prove the conversion succeeded against your downstream readers in a test environment before running it in production.

**No backup safety net for `convert_to_delta` or `uniform`.** The `keep_backup` flag only applies to the temp+rename CTAS strategies (`ctas_iceberg`, `ctas_parquet`). `convert_to_delta` mutates the table in place; UniForm only adds metadata; neither produces a separate backup table. If you want a safety net for those strategies, take a snapshot or clone the table to a parallel FQN before submitting.

**Hudi pairs are gated.** All four Hudi cells (any → HUDI, HUDI → any) are not yet executable. The UI shows them as disabled with a tooltip explaining the runtime sponsorship gate; the API returns a structured 422 for any Hudi pair: *"Some target pairs are not yet supported in this release. … Offending targets: edp_dev.bronze.x (DELTA→HUDI)."* The framework supports adding Hudi as a fifth strategy when the runtime decision lands; today the matrix has the cells reserved but unfilled.

- - -

## When to use convert vs. clone

The two-line summary, since this is the question that drives every operator's first decision:

| Situation | Use |
|---|---|
| Move data to a new catalog/schema, source unchanged | `POST /api/clone` |
| Source is Iceberg/Parquet, you want it to *be* Delta going forward | `POST /api/convert-to-delta` (target=DELTA) |
| Delta table that downstream Iceberg readers also need | `POST /api/convert-to-delta` (target=ICEBERG, `iceberg_physical=false` — UniForm metadata, no data movement) |
| Delta table you want as a *physical* Iceberg table for an external engine that doesn't read UniForm | `POST /api/convert-to-delta` (target=ICEBERG, `iceberg_physical=true`) |
| Same FQN, same data, but in raw Parquet for a downstream tool that insists | `POST /api/convert-to-delta` (target=PARQUET) — accept the history loss |
| You have downstream readers in the *original* format still in use | **Neither** — they'll break. Stand up a parallel target via clone, migrate readers, then convert the source. |
| Your Iceberg source has hidden partitioning that breaks clone | Not this — the convert preflight refuses hidden-partitioned Iceberg sources for the same reason clone does |

The last row is the trap. If you have downstream readers that still need the original format, the answer is neither convert nor a single clone. The right migration is: clone to a parallel FQN in the new format, migrate the readers one at a time onto the parallel copy, and only then convert the original FQN — which by that point has no remaining readers in the old format. The intermediate state has two tables; the end state has one. Convert closes the migration; it doesn't substitute for it.

- - -

## Try it

The Convert page lives at `/convert` in any Clone-Xs instance. Pick a catalog, pick a schema, pick the tables you want to convert, set a target format per row, choose `iceberg_physical` and `keep_backup`, dry-run to preview the plan, then submit with `confirm_destructive` set. The dry-run renders every step in the multi-step plan, including the labels, so you can audit the SQL before it runs.

Programmatic callers hit `POST /api/convert-to-delta` directly with the JSON shape above. Audit history lives at `GET /api/convert-to-delta/history?limit=50&status=failed&fqn_like=edp.bronze.%25&destination_format=ICEBERG` for after-the-fact analysis, with filters on status, FQN pattern, dry-run vs. real, operation_id, and destination format. The audit table itself (`<audit_catalog>.logs.convert_operations`) is queryable directly if you want to roll your own dashboards.

Clone-Xs is open-source under MIT. The convert path lives in [`src/convert_to_delta.py`](https://github.com/viral0216/clone-xs/blob/main/src/convert_to_delta.py) (orchestrator), [`src/format_strategies.py`](https://github.com/viral0216/clone-xs/blob/main/src/format_strategies.py) (the four strategy primitives), and [`src/format_compat.py`](https://github.com/viral0216/clone-xs/blob/main/src/format_compat.py) (the preflight). The full feature spec, including the API and history endpoint reference, is at [`docs/guide/convert`](https://github.com/viral0216/clone-xs/blob/main/docs/docs/guide/convert.md).

A companion piece for platform leaders and decision-makers — covering the cost trade-offs of dual-format storage, the governance implications of UniForm vs. physical Iceberg for compliance audits, and how the strategy choice shows up on the storage line item — is on the docket. This post focused on the engineering surface; the business surface is its own conversation.

If the converter helps, star the repo or open an issue. The Hudi gate is the most-asked feature; if your workload would benefit from it, file an issue describing the source side (Hudi version, table size, MOR vs. COW) and that helps prioritise the runtime decision.

- - -

*Built by Viral Patel. Clone-Xs is open-source under MIT — github.com/viral0216/clone-xs.*

**Tags:** Databricks, Unity Catalog, Delta Lake, Apache Iceberg, Apache Parquet, UniForm, Lakehouse, Format Migration, CONVERT TO DELTA, CTAS
