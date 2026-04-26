# Cross-Workspace and Cross-Cloud Unity Catalog Migration with Delta Sharing + DEEP CLONE

How Clone-Xs v0.11.0 migrates a full Databricks Unity Catalog — schemas, tables, views, SQL functions, volumes, grants, tags, ownership, column masks, and row filters — between workspaces, including across AWS, Azure, and GCP, in a single config.

> **Status: under development.** The cross-workspace migration capability described in this post is in active development at the time of writing (April 2026) and is targeted for general availability in May 2026. The internals, configuration shape, and UI shown here may shift before release. Watch the [Clone-Xs changelog](https://github.com/viral0216/clone-xs/blob/main/docs/docs/reference/changelog.md) for the formal v0.11.0 announcement.

- - -

If you've ever migrated a Unity Catalog from one Databricks workspace to another, you know the drill: open a notebook, hand-roll a Delta Share, create a recipient, hope you got the global metastore sharing identifier in the right shape, then `DEEP CLONE` table by table. Halfway through, a column mask blocks `ALTER SHARE ADD TABLE`. A view fails because `SHOW CREATE TABLE` returned a 2-part name that resolves against the wrong catalog on the target. Grants don't transfer. The recipient is orphaned. You restart.

Multiply that by cross-cloud — production on AWS, disaster recovery on Azure, two metastores, two regions, two networks — and "migrate the catalog" becomes a multi-week project with a fragile notebook nobody wants to maintain.

Clone-Xs v0.11.0 turns it into a single config. Pick a source catalog, pick a saved target connection, click run. Delta Sharing sets up the wire, `DEEP CLONE` lands data physically in the target's cloud storage, and views, SQL functions, volumes, grants, tags, and ownership replay automatically. When it's done, the share tears down (or stays up for incremental sync) and the target catalog lives entirely in the target cloud — fully independent of the source.

This post is about that migration: when to use it, what it does, every interesting thing that goes wrong, and how the orchestrator absorbs each one.

> [INSERT IMAGE: screenshots/01-target-workspace-card.png]
> Caption: The Target Workspace card on the Clone page. Saved connections live in browser localStorage — PATs never persist to the server.

- - -

## When to use it

Cross-workspace migration is a different problem from same-workspace cloning. Same-workspace clone is a SQL convenience — `CREATE TABLE ... DEEP CLONE` runs in one warehouse, in one metastore, with shared storage credentials. Cross-workspace migration crosses an authentication boundary, a metastore boundary, and (often) a cloud boundary. Four scenarios drive most adoption:

- **Disaster recovery replica** — keep a hot standby catalog in a different region or cloud. Production runs in AWS us-east-1; DR sits in Azure eastus. `incremental` sync mode keeps the standby fresh.
- **Cross-cloud migration** — your CFO decided AWS is too expensive (or Azure has the better discount). Move production end to end without a hand-coded notebook.
- **Workspace consolidation** — three teams stood up three workspaces over three years. You're pulling their catalogs into one canonical workspace.
- **Compliance isolation** — materialize a sanitized copy of production into a locked-down workspace your auditors have read-only access to. Source-side masks stay applied; target-side masks get re-applied automatically.

The real-world scenario that drove the v0.11.0 design: production catalog `retail_prod` in an AWS workspace, new compliance requirement to run DR in Azure, source and target on different metastores, hundreds of tables, dozens of views, a few SQL UDFs, column masks on every PII field. Pick the source catalog in Clone-Xs, tick "Clone to a different workspace," select the saved Azure target, click run. Clone-Xs handles the rest.

- - -

## Prerequisites

Before kicking off a migration, four things need to be true. Clone-Xs's preflight (`POST /api/target/validate`) checks most of them in 1–2 seconds and tells you exactly what's missing — but knowing what's required up front saves a round-trip.

### 1. Platform requirements

- **Unity Catalog enabled** on both workspaces. Hive-metastore catalogs aren't supported — Delta Sharing is a UC-native protocol.
- **Premium tier or above** on both Databricks workspaces. Delta Sharing isn't available on Standard.
- **Delta Sharing enabled** on both metastores (Databricks-to-Databricks sharing). In the Databricks UI: *Catalog → settings (gear icon) → Delta Sharing*. The metastore admin sets this.
- **Two distinct metastores.** Source and target must have different `global_metastore_id`s — Delta Sharing requires it. Clone-Xs's same-metastore preflight enforces this in 1–2 seconds (see "Three things that made this hard," below).
- **A SQL warehouse on each side.** The source warehouse runs `DESCRIBE`, `SHOW`, and `ALTER SHARE`; the target warehouse runs `CREATE CATALOG`, `DEEP CLONE`, view/function DDL, and metadata replay. Either warehouse can be any of the three Databricks SQL warehouse types — **serverless**, **pro**, or **classic** — they all expose the same Statement Execution API the orchestrator uses. Clone-Xs auto-starts a STOPPED target warehouse if the credentials allow it.

  :::note Serverless SQL warehouse vs serverless job mode
  Clone-Xs has a separate **serverless job mode** (`serverless: true` + a UC volume path) that bypasses warehouses entirely and runs the clone as a `spark_python_task` on Databricks serverless compute. That mode is currently only wired for **same-workspace** clones — the cross-workspace orchestrator always runs through SQL warehouses. If you want zero-warehouse execution, that's a same-workspace-only path today; cross-workspace serverless execution is on the roadmap.
  :::

### 2. Source workspace permissions

The principal Clone-Xs uses on the **source** side (your active Databricks credentials) needs:

| Privilege | On | For |
|---|---|---|
| `USE CATALOG`, `USE SCHEMA` | Source catalog + every schema in scope | Listing schemas/tables/views/functions/volumes |
| `SELECT` | Every source table in scope | Required by `ALTER SHARE ADD TABLE` |
| `EXECUTE` | Every source function in scope | `DESCRIBE FUNCTION EXTENDED` to extract DDL |
| `READ VOLUME` | Every source volume in scope | Listing files for volume copy |
| `CREATE SHARE` | Source metastore | `CREATE SHARE clone_xs_share_<sha1>` |
| `CREATE RECIPIENT` | Source metastore | `CREATE RECIPIENT … USING ID '<target_id>'` |
| `MODIFY` | Source tables that have masks/filters | Only required if `auto_handle_masks: true` — to `ALTER COLUMN DROP/SET MASK` |

The simplest path: the source principal is a **metastore admin**, which implies all of the above. For least-privilege deployments, grant the discrete privileges above to a service principal Clone-Xs runs as.

### 3. Target workspace permissions

The principal in your saved **target connection** (PAT, Service Principal, or CLI profile credentials) needs:

| Privilege | On | For |
|---|---|---|
| `CREATE PROVIDER` | Target metastore | One-time, when first run consumes the share. After that the provider exists and Clone-Xs reuses it. |
| `USE PROVIDER` | The source's provider on target | `CREATE CATALOG … USING SHARE` |
| `CREATE CATALOG` | Target metastore | Both the destination catalog (`retail_prod_dr`) and the shared-catalog wrapper (`clone_xs_shared_<sha1>`) |
| `USE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE` | Destination catalog | Every DEEP CLONE statement |
| `MODIFY` | Destination tables (after creation) | Re-applying masks/filters when `auto_handle_masks: true`; SET TAGS, ALTER OWNER |
| Storage credential access | Target managed-location, if any | `DEEP CLONE` writes data files to target storage; without storage permissions DEEP CLONE fails at file write |

If you're cloning into a **new managed catalog** with `MANAGED LOCATION '<url>'`, the target principal also needs `CREATE EXTERNAL LOCATION` (or to have the storage credential pre-granted to it). Cloning into an **existing catalog** (the common case) skips this — the catalog already has its storage credential wired up.

As with source, the simplest path is a target metastore admin or a service principal granted the privileges above.

### 4. Source table compatibility

Not every UC object can ride a Delta Share — and these constraints flow from Databricks, not Clone-Xs. The orchestrator surfaces each as a row in the run report rather than aborting the whole job:

- **Delta-format tables only.** Foreign tables (Lakehouse Federation), Iceberg-managed tables, and external Parquet tables can't be shared. They're listed as `skipped` with a reason in the report.
- **Streaming tables** are not migrated through this pipeline (the v0.11.0 *Streaming / MV data clone* preview handles those separately via DLT pipeline generation).
- **Column masks and row filters** block `ALTER SHARE ADD TABLE` unless `auto_handle_masks: true` is set. With it on, masks are inventoried, dropped on source, re-applied on target, and (mode-dependent) restored on source. Without it, masked tables fail and any view joining them fails too.
- **Materialized views** are migrated as DDL (the definition replays on target), but the data is rebuilt from sources on the target — there's no DEEP CLONE for MVs.
- **Volumes**: managed volumes copy file-by-file through the Databricks Files API up to `volume_max_file_mb` (default 500 MB). Files larger than the cap are skipped with a warning. External volumes are recreated on the target pointing at **the same storage URL as source** — no files are copied — so the target metastore must already have an external location and storage credential that grants it access to that URL. External volumes without a `storage_location` are skipped.

### 5. Cross-cloud caveats

When source and target are in different clouds (or even different regions of the same cloud), three things change:

- **Egress charges** — `DEEP CLONE` reads source data files through the Delta Sharing endpoint, which means the source cloud bills egress for every byte that lands in the target. A 2 TB catalog at AWS's typical $0.09/GB cross-region egress is ~$180 once. For ongoing `incremental` sync, you only pay egress on the daily delta.
- **Authentication mode** — cross-cloud means cross-control-plane. The target connection supports three auth methods: PAT, Service Principal (`client_id` + `client_secret`), and CLI Profile. PATs and Service Principals work everywhere. If you're using `auth_method: profile`, the CLI profile must be valid on the machine Clone-Xs runs on (typically not a problem for desktop / CI runners; sometimes a gotcha for Databricks App deployments where the CLI config isn't mounted).
- **Principal resolution** — user names, group names, and service principal names must match across the two metastores for grants to replay. SCIM-synced AD groups generally do; ad-hoc workspace-local users generally don't. Mismatches show up as `grants_skipped` in the run report.

- - -

## The pipeline in six stages

The whole migration runs as six sequential stages. Source workspace creates a Delta Share and a Recipient that points at the target metastore's global sharing id. Target workspace consumes the share as a catalog and `DEEP CLONE`s every table into target storage. Then DDL replays for views, functions, and volumes; metadata replays for grants, tags, and ownership; and the share comes down (or stays up, your choice).

> [INSERT IMAGE: screenshots/02-pipeline-diagram.png]
> Caption: The cross-workspace migration pipeline — six stages from source share creation to target teardown. Rendered live in the Preview Panel before each run.

```
Source workspace                          Target workspace
─────────────────                         ─────────────────
1. CREATE SHARE                ───┐
2. ALTER SHARE ADD TABLE …         │  Delta Sharing (the wire)
3. CREATE RECIPIENT USING ID    ───┘
   '<target_metastore_sharing_id>'
                                          4. CREATE CATALOG ... USING SHARE
                                          5. DEEP CLONE every table
                                          6. Replay views/functions/volumes
                                             grants/tags/ownership
   ←── teardown share + recipient ────────  (optional: keep for sync)
```

The key property: after stage 5, the target catalog's tables hold their own files in **target cloud storage**. The Delta Share is a wire protocol, not a live dependency. Tear it down and the target keeps working. Cross-cloud egress charges hit once, during stage 5 — not on every query forever.

The whole orchestrator is one Python file: [`src/clone_cross_workspace.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_cross_workspace.py). About 1,700 lines, readable top-to-bottom, no external dependencies beyond the Databricks SDK.

- - -

## Anatomy of a real run

Here's an actual run from one of our test workspaces — `demo_quick` (Azure UK South) to `demo_quick_01` (Azure West Europe), `force_full` mode, 28 tables. The complete log is 600 lines; this is the structural skeleton, lightly edited:

```
[10:32:37] Job 70888c83 started — clone_cross_workspace
INFO  CROSS-WORKSPACE MIGRATION
INFO    Source:      adb-7405610926268885.5.azuredatabricks.net / demo_quick
INFO    Target:      adb-7405614700164753.13.azuredatabricks.net / demo_quick_01
INFO    Sync mode:   force_full
WARN  data_sync_mode=force_full — target tables will be overwritten by source.
INFO  Listing schemas in source catalog 'demo_quick'...
INFO  Found 6 schemas
INFO    bronze: 5 tables
INFO    healthcare: 23 tables
INFO    silver: 0 tables       (etc.)
INFO  Resolving target metastore sharing identifier...
INFO  Target metastore sharing id: azure:westeurope:a649b7f5-177b-40ae-a9b9-78a4e55aac84
INFO  Source metastore sharing id: azure:uksouth:0a071f97-ef75-4147-adc8-b2d8b0288b40
INFO    Share:       clone_xs_share_6dd41a34
INFO    Recipient:   clone_xs_recipient_6dd41a34
INFO    Shared cat:  clone_xs_shared_6dd41a34
INFO  Creating Delta Share on source: clone_xs_share_6dd41a34
INFO  SQL: CREATE SHARE IF NOT EXISTS `clone_xs_share_6dd41a34`
INFO  Recipient clone_xs_recipient_6dd41a34 already exists — reusing
INFO  Verified recipient points at azure:westeurope:a649b7f5-...
INFO  Share sync: 28 existing, 0 to add, 0 to remove (prune_extras=False)
INFO  Locating source provider on target workspace...
INFO  Found source provider on target: omshiv
INFO  SQL: CREATE CATALOG IF NOT EXISTS `clone_xs_shared_6dd41a34`
        USING SHARE `omshiv`.`clone_xs_share_6dd41a34`
INFO  DEEP CLONE 28 tables (parallel=10)...
INFO  SQL: CREATE TABLE `demo_quick_01`.`healthcare`.`claims`
        DEEP CLONE `clone_xs_shared_6dd41a34`.`healthcare`.`claims`
…26 more tables in parallel…
INFO  Migrating views...
INFO    data_catalog: 4 views
INFO    gold: 4 views
INFO    healthcare: 20 views
INFO    silver: 5 views
INFO  Migrating functions...
INFO    healthcare: 24 functions
INFO  Migrating volumes...
INFO    healthcare: 2 volumes
INFO  Replaying metadata (grants / owners / tags)...
INFO  SQL: ALTER TABLE `demo_quick_01`.`healthcare`.`claims`
        SET TAGS ('data_classification' = 'confidential')
INFO  ========================================================================
INFO  MIGRATION SUCCESS: tables=28/28 (failed=0); views=33 (failed=0);
      functions=24 (failed=0); volumes=2 (failed=0); tags=4
INFO  cleanup_after_clone=false — leaving share/recipient/shared-catalog
      intact for incremental re-runs
[10:34:59] Job 70888c83 completed successfully
```

**2 minutes 22 seconds, end to end, across two Azure regions on different metastores.** A few things to notice in that log that the rest of the post unpacks:

- The metastore sharing IDs (`azure:uksouth:0a071f97-...`) — the format that trips up half of all hand-rolled migrations.
- `clone_xs_share_6dd41a34` — deterministic name derived from a SHA-1 of `(source_host, source_catalog, target_host, dest_catalog, target_metastore_id)`. The same source→target pair always produces the same name.
- `Recipient ... already exists — reusing` — second run, same names, no orphans.
- `Share sync: 28 existing, 0 to add, 0 to remove` — the share-membership diff: `SHOW ALL IN SHARE` minus the current source table list, only the delta hits Databricks.
- `Found source provider on target: omshiv` — the target's view of the source's metastore. Clone-Xs polls until it appears (recipients become providers asynchronously on the consuming side; this typically takes 5–15 seconds on first run).

- - -

## What gets migrated

The full Unity Catalog object surface, not just tables:

| Object | How it migrates | Known limits |
|---|---|---|
| **Schemas** | `CREATE SCHEMA IF NOT EXISTS` on target | None |
| **Managed + external tables** | DEEP CLONE through the Delta Share | Tables with column masks need `auto_handle_masks: true` (see below) |
| **Views** | `SHOW CREATE TABLE` on source → catalog-reference rewrite (regex-based, backticked + bare forms, case-insensitive) → `_qualify_create_target` rewrites the CREATE target to 3-part → `CREATE OR REPLACE VIEW` on target | Views referencing catalogs outside the migration scope log an error and skip |
| **SQL functions** | `DESCRIBE FUNCTION EXTENDED` → DDL extraction → catalog rewrite → `CREATE OR REPLACE FUNCTION` | Python UDFs with catalog names embedded as string literals don't get rewritten |
| **Volumes** | `CREATE VOLUME` (managed) or `CREATE EXTERNAL VOLUME ... LOCATION` (external) via SDK | Managed volume contents copied via Files API with a default 500 MB per-file cap (`volume_max_file_mb`) |
| **Grants** | `SHOW GRANTS ON <object>` on source → `GRANT … ON <dest-object> TO <principal>` on target | Principals that don't exist on the destination metastore are skipped with a debug log |
| **Ownership** | SDK `tables.update` / `schemas.update`, or SQL `ALTER … OWNER TO` | Best-effort; a single failure never aborts the run |
| **Tags** | Read from `system.information_schema.table_tags` → `ALTER TABLE … SET TAGS ('k' = 'v')` on target | Requires target metastore to have system tables enabled |
| **Column masks + row filters** | Inventoried via `DESCRIBE EXTENDED`, dropped on source for sharing, re-applied on target post-clone | Requires `auto_handle_masks: true`. See below for the full state machine. |
| **Constraints, comments, properties** | Replayed from source metadata after table creation | Default-on; toggleable via `copy_constraints`, `copy_comments`, `copy_properties` |

Stage 4 (views/functions/volumes) intentionally runs **after** tables, because views and functions reference them. Stage 5 (metadata replay) runs last, per object, so a failing GRANT never blocks data migration.

The orchestrator tracks per-stage outcomes in a `CrossWorkspaceResult` dataclass and surfaces it both in the UI and as JSON: `tables_cloned`, `tables_failed`, `views_migrated`, `functions_failed`, `volume_files_copied`, `volume_bytes_copied`, `grants_replayed`, `grants_skipped`, `tags_replayed`, etc. The clone report screenshot at the top of this post is rendered from that dataclass.

- - -

## Three things that made this hard

The headline is "Delta Sharing + DEEP CLONE." The actually-hard parts are three foot-guns Clone-Xs absorbs so you don't trip on them.

### 1. Column masks and row filters break Delta Share

Delta Sharing refuses to share any table that has a column mask or row filter applied. The migration fails at `ALTER SHARE ADD TABLE`, and any view that joins a masked table then fails on the target with `TABLE_OR_VIEW_NOT_FOUND`. If you've ever protected PII columns with a mask UDF — and most production catalogs have — you'll hit this on the first run.

Clone-Xs solves it with one config flag:

```yaml
target_workspace:
  host: "https://adb-target.azuredatabricks.net"
  auth_method: "pat"
  token: "dapi..."
  warehouse_id: "abc123"
  auto_handle_masks: true        # ← the fix
```

Behind that flag is a small state machine. The orchestrator parses `DESCRIBE EXTENDED` output looking for two specific section headers — `# Column Masks` and `# Row Filter` — and captures them in a `TableProtections` dataclass:

```
DESCRIBE EXTENDED demo_quick.healthcare.patients

…regular column rows…

# Column Masks
col_name='email',  data_type='`demo_quick`.`healthcare`.`mask_email`'
col_name='ssn',    data_type='`demo_quick`.`healthcare`.`mask_ssn`'

# Row Filter
col_name='Function', data_type='`demo_quick`.`healthcare`.`row_filter_facilities`'
col_name='Columns',  data_type='state'
```

Then for each masked table the orchestrator runs:

```sql
-- 1. drop on source so the table can be added to the share
ALTER TABLE demo_quick.healthcare.patients ALTER COLUMN email DROP MASK;
ALTER TABLE demo_quick.healthcare.patients ALTER COLUMN ssn   DROP MASK;
ALTER TABLE demo_quick.healthcare.patients DROP ROW FILTER;

-- 2. add to share — now succeeds
ALTER SHARE clone_xs_share_6dd41a34 ADD TABLE demo_quick.healthcare.patients;

-- 3. (after function migration finishes, so the mask UDFs exist on target)
--    re-apply on target with FQN rewritten src→dst
ALTER TABLE demo_quick_01.healthcare.patients ALTER COLUMN email
  SET MASK `demo_quick_01`.`healthcare`.`mask_email`;
ALTER TABLE demo_quick_01.healthcare.patients ALTER COLUMN ssn
  SET MASK `demo_quick_01`.`healthcare`.`mask_ssn`;
ALTER TABLE demo_quick_01.healthcare.patients
  SET ROW FILTER `demo_quick_01`.`healthcare`.`row_filter_facilities` ON (state);

-- 4. in finally block: restore on source (one-shot modes only)
ALTER TABLE demo_quick.healthcare.patients ALTER COLUMN email
  SET MASK `demo_quick`.`healthcare`.`mask_email`;
-- … etc
```

The catalog rewriter handles both `` `demo_quick`. `` and bare `demo_quick.` forms — DESCRIBE EXTENDED is inconsistent about quoting. Step 4 is **conditional on `data_sync_mode`**: for `snapshot_once` and `force_full`, masks restore on source. For `incremental`, masks stay dropped because re-applying them would invalidate the ongoing Delta Share — Databricks revokes share permissions when masks reappear on shared tables. A WARNING surfaces in the run log so you know what state your source ended up in.

If you leave `auto_handle_masks: false` (the default), masked tables fail at `ALTER SHARE ADD TABLE` and dependent views fail too. The default is `false` because mutating production source state without explicit consent is a bad surprise.

### 2. Same-metastore is a silent foot-gun

Two Databricks workspaces can share a single Unity Catalog metastore. If you accidentally point a cross-workspace clone at a target on the same metastore, `CREATE RECIPIENT IF NOT EXISTS` against your own metastore silently no-ops in Databricks. Thirty seconds later you get a confusing "phantom recipient" error and no data has moved.

Clone-Xs adds a same-metastore preflight that compares `global_metastore_id` from both sides and fails fast in 1–2 seconds:

```
Source and target workspaces are in the same Unity Catalog metastore
(<your-metastore-uuid>). Delta Sharing requires distinct metastores —
you cannot share to yourself.

Fix: untick 'Clone to a different workspace' and run a normal in-metastore
clone instead. Same metastore = same UC = no Delta Sharing required.
```

Two implementation details worth calling out:

**Use `metastores.summary()`, not `metastores.current()`.** The `current()` API returns the bare metastore UUID — but `CREATE RECIPIENT USING ID` requires the **global** form, `<cloud>:<region>:<uuid>`, e.g. `azure:uksouth:0a071f97-ef75-4147-adc8-b2d8b0288b40`. Hand-rolled migrations using `metastores.current()` get `INVALID_PARAMETER_VALUE: ... is an invalid id for metastore` 15 minutes in. `summary()` returns the global form correctly. Clone-Xs surfaces it via `POST /api/target/validate` so you can pre-flight before kicking off a 4-hour migration.

**The check is on `global_metastore_id`, not workspace URL.** Two workspaces on the same metastore have different URLs but identical metastore IDs. Comparing URLs would let the foot-gun through.

### 3. View DDL returns 2-part names that resolve against the wrong catalog

`SHOW CREATE TABLE` for a view returns 2-part schema-qualified names. Those names resolve against the **target warehouse's current catalog**, not the destination catalog the migration is writing to. The result: half your views fail with `[SCHEMA_NOT_FOUND] dbr_xxx.<schema>` for no obvious reason.

Here's what `SHOW CREATE TABLE` returned for one view in our test run:

```sql
CREATE VIEW healthcare.v_active_referrals (
  referral_id, patient_id, …, first_name, last_name)
AS SELECT r.*, p.first_name, p.last_name
   FROM demo_quick.healthcare.referrals r
   JOIN demo_quick.healthcare.patients p ON r.patient_id = p.patient_id
   WHERE r.status = 'pending'
```

Two problems on the target:

1. The CREATE target is `healthcare.v_active_referrals` — 2-part. The target warehouse's current catalog might be `dbr_xxx`, so this tries to create `dbr_xxx.healthcare.v_active_referrals` and fails.
2. The body references `demo_quick.healthcare.referrals` — the source catalog name, which doesn't exist on the target.

Clone-Xs fixes both with two regex-based rewriters:

```sql
CREATE OR REPLACE VIEW `demo_quick_01`.healthcare.v_active_referrals (    -- ← _qualify_create_target
  referral_id, patient_id, …, first_name, last_name)
AS SELECT r.*, p.first_name, p.last_name
   FROM demo_quick_01.healthcare.referrals r                             -- ← _rewrite_catalog_refs
   JOIN demo_quick_01.healthcare.patients p ON r.patient_id = p.patient_id
   WHERE r.status = 'pending'
```

`_qualify_create_target()` injects the destination catalog so the CREATE target is always 3-part. `_rewrite_catalog_refs()` rewrites source catalog references in the body — both backticked and bare forms, case-insensitive — to the destination catalog. Same fix applies to function migration.

It's the kind of bug that doesn't show up in test catalogs (where you set the warehouse's default catalog to match the destination) but bites every real cross-workspace migration.

- - -

## Deterministic naming and the share-membership diff

A cross-workspace migration is rarely a one-shot. You hydrate the DR replica today and then keep it current for months. The unreleased follow-on to v0.11.0 introduces two pieces that make incremental sync work cleanly.

**Deterministic share names.** Share, recipient, and shared-catalog names are derived as `clone_xs_share_<sha1>`, `clone_xs_recipient_<sha1>`, `clone_xs_shared_<sha1>` from the tuple `(source_host, source_catalog, target_host, dest_catalog, target_metastore_id)`. The first 8 hex chars of SHA-1 give a stable suffix:

```
clone_xs_share_6dd41a34       ← from sha1("adb-7405…|demo_quick|adb-7405…|
clone_xs_recipient_6dd41a34      demo_quick_01|azure:westeurope:a649b7f5-…")
clone_xs_shared_6dd41a34
```

The same source→target pair always produces the same name. The first run creates the Delta Sharing objects; the second run reuses them and skips the handshake entirely. No orphaned `clone_xs_*_<random>` objects piling up in your Delta Sharing list, no "Recipient already exists" errors on retries.

There's also a **recipient verification step on reuse**. If `clone_xs_recipient_6dd41a34` already exists on source, Clone-Xs runs `DESCRIBE RECIPIENT` and checks its `USING ID` against the current target's `global_metastore_id`. If they don't match — say someone manually re-pointed the recipient at a different metastore — the run fails loudly instead of silently leaking data to the wrong destination. The check is fast (one round-trip) and prevents one of the worst possible outcomes.

**Share-membership diff.** Re-runs only `ALTER SHARE ADD TABLE` for tables that aren't already in the share. The orchestrator calls `SHOW ALL IN SHARE clone_xs_share_6dd41a34` to get the current alias set, diffs against the source table list, and only emits SQL for the delta:

```
INFO  Share sync: 28 existing, 0 to add, 0 to remove (prune_extras=False)
```

Set `prune_share_extras: true` to also `REMOVE TABLE` for tables that were in the share but no longer exist in the source — useful when you've dropped a deprecated table on source and want it pruned from the DR mirror too. (It defaults off because pruning is destructive on the share side.)

- - -

## Three sync modes for re-runs

What happens to the data on re-run is controlled by `data_sync_mode` on `target_workspace`:

| Mode | SQL emitted per table | Re-run behaviour | When to use |
|---|---|---|---|
| `snapshot_once` (default) | `CREATE TABLE IF NOT EXISTS dst DEEP CLONE src` | No-op on existing tables. Only newly-added tables in source get cloned. | One-time hydration. Target is meant to drift independently after the initial copy. Safest mode — never overwrites target. |
| `incremental` | `CREATE OR REPLACE TABLE dst DEEP CLONE src` | Reads both Delta logs and copies only files added since the last clone. **Overwrites any target-side writes to cloned tables.** | Source is system of record; target is a read-replica or DR mirror. |
| `force_full` | `DROP TABLE IF EXISTS dst; CREATE TABLE dst DEEP CLONE src` | Full re-clone every run. | Recovery from corruption, or after a schema change you want to apply cleanly. |

`incremental` and `force_full` log a WARNING at run start because of the data-loss implication — `DEEP CLONE` is a one-way mirror. Databricks doesn't expose `MERGE` semantics for clone, so any row inserted on the target after a previous clone is lost on re-run in those modes. The 3-button picker lives on the Target Workspace card in Settings, and the Preview step (just before you confirm a clone) surfaces an amber warning row when the active config has anything but the default mode.

The `incremental` mode is the interesting one for ongoing sync: Databricks `DEEP CLONE` is internally aware of which files it copied last time (it tracks the source's Delta version in the destination's metadata) and only copies files added or changed since. A 2 TB catalog with a 100 MB daily delta clones in seconds, not hours.

> [INSERT IMAGE: screenshots/03-data-sync-mode-picker.png]
> Caption: Data sync mode picker on the Target Workspace card with the data-loss warning.

- - -

## UI walkthrough

**Configure target workspaces once in Settings.** Open `/settings → Target Workspaces → + Add target` and fill in: name, target host (full workspace URL), auth method (PAT, Service Principal, or CLI Profile), credentials, target SQL warehouse (used for DDL + DEEP CLONE on the target side), default sync mode, `auto_handle_masks` toggle, and an optional "keep migration share" flag for audit/debug.

Saved connections **live in browser localStorage** as `clxs_target_connections`, not on the server. PATs and client secrets never persist to disk — each clone request sends them inline, sourced from the picked entry. Each saved connection card auto-shows `✓ Logged in as <user>` (resolved via the lightweight `POST /api/target/whoami` endpoint) so you can spot stale or wrong-identity tokens at a glance.

**On the Clone page, just pick the saved target.** Step 1 has a "Clone to a different workspace" checkbox. Tick it, and a compact picker appears:

```
☑ Clone to a different workspace
─────────────────────────────────────────────────────────
Target connection: [ prod-azure ▼ ]  [ Test ]  Manage in Settings →
https://adb-7405….azuredatabricks.net · PAT · WH e83992177db8bdd5 · snapshot_once
```

When the box is ticked, the **Destination Catalog dropdown switches its data source** — it now lists catalogs that exist in the **target** workspace (with `(from target 'prod-azure')` shown next to the label). You pick an existing target catalog or `+ Create New Catalog` to provision a fresh one. **Test** runs auth + metastore sharing + warehouse existence checks (and starts the warehouse if it's STOPPED) without committing any state.

> [INSERT IMAGE: screenshots/04-clone-page-target-picker.png]
> Caption: The Clone page with "Clone to a different workspace" enabled. Destination dropdown sources catalogs from the target workspace, not the source.

- - -

## API usage

Pre-flight the target before kicking off a long migration:

```bash
curl -X POST $CLXS_HOST/api/target/validate \
  -H "Content-Type: application/json" \
  -d '{
    "host": "https://adb-target.azuredatabricks.net",
    "auth_method": "pat",
    "token": "dapi...",
    "warehouse_id": "abc123"
  }'
# {
#   "ok": true,
#   "user": "viral.patel@example.com",
#   "catalog_count": 14,
#   "metastore_sharing_id": "azure:westeurope:a649b7f5-177b-40ae-a9b9-78a4e55aac84"
# }
```

Then kick off the migration — same `POST /api/clone` endpoint, just supply `target_workspace`:

```bash
curl -X POST $CLXS_HOST/api/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "retail_prod",
    "destination_catalog": "retail_prod_dr",
    "target_workspace": {
      "host": "https://adb-target.azuredatabricks.net",
      "auth_method": "pat",
      "token": "dapi...",
      "warehouse_id": "abc123",
      "data_sync_mode": "incremental",
      "auto_handle_masks": true
    }
  }'
# { "job_id": "...", "status": "queued" }
```

Or in YAML for `clxs clone`:

```yaml
source_catalog: retail_prod
destination_catalog: retail_prod_dr

target_workspace:
  host: "https://adb-target.azuredatabricks.net"
  auth_method: "pat"
  token: ""
  warehouse_id: ""
  data_sync_mode: "snapshot_once"
  auto_handle_masks: false
  cleanup_after_clone: false   # keep deterministic share/recipient for re-runs
  prune_share_extras: false    # also REMOVE TABLE on tables no longer in source

# Toggle which object types migrate (all default true)
clone_views: true
clone_functions: true
clone_volumes: true
volume_max_file_mb: 500       # per-file cap for volume copies

# Standard clone flags also apply
copy_permissions: true        # GRANTs replayed via SHOW GRANTS
copy_ownership: true          # ALTER … OWNER TO … on target
copy_tags: true               # replayed from system.information_schema
```

The same-catalog-name guard is automatically waived when `target_workspace` is set — it's legitimate for `retail_prod` on AWS to clone to `retail_prod` on Azure with identical names on a different metastore. The Pydantic `model_validator` on `CloneRequest` checks `target_workspace is not None` before the same-name check.

- - -

## Try it

```bash
pip install clone-xs
clxs serve
```

Open `http://localhost:8000/docs` for the API or `http://localhost:3000` for the web UI. On the Clone page, tick **Clone to a different workspace** in step 1; if you've configured a target connection in Settings, it'll appear in the picker. If not, **Manage in Settings →** takes you to the form. Saved connections live in browser localStorage — PATs never persist to the server, never end up in `clone_config.yaml`, never get flagged by git secret-scanning.

Cross-workspace migration is open source under the MIT license. The orchestrator is one Python file at [`src/clone_cross_workspace.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_cross_workspace.py) — about 1,700 lines, no external dependencies beyond the Databricks SDK. Authentication for the target connection: **PAT**, **Service Principal** (`client_id` + `client_secret`), or **CLI Profile**. Compute: SQL warehouses on both sides — any of serverless / pro / classic warehouse types. Storage: every clone job's audit row is written to the **source** workspace's audit catalog (`run_logs`, `clone_operations`, `clone_metrics` Delta tables) by the JobManager — destination-side audit replay is on the roadmap.

GitHub: [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs). Star it if you find it useful, open an issue if you hit a migration edge case we haven't seen.

- - -

*Clone-Xs is an open-source toolkit for Databricks Unity Catalog. v0.11.0 adds cross-workspace and cross-cloud catalog migration via Delta Sharing + DEEP CLONE. Built by Viral Patel.*

- - -

**Tags:** Databricks, Unity Catalog, Delta Sharing, DEEP CLONE, Cross-Cloud, Cross-Workspace, Disaster Recovery, Open Source, Data Engineering, Data Migration
