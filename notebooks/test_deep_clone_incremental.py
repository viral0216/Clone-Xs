# Databricks notebook source

# MAGIC %md
# MAGIC # Empirical test — `CREATE OR REPLACE TABLE … DEEP CLONE` is incremental
# MAGIC
# MAGIC This notebook verifies the claim made in the Clone-Xs cross-workspace
# MAGIC migration blog post and used by the `incremental` `data_sync_mode`:
# MAGIC
# MAGIC > Databricks tracks the source version cloned-from in the destination's
# MAGIC > metadata; on re-run, it consults the source Delta log and copies only
# MAGIC > the files added since that version. Overwrites any target-side writes
# MAGIC > to cloned tables (state mirrors source).
# MAGIC
# MAGIC The test runs six phases against a Unity Catalog table you control:
# MAGIC
# MAGIC 1. **Setup** — create source table with 1,000 rows.
# MAGIC 2. **First clone** — `CREATE OR REPLACE TABLE dst DEEP CLONE src`. Capture file count, history, clone-source version.
# MAGIC 3. **Source increments** — append 50 new rows to source.
# MAGIC 4. **Re-clone** — same `CREATE OR REPLACE TABLE dst DEEP CLONE src`. **Assert** only new files were copied, old files preserved, history shows two CLONE commits.
# MAGIC 5. **Target-side write** — INSERT a row directly into `dst`, then re-clone. **Assert** the target-side row is gone after re-clone (state mirrors source).
# MAGIC 6. **Cleanup** — drop test tables.
# MAGIC
# MAGIC Each phase prints a clear ✓/✗ next to its assertions so you can scan
# MAGIC the output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Edit the two cells below to point at a catalog/schema you have
# MAGIC `CREATE TABLE` / `MODIFY` / `DROP` privileges on. The test creates two
# MAGIC tables (`*_src` and `*_dst`), exercises them, and drops them in the
# MAGIC cleanup phase.

# COMMAND ----------

CATALOG = "main"           # ← edit
SCHEMA = "default"         # ← edit
TEST_PREFIX = "clone_xs_incr_test"  # tables are named <PREFIX>_src and <PREFIX>_dst

SRC_FQN = f"{CATALOG}.{SCHEMA}.{TEST_PREFIX}_src"
DST_FQN = f"{CATALOG}.{SCHEMA}.{TEST_PREFIX}_dst"

# Asserts go through this so a failure is visible but the notebook keeps running.
_PASS = "✅"
_FAIL = "❌"
_results: list[tuple[str, bool, str]] = []

def check(label: str, condition: bool, detail: str = "") -> bool:
    mark = _PASS if condition else _FAIL
    print(f"  {mark} {label}" + (f" — {detail}" if detail else ""))
    _results.append((label, condition, detail))
    return condition

print(f"Source: {SRC_FQN}")
print(f"Dest:   {DST_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — Setup: create source table, 1000 rows

# COMMAND ----------

print("Phase 1 — setup")

spark.sql(f"DROP TABLE IF EXISTS {SRC_FQN}")
spark.sql(f"DROP TABLE IF EXISTS {DST_FQN}")

spark.sql(f"""
    CREATE TABLE {SRC_FQN} (
        id BIGINT,
        payload STRING,
        created_at TIMESTAMP
    )
    USING DELTA
""")

# Force the initial 1000 rows into ONE file by repartition(1) — keeps the
# file-count math obvious in later phases.
df = spark.range(1000).selectExpr(
    "id",
    "concat('row-', cast(id as string)) AS payload",
    "current_timestamp() AS created_at",
).repartition(1)
df.write.mode("append").saveAsTable(SRC_FQN)

src_v0 = spark.sql(f"DESCRIBE HISTORY {SRC_FQN} LIMIT 1").collect()[0]["version"]
src_detail = spark.sql(f"DESCRIBE DETAIL {SRC_FQN}").collect()[0]
print(f"  source version: {src_v0}")
print(f"  source numFiles: {src_detail['numFiles']}")
print(f"  source sizeInBytes: {src_detail['sizeInBytes']}")

check("source table created", src_detail["numFiles"] >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 — First clone (baseline)
# MAGIC
# MAGIC Capture the destination's state immediately after the initial clone.
# MAGIC Key things we'll inspect:
# MAGIC
# MAGIC - **`numFiles`** on the destination — should match source's file count (data was physically copied).
# MAGIC - **`DESCRIBE HISTORY dst`** — should show one `CLONE` commit with `operationParameters.sourceVersion` = source's current version.
# MAGIC - The destination's data file paths under its storage location — list them so we can compare in Phase 4.

# COMMAND ----------

print("Phase 2 — first clone")

spark.sql(f"CREATE OR REPLACE TABLE {DST_FQN} DEEP CLONE {SRC_FQN}")

dst_detail_p2 = spark.sql(f"DESCRIBE DETAIL {DST_FQN}").collect()[0]
dst_history_p2 = spark.sql(f"DESCRIBE HISTORY {DST_FQN}").collect()
dst_storage_p2 = dst_detail_p2["location"]

# Capture the SET of files the destination's current version references.
# Use Spark's `.inputFiles()` rather than `dbutils.fs.ls(<location>)` because
# `dbutils.fs.ls` tries to talk to UC-managed ADLS directly and fails with
# `KeyProviderException: Invalid configuration value detected for
# fs.azure.account.key` when the cluster doesn't have a direct account-key.
# `inputFiles()` goes through Spark's UC-mediated credential chain — same path
# as `spark.table()`, which already works.
dst_files_p2 = sorted(spark.read.format("delta").table(DST_FQN).inputFiles())

print(f"  dst numFiles: {dst_detail_p2['numFiles']}")
print(f"  dst sizeInBytes: {dst_detail_p2['sizeInBytes']}")
print(f"  dst storage: {dst_storage_p2}")
print(f"  dst inputFiles (count): {len(dst_files_p2)}")
print(f"  dst history rows: {len(dst_history_p2)}")
for row in dst_history_p2:
    print(f"    v{row['version']:>3}  {row['operation']}  params={row['operationParameters']}")

# Find the CLONE commit and its recorded sourceVersion.
clone_commit_p2 = next((r for r in dst_history_p2 if r["operation"] == "CLONE"), None)
recorded_src_version_p2 = (
    int(clone_commit_p2["operationParameters"].get("sourceVersion"))
    if clone_commit_p2 else None
)

check("dst has CLONE commit", clone_commit_p2 is not None)
check(
    "dst recorded source version matches src current version",
    recorded_src_version_p2 == src_v0,
    f"recorded={recorded_src_version_p2}, src current={src_v0}",
)
check(
    "dst row count matches src row count",
    spark.table(DST_FQN).count() == spark.table(SRC_FQN).count(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 — Source increments: append 50 new rows
# MAGIC
# MAGIC The append produces exactly **one new Parquet file** on the source
# MAGIC (because we use `repartition(1)`). We'll record the source's new
# MAGIC version and the count of files added so we can predict what should
# MAGIC land on the destination after the re-clone.

# COMMAND ----------

print("Phase 3 — source increment (+50 rows)")

src_detail_pre_increment = spark.sql(f"DESCRIBE DETAIL {SRC_FQN}").collect()[0]
src_files_before = src_detail_pre_increment["numFiles"]

increment_df = spark.range(1000, 1050).selectExpr(
    "id",
    "concat('row-', cast(id as string)) AS payload",
    "current_timestamp() AS created_at",
).repartition(1)
increment_df.write.mode("append").saveAsTable(SRC_FQN)

src_v1 = spark.sql(f"DESCRIBE HISTORY {SRC_FQN} LIMIT 1").collect()[0]["version"]
src_detail_post_increment = spark.sql(f"DESCRIBE DETAIL {SRC_FQN}").collect()[0]
src_files_added_in_increment = src_detail_post_increment["numFiles"] - src_files_before

print(f"  source version: {src_v0} → {src_v1}")
print(f"  source numFiles: {src_files_before} → {src_detail_post_increment['numFiles']}")
print(f"  source new files added by increment: {src_files_added_in_increment}")
print(f"  source row count: {spark.table(SRC_FQN).count()}")

check("source version advanced", src_v1 > src_v0)
check(
    "exactly one new source file was added",
    src_files_added_in_increment == 1,
    f"actual={src_files_added_in_increment}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 — Re-clone (the incremental test)
# MAGIC
# MAGIC Run the same `CREATE OR REPLACE TABLE dst DEEP CLONE src` again.
# MAGIC Three things have to hold for the incremental claim to be true:
# MAGIC
# MAGIC 1. **Existing destination files survive.** The Parquet files that were
# MAGIC    in `dst_storage_p2` should still be there afterwards. If Databricks
# MAGIC    were doing a full re-copy, every file would be a new path.
# MAGIC 2. **Only the source increment's worth of new files lands.** The
# MAGIC    destination's `numFiles` should grow by exactly the number of
# MAGIC    source files added in Phase 3.
# MAGIC 3. **The history shows a *new* `CLONE` commit.** Not a `WRITE` or a
# MAGIC    `DROP` + `CREATE` — a second `CLONE` with an updated `sourceVersion`
# MAGIC    pointing at Phase 3's new version.
# MAGIC
# MAGIC The `operationMetrics` on the CLONE commit also expose
# MAGIC `numFilesCopied` — for an incremental re-clone, this should be just
# MAGIC the count of *new* source files.

# COMMAND ----------

print("Phase 4 — re-clone (incremental test)")

spark.sql(f"CREATE OR REPLACE TABLE {DST_FQN} DEEP CLONE {SRC_FQN}")

dst_detail_p4 = spark.sql(f"DESCRIBE DETAIL {DST_FQN}").collect()[0]
dst_history_p4 = spark.sql(f"DESCRIBE HISTORY {DST_FQN}").collect()
# Same approach as Phase 2 — Spark's inputFiles() honours UC credentials.
dst_files_p4 = sorted(spark.read.format("delta").table(DST_FQN).inputFiles())

print(f"  dst numFiles: {dst_detail_p2['numFiles']} → {dst_detail_p4['numFiles']}")
print(f"  dst sizeInBytes: {dst_detail_p2['sizeInBytes']} → {dst_detail_p4['sizeInBytes']}")
print(f"  dst inputFiles: {len(dst_files_p2)} → {len(dst_files_p4)}")
print(f"  dst history rows: {len(dst_history_p4)}")
for row in dst_history_p4:
    metrics = dict(row["operationMetrics"]) if row["operationMetrics"] else {}
    metrics_compact = {k: metrics[k] for k in ("numFilesCopied", "numCopiedFiles", "removedFilesCount", "numOutputRows", "executionTimeMs") if k in metrics}
    print(f"    v{row['version']:>3}  {row['operation']}  params={row['operationParameters']} metrics={metrics_compact}")

# Set comparison — every file from Phase 2 must still be present in Phase 4.
preserved_files = set(dst_files_p2) & set(dst_files_p4)
new_files = set(dst_files_p4) - set(dst_files_p2)
removed_files = set(dst_files_p2) - set(dst_files_p4)

print(f"  preserved Parquet files: {len(preserved_files)}")
print(f"  new Parquet files:       {len(new_files)}")
print(f"  removed Parquet files:   {len(removed_files)}")

# Find the *second* CLONE commit (most recent).
clone_commits = [r for r in dst_history_p4 if r["operation"] == "CLONE"]
latest_clone = clone_commits[0] if clone_commits else None  # history is desc by version

recorded_src_version_p4 = (
    int(latest_clone["operationParameters"].get("sourceVersion"))
    if latest_clone else None
)

clone_metrics_p4 = (
    dict(latest_clone["operationMetrics"]) if (latest_clone and latest_clone["operationMetrics"]) else {}
)
num_files_copied_p4 = int(
    clone_metrics_p4.get("numFilesCopied")
    or clone_metrics_p4.get("numCopiedFiles")
    or 0
)

# THE CORE ASSERTIONS
check(
    "every Phase-2 dst file is still present after re-clone",
    len(removed_files) == 0,
    f"{len(removed_files)} file(s) disappeared — would suggest full re-copy",
)
check(
    "dst has TWO CLONE commits in history (incremental, not full replace)",
    len(clone_commits) >= 2,
    f"clone commit count={len(clone_commits)}",
)
check(
    "second CLONE commit's sourceVersion equals src's current version",
    recorded_src_version_p4 == src_v1,
    f"recorded={recorded_src_version_p4}, src current={src_v1}",
)
check(
    "dst numFiles grew by exactly the source increment",
    dst_detail_p4["numFiles"] - dst_detail_p2["numFiles"] == src_files_added_in_increment,
    f"dst growth={dst_detail_p4['numFiles'] - dst_detail_p2['numFiles']}, "
    f"src increment={src_files_added_in_increment}",
)
check(
    "operationMetrics.numFilesCopied on second CLONE equals source increment",
    num_files_copied_p4 == src_files_added_in_increment,
    f"numFilesCopied={num_files_copied_p4}, src increment={src_files_added_in_increment}",
)
check(
    "dst row count matches src row count after re-clone",
    spark.table(DST_FQN).count() == spark.table(SRC_FQN).count(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5 — Target-side write overwrite test
# MAGIC
# MAGIC The blog claims:
# MAGIC > Overwrites any target-side writes to cloned tables (state mirrors source).
# MAGIC
# MAGIC Test:
# MAGIC 1. Insert a row directly into `dst` that doesn't exist in `src`.
# MAGIC 2. Verify `dst` row count > `src` row count.
# MAGIC 3. Re-run the same `CREATE OR REPLACE TABLE dst DEEP CLONE src`.
# MAGIC 4. Verify `dst` row count == `src` row count and the target-side row is gone.

# COMMAND ----------

print("Phase 5 — target-side write overwrite test")

# Distinct id (-1) so we can search for it specifically afterwards.
TARGET_SIDE_ID = -1
spark.sql(f"""
    INSERT INTO {DST_FQN} VALUES
    ({TARGET_SIDE_ID}, 'TARGET_SIDE_INSERT_should_be_overwritten', current_timestamp())
""")

src_count_pre = spark.table(SRC_FQN).count()
dst_count_pre = spark.table(DST_FQN).count()
target_side_present_pre = spark.sql(
    f"SELECT count(*) AS c FROM {DST_FQN} WHERE id = {TARGET_SIDE_ID}"
).collect()[0]["c"]

print(f"  before re-clone: src rows={src_count_pre}, dst rows={dst_count_pre}, target-side row present={target_side_present_pre}")

check(
    "target-side row exists in dst before re-clone",
    target_side_present_pre == 1,
)
check(
    "dst row count > src row count before re-clone",
    dst_count_pre > src_count_pre,
)

# Re-clone — this should overwrite the target-side write.
spark.sql(f"CREATE OR REPLACE TABLE {DST_FQN} DEEP CLONE {SRC_FQN}")

src_count_post = spark.table(SRC_FQN).count()
dst_count_post = spark.table(DST_FQN).count()
target_side_present_post = spark.sql(
    f"SELECT count(*) AS c FROM {DST_FQN} WHERE id = {TARGET_SIDE_ID}"
).collect()[0]["c"]

print(f"  after re-clone:  src rows={src_count_post}, dst rows={dst_count_post}, target-side row present={target_side_present_post}")

check(
    "target-side row was wiped by re-clone",
    target_side_present_post == 0,
)
check(
    "dst row count equals src row count after re-clone",
    dst_count_post == src_count_post,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6 — Cleanup

# COMMAND ----------

print("Phase 6 — cleanup")

spark.sql(f"DROP TABLE IF EXISTS {SRC_FQN}")
spark.sql(f"DROP TABLE IF EXISTS {DST_FQN}")
print("  dropped test tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed = sum(1 for _, ok, _ in _results if ok)
total = len(_results)
print(f"\n{'=' * 60}")
print(f"  {passed} / {total} assertions passed")
print(f"{'=' * 60}\n")

for label, ok, detail in _results:
    mark = _PASS if ok else _FAIL
    print(f"  {mark} {label}" + (f" — {detail}" if detail else ""))

if passed == total:
    print(
        "\nAll assertions passed. The blog claim is empirically correct on this runtime:\n"
        "  • CREATE OR REPLACE TABLE dst DEEP CLONE src is incremental on re-runs.\n"
        "  • Existing destination files are preserved; only new source files are copied.\n"
        "  • The destination's CLONE commit records the source version it was cloned from.\n"
        "  • Target-side writes are wiped on re-clone (state mirrors source)."
    )
else:
    print(
        "\nOne or more assertions failed. Re-read the per-phase output above to see which"
        "\nassumption broke. Possible explanations:\n"
        "  • Older Databricks Runtime: incremental DEEP CLONE landed in DBR 11+; very old"
        "    runtimes may always do a full re-copy.\n"
        "  • Photon vs non-Photon engines may differ on operationMetrics field names.\n"
        "  • If `removed Parquet files > 0`, the runtime is doing something stranger than"
        "    incremental clone — file the run output as evidence and let's revisit the"
        "    blog claim."
    )
