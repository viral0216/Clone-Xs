# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs Diagnostic — TARGET workspace
# MAGIC
# MAGIC Run this notebook **on the TARGET Databricks workspace** (the one whose
# MAGIC catalog you want to clone INTO). Use the **same SQL warehouse** that
# MAGIC Clone-Xs is configured to use as the target warehouse.
# MAGIC
# MAGIC Run **each cell separately** and share the output of every cell.
# MAGIC
# MAGIC **Important:** if you've used a Personal Access Token (PAT) in Clone-Xs's
# MAGIC target workspace config, log into this notebook UI as the **same user**
# MAGIC who owns that PAT. Otherwise the diagnostic results won't reflect what
# MAGIC Clone-Xs sees.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1 — Identity check
# MAGIC Confirm which user this SQL session authenticates as. Should match the
# MAGIC owner of the target PAT you've configured in Clone-Xs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS target_user;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Target metastore identifier
# MAGIC The output is the TARGET metastore's global sharing id — this is the
# MAGIC value Clone-Xs uses as `USING ID` when creating the recipient on source.
# MAGIC Verify it matches what you see in Clone-Xs logs:
# MAGIC `Target metastore sharing id: <value>`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_metastore() AS target_metastore;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3 — Has the source provider appeared on target?
# MAGIC When the source side runs CREATE RECIPIENT pointing at this metastore's
# MAGIC sharing id, the source's metastore should show up here as a provider.
# MAGIC If you've ever successfully completed step 1-3 of a Clone-Xs cross-workspace
# MAGIC clone, there should be at least one provider listed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PROVIDERS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4 — Inspect any active providers (run if Cell 3 returned rows)
# MAGIC For each provider name from Cell 3, see what shares they offer to you.
# MAGIC Edit the name below if you want to inspect a specific one.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIT to a provider name from Cell 3
# MAGIC -- SHOW SHARES IN PROVIDER `<provider_name_from_cell_3>`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5 — What metastore-level privileges does your identity have on target?
# MAGIC Look for `USE PROVIDER`, `USE SHARE`, `CREATE CATALOG`. Without these,
# MAGIC Clone-Xs can't read shares or create the destination catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON METASTORE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6 — Existing destination catalog (if Clone-Xs has run before)
# MAGIC Replace `demo_quick_01` with your actual destination catalog name.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIT to your destination catalog name
# MAGIC SHOW SCHEMAS IN demo_quick_01;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 7 — Existing shared catalog created by Clone-Xs (if any)
# MAGIC Replace the name below with the `Shared cat:` value from your Clone-Xs
# MAGIC logs (looks like `clone_xs_shared_<8-hex-chars>`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIT to the shared catalog name from Clone-Xs logs
# MAGIC SHOW SCHEMAS IN clone_xs_shared_143c66be;
