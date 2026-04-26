# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs Diagnostic — SOURCE workspace
# MAGIC
# MAGIC Run this notebook **on the SOURCE Databricks workspace** (the one whose
# MAGIC catalog you want to clone FROM). Use the **same SQL warehouse** that
# MAGIC Clone-Xs is configured to use (the source `sql_warehouse_id`).
# MAGIC
# MAGIC Run **each cell separately** and share the output of every cell.
# MAGIC The cells are intentionally one statement each so we don't lose output
# MAGIC if one errors.
# MAGIC
# MAGIC **Important:** if you've used a Personal Access Token (PAT) in Clone-Xs,
# MAGIC make sure you're logged into this notebook UI as the **same user** who
# MAGIC owns that PAT. Otherwise the diagnostic results won't reflect what
# MAGIC Clone-Xs sees.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1 — Identity check
# MAGIC Confirm which user this SQL session authenticates as. Should match the
# MAGIC owner of the PAT you've configured in Clone-Xs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS source_user;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Source metastore identifier
# MAGIC The output is the SOURCE metastore's global sharing id.
# MAGIC Compare to the TARGET metastore id (which from your earlier logs is
# MAGIC `azure:westeurope:a649b7f5-177b-40ae-a9b9-78a4e55aac84`).
# MAGIC If they're equal, both workspaces share one metastore and Delta Sharing
# MAGIC isn't valid between them.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_metastore() AS source_metastore;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3 — Clean up any leftover diagnostic recipient
# MAGIC Safe to run even if it doesn't exist.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP RECIPIENT IF EXISTS test_clone_xs_diag_src;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4 — List ALL recipients on source
# MAGIC Look for `clone_xs_recipient_143c66be` (or the latest recipient name from
# MAGIC your Clone-Xs error). If it appears here, the recipient exists; the
# MAGIC question becomes one of ownership/visibility for the Clone-Xs PAT identity.
# MAGIC If it does NOT appear, the recipient was never actually created on this
# MAGIC metastore.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW RECIPIENTS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5 — Manually create a fresh test recipient
# MAGIC Uses the TARGET metastore id from your prior Clone-Xs logs.
# MAGIC Expected outcomes:
# MAGIC - **Succeeds** → your identity has CREATE RECIPIENT and the metastore accepts the call
# MAGIC - **"already exists"** → ghost from a previous diagnostic run; skip ahead to Cell 8 then re-run
# MAGIC - **"permission denied"** → your identity lacks CREATE RECIPIENT — see Cell 7

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE RECIPIENT test_clone_xs_diag_src
# MAGIC USING ID 'azure:westeurope:a649b7f5-177b-40ae-a9b9-78a4e55aac84';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6 — Did the new recipient actually appear?
# MAGIC If Cell 5 succeeded, the recipient should be in this list.
# MAGIC If it's NOT here despite Cell 5 succeeding, that's a Databricks-side
# MAGIC visibility bug — would need a support ticket.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW RECIPIENTS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 7 — What metastore-level privileges does your identity have?
# MAGIC Look for `CREATE RECIPIENT`, `CREATE SHARE`, and `USE METASTORE` lines.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON METASTORE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 8 — Inspect the test recipient (only if Cell 5 succeeded)
# MAGIC The `owner` row is the most useful field — confirms which identity owns
# MAGIC the recipient. The `metastore_id` row should match the value from Cell 2.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE RECIPIENT test_clone_xs_diag_src;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 9 — Inspect the actual stuck recipient (if it shows up in Cell 4 or 6)
# MAGIC Replace the name below with the actual recipient name Clone-Xs is
# MAGIC failing on (look at the most recent Clone-Xs error message).
# MAGIC The `owner` row will tell us if it's owned by a different identity than
# MAGIC the one running this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIT this name to match the recipient Clone-Xs is failing on
# MAGIC DESCRIBE RECIPIENT clone_xs_recipient_143c66be;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 10 — Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP RECIPIENT IF EXISTS test_clone_xs_diag_src;
