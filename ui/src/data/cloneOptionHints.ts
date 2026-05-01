// One-line descriptions shown in the Options step tooltips.
// Same content drives the docs table in docs/docs/reference/configuration.md.
// Keep entries concise — the tooltip popup has a 288 px max-width.

export const cloneOptionHints: Record<string, string> = {
  // Clone & Load
  clone_type:
    "DEEP copies all data files into the destination (independent of source). SHALLOW only copies metadata — destination points at source files.",
  load_type:
    "FULL re-clones every table on each run. INCREMENTAL only clones tables whose Delta version advanced since the last run.",
  data_sync_mode:
    "snapshot_once = clone only new tables (default, safe). incremental = mirror source updates into target — overwrites target writes. force_full = drop + reclone every run.",
  auto_handle_masks:
    "Drop column masks / row filters on source so masked tables can be added to the Delta Share, then re-apply on target. Restored on source for snapshot_once / force_full; left dropped for incremental.",

  // Compute
  serverless:
    "Run the clone as a serverless Databricks job instead of against a SQL warehouse. Zero-warehouse cost for one-offs and CI.",
  volume:
    "Unity Catalog volume path where Clone-Xs uploads itself for the serverless job to execute.",

  // Performance
  max_workers:
    "Number of schemas processed in parallel. Raise for wide catalogs; each worker holds one warehouse slot.",
  parallel_tables:
    "Tables cloned in parallel within a single schema. Raise for many small tables; lower for big tables on a shared warehouse.",
  max_parallel_queries:
    "Upper bound on concurrent SQL statements across all workers. Prevents warehouse saturation.",
  max_rps:
    "Rate-limit statements per second across all workers. 0 = unlimited. Use to protect shared upstream systems.",
  order_by_size:
    "Clone order by table byte-size. 'desc' = biggest first (fails fast on storage issues); 'asc' = small tables finish early.",
  throttle:
    "Pre-defined throughput profile. 'low' = minimal warehouse load; 'max' = no self-limiting. Overrides parallel_tables and max_parallel_queries.",

  // Copy options
  copy_permissions:
    "Copy Unity Catalog grants (SELECT, MODIFY, etc.) from source objects to destination.",
  copy_ownership:
    "Set the destination object's OWNER to match the source.",
  copy_tags:
    "Copy Unity Catalog tags (key-value annotations) on catalogs, schemas, tables, and columns.",
  copy_properties:
    "Copy Delta table properties (delta.autoOptimize, delta.minReaderVersion, etc.).",
  copy_security:
    "Copy row filters and column masks attached to source tables.",
  copy_constraints:
    "Copy NOT NULL and CHECK constraints from source tables.",
  copy_comments:
    "Copy table and column comments.",

  // Features
  enable_rollback:
    "Write a rollback manifest so `clxs rollback` can undo the clone later.",
  auto_rollback:
    "Automatically trigger rollback if post-clone validation detects more mismatches than Rollback Threshold.",
  validate_after_clone:
    "Run row-count validation after each table clone completes.",
  validate_checksum:
    "Use SHA-256 over hashed columns in addition to row counts. Slower but catches silent data drift.",
  force_reclone:
    "Drop and recreate destination tables even when they already exist. Otherwise existing tables are skipped.",
  schema_only:
    "Create destination schemas + empty tables but skip the actual data copy. Useful for schema-migration dry runs.",
  generate_report:
    "Emit an HTML audit report summarising what was cloned, mismatches, and timings.",
  show_progress:
    "Render live progress bars in the CLI / job logs.",
  checkpoint:
    "Persist per-table progress to a checkpoint file so interrupted clones can resume where they left off.",
  require_approval:
    "Pause the job before any write operation and wait for manual approval in the UI.",
  impact_check:
    "Pre-flight scan for downstream dependencies (views, jobs, dashboards) that reference the destination.",
  skip_unused:
    "Skip tables with zero recent usage in system.access.table_lineage. Trims the scope of dev-refresh jobs.",
  verbose:
    "Emit DEBUG-level logs for every SQL statement. Large output volume — use for troubleshooting only.",

  // Threshold
  rollback_threshold:
    "Maximum percentage of row mismatches tolerated before Auto Rollback on Fail kicks in.",

  // Filtering
  include_schemas:
    "Comma-separated schema names to clone. Empty = all schemas (minus excludes).",
  exclude_schemas:
    "Comma-separated schemas to skip. 'information_schema' and 'default' are excluded by default.",
  include_tables_regex:
    "Only tables + views whose name matches this regex are cloned. Applies after include/exclude schemas.",
  exclude_tables_regex:
    "Tables + views whose name matches this regex are skipped. Takes precedence over include regex.",

  // Time travel
  as_of_timestamp:
    "Clone each source table as it existed at this timestamp. Requires the source's Delta version retention to cover this point.",
  as_of_version:
    "Clone each source table at this specific Delta transaction version.",

  // Advanced
  where_clause:
    "Per-table row predicate applied to DEEP clones. Only rows matching the predicate are copied to the destination.",
  ttl:
    "Auto-expiry for the destination catalog (e.g. 7d, 30d, 2w). A background cleanup job drops expired catalogs.",
  template:
    "Named config preset (e.g. dev-refresh, dr-replica) that overrides common flags. See `clxs templates list` for available presets.",
};
