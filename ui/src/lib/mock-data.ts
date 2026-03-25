/**
 * Mock data for "Explore Clone-Xs" demo mode.
 * Returns realistic responses when no Databricks connection is available.
 */

const now = new Date();
const daysAgo = (d: number) => new Date(now.getTime() - d * 86400000).toISOString();
const hoursAgo = (h: number) => new Date(now.getTime() - h * 3600000).toISOString();
const minsAgo = (m: number) => new Date(now.getTime() - m * 60000).toISOString();

const MOCK_JOBS = [
  { job_id: "clx-001", job_type: "clone", source_catalog: "prod_catalog", destination_catalog: "dev_catalog", clone_type: "SHALLOW", status: "completed", started_at: hoursAgo(2), completed_at: hoursAgo(1.5), duration_seconds: 1800, tables_cloned: 42, views_cloned: 12, error_message: null },
  { job_id: "clx-002", job_type: "clone", source_catalog: "analytics", destination_catalog: "analytics_dev", clone_type: "DEEP", status: "completed", started_at: hoursAgo(5), completed_at: hoursAgo(4), duration_seconds: 3600, tables_cloned: 28, views_cloned: 8, error_message: null },
  { job_id: "clx-003", job_type: "sync", source_catalog: "prod_catalog", destination_catalog: "staging_catalog", clone_type: "SHALLOW", status: "running", started_at: minsAgo(15), completed_at: null, duration_seconds: null, tables_cloned: 18, views_cloned: 3, error_message: null },
  { job_id: "clx-004", job_type: "clone", source_catalog: "staging_catalog", destination_catalog: "qa_catalog", clone_type: "DEEP", status: "completed", started_at: daysAgo(1), completed_at: daysAgo(0.95), duration_seconds: 2400, tables_cloned: 35, views_cloned: 10, error_message: null },
  { job_id: "clx-005", job_type: "clone", source_catalog: "prod_catalog", destination_catalog: "dr_catalog", clone_type: "DEEP", status: "failed", started_at: daysAgo(1), completed_at: daysAgo(0.98), duration_seconds: 120, tables_cloned: 3, views_cloned: 0, error_message: "Permission denied: USAGE on schema prod_catalog.finance" },
  { job_id: "clx-006", job_type: "sync", source_catalog: "analytics", destination_catalog: "analytics_staging", clone_type: "SHALLOW", status: "completed", started_at: daysAgo(2), completed_at: daysAgo(1.9), duration_seconds: 900, tables_cloned: 15, views_cloned: 5, error_message: null },
  { job_id: "clx-007", job_type: "clone", source_catalog: "dev_catalog", destination_catalog: "test_catalog", clone_type: "SHALLOW", status: "completed", started_at: daysAgo(3), completed_at: daysAgo(2.95), duration_seconds: 600, tables_cloned: 22, views_cloned: 7, error_message: null },
  { job_id: "clx-008", job_type: "clone", source_catalog: "prod_catalog", destination_catalog: "backup_catalog", clone_type: "DEEP", status: "running", started_at: minsAgo(8), completed_at: null, duration_seconds: null, tables_cloned: 10, views_cloned: 0, error_message: null },
  { job_id: "clx-009", job_type: "clone", source_catalog: "staging_catalog", destination_catalog: "uat_catalog", clone_type: "SHALLOW", status: "cancelled", started_at: daysAgo(4), completed_at: daysAgo(3.99), duration_seconds: 60, tables_cloned: 1, views_cloned: 0, error_message: "Cancelled by user" },
  { job_id: "clx-010", job_type: "clone", source_catalog: "analytics", destination_catalog: "ml_catalog", clone_type: "DEEP", status: "completed", started_at: daysAgo(5), completed_at: daysAgo(4.9), duration_seconds: 4200, tables_cloned: 50, views_cloned: 15, error_message: null },
];

const MOCK_DASHBOARD_STATS = {
  total_clones: 234,
  succeeded: 210,
  failed: 15,
  running: 9,
  success_rate: 89.7,
  avg_duration: 45.2,
  max_duration: 320,
  min_duration: 5,
  total_tables_cloned: 4280,
  total_views_cloned: 1120,
  total_volumes_cloned: 340,
  total_data_bytes: 1_073_741_824_000,
  avg_tables_per_clone: 18.3,
  by_status: { completed: 210, failed: 15, running: 9 },
  clone_type_split: { SHALLOW: 142, DEEP: 92 },
  operation_type_split: { clone: 180, sync: 45, "incremental-sync": 9 },
  top_catalogs: [
    { catalog: "prod_catalog", count: 78 },
    { catalog: "analytics", count: 45 },
    { catalog: "dev_catalog", count: 32 },
    { catalog: "staging_catalog", count: 28 },
    { catalog: "ml_catalog", count: 18 },
  ],
  active_users: [
    { user: "data-engineer@company.com", count: 85 },
    { user: "ml-team@company.com", count: 52 },
    { user: "analytics@company.com", count: 41 },
    { user: "devops@company.com", count: 30 },
    { user: "qa-team@company.com", count: 26 },
  ],
  peak_hours: [
    { hour: 8, count: 28 }, { hour: 9, count: 42 }, { hour: 10, count: 38 },
    { hour: 11, count: 35 }, { hour: 12, count: 20 }, { hour: 13, count: 25 },
    { hour: 14, count: 40 }, { hour: 15, count: 36 }, { hour: 16, count: 30 },
    { hour: 17, count: 22 }, { hour: 18, count: 12 }, { hour: 19, count: 8 },
  ],
  activity: [
    { day: "Mon", date: daysAgo(6).slice(0, 10), clones: 38, success: 34, failed: 4 },
    { day: "Tue", date: daysAgo(5).slice(0, 10), clones: 45, success: 42, failed: 3 },
    { day: "Wed", date: daysAgo(4).slice(0, 10), clones: 32, success: 30, failed: 2 },
    { day: "Thu", date: daysAgo(3).slice(0, 10), clones: 50, success: 46, failed: 4 },
    { day: "Fri", date: daysAgo(2).slice(0, 10), clones: 28, success: 26, failed: 2 },
    { day: "Sat", date: daysAgo(1).slice(0, 10), clones: 12, success: 11, failed: 1 },
    { day: "Sun", date: daysAgo(0).slice(0, 10), clones: 8, success: 7, failed: 1 },
  ],
  week_over_week: { this_week: 213, last_week: 190, change_pct: 12.1 },
  recent_jobs: MOCK_JOBS.slice(0, 8).map((j, i) => ({ ...j, job_id: `recent-${String(i + 1).padStart(3, "0")}` })),
};

const MOCK_CATALOGS = ["prod_catalog", "dev_catalog", "staging_catalog", "analytics"];

const MOCK_SCHEMAS: Record<string, string[]> = {
  prod_catalog: ["bronze", "silver", "gold", "raw", "curated"],
  dev_catalog: ["bronze", "silver", "gold", "sandbox", "testing"],
  staging_catalog: ["bronze", "silver", "gold", "etl_staging"],
  analytics: ["reporting", "dashboards", "ml_features", "aggregates"],
};

const MOCK_TABLES: Record<string, string[]> = {
  bronze: ["customers_raw", "orders_raw", "products_raw", "transactions_raw", "events_raw"],
  silver: ["customers_clean", "orders_enriched", "products_master", "transactions_validated", "events_deduped"],
  gold: ["customer_360", "revenue_summary", "product_analytics", "daily_metrics", "user_segments"],
  raw: ["ingest_customers", "ingest_orders", "ingest_products", "ingest_logs"],
  curated: ["dim_customer", "dim_product", "fact_orders", "fact_revenue", "agg_daily_sales"],
  sandbox: ["experiment_01", "tmp_analysis", "adhoc_query_results"],
  testing: ["test_customers", "test_orders", "test_products"],
  etl_staging: ["stg_customers", "stg_orders", "stg_products", "stg_transactions"],
  reporting: ["monthly_revenue", "customer_churn", "product_performance", "regional_sales"],
  dashboards: ["exec_summary", "ops_metrics", "sales_dashboard", "marketing_funnel"],
  ml_features: ["customer_features", "product_embeddings", "transaction_features", "risk_scores"],
  aggregates: ["daily_totals", "weekly_summary", "monthly_rollup"],
};

const MOCK_NOTIFICATIONS = {
  unread_count: 5,
  items: [
    { type: "success", message: "Clone prod_catalog → dev_catalog completed (42 tables)", timestamp: hoursAgo(1.5), status: "completed", job_id: "clx-001" },
    { type: "error", message: "Clone prod_catalog → dr_catalog failed: Permission denied", timestamp: daysAgo(1), status: "failed", job_id: "clx-005" },
    { type: "info", message: "Sync analytics → analytics_staging started", timestamp: minsAgo(15), status: "running", job_id: "clx-003" },
    { type: "success", message: "Deep clone analytics → analytics_dev completed (28 tables)", timestamp: hoursAgo(4), status: "completed", job_id: "clx-002" },
    { type: "info", message: "Preflight checks passed for staging_catalog → qa_catalog", timestamp: daysAgo(1), status: "completed", job_id: "clx-004" },
  ],
};

const MOCK_WAREHOUSES = [
  { id: "wh-001", name: "Clone-Xs Serverless", size: "2X-Small", state: "RUNNING", type: "PRO" },
  { id: "wh-002", name: "Analytics Warehouse", size: "Medium", state: "RUNNING", type: "PRO" },
  { id: "wh-003", name: "ETL Warehouse", size: "Large", state: "STOPPED", type: "CLASSIC" },
];

const MOCK_CATALOG_HEALTH = {
  catalogs: [
    { catalog: "prod_catalog", total: 78, succeeded: 72, failed: 6, score: 92 },
    { catalog: "analytics", total: 45, succeeded: 43, failed: 2, score: 96 },
    { catalog: "dev_catalog", total: 32, succeeded: 30, failed: 2, score: 94 },
    { catalog: "staging_catalog", total: 28, succeeded: 25, failed: 3, score: 89 },
  ],
};

const MOCK_PREFLIGHT = {
  ready: true,
  checks: [
    { name: "Workspace Connectivity", status: "passed", message: "Connected to demo.azuredatabricks.net" },
    { name: "SQL Warehouse", status: "passed", message: "Warehouse 'Clone-Xs Serverless' is running" },
    { name: "Source Catalog Access", status: "passed", message: "USAGE granted on prod_catalog" },
    { name: "Destination Catalog Access", status: "passed", message: "CREATE SCHEMA granted on dev_catalog" },
    { name: "Unity Catalog Enabled", status: "passed", message: "Unity Catalog is enabled" },
    { name: "Storage Credentials", status: "passed", message: "External storage configured" },
  ],
};

/* ── Audit ── */

const MOCK_AUDIT = Array.from({ length: 15 }, (_, i) => ({
  id: `audit-${String(i + 1).padStart(3, "0")}`,
  job_id: MOCK_JOBS[i % MOCK_JOBS.length].job_id,
  operation: ["clone", "sync", "rollback", "validate", "pii-scan"][i % 5],
  source_catalog: MOCK_JOBS[i % MOCK_JOBS.length].source_catalog,
  destination_catalog: MOCK_JOBS[i % MOCK_JOBS.length].destination_catalog,
  user: ["data-engineer@company.com", "ml-team@company.com", "analytics@company.com", "devops@company.com"][i % 4],
  status: ["completed", "completed", "completed", "failed", "completed"][i % 5],
  started_at: daysAgo(i * 0.5),
  completed_at: daysAgo(i * 0.5 - 0.02),
  duration_seconds: [45, 120, 30, 8, 65, 180, 22, 90, 15, 240, 55, 38, 72, 110, 28][i],
  tables_affected: [42, 28, 35, 3, 42, 15, 22, 10, 1, 50, 18, 25, 30, 40, 12][i],
  message: i === 3 ? "Permission denied on finance schema" : `${["Clone", "Sync", "Rollback", "Validate", "PII scan"][i % 5]} completed successfully`,
}));

const MOCK_JOB_LOGS = [
  "[INFO] Starting clone operation: prod_catalog → dev_catalog",
  "[INFO] Clone type: SHALLOW | Dry run: false",
  "[INFO] Warehouse: Clone-Xs Serverless (wh-001)",
  "[INFO] Discovering schemas in prod_catalog...",
  "[INFO] Found 5 schemas: bronze, silver, gold, raw, curated",
  "[INFO] Discovering tables in bronze...",
  "[INFO] Found 5 tables in bronze",
  "[OK] Cloned bronze.customers_raw (SHALLOW) — 2.3s",
  "[OK] Cloned bronze.orders_raw (SHALLOW) — 1.8s",
  "[OK] Cloned bronze.products_raw (SHALLOW) — 0.9s",
  "[OK] Cloned bronze.transactions_raw (SHALLOW) — 3.1s",
  "[OK] Cloned bronze.events_raw (SHALLOW) — 1.2s",
  "[INFO] Discovering tables in silver...",
  "[OK] Cloned silver.customers_clean (SHALLOW) — 2.0s",
  "[OK] Cloned silver.orders_enriched (SHALLOW) — 2.5s",
  "[OK] Cloned silver.products_master (SHALLOW) — 1.1s",
  "[PROGRESS] 18/42 tables cloned (43%)",
  "[OK] Cloned gold.customer_360 (SHALLOW) — 4.2s",
  "[OK] Cloned gold.revenue_summary (SHALLOW) — 3.8s",
  "[PROGRESS] 32/42 tables cloned (76%)",
  "[OK] Cloned curated.dim_customer (SHALLOW) — 1.5s",
  "[OK] Cloned curated.fact_orders (SHALLOW) — 2.8s",
  "[INFO] Cloning views...",
  "[OK] Cloned 12 views",
  "[INFO] Clone completed: 42 tables, 12 views in 45.2s",
  "[OK] All operations completed successfully",
];

/* ── Templates ── */

const MOCK_TEMPLATES = [
  { id: "tpl-001", name: "Production to Dev", description: "Shallow clone production catalog to development for testing", source_catalog: "prod_catalog", destination_catalog: "dev_catalog", clone_type: "SHALLOW", include_schemas: [], exclude_schemas: ["_audit", "_system"], copy_permissions: false, copy_tags: true, tags: ["dev", "testing"], created_by: "data-engineer@company.com", created_at: daysAgo(30), used_count: 45 },
  { id: "tpl-002", name: "DR Backup", description: "Deep clone production catalog for disaster recovery", source_catalog: "prod_catalog", destination_catalog: "dr_catalog", clone_type: "DEEP", include_schemas: [], exclude_schemas: [], copy_permissions: true, copy_tags: true, tags: ["dr", "backup", "production"], created_by: "devops@company.com", created_at: daysAgo(60), used_count: 12 },
  { id: "tpl-003", name: "Analytics Refresh", description: "Sync analytics catalog from production data", source_catalog: "prod_catalog", destination_catalog: "analytics", clone_type: "SHALLOW", include_schemas: ["gold", "curated"], exclude_schemas: [], copy_permissions: false, copy_tags: true, tags: ["analytics", "reporting"], created_by: "analytics@company.com", created_at: daysAgo(15), used_count: 28 },
  { id: "tpl-004", name: "QA Environment", description: "Deep clone staging to QA for integration testing", source_catalog: "staging_catalog", destination_catalog: "qa_catalog", clone_type: "DEEP", include_schemas: ["bronze", "silver"], exclude_schemas: [], copy_permissions: true, copy_tags: true, tags: ["qa", "testing"], created_by: "qa-team@company.com", created_at: daysAgo(20), used_count: 18 },
  { id: "tpl-005", name: "ML Feature Store", description: "Clone ML features for model training environments", source_catalog: "analytics", destination_catalog: "ml_catalog", clone_type: "DEEP", include_schemas: ["ml_features"], exclude_schemas: [], copy_permissions: false, copy_tags: true, tags: ["ml", "features"], created_by: "ml-team@company.com", created_at: daysAgo(10), used_count: 8 },
];

/* ── Config ── */

const MOCK_CONFIG = {
  default_clone_type: "SHALLOW",
  default_warehouse_id: "wh-001",
  max_workers: 8,
  parallel_tables: 4,
  copy_permissions: true,
  copy_tags: true,
  copy_properties: true,
  enable_rollback: true,
  validate_after_clone: true,
  log_level: "INFO",
  notification_email: "admin@company.com",
  retention_days: 30,
};

const MOCK_CONFIG_PROFILES = [
  { name: "default", description: "Default configuration", is_active: true },
  { name: "fast-clone", description: "Optimized for speed (shallow, no validation)", is_active: false },
  { name: "full-backup", description: "Complete backup with all options enabled", is_active: false },
];

/* ── RBAC ── */

const MOCK_RBAC_POLICIES = [
  { id: "rbac-001", name: "Clone Admins", role: "admin", principals: ["data-engineer@company.com", "devops@company.com"], permissions: ["clone", "sync", "rollback", "delete", "manage_templates", "manage_rbac"], catalogs: ["*"], created_at: daysAgo(90) },
  { id: "rbac-002", name: "Analytics Team", role: "operator", principals: ["analytics@company.com", "ml-team@company.com"], permissions: ["clone", "sync", "view"], catalogs: ["analytics", "ml_catalog"], created_at: daysAgo(60) },
  { id: "rbac-003", name: "QA Readonly", role: "viewer", principals: ["qa-team@company.com"], permissions: ["view", "diff", "validate"], catalogs: ["staging_catalog", "qa_catalog"], created_at: daysAgo(30) },
  { id: "rbac-004", name: "Dev Self-Service", role: "operator", principals: ["dev-team@company.com"], permissions: ["clone", "view", "diff"], catalogs: ["dev_catalog", "test_catalog"], created_at: daysAgo(45) },
];

/* ── Storage Metrics ── */

const MOCK_STORAGE_METRICS = {
  catalog: "prod_catalog",
  total_size_bytes: 524_288_000_000,
  tables: [
    { schema: "bronze", table: "transactions_raw", size_bytes: 85_000_000_000, rows: 1_200_000_000, last_modified: daysAgo(0.5), format: "DELTA", partitions: 365, files: 4200, last_vacuum: daysAgo(1), last_optimize: daysAgo(0.5) },
    { schema: "bronze", table: "events_raw", size_bytes: 62_000_000_000, rows: 890_000_000, last_modified: daysAgo(0.3), format: "DELTA", partitions: 180, files: 3100, last_vacuum: daysAgo(2), last_optimize: daysAgo(1) },
    { schema: "silver", table: "orders_enriched", size_bytes: 45_000_000_000, rows: 450_000_000, last_modified: daysAgo(0.5), format: "DELTA", partitions: 90, files: 1800, last_vacuum: daysAgo(1), last_optimize: daysAgo(0.5) },
    { schema: "gold", table: "customer_360", size_bytes: 12_000_000_000, rows: 25_000_000, last_modified: daysAgo(1), format: "DELTA", partitions: 12, files: 240, last_vacuum: daysAgo(3), last_optimize: daysAgo(1) },
    { schema: "gold", table: "revenue_summary", size_bytes: 8_000_000_000, rows: 18_000_000, last_modified: daysAgo(1), format: "DELTA", partitions: 24, files: 180, last_vacuum: daysAgo(2), last_optimize: daysAgo(1) },
  ],
};

/* ── Profiling ── */

const MOCK_PROFILE = {
  catalog: "prod_catalog",
  schema: "bronze",
  table: "customers_raw",
  row_count: 2_500_000,
  column_count: 15,
  size_bytes: 3_200_000_000,
  columns: [
    { name: "customer_id", type: "BIGINT", null_count: 0, null_pct: 0, distinct_count: 2_500_000, min: "1", max: "2500000", mean: "1250000", stddev: "721688" },
    { name: "email", type: "STRING", null_count: 1250, null_pct: 0.05, distinct_count: 2_498_750, min: "aaa@example.com", max: "zzz@test.com", mean: null, stddev: null },
    { name: "first_name", type: "STRING", null_count: 500, null_pct: 0.02, distinct_count: 45_000, min: "Aaron", max: "Zoe", mean: null, stddev: null },
    { name: "last_name", type: "STRING", null_count: 200, null_pct: 0.008, distinct_count: 82_000, min: "Abbott", max: "Zwick", mean: null, stddev: null },
    { name: "phone", type: "STRING", null_count: 125_000, null_pct: 5.0, distinct_count: 2_375_000, min: "+1-200-000-0001", max: "+1-999-999-9999", mean: null, stddev: null },
    { name: "created_at", type: "TIMESTAMP", null_count: 0, null_pct: 0, distinct_count: 2_200_000, min: "2020-01-01T00:00:00Z", max: daysAgo(0), mean: null, stddev: null },
    { name: "country", type: "STRING", null_count: 800, null_pct: 0.032, distinct_count: 195, min: "Afghanistan", max: "Zimbabwe", mean: null, stddev: null },
    { name: "revenue_total", type: "DOUBLE", null_count: 50_000, null_pct: 2.0, distinct_count: 1_800_000, min: "0.00", max: "999999.99", mean: "4523.87", stddev: "12450.23" },
  ],
};

/* ── Lineage ── */

const MOCK_LINEAGE = {
  nodes: [
    { id: "bronze.customers_raw", type: "table", schema: "bronze", name: "customers_raw" },
    { id: "bronze.orders_raw", type: "table", schema: "bronze", name: "orders_raw" },
    { id: "bronze.products_raw", type: "table", schema: "bronze", name: "products_raw" },
    { id: "silver.customers_clean", type: "table", schema: "silver", name: "customers_clean" },
    { id: "silver.orders_enriched", type: "table", schema: "silver", name: "orders_enriched" },
    { id: "silver.products_master", type: "table", schema: "silver", name: "products_master" },
    { id: "gold.customer_360", type: "table", schema: "gold", name: "customer_360" },
    { id: "gold.revenue_summary", type: "table", schema: "gold", name: "revenue_summary" },
    { id: "reporting.monthly_revenue", type: "view", schema: "reporting", name: "monthly_revenue" },
  ],
  edges: [
    { source: "bronze.customers_raw", target: "silver.customers_clean" },
    { source: "bronze.orders_raw", target: "silver.orders_enriched" },
    { source: "bronze.products_raw", target: "silver.products_master" },
    { source: "silver.customers_clean", target: "gold.customer_360" },
    { source: "silver.orders_enriched", target: "gold.customer_360" },
    { source: "silver.orders_enriched", target: "gold.revenue_summary" },
    { source: "silver.products_master", target: "gold.revenue_summary" },
    { source: "gold.revenue_summary", target: "reporting.monthly_revenue" },
  ],
};

/* ── Cost Estimation ── */

const MOCK_ESTIMATE = {
  source_catalog: "prod_catalog",
  destination_catalog: "dev_catalog",
  clone_type: "SHALLOW",
  estimated_storage_bytes: 524_288_000,
  estimated_dbu_cost: 12.50,
  estimated_duration_seconds: 180,
  tables: 42,
  views: 12,
  breakdown: [
    { schema: "bronze", tables: 5, size_bytes: 210_000_000, estimated_cost: 5.25 },
    { schema: "silver", tables: 5, size_bytes: 180_000_000, estimated_cost: 4.50 },
    { schema: "gold", tables: 5, size_bytes: 90_000_000, estimated_cost: 2.25 },
    { schema: "raw", tables: 4, size_bytes: 30_000_000, estimated_cost: 0.35 },
    { schema: "curated", tables: 5, size_bytes: 14_288_000, estimated_cost: 0.15 },
  ],
};

/* ── Rollback ── */

const MOCK_ROLLBACK_LOGS = [
  { id: "rb-001", job_id: "clx-001", operation: "rollback", catalog: "dev_catalog", status: "completed", tables_rolled_back: 42, started_at: daysAgo(3), completed_at: daysAgo(2.99), user: "data-engineer@company.com" },
  { id: "rb-002", job_id: "clx-004", operation: "rollback", catalog: "qa_catalog", status: "completed", tables_rolled_back: 35, started_at: daysAgo(7), completed_at: daysAgo(6.98), user: "qa-team@company.com" },
];

/* ── Snapshots ── */

const MOCK_SNAPSHOTS = [
  { id: "snap-001", catalog: "prod_catalog", created_at: daysAgo(1), schemas: 5, tables: 42, views: 12, size_bytes: 524_288_000_000, user: "devops@company.com" },
  { id: "snap-002", catalog: "analytics", created_at: daysAgo(7), schemas: 4, tables: 28, views: 8, size_bytes: 128_000_000_000, user: "analytics@company.com" },
  { id: "snap-003", catalog: "prod_catalog", created_at: daysAgo(14), schemas: 5, tables: 40, views: 11, size_bytes: 510_000_000_000, user: "devops@company.com" },
];

/* ── Governance ── */

const MOCK_GOV_CERTIFICATIONS = [
  { id: "cert-001", catalog: "prod_catalog", schema: "gold", table: "customer_360", level: "gold", certified_by: "data-governance@company.com", certified_at: daysAgo(10), expires_at: daysAgo(-80), status: "active", notes: "Production-ready customer master" },
  { id: "cert-002", catalog: "prod_catalog", schema: "gold", table: "revenue_summary", level: "gold", certified_by: "data-governance@company.com", certified_at: daysAgo(15), expires_at: daysAgo(-75), status: "active", notes: "Audited revenue data" },
  { id: "cert-003", catalog: "analytics", schema: "reporting", table: "monthly_revenue", level: "silver", certified_by: "analytics@company.com", certified_at: daysAgo(5), expires_at: daysAgo(-85), status: "active", notes: "Reporting layer" },
  { id: "cert-004", catalog: "prod_catalog", schema: "silver", table: "customers_clean", level: "silver", certified_by: "data-engineer@company.com", certified_at: daysAgo(20), expires_at: daysAgo(-70), status: "active", notes: "Cleaned customer data" },
  { id: "cert-005", catalog: "prod_catalog", schema: "bronze", table: "orders_raw", level: "bronze", certified_by: "data-engineer@company.com", certified_at: daysAgo(30), expires_at: daysAgo(-60), status: "active", notes: "Raw ingestion, needs cleansing" },
];

const MOCK_DQ_RESULTS = [
  { rule_id: "dq-001", rule_name: "Not Null: customer_id", table: "customers_raw", column: "customer_id", status: "passed", pass_rate: 100.0, rows_checked: 2_500_000, rows_failed: 0, executed_at: hoursAgo(6) },
  { rule_id: "dq-002", rule_name: "Unique: email", table: "customers_raw", column: "email", status: "passed", pass_rate: 99.95, rows_checked: 2_500_000, rows_failed: 1250, executed_at: hoursAgo(6) },
  { rule_id: "dq-003", rule_name: "Range: revenue_total >= 0", table: "customers_raw", column: "revenue_total", status: "passed", pass_rate: 100.0, rows_checked: 2_450_000, rows_failed: 0, executed_at: hoursAgo(6) },
  { rule_id: "dq-004", rule_name: "Freshness: < 24h", table: "orders_raw", column: null, status: "passed", pass_rate: 100.0, rows_checked: 1, rows_failed: 0, executed_at: hoursAgo(6) },
  { rule_id: "dq-005", rule_name: "Not Null: order_id", table: "orders_raw", column: "order_id", status: "warning", pass_rate: 99.8, rows_checked: 5_000_000, rows_failed: 10_000, executed_at: hoursAgo(6) },
];

const MOCK_SLA_STATUS = [
  { id: "sla-001", name: "Gold tables freshness", target: "< 1 hour", current: "45 minutes", status: "met", table: "gold.customer_360", checked_at: minsAgo(15) },
  { id: "sla-002", name: "Bronze ingestion lag", target: "< 15 minutes", current: "8 minutes", status: "met", table: "bronze.events_raw", checked_at: minsAgo(10) },
  { id: "sla-003", name: "DQ pass rate > 99%", target: "> 99%", current: "99.95%", status: "met", table: "silver.customers_clean", checked_at: hoursAgo(1) },
  { id: "sla-004", name: "Reporting availability", target: "99.9% uptime", current: "99.95%", status: "met", table: "reporting.monthly_revenue", checked_at: hoursAgo(2) },
];

const MOCK_GOV_CHANGES = [
  { id: "chg-001", entity_type: "table", entity: "prod_catalog.gold.customer_360", change_type: "schema_change", description: "Added column: loyalty_tier (STRING)", user: "data-engineer@company.com", timestamp: daysAgo(1) },
  { id: "chg-002", entity_type: "table", entity: "prod_catalog.bronze.orders_raw", change_type: "data_update", description: "Bulk load: 150,000 new rows", user: "etl-pipeline", timestamp: daysAgo(0.5) },
  { id: "chg-003", entity_type: "schema", entity: "prod_catalog.archive", change_type: "created", description: "New schema created for historical data", user: "devops@company.com", timestamp: daysAgo(3) },
  { id: "chg-004", entity_type: "table", entity: "analytics.ml_features.risk_scores", change_type: "permission_change", description: "Granted SELECT to ml-team@company.com", user: "data-governance@company.com", timestamp: daysAgo(2) },
  { id: "chg-005", entity_type: "table", entity: "prod_catalog.silver.products_master", change_type: "tag_update", description: "Added tag: pii=false, classification=internal", user: "data-engineer@company.com", timestamp: daysAgo(4) },
];

const MOCK_ODCS_CONTRACTS = [
  { id: "odcs-001", name: "Customer Master Contract", version: "1.2.0", status: "active", owner: "data-governance@company.com", schema: "gold", table: "customer_360", sla: "99.9%", quality_score: 96, created_at: daysAgo(60), updated_at: daysAgo(5) },
  { id: "odcs-002", name: "Revenue Data Contract", version: "2.0.0", status: "active", owner: "finance@company.com", schema: "gold", table: "revenue_summary", sla: "99.5%", quality_score: 98, created_at: daysAgo(45), updated_at: daysAgo(10) },
  { id: "odcs-003", name: "Orders Ingestion Contract", version: "1.0.0", status: "draft", owner: "data-engineer@company.com", schema: "bronze", table: "orders_raw", sla: "99%", quality_score: 92, created_at: daysAgo(7), updated_at: daysAgo(2) },
];

const MOCK_GOV_GLOSSARY = [
  { term_id: "t1", name: "Customer", definition: "An individual or organization that purchases products or services", domain: "CRM", status: "approved", owner: "data-governance@company.com", linked_columns: ["gold.customer_360.customer_id", "silver.customers_clean.customer_id"], created_at: daysAgo(90) },
  { term_id: "t2", name: "Revenue", definition: "Total income generated from sales of goods and services", domain: "Finance", status: "approved", owner: "finance@company.com", linked_columns: ["gold.revenue_summary.total_revenue"], created_at: daysAgo(60) },
  { term_id: "t3", name: "Churn Rate", definition: "Percentage of customers who stop using the product in a given period", domain: "Analytics", status: "approved", owner: "analytics@company.com", linked_columns: ["reporting.customer_churn.churn_rate"], created_at: daysAgo(30) },
  { term_id: "t4", name: "PII", definition: "Personally Identifiable Information — data that can identify an individual", domain: "Compliance", status: "approved", owner: "legal@company.com", linked_columns: [], created_at: daysAgo(120) },
];

/* ── Table Info ── */

const MOCK_TABLE_INFO = {
  catalog: "prod_catalog",
  schema: "bronze",
  table: "customers_raw",
  type: "MANAGED",
  format: "DELTA",
  owner: "data-engineer@company.com",
  created_at: daysAgo(180),
  last_modified: daysAgo(0.5),
  row_count: 2_500_000,
  size_bytes: 3_200_000_000,
  columns: [
    { name: "customer_id", type: "BIGINT", nullable: false, comment: "Primary key" },
    { name: "email", type: "STRING", nullable: true, comment: "Customer email address" },
    { name: "first_name", type: "STRING", nullable: true, comment: null },
    { name: "last_name", type: "STRING", nullable: true, comment: null },
    { name: "phone", type: "STRING", nullable: true, comment: null },
    { name: "country", type: "STRING", nullable: true, comment: "ISO country code" },
    { name: "created_at", type: "TIMESTAMP", nullable: false, comment: "Record creation timestamp" },
    { name: "revenue_total", type: "DOUBLE", nullable: true, comment: "Lifetime revenue" },
  ],
  properties: { "delta.minReaderVersion": "1", "delta.minWriterVersion": "2" },
  tags: { "classification": "internal", "team": "data-engineering" },
  partitions: ["country"],
};

/**
 * Lookup mock response for a given API path.
 * Returns undefined if no mock exists (falls through to real API).
 */
export function getMockResponse(path: string, _body?: unknown): unknown | undefined {
  // ── Auth ──
  if (path === "/auth/status") return { authenticated: true, user: "demo@clonexs.io", host: "https://demo.azuredatabricks.net", auth_method: "demo" };
  if (path === "/auth/logout") return { ok: true };
  if (path === "/auth/warehouses") return MOCK_WAREHOUSES;
  if (path === "/auth/volumes") return [];
  if (path === "/auth/test-warehouse") return { success: true, message: "Warehouse is running" };

  // ── Health ──
  if (path === "/health") return { status: "ok", service: "Clone-Xs", runtime: "standalone" };

  // ── Dashboard & Monitoring ──
  if (path === "/monitor/metrics") return MOCK_DASHBOARD_STATS;
  if (path === "/notifications") return MOCK_NOTIFICATIONS;
  if (path === "/catalog-health") return MOCK_CATALOG_HEALTH;
  if (path === "/monitor") return { status: "healthy", last_sync: hoursAgo(2), tables_in_sync: 38, tables_drifted: 4 };

  // ── Catalogs & Explorer ──
  if (path === "/catalogs") return MOCK_CATALOGS;
  if (path.match(/^\/catalogs\/[^/]+\/schemas$/)) {
    const catalog = path.split("/")[2];
    return MOCK_SCHEMAS[catalog] || ["default"];
  }
  if (path.match(/^\/catalogs\/[^/]+\/[^/]+\/tables$/)) {
    const schema = path.split("/")[3];
    return MOCK_TABLES[schema] || ["table_1", "table_2", "table_3"];
  }
  if (path.match(/^\/catalogs\/[^/]+\/[^/]+\/[^/]+\/info$/)) return MOCK_TABLE_INFO;

  // ── Clone Jobs ──
  if (path === "/clone/jobs") return MOCK_JOBS;
  if (path.match(/^\/clone\/[^/]+$/)) {
    const jobId = path.split("/")[2];
    const found = MOCK_JOBS.find(j => j.job_id === jobId);
    // In demo mode, always return "completed" so polling stops
    if (found) return { ...found, status: "completed", completed_at: found.completed_at || now.toISOString() };
    return { job_id: jobId, status: "completed", source_catalog: "prod_catalog", destination_catalog: "dev_catalog", clone_type: "SHALLOW", started_at: minsAgo(2), completed_at: now.toISOString(), duration_seconds: 120, tables_cloned: 42, views_cloned: 12, error_message: null };
  }
  if (path === "/clone") return { job_id: "clx-demo-" + Date.now(), status: "completed", message: "Demo clone completed", tables_cloned: 42, views_cloned: 12, duration_seconds: 45 };

  // ── Audit ──
  if (path === "/audit") return MOCK_AUDIT;
  if (path.match(/^\/audit\/[^/]+\/logs$/)) return MOCK_JOB_LOGS;

  // ── Preflight ──
  if (path === "/preflight" || path.startsWith("/preflight")) return MOCK_PREFLIGHT;

  // ── Analysis ──
  if (path === "/diff") return { schemas: { only_in_source: ["raw"], only_in_dest: ["archive"], in_both: ["bronze", "silver", "gold"] }, tables: { only_in_source: ["events_raw", "logs_raw"], only_in_dest: ["old_customers"], in_both: ["customers", "orders", "products", "transactions"] } };
  if (path === "/compare") return { matches: 38, mismatches: 4, missing_in_dest: 2, extra_in_dest: 1, columns: [{ table: "customers_raw", column: "email", source_type: "STRING", dest_type: "STRING", match: true }, { table: "orders_raw", column: "amount", source_type: "INT", dest_type: "DOUBLE", match: false }] };
  if (path === "/validate") return { valid: true, issues: [{ table: "orders_raw", issue: "Column type mismatch: amount INT→DOUBLE", severity: "warning" }] };
  if (path === "/stats") return { catalog: "prod_catalog", total_schemas: 5, total_tables: 42, total_views: 12, total_volumes: 8, total_functions: 3 };
  if (path === "/search") return { results: [{ catalog: "prod_catalog", schema: "bronze", name: "customers_raw", type: "TABLE", match_field: "name" }, { catalog: "prod_catalog", schema: "gold", name: "customer_360", type: "TABLE", match_field: "name" }, { catalog: "analytics", schema: "ml_features", name: "customer_features", type: "TABLE", match_field: "name" }] };
  if (path === "/column-usage") return { columns: [{ column: "customer_id", tables_using: ["customers_raw", "customers_clean", "customer_360"], read_count: 1250, write_count: 45 }, { column: "email", tables_using: ["customers_raw", "customers_clean"], read_count: 890, write_count: 12 }] };
  if (path === "/pii-scan") return { catalog: "prod_catalog", tables_scanned: 42, pii_columns_found: 7, columns: [{ table: "customers_raw", column: "email", pii_type: "EMAIL", confidence: 0.98 }, { table: "customers_raw", column: "phone", pii_type: "PHONE", confidence: 0.95 }, { table: "customers_raw", column: "ssn", pii_type: "SSN", confidence: 0.99 }, { table: "orders_raw", column: "billing_address", pii_type: "ADDRESS", confidence: 0.87 }, { table: "customers_raw", column: "first_name", pii_type: "NAME", confidence: 0.82 }, { table: "customers_raw", column: "last_name", pii_type: "NAME", confidence: 0.85 }, { table: "customers_raw", column: "country", pii_type: "LOCATION", confidence: 0.72 }] };
  if (path === "/schema-drift") return { drifts: [{ table: "customers_raw", column: "address", change: "added", old_type: null, new_type: "STRING" }, { table: "orders_raw", column: "amount", change: "type_changed", old_type: "INT", new_type: "DOUBLE" }, { table: "products_raw", column: "weight_kg", change: "added", old_type: null, new_type: "FLOAT" }, { table: "events_raw", column: "old_field", change: "removed", old_type: "STRING", new_type: null }] };
  if (path === "/lineage") return MOCK_LINEAGE;
  if (path === "/profile") return MOCK_PROFILE;
  if (path === "/estimate") return MOCK_ESTIMATE;

  // ── Sync ──
  if (path === "/sync") return { job_id: "sync-demo-" + Date.now(), status: "completed", message: "Demo sync completed", tables_synced: 38, duration_seconds: 90 };
  if (path === "/incremental-sync") return { job_id: "isync-demo-" + Date.now(), status: "completed", message: "Demo incremental sync completed", tables_synced: 12, duration_seconds: 30 };

  // ── Storage Metrics ──
  if (path === "/storage-metrics") return MOCK_STORAGE_METRICS;
  if (path === "/check-predictive-optimization") return { enabled: true, tables_optimized: 18, savings_pct: 23.5, recommendations: [{ table: "bronze.transactions_raw", action: "OPTIMIZE", reason: "4200 small files detected", estimated_savings: "15%" }, { table: "bronze.events_raw", action: "VACUUM", reason: "Retained files older than 7 days", estimated_savings: "8%" }] };
  if (path === "/optimize" || path === "/vacuum") return { status: "completed", tables_processed: 5, duration_seconds: 120 };

  // ── Rollback & Snapshots ──
  if (path === "/rollback/logs" || path === "/rollback") return MOCK_ROLLBACK_LOGS;
  if (path === "/snapshot") return MOCK_SNAPSHOTS;
  if (path === "/export-metadata") return { status: "completed", file: "metadata_export_demo.json", records: 420 };

  // ── Config & Templates ──
  if (path === "/config") return MOCK_CONFIG;
  if (path === "/config/profiles") return MOCK_CONFIG_PROFILES;
  if (path === "/templates") return MOCK_TEMPLATES;

  // ── RBAC ──
  if (path === "/rbac/policies") return MOCK_RBAC_POLICIES;

  // ── Warehouse Operations ──
  if (path === "/warehouse/start" || path === "/warehouse/stop") return { status: "ok" };
  if (path.startsWith("/config/warehouse")) return { status: "ok" };

  // ── Governance ──
  if (path === "/governance/init") return { status: "ok", message: "Governance tables initialized" };
  if (path === "/governance/glossary") return MOCK_GOV_GLOSSARY;
  if (path.match(/^\/governance\/glossary\/.+/)) return MOCK_GOV_GLOSSARY[0];
  if (path === "/governance/certifications") return MOCK_GOV_CERTIFICATIONS;
  if (path.match(/^\/governance\/dq\/results$|^\/governance\/dq-results$/)) return MOCK_DQ_RESULTS;
  if (path.match(/^\/governance\/dq/)) return MOCK_DQ_RESULTS;
  if (path.match(/^\/governance\/sla\/status$|^\/governance\/sla/)) return MOCK_SLA_STATUS;
  if (path === "/governance/changes") return MOCK_GOV_CHANGES;
  if (path === "/governance/odcs/import") return { contract_id: "odcs-demo-" + Date.now(), name: "Imported Contract", status: "active" };
  if (path.match(/^\/governance\/odcs\/contracts\/[^/]+\/validate$/)) return { compliant: true, total_violations: 0, checks: [{ name: "Schema compliance", status: "passed" }, { name: "SLA compliance", status: "passed" }, { name: "Quality compliance", status: "passed" }] };
  if (path.match(/^\/governance\/odcs\/contracts\/[^/]+\/export$/)) return "kind: DataContract\napiVersion: v3.0.0\ntype: tables\nstatus: active";
  if (path.match(/^\/governance\/odcs\/contracts$|^\/governance\/odcs$/)) return MOCK_ODCS_CONTRACTS;
  if (path.match(/^\/governance\/odcs\/.+/)) return MOCK_ODCS_CONTRACTS[0];
  if (path === "/governance/search") return { results: [{ type: "table", name: "customer_360", catalog: "prod_catalog", schema: "gold", tags: ["certified", "pii-free"] }] };
  if (path.startsWith("/governance/dq-rules")) return [];
  if (path.startsWith("/governance/contracts")) return MOCK_ODCS_CONTRACTS;
  if (path.startsWith("/governance")) return [];

  // ── Generate / Demo Data ──
  if (path === "/generate") return { job_id: "gen-demo-" + Date.now(), status: "completed", files: ["main.tf", "variables.tf", "outputs.tf"], format: "terraform" };
  if (path.match(/^\/generate\/.+/)) return { status: "completed", files: ["main.tf", "variables.tf", "outputs.tf"] };
  if (path === "/demo-data") return { job_id: "dd-demo-" + Date.now(), status: "completed", catalog: "demo_catalog", tables_created: 100 };
  if (path.match(/^\/demo-data\/.+/)) return { status: "completed", catalog: "demo_catalog", tables_created: 100 };

  // ── Compliance ──
  if (path === "/compliance") return { score: 94, checks: [{ name: "Access Controls", status: "passed", score: 98 }, { name: "Data Classification", status: "passed", score: 92 }, { name: "Audit Logging", status: "passed", score: 96 }, { name: "Encryption", status: "passed", score: 100 }, { name: "PII Protection", status: "warning", score: 85 }] };

  // ── Catch-all for any POST that hasn't matched ──
  if (_body !== undefined) return { status: "ok", message: "Demo mode — operation simulated" };

  return undefined;
}
