import { http, HttpResponse } from "msw";

/**
 * Default MSW handlers for common API endpoints.
 * Individual tests can override these by providing their own handlers.
 */
export const handlers = [
  // ── Health ───────────────────────────────────────────────────────────
  http.get("/api/health", () => {
    return HttpResponse.json({ status: "ok", runtime: "standalone" });
  }),

  // ── Auth ─────────────────────────────────────────────────────────────
  http.get("/api/auth/status", () => {
    return HttpResponse.json({
      authenticated: false,
      user: null,
      host: null,
      auth_method: null,
    });
  }),

  http.post("/api/auth/login", async ({ request }) => {
    const body = (await request.json()) as Record<string, string>;
    if (body.host && body.token) {
      return HttpResponse.json({
        authenticated: true,
        user: "test-user@databricks.com",
        session_id: "test-session-123",
      });
    }
    return HttpResponse.json(
      { detail: "Host and token are required" },
      { status: 400 }
    );
  }),

  http.post("/api/auth/logout", () => {
    return HttpResponse.json({ status: "ok" });
  }),

  http.get("/api/auth/warehouses", () => {
    return HttpResponse.json([
      { id: "wh-001", name: "Starter Warehouse", size: "Small", state: "RUNNING", cluster_size: "2X-Small" },
      { id: "wh-002", name: "Dev Warehouse", size: "Medium", state: "STOPPED", cluster_size: "Small" },
    ]);
  }),

  http.get("/api/auth/env-vars", () => {
    return HttpResponse.json({});
  }),

  http.get("/api/auth/volumes", () => {
    return HttpResponse.json([]);
  }),

  // ── Clone ────────────────────────────────────────────────────────────
  http.get("/api/clone/jobs", () => {
    return HttpResponse.json([
      {
        job_id: "job-001",
        status: "completed",
        job_type: "clone",
        source_catalog: "prod",
        destination_catalog: "dev",
        progress: 100,
        created_at: new Date().toISOString(),
      },
    ]);
  }),

  http.post("/api/clone", () => {
    return HttpResponse.json({ job_id: "job-002", status: "queued" });
  }),

  // ── Config ───────────────────────────────────────────────────────────
  http.get("/api/config", () => {
    return HttpResponse.json({
      source_catalog: "",
      destination_catalog: "",
      max_workers: 10,
      parallel_tables: 10,
      max_parallel_queries: 10,
      audit_trail: { catalog: "clone_audit", schema: "logs" },
    });
  }),

  http.get("/api/config/profiles", () => {
    return HttpResponse.json({ profiles: { default: {} } });
  }),

  // ── Catalogs ─────────────────────────────────────────────────────────
  http.get("/api/catalogs", () => {
    return HttpResponse.json(["test_catalog", "prod_catalog"]);
  }),

  http.get("/api/catalogs/:catalog/schemas", () => {
    return HttpResponse.json(["default", "bronze", "silver"]);
  }),

  http.get("/api/catalogs/:catalog/:schema/tables", () => {
    return HttpResponse.json(["table_a", "table_b"]);
  }),

  // ── Analysis ─────────────────────────────────────────────────────────
  http.post("/api/diff", () => {
    return HttpResponse.json({ missing: [], extra: [], matching: [], summary: {} });
  }),

  http.post("/api/compare", () => {
    return HttpResponse.json({ tables: [], summary: {} });
  }),

  http.post("/api/validate", () => {
    return HttpResponse.json({ results: [], passed: true });
  }),

  http.post("/api/stats", () => {
    return HttpResponse.json({ catalogs: [], summary: {} });
  }),

  http.post("/api/search", () => {
    return HttpResponse.json({ results: [] });
  }),

  http.post("/api/profile", () => {
    return HttpResponse.json({ profiles: [] });
  }),

  http.post("/api/estimate", () => {
    return HttpResponse.json({ estimated_cost: 0, details: [] });
  }),

  http.post("/api/storage-metrics", () => {
    return HttpResponse.json({ tables: [], summary: {} });
  }),

  // ── Management ───────────────────────────────────────────────────────
  http.post("/api/preflight", () => {
    return HttpResponse.json({ checks: [], passed: true });
  }),

  http.get("/api/rollback/logs", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/pii-scan", () => {
    return HttpResponse.json({ results: [], scan_id: "scan-001" });
  }),

  http.get("/api/pii-scans", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/pii-patterns", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/sync", () => {
    return HttpResponse.json({ job_id: "sync-001", status: "queued" });
  }),

  http.get("/api/audit", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/compliance", () => {
    return HttpResponse.json({ results: [], passed: true });
  }),

  http.get("/api/templates", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/schedule", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/lineage", () => {
    return HttpResponse.json({ nodes: [], edges: [] });
  }),

  http.post("/api/impact", () => {
    return HttpResponse.json({ affected: [] });
  }),

  http.post("/api/preview", () => {
    return HttpResponse.json({ tables: [] });
  }),

  http.get("/api/rbac/policies", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/plugins", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/notifications", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/catalog-health", () => {
    return HttpResponse.json({ score: 85, issues: [] });
  }),

  http.get("/api/monitor/metrics", () => {
    return HttpResponse.json({ active_jobs: 0, queued_jobs: 0 });
  }),

  http.get("/api/cache/stats", () => {
    return HttpResponse.json({ hits: 0, misses: 0, size: 0 });
  }),

  // ── System Insights ──────────────────────────────────────────────────
  http.post("/api/system-insights/summary", () => {
    return HttpResponse.json({
      billing: [], optimization: [], job_runs: [], lineage: [], storage: [],
      summary: { total_dbus: 0, total_jobs: 0, failed_jobs: 0, total_storage_gb: 0 },
    });
  }),

  http.post("/api/system-insights/billing", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/system-insights/warehouses", () => {
    return HttpResponse.json({ warehouses: [], summary: {}, warnings: [] });
  }),

  http.post("/api/system-insights/clusters", () => {
    return HttpResponse.json({ clusters: [], summary: {}, recent_events: [] });
  }),

  http.post("/api/system-insights/pipelines", () => {
    return HttpResponse.json({ pipelines: [], summary: {}, events: [] });
  }),

  http.post("/api/system-insights/query-performance", () => {
    return HttpResponse.json({ queries: [], summary: {}, slowest: [], by_warehouse: [] });
  }),

  http.post("/api/system-insights/metastore", () => {
    return HttpResponse.json({ metastore: {}, catalogs_count: 0, schemas_count: 0 });
  }),

  http.post("/api/system-insights/alerts", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/system-insights/jobs", () => {
    return HttpResponse.json([]);
  }),

  // ── Governance ───────────────────────────────────────────────────────
  http.get("/api/governance/glossary", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/governance/search", () => {
    return HttpResponse.json({ results: [] });
  }),

  http.get("/api/governance/dq/rules", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dq/results", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dq/history", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/certifications", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/sla/rules", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/sla/status", () => {
    return HttpResponse.json({ compliant: true, violations: [] });
  }),

  http.get("/api/governance/odcs/contracts", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dqx/checks", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dqx/dashboard", () => {
    return HttpResponse.json({ total_checks: 0, passed: 0, failed: 0, tables_profiled: 0 });
  }),

  http.get("/api/governance/dqx/functions", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dqx/results", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/dqx/profiles", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/governance/changes", () => {
    return HttpResponse.json([]);
  }),

  // ── ML Assets ────────────────────────────────────────────────────────
  http.post("/api/ml-assets/list", () => {
    return HttpResponse.json({ models: [], feature_tables: [], endpoints: [] });
  }),

  http.get("/api/ml-assets/serving-endpoints", () => {
    return HttpResponse.json([]);
  }),

  // ── Federation ───────────────────────────────────────────────────────
  http.get("/api/federation/catalogs", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/federation/connections", () => {
    return HttpResponse.json([]);
  }),

  // ── Delta Sharing ────────────────────────────────────────────────────
  http.get("/api/delta-sharing/shares", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/delta-sharing/recipients", () => {
    return HttpResponse.json([]);
  }),

  // ── Advanced Tables ──────────────────────────────────────────────────
  http.post("/api/advanced-tables/list", () => {
    return HttpResponse.json({ tables: [] });
  }),

  // ── Lakehouse Monitor ────────────────────────────────────────────────
  http.post("/api/lakehouse-monitor/list", () => {
    return HttpResponse.json({ monitors: [] });
  }),

  // ── AI ───────────────────────────────────────────────────────────────
  http.get("/api/ai/status", () => {
    return HttpResponse.json({ available: false });
  }),

  // ── Incremental ──────────────────────────────────────────────────────
  http.post("/api/incremental/check", () => {
    return HttpResponse.json({ changed_tables: [] });
  }),

  // ── Generate ─────────────────────────────────────────────────────────
  http.get("/api/generate/clone-jobs", () => {
    return HttpResponse.json([]);
  }),

  // ── Notifications prefs ──────────────────────────────────────────────
  http.get("/api/notifications/preferences", () => {
    return HttpResponse.json({ email: false, slack: false });
  }),

  http.get("/api/notifications/webhooks", () => {
    return HttpResponse.json([]);
  }),

  // ── Dependencies ─────────────────────────────────────────────────────
  http.get("/api/functions/:catalog", () => {
    return HttpResponse.json([]);
  }),

  http.get("/api/views/:catalog", () => {
    return HttpResponse.json([]);
  }),
];

/**
 * Authenticated handler overrides — use with server.use(...authenticatedHandlers)
 * in tests that need to render authenticated pages.
 */
export const authenticatedHandlers = [
  http.get("/api/auth/status", () => {
    return HttpResponse.json({
      authenticated: true,
      user: "test-user@databricks.com",
      host: "https://test.azuredatabricks.net",
      auth_method: "pat",
      session_id: "test-session-123",
    });
  }),
];
