import { http, HttpResponse } from "msw";

/**
 * Default MSW handlers for common API endpoints.
 * Individual tests can override these by providing their own handlers.
 */
export const handlers = [
  // Health check
  http.get("/api/health", () => {
    return HttpResponse.json({ status: "ok", runtime: "standalone" });
  }),

  // Auth status — default: not authenticated
  http.get("/api/auth/status", () => {
    return HttpResponse.json({
      authenticated: false,
      user: null,
      host: null,
      auth_method: null,
    });
  }),

  // Auth login
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

  // Auth logout
  http.post("/api/auth/logout", () => {
    return HttpResponse.json({ status: "ok" });
  }),

  // Warehouses
  http.get("/api/auth/warehouses", () => {
    return HttpResponse.json([
      {
        id: "wh-001",
        name: "Starter Warehouse",
        size: "Small",
        state: "RUNNING",
        cluster_size: "2X-Small",
      },
      {
        id: "wh-002",
        name: "Dev Warehouse",
        size: "Medium",
        state: "STOPPED",
        cluster_size: "Small",
      },
    ]);
  }),

  // Clone jobs
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

  // Dashboard stats
  http.get("/api/audit", () => {
    return HttpResponse.json([]);
  }),

  // Notifications
  http.get("/api/notifications", () => {
    return HttpResponse.json([]);
  }),

  // Config
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

  // Catalog health
  http.get("/api/catalog-health", () => {
    return HttpResponse.json({ score: 85, issues: [] });
  }),

  // Monitor metrics
  http.get("/api/monitor/metrics", () => {
    return HttpResponse.json({ active_jobs: 0, queued_jobs: 0 });
  }),
];
