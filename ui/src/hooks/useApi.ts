import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api-client";
import type { AuthStatus, WarehouseInfo, CloneJob } from "@/types/api";

export function useAuthStatus() {
  return useQuery<AuthStatus>({
    queryKey: ["auth-status"],
    queryFn: () => api.get("/auth/status"),
    staleTime: 120_000, // 2 min — auth status rarely changes
    retry: false,
  });
}

export function useWarehouses() {
  return useQuery<WarehouseInfo[]>({
    queryKey: ["warehouses"],
    queryFn: () => api.get("/auth/warehouses"),
    staleTime: 300_000, // 5 min — warehouse list is stable
    retry: false,
  });
}

export interface VolumeInfo {
  catalog: string;
  schema: string;
  name: string;
  type: string;
  path: string;
}

export function useVolumes() {
  return useQuery<VolumeInfo[]>({
    queryKey: ["volumes"],
    queryFn: () => api.get("/auth/volumes"),
    retry: false,
  });
}

export function useCloneJobs() {
  return useQuery<CloneJob[]>({
    queryKey: ["clone-jobs"],
    queryFn: () => api.get("/clone/jobs"),
    refetchInterval: 5000,
  });
}

export interface DashboardStats {
  total_clones: number;
  succeeded: number;
  failed: number;
  running: number;
  success_rate: number;
  avg_duration: number;
  max_duration: number;
  min_duration: number;
  total_tables_cloned: number;
  total_views_cloned: number;
  total_volumes_cloned: number;
  total_data_bytes: number;
  avg_tables_per_clone: number;
  by_status: Record<string, number>;
  clone_type_split: Record<string, number>;
  operation_type_split: Record<string, number>;
  top_catalogs: { catalog: string; count: number }[];
  active_users: { user: string; count: number }[];
  peak_hours: { hour: number; count: number }[];
  activity: { day: string; date: string; clones: number; success: number; failed: number }[];
  week_over_week: { this_week: number; last_week: number; change_pct: number };
  recent_jobs: {
    job_id: string;
    job_type?: string;
    source_catalog: string;
    destination_catalog: string;
    clone_type?: string;
    status: string;
    started_at?: string;
    completed_at?: string;
    duration_seconds?: number;
    error_message?: string;
  }[];
}

export function useDashboardStats() {
  return useQuery<DashboardStats>({
    queryKey: ["dashboard-stats"],
    queryFn: () => api.get("/monitor/metrics"),
    staleTime: 60_000, // 1 min — dashboard doesn't need real-time
    refetchInterval: 60_000,
    retry: 1,
  });
}

export interface Notification {
  type: "success" | "error" | "info";
  message: string;
  timestamp: string;
  status: string;
  job_id: string;
}

export interface NotificationsData {
  unread_count: number;
  items: Notification[];
}

export function useNotifications() {
  const since = localStorage.getItem("notifications_last_seen") || "";
  return useQuery<NotificationsData>({
    queryKey: ["notifications", since],
    queryFn: () => api.get(`/notifications${since ? `?since=${encodeURIComponent(since)}` : ""}`),
    refetchInterval: 60000,
    retry: 1,
  });
}

export interface CatalogHealth {
  catalog: string;
  total: number;
  succeeded: number;
  failed: number;
  last_operation?: string;
  score: number;
  tables_cloned?: number;
  tables_failed?: number;
  total_bytes?: number;
}

export function useCatalogHealth() {
  return useQuery<{ catalogs: CatalogHealth[] }>({
    queryKey: ["catalog-health"],
    queryFn: () => api.get("/catalog-health"),
    staleTime: 120_000, // 2 min — health data is semi-stable
    refetchInterval: 120_000,
    retry: 1,
  });
}

export function useStartClone() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/clone", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clone-jobs"] }),
  });
}

export function useValidateTarget() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/target/validate", req),
  });
}

export function useTargetWarehouses() {
  return useMutation<WarehouseInfo[], Error, Record<string, unknown>>({
    mutationFn: (req) => api.post("/target/warehouses", req),
  });
}

// ──────────────────────────────────────────────────────────────────
// Saved target connections — stored in browser localStorage. The server
// is intentionally stateless w.r.t. target creds; clones send full creds
// inline in the request body, sourced from the localStorage entry.
// ──────────────────────────────────────────────────────────────────

export interface TargetConnection {
  name: string;
  host: string;
  auth_method: "pat" | "service_principal" | "profile";
  token?: string;
  client_id?: string;
  client_secret?: string;
  profile?: string;
  warehouse_id: string;
  keep_share?: boolean;
  data_sync_mode?: "snapshot_once" | "incremental" | "force_full";
  auto_handle_masks?: boolean;
}

const TARGETS_KEY = "clxs_target_connections";

function readTargets(): TargetConnection[] {
  try {
    const raw = localStorage.getItem(TARGETS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeTargets(list: TargetConnection[]): void {
  localStorage.setItem(TARGETS_KEY, JSON.stringify(list));
}

export function findTargetConnection(name: string): TargetConnection | null {
  return readTargets().find((c) => c.name === name) ?? null;
}

// Build the body POSTed to /target/* endpoints from a stored connection.
// Strips the sentinel "***" placeholder that some legacy entries may carry.
export function targetCredsBody(conn: TargetConnection): Record<string, unknown> {
  const body: Record<string, unknown> = {
    host: conn.host,
    auth_method: conn.auth_method,
  };
  if (conn.auth_method === "pat") body.token = conn.token;
  if (conn.auth_method === "service_principal") {
    body.client_id = conn.client_id;
    body.client_secret = conn.client_secret;
  }
  if (conn.auth_method === "profile") body.profile = conn.profile;
  return body;
}

export function useTargetConnections() {
  return useQuery<TargetConnection[]>({
    queryKey: ["target-connections"],
    queryFn: () => Promise.resolve(readTargets()),
    staleTime: Infinity,
  });
}

export function useCreateTargetConnection() {
  const qc = useQueryClient();
  return useMutation<TargetConnection, Error, TargetConnection>({
    mutationFn: async (conn) => {
      const list = readTargets();
      if (list.some((c) => c.name === conn.name)) {
        throw new Error(`Target connection '${conn.name}' already exists`);
      }
      list.push(conn);
      writeTargets(list);
      return conn;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["target-connections"] }),
  });
}

export function useUpdateTargetConnection() {
  const qc = useQueryClient();
  return useMutation<TargetConnection, Error, { name: string; patch: Partial<TargetConnection> }>({
    mutationFn: async ({ name, patch }) => {
      const list = readTargets();
      const idx = list.findIndex((c) => c.name === name);
      if (idx < 0) throw new Error(`Target connection '${name}' not found`);
      const merged = { ...list[idx], ...patch, name };
      list[idx] = merged;
      writeTargets(list);
      return merged;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["target-connections"] }),
  });
}

export function useDeleteTargetConnection() {
  const qc = useQueryClient();
  return useMutation<{ name: string }, Error, string>({
    mutationFn: async (name) => {
      const list = readTargets();
      const remaining = list.filter((c) => c.name !== name);
      if (remaining.length === list.length) throw new Error(`Target connection '${name}' not found`);
      writeTargets(remaining);
      return { name };
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["target-connections"] }),
  });
}

// Test takes a connection NAME (resolved from localStorage to creds), then
// posts the inline creds to the existing /target/validate endpoint.
export function useTestTargetConnection() {
  return useMutation<any, Error, string>({
    mutationFn: async (name) => {
      const conn = findTargetConnection(name);
      if (!conn) throw new Error(`Target connection '${name}' not found`);
      const body = { ...targetCredsBody(conn), warehouse_id: conn.warehouse_id };
      return api.post("/target/validate", body);
    },
  });
}

// Lightweight identity check — used by /settings to display "Logged in as"
// for each saved target without running the full validate flow.
export function useTargetWhoami(connectionName: string | null | undefined) {
  return useQuery<{ user: string | null; host: string }>({
    queryKey: ["target-whoami", connectionName],
    queryFn: () => {
      const conn = findTargetConnection(connectionName!);
      if (!conn) throw new Error(`Target connection '${connectionName}' not found`);
      return api.post("/target/whoami", targetCredsBody(conn));
    },
    enabled: !!connectionName,
    staleTime: 5 * 60_000,
    retry: false,
  });
}

// Resolves connection name → creds from localStorage, then POSTs to the
// stateless /target/catalogs endpoint.
export function useTargetCatalogs(connectionName: string | null | undefined) {
  return useQuery<string[]>({
    queryKey: ["target-catalogs", connectionName],
    queryFn: () => {
      const conn = findTargetConnection(connectionName!);
      if (!conn) throw new Error(`Target connection '${connectionName}' not found`);
      return api.post("/target/catalogs", targetCredsBody(conn));
    },
    enabled: !!connectionName,
    staleTime: 60_000,
    retry: false,
  });
}

export function useEstimate() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/estimate", req),
  });
}

export function useStartSync() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/sync", req),
  });
}

export function useIncrementalCheck() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/incremental/check", req),
  });
}

export function useStartIncrementalSync() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/incremental/sync", req),
  });
}

export function useSchemaEvolutionDetect() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/schema-evolution/detect", req),
  });
}

export function useCdfCheck() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/incremental/cdf-check", req),
  });
}

export function useSchedules() {
  return useQuery({
    queryKey: ["schedules"],
    queryFn: () => api.get<any[]>("/schedules"),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

export function useCreateSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post<any>("/schedules", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
}

export function usePauseSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post<any>(`/schedules/${encodeURIComponent(id)}/pause`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
}

export function useResumeSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post<any>(`/schedules/${encodeURIComponent(id)}/resume`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
}

export function useDeleteSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.delete(`/schedules/${encodeURIComponent(id)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
}

export function useSyncJobs() {
  return useQuery({
    queryKey: ["sync-jobs"],
    queryFn: async () => {
      const all = await api.get<any[]>("/clone/jobs");
      // Backend job_type is "sync" or "incremental_sync"
      return (all || []).filter((j) => {
        const t = (j.job_type || "").toLowerCase();
        return t === "sync" || t === "incremental_sync";
      });
    },
    staleTime: 15_000,
    refetchInterval: 20_000,
  });
}

export function useDiffPreview() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/diff", req),
  });
}

export function useSnapshots(catalog?: string | null) {
  const qs = catalog ? `?source_catalog=${encodeURIComponent(catalog)}` : "";
  return useQuery({
    queryKey: ["clone-snapshots", catalog || null],
    queryFn: () => api.get<any[]>(`/clone-snapshots${qs}`),
    staleTime: 30_000,
  });
}

export function useCreateSnapshot() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/clone-snapshots", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clone-snapshots"] }),
  });
}

export function useDeleteSnapshot() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.delete(`/clone-snapshots/${encodeURIComponent(id)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clone-snapshots"] }),
  });
}

export function useSchemaObjects(catalog: string | null, schema: string | null) {
  return useQuery({
    queryKey: ["schema-objects", catalog, schema],
    queryFn: () => api.get<{
      tables: string[];
      views: string[];
      functions: string[];
      volumes: string[];
    }>(`/catalogs/${encodeURIComponent(catalog!)}/${encodeURIComponent(schema!)}/objects`),
    enabled: !!catalog && !!schema,
    staleTime: 5 * 60_000,
  });
}

export function useDiff() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; destination_catalog: string; warehouse_id?: string }) =>
      api.post("/diff", req),
  });
}

export function useValidate() {
  return useMutation({
    mutationFn: (req: Record<string, unknown>) => api.post("/validate", req),
  });
}

export function useStats() {
  return useMutation({
    mutationFn: async (req: { source_catalog: string; warehouse_id?: string }) => {
      const result = await api.post("/stats", req);
      // Cache in sessionStorage for page navigation persistence
      try { sessionStorage.setItem(`clxs-stats-${req.source_catalog}`, JSON.stringify(result)); } catch {}
      return result;
    },
  });
}

/** Load cached stats for a catalog (survives page navigation) */
export function getCachedStats(catalog: string): any | null {
  try {
    const cached = sessionStorage.getItem(`clxs-stats-${catalog}`);
    return cached ? JSON.parse(cached) : null;
  } catch { return null; }
}

export function useSearch() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; pattern: string; search_columns?: boolean }) =>
      api.post("/search", req),
  });
}

export function usePreflight() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; destination_catalog: string; warehouse_id?: string }) =>
      api.post("/preflight", req),
  });
}

export function usePiiScan() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; no_exit_code?: boolean }) =>
      api.post("/pii-scan", req),
  });
}

export function useSchemaDrift() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; destination_catalog: string }) =>
      api.post("/schema-drift", req),
  });
}

export function useSync() {
  return useMutation({
    mutationFn: (req: { source_catalog: string; destination_catalog: string; dry_run?: boolean; drop_extra?: boolean }) =>
      api.post("/sync", req),
  });
}

export function useColumnUsage() {
  return useMutation({
    mutationFn: (req: { catalog: string; table?: string; days?: number }) =>
      api.post("/column-usage", req),
  });
}

// ── FinOps Hooks (system tables, 10-min cache) ─────────────────────

const FINOPS_STALE = 600_000;   // 10 min — matches backend file cache TTL
const FINOPS_REFETCH = 600_000; // 10 min auto-refresh

export function useFinOpsConfig() {
  return useQuery<{ price_per_gb?: number; currency?: string }>({
    queryKey: ["finops-config"],
    queryFn: () => api.get("/config"),
    staleTime: FINOPS_STALE,
    retry: false,
    select: (cfg: any) => ({ price_per_gb: cfg?.price_per_gb ?? 0.023, currency: cfg?.currency ?? "USD" }),
  });
}

export function useBillingCost(days: number = 30) {
  return useQuery<any>({
    queryKey: ["finops-billing", days],
    queryFn: () => api.get(`/finops/billing?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useFinOpsWarehouses() {
  return useQuery<any>({
    queryKey: ["finops-warehouses"],
    queryFn: () => api.get("/finops/warehouses"),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useWarehouseEvents(days: number = 7) {
  return useQuery<any>({
    queryKey: ["finops-wh-events", days],
    queryFn: () => api.get(`/finops/warehouse-events?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useFinOpsClusters() {
  return useQuery<any>({
    queryKey: ["finops-clusters"],
    queryFn: () => api.get("/finops/clusters"),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useNodeUtilization(days: number = 7) {
  return useQuery<any>({
    queryKey: ["finops-node-util", days],
    queryFn: () => api.get(`/finops/node-utilization?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useFinOpsQueryStats(days: number = 30) {
  return useQuery<any>({
    queryKey: ["finops-query-stats", days],
    queryFn: () => api.get(`/finops/query-stats?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useFinOpsStorage(catalog: string) {
  return useQuery<any>({
    queryKey: ["finops-storage", catalog],
    queryFn: () => api.get(`/finops/storage?catalog=${catalog}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    enabled: !!catalog,
    retry: 1,
  });
}

export function useFinOpsRecommendations(catalog: string) {
  return useQuery<any>({
    queryKey: ["finops-recommendations", catalog],
    queryFn: () => api.get(`/finops/recommendations?catalog=${catalog}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    enabled: !!catalog,
    retry: 1,
  });
}

export function useSystemTableStatus() {
  return useQuery<any>({
    queryKey: ["finops-system-status"],
    queryFn: () => api.get("/finops/system-status"),
    staleTime: FINOPS_STALE,
    retry: false,
  });
}

// Cost Attribution
export function useQueryCosts(days: number = 30) {
  return useQuery<any>({
    queryKey: ["finops-query-costs", days],
    queryFn: () => api.get(`/finops/query-costs?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

export function useJobCosts(days: number = 30) {
  return useQuery<any>({
    queryKey: ["finops-job-costs", days],
    queryFn: () => api.get(`/finops/job-costs?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: 1,
  });
}

// Azure (supplementary — deferred)
export function useAzureCosts(days: number = 30) {
  return useQuery<any>({
    queryKey: ["finops-azure-costs", days],
    queryFn: () => api.get(`/finops/azure/costs?days=${days}`),
    staleTime: FINOPS_STALE,
    refetchInterval: FINOPS_REFETCH,
    retry: false,
  });
}
