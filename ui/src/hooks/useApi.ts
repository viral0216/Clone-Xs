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
    mutationFn: (req: { source_catalog: string; warehouse_id?: string }) =>
      api.post("/stats", req),
  });
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
