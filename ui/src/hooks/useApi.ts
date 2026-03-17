import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api-client";
import type { AuthStatus, WarehouseInfo, CloneJob } from "@/types/api";

export function useAuthStatus() {
  return useQuery<AuthStatus>({
    queryKey: ["auth-status"],
    queryFn: () => api.get("/auth/status"),
    retry: false,
  });
}

export function useWarehouses() {
  return useQuery<WarehouseInfo[]>({
    queryKey: ["warehouses"],
    queryFn: () => api.get("/auth/warehouses"),
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
    refetchInterval: 30000,
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
  return useQuery<NotificationsData>({
    queryKey: ["notifications"],
    queryFn: () => api.get("/notifications"),
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
    refetchInterval: 60000,
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
