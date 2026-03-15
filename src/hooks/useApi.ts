"use client";

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

export function useCloneJobs() {
  return useQuery<CloneJob[]>({
    queryKey: ["clone-jobs"],
    queryFn: () => api.get("/clone/jobs"),
    refetchInterval: 5000,
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
