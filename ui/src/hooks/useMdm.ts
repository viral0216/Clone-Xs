import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api-client";

// ---- Queries ----

export function useMdmDashboard() {
  return useQuery({ queryKey: ["mdm", "dashboard"], queryFn: () => api.get("/mdm/dashboard"), retry: 1 });
}

export function useMdmEntities(entityType?: string, status?: string) {
  const params = new URLSearchParams();
  if (entityType) params.set("entity_type", entityType);
  if (status) params.set("status", status);
  const qs = params.toString();
  return useQuery({ queryKey: ["mdm", "entities", entityType, status], queryFn: () => api.get(`/mdm/entities${qs ? `?${qs}` : ""}`), retry: 1 });
}

export function useMdmEntity(entityId: string) {
  return useQuery({ queryKey: ["mdm", "entity", entityId], queryFn: () => api.get(`/mdm/entities/${entityId}`), enabled: !!entityId, retry: 1 });
}

export function useMdmPairs(entityType?: string, status?: string) {
  const params = new URLSearchParams();
  if (entityType) params.set("entity_type", entityType);
  if (status) params.set("status", status);
  const qs = params.toString();
  return useQuery({ queryKey: ["mdm", "pairs", entityType, status], queryFn: () => api.get(`/mdm/pairs${qs ? `?${qs}` : ""}`), retry: 1 });
}

export function useMdmRules(entityType?: string) {
  const qs = entityType ? `?entity_type=${entityType}` : "";
  return useQuery({ queryKey: ["mdm", "rules", entityType], queryFn: () => api.get(`/mdm/rules${qs}`), retry: 1 });
}

export function useMdmStewardship(status?: string, priority?: string) {
  const params = new URLSearchParams();
  if (status) params.set("status", status);
  if (priority) params.set("priority", priority);
  const qs = params.toString();
  return useQuery({ queryKey: ["mdm", "stewardship", status, priority], queryFn: () => api.get(`/mdm/stewardship${qs ? `?${qs}` : ""}`), retry: 1 });
}

export function useMdmHierarchies() {
  return useQuery({ queryKey: ["mdm", "hierarchies"], queryFn: () => api.get("/mdm/hierarchies"), retry: 1 });
}

export function useMdmHierarchy(hierarchyId: string) {
  return useQuery({ queryKey: ["mdm", "hierarchy", hierarchyId], queryFn: () => api.get(`/mdm/hierarchies/${hierarchyId}`), enabled: !!hierarchyId, retry: 1 });
}

// ---- Mutations ----

export function useInitMdm() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: () => api.post("/mdm/init", {}), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm"] }) });
}

export function useIngestSource() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { catalog: string; schema_name: string; table: string; entity_type: string; key_column: string; trust_score?: number }) => api.post("/mdm/ingest", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm"] }) });
}

export function useDetectDuplicates() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { entity_type: string; auto_merge_threshold?: number; review_threshold?: number }) => api.post("/mdm/detect", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm"] }) });
}

export function useMergeRecords() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { pair_id: string; strategy?: string }) => api.post("/mdm/merge", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm"] }) });
}

export function useSplitRecord() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { entity_id: string }) => api.post("/mdm/split", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm"] }) });
}

export function useCreateRule() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { entity_type: string; name: string; field: string; match_type: string; weight?: number; threshold?: number; enabled?: boolean }) => api.post("/mdm/rules", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "rules"] }) });
}

export function useDeleteRule() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (ruleId: string) => api.delete(`/mdm/rules/${ruleId}`), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "rules"] }) });
}

export function useApproveTask() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (taskId: string) => api.post(`/mdm/stewardship/${taskId}/approve`, {}), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "stewardship"] }) });
}

export function useRejectTask() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: ({ taskId, reason }: { taskId: string; reason?: string }) => api.post(`/mdm/stewardship/${taskId}/reject`, { reason: reason || "" }), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "stewardship"] }) });
}

export function useCreateHierarchy() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { name: string; entity_type: string }) => api.post("/mdm/hierarchies", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "hierarchies"] }) });
}

export function useCreateEntity() {
  const qc = useQueryClient();
  return useMutation({ mutationFn: (data: { entity_type: string; display_name: string; attributes?: Record<string, string> }) => api.post("/mdm/entities", data), onSuccess: () => qc.invalidateQueries({ queryKey: ["mdm", "entities"] }) });
}
