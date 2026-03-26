/**
 * Hooks for AI-powered features — summaries, clone builder, DQ suggestions.
 */

import { useQuery, useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api-client";

interface AISummaryResponse {
  summary: string;
  available: boolean;
  reason?: string;
}

interface AIStatusResponse {
  available: boolean;
  reason?: string;
  model?: string;
}

interface CloneBuilderResponse {
  config: Record<string, unknown>;
  explanation: string;
  available: boolean;
  reason?: string;
}

interface DQSuggestionResponse {
  suggestions: Record<string, unknown>[];
  available: boolean;
  reason?: string;
}

interface PIIRemediationResponse {
  recommendations: Record<string, unknown>[];
  summary: string;
  available: boolean;
  reason?: string;
}

/** Check if AI features are available. */
export function useAiStatus() {
  return useQuery<AIStatusResponse>({
    queryKey: ["ai-status"],
    queryFn: () => api.get<AIStatusResponse>("/ai/status"),
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: false,
  });
}

/** Generate an AI narrative summary. */
export function useAiSummary() {
  return useMutation<AISummaryResponse, Error, { contextType: string; data: Record<string, unknown> }>({
    mutationFn: ({ contextType, data }) =>
      api.post<AISummaryResponse>("/ai/summarize", {
        context_type: contextType,
        data,
      }),
  });
}

/** Parse natural language into a clone config. */
export function useCloneBuilder() {
  return useMutation<CloneBuilderResponse, Error, { query: string; availableCatalogs?: string[] }>({
    mutationFn: ({ query, availableCatalogs }) =>
      api.post<CloneBuilderResponse>("/ai/clone-builder", {
        query,
        available_catalogs: availableCatalogs ?? [],
      }),
  });
}

/** Get DQ rule suggestions from profiling results. */
export function useDqSuggestions() {
  return useMutation<DQSuggestionResponse, Error, { profilingResults: Record<string, unknown>; tableName?: string }>({
    mutationFn: ({ profilingResults, tableName }) =>
      api.post<DQSuggestionResponse>("/ai/dq-suggestions", {
        profiling_results: profilingResults,
        table_name: tableName ?? "",
      }),
  });
}

/** Get PII remediation recommendations. */
export function usePiiRemediation() {
  return useMutation<PIIRemediationResponse, Error, { scanResults: Record<string, unknown> }>({
    mutationFn: ({ scanResults }) =>
      api.post<PIIRemediationResponse>("/ai/pii-remediation", {
        scan_results: scanResults,
      }),
  });
}
