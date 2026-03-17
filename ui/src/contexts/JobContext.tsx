// @ts-nocheck
import { createContext, useContext, useState, useCallback } from "react";

/**
 * Global job state that persists across page navigation.
 *
 * Each page stores its results under a unique key (e.g. "pii", "preflight", "diff").
 * When a user navigates away and comes back, the page reads from this context
 * instead of starting fresh.
 */

export interface JobEntry {
  /** Current status: idle, loading, success, error */
  status: "idle" | "loading" | "success" | "error";
  /** Input parameters used (so the page can restore form state) */
  params: Record<string, any>;
  /** Result data from the API */
  data: any;
  /** Error message if failed */
  error: string | null;
  /** Timestamp when the job started */
  startedAt: string | null;
  /** Timestamp when the job completed */
  completedAt: string | null;
}

interface JobContextValue {
  /** Get the current job state for a page */
  getJob: (key: string) => JobEntry | null;
  /** Set a job as loading (clears previous result) */
  startJob: (key: string, params: Record<string, any>) => void;
  /** Set a job as completed with data */
  completeJob: (key: string, data: any) => void;
  /** Set a job as failed with error */
  failJob: (key: string, error: string) => void;
  /** Clear a job's state */
  clearJob: (key: string) => void;
  /** Check if a job is currently loading */
  isLoading: (key: string) => boolean;
}

const JobContext = createContext<JobContextValue | null>(null);

export function JobProvider({ children }: { children: React.ReactNode }) {
  const [jobs, setJobs] = useState<Record<string, JobEntry>>({});

  const getJob = useCallback(
    (key: string) => jobs[key] || null,
    [jobs],
  );

  const startJob = useCallback((key: string, params: Record<string, any>) => {
    setJobs((prev) => ({
      ...prev,
      [key]: {
        status: "loading",
        params,
        data: null,
        error: null,
        startedAt: new Date().toISOString(),
        completedAt: null,
      },
    }));
  }, []);

  const completeJob = useCallback((key: string, data: any) => {
    setJobs((prev) => ({
      ...prev,
      [key]: {
        ...prev[key],
        status: "success",
        data,
        error: null,
        completedAt: new Date().toISOString(),
      },
    }));
  }, []);

  const failJob = useCallback((key: string, error: string) => {
    setJobs((prev) => ({
      ...prev,
      [key]: {
        ...prev[key],
        status: "error",
        error,
        completedAt: new Date().toISOString(),
      },
    }));
  }, []);

  const clearJob = useCallback((key: string) => {
    setJobs((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }, []);

  const isLoading = useCallback(
    (key: string) => jobs[key]?.status === "loading",
    [jobs],
  );

  return (
    <JobContext.Provider value={{ getJob, startJob, completeJob, failJob, clearJob, isLoading }}>
      {children}
    </JobContext.Provider>
  );
}

export function useJobContext() {
  const ctx = useContext(JobContext);
  if (!ctx) throw new Error("useJobContext must be used within a JobProvider");
  return ctx;
}

/**
 * Hook for pages to use persistent job state.
 *
 * Usage:
 *   const { job, run, clear } = usePageJob("pii");
 *
 *   // Run a scan
 *   run({ sourceCatalog: "prod" }, async () => {
 *     return await api.post("/pii-scan", { source_catalog: "prod" });
 *   });
 *
 *   // Access persisted results
 *   if (job?.status === "success") { ... job.data ... }
 */
export function usePageJob(key: string) {
  const { getJob, startJob, completeJob, failJob, clearJob, isLoading } = useJobContext();

  const rawJob = getJob(key);

  // Auto-clear stale "loading" jobs (stuck for > 5 minutes)
  const job = rawJob?.status === "loading" && rawJob.startedAt
    ? (() => {
        const elapsed = Date.now() - new Date(rawJob.startedAt).getTime();
        if (elapsed > 5 * 60 * 1000) {
          // Stale — treat as idle (don't show spinner)
          return null;
        }
        return rawJob;
      })()
    : rawJob;

  const run = useCallback(
    async (params: Record<string, any>, fn: () => Promise<any>) => {
      startJob(key, params);
      try {
        const result = await fn();
        completeJob(key, result);
        return result;
      } catch (e: any) {
        failJob(key, e.message || "Operation failed");
        throw e;
      }
    },
    [key, startJob, completeJob, failJob],
  );

  const clear = useCallback(() => clearJob(key), [key, clearJob]);

  return {
    job,
    run,
    clear,
    isRunning: job?.status === "loading",
  };
}
