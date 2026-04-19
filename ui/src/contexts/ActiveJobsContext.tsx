/**
 * Global Active Jobs Context — polls all jobs in the background,
 * survives page navigation, sends notifications on completion.
 */
import { createContext, useContext, useState, useEffect, useRef, useCallback } from "react";
import { api } from "@/lib/api-client";

export interface Job {
  job_id: string;
  job_type: string;
  status: string;
  source_catalog: string | null;
  destination_catalog: string | null;
  clone_type: string | null;
  progress: any;
  result: any;
  error: string | null;
  run_url: string | null;
  logs: string[];
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
}

interface ActiveJobsContextValue {
  jobs: Job[];
  activeJobs: Job[];
  completedJobs: Job[];
  getJob: (id: string) => Job | undefined;
  refreshNow: () => void;
  activeCount: number;
}

const ActiveJobsContext = createContext<ActiveJobsContextValue>({
  jobs: [],
  activeJobs: [],
  completedJobs: [],
  getJob: () => undefined,
  refreshNow: () => {},
  activeCount: 0,
});

export function useActiveJobs() {
  return useContext(ActiveJobsContext);
}

const POLL_INTERVAL = 5000; // 5 seconds

export function ActiveJobsProvider({ children }: { children: React.ReactNode }) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const prevActiveIds = useRef<Set<string>>(new Set());
  const controllerRef = useRef<AbortController | null>(null);

  const fetchJobs = useCallback(async () => {
    // Abort any previous in-flight request
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;

    try {
      const data = await api.get<Job[]>("/clone/jobs", { signal: controller.signal });
      const jobList = Array.isArray(data) ? data : [];
      setJobs(jobList);

      // Detect newly completed jobs for notifications
      const currentActive = new Set(
        jobList.filter(j => j.status === "running" || j.status === "queued").map(j => j.job_id)
      );
      const prevActive = prevActiveIds.current;

      for (const id of prevActive) {
        if (!currentActive.has(id)) {
          // Job was active before, now it's not — it just completed/failed
          const job = jobList.find(j => j.job_id === id);
          if (job && document.hidden) {
            // Send browser notification if tab is not focused
            try {
              if (Notification.permission === "granted") {
                const failed = job.status === "failed";
                new Notification("Clone-Xs", {
                  body: failed
                    ? `Job ${job.job_id} failed: ${job.error || "Unknown error"}`
                    : `Job ${job.job_id} (${job.job_type || "clone"}) completed successfully`,
                  icon: "/favicon.svg",
                });
              }
            } catch { /* notifications not supported */ }
          }
        }
      }

      prevActiveIds.current = currentActive;
    } catch {
      // Ignore aborted/network errors
    }
  }, []);

  // Request notification permission on mount
  useEffect(() => {
    if (typeof Notification !== "undefined" && Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  }, []);

  // Poll every 5 seconds
  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, POLL_INTERVAL);
    return () => {
      clearInterval(interval);
      controllerRef.current?.abort();
    };
  }, [fetchJobs]);

  const activeJobs = jobs.filter(j => j.status === "running" || j.status === "queued");
  const completedJobs = jobs.filter(j => j.status !== "running" && j.status !== "queued");

  const getJob = useCallback((id: string) => jobs.find(j => j.job_id === id), [jobs]);

  return (
    <ActiveJobsContext.Provider value={{
      jobs,
      activeJobs,
      completedJobs,
      getJob,
      refreshNow: fetchJobs,
      activeCount: activeJobs.length,
    }}>
      {children}
    </ActiveJobsContext.Provider>
  );
}
