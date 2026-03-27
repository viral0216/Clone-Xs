import { useState, useEffect, useRef, useCallback } from "react";
import { api } from "@/lib/api-client";

/**
 * Hook for polling background jobs with tab-visibility awareness.
 *
 * Features:
 * - Polls at `interval` when tab is active
 * - Immediately polls when tab returns to foreground (visibilitychange)
 * - Fires browser Notification on completion if tab was backgrounded
 * - Stores active job_id in sessionStorage for page reconnection
 * - Calls onComplete/onError callbacks
 *
 * Usage:
 *   const { jobData, isPolling, startPolling, stopPolling } = useBackgroundJob({
 *     storageKey: "batch-recon-job",
 *     pollUrl: (id) => `/reconciliation/batch-validate/${id}`,
 *     interval: 2000,
 *     isComplete: (data) => data.status === "completed" || data.status === "failed",
 *     onComplete: (data) => { setResults(data.result); },
 *   });
 */

interface UseBackgroundJobOptions {
  storageKey: string;
  pollUrl: (jobId: string) => string;
  interval?: number;
  isComplete: (data: any) => boolean;
  onComplete?: (data: any) => void;
  onError?: (error: string) => void;
  onProgress?: (data: any) => void;
  notificationTitle?: string;
}

export function useBackgroundJob({
  storageKey,
  pollUrl,
  interval = 2000,
  isComplete,
  onComplete,
  onError,
  onProgress,
  notificationTitle = "Clone-Xs",
}: UseBackgroundJobOptions) {
  const [jobId, setJobId] = useState<string | null>(() => {
    try { return sessionStorage.getItem(`bg_job_${storageKey}`); } catch { return null; }
  });
  const [jobData, setJobData] = useState<any>(null);
  const [isPolling, setIsPolling] = useState(false);
  const intervalRef = useRef<number | null>(null);
  const wasBackgrounded = useRef(false);
  const jobIdRef = useRef(jobId);

  // Keep ref in sync
  useEffect(() => { jobIdRef.current = jobId; }, [jobId]);

  // Persist job_id to sessionStorage
  useEffect(() => {
    try {
      if (jobId) sessionStorage.setItem(`bg_job_${storageKey}`, jobId);
      else sessionStorage.removeItem(`bg_job_${storageKey}`);
    } catch {}
  }, [jobId, storageKey]);

  const poll = useCallback(async () => {
    const id = jobIdRef.current;
    if (!id) return;
    try {
      const data = await api.get(pollUrl(id));
      setJobData(data);
      onProgress?.(data);

      if (isComplete(data)) {
        stopPolling();
        const failed = data.status === "failed";
        if (failed) {
          onError?.(data.error || "Job failed");
        } else {
          onComplete?.(data);
        }
        // Browser notification if tab was backgrounded
        if (wasBackgrounded.current && typeof Notification !== "undefined" && Notification.permission === "granted") {
          new Notification(notificationTitle, {
            body: failed ? `Job failed: ${data.error || "Unknown error"}` : "Job completed successfully",
            icon: "/favicon.ico",
          });
        }
        wasBackgrounded.current = false;
      }
    } catch {
      // Network error — keep polling, will retry next interval
    }
  }, [pollUrl, isComplete, onComplete, onError, onProgress, notificationTitle]);

  const startPolling = useCallback((id: string) => {
    setJobId(id);
    setIsPolling(true);
    setJobData(null);
    wasBackgrounded.current = false;
  }, []);

  const stopPolling = useCallback(() => {
    setIsPolling(false);
    setJobId(null);
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  // Set up polling interval
  useEffect(() => {
    if (!isPolling || !jobId) return;

    // Initial poll
    poll();

    intervalRef.current = window.setInterval(poll, interval);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [isPolling, jobId, interval, poll]);

  // Visibility change — poll immediately when tab comes back
  useEffect(() => {
    if (!isPolling) return;

    function onVisibilityChange() {
      if (document.hidden) {
        wasBackgrounded.current = true;
      } else {
        // Tab came back — poll immediately
        poll();
      }
    }

    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => document.removeEventListener("visibilitychange", onVisibilityChange);
  }, [isPolling, poll]);

  // Request notification permission on mount
  useEffect(() => {
    if (typeof Notification !== "undefined" && Notification.permission === "default") {
      Notification.requestPermission();
    }
  }, []);

  // Reconnect to existing job on mount (from sessionStorage)
  useEffect(() => {
    if (jobId && !isPolling) {
      setIsPolling(true);
    }
  }, []);

  return {
    jobId,
    jobData,
    isPolling,
    startPolling,
    stopPolling,
  };
}
