import { useCallback, useEffect, useRef } from "react";
import { api } from "@/lib/api-client";
import { useJobContext, type JobEntry } from "@/contexts/JobContext";

/**
 * Durable in-flight job tracker.
 *
 * Survives page navigation AND browser refresh. The hook reconnects to a
 * still-running server-side job on mount, polls until completion, and persists
 * progress (including a capped chart-history buffer) in JobContext.
 *
 * Why this exists:
 *   Pages with long-running jobs (clone, sync, demo-data, IaC, reconciliation)
 *   used to track the job_id in component-local useState. On unmount React
 *   dropped the state and cleared the polling interval — coming back showed a
 *   blank slate even though the job was still running on the server.
 *
 *   This hook moves all of that into JobContext (sessionStorage-backed) so
 *   navigation no longer destroys progress.
 *
 * Usage:
 *   const job = useDurableJob({
 *     key: "clone",
 *     pollUrl: (id) => `/clone/${id}`,
 *     pollInterval: 2000,
 *     isComplete: (data) => ["completed", "failed", "cancelled"].includes(data.status),
 *     captureProgress: (data) => data.progress,    // optional — for chart series
 *     historyCap: 200,                             // optional — default 200
 *     notificationTitle: "Clone-Xs",
 *   });
 *
 *   // Submit:
 *   await job.start({ source: "prod" }, async () => {
 *     const res = await api.post("/clone", body);
 *     return res.job_id;   // returning the id wires up polling
 *   });
 *
 *   // Read state:
 *   job.entry?.status        // "idle" | "loading" | "success" | "error"
 *   job.entry?.data          // server's job dict (status, progress, logs, result, …)
 *   job.entry?.progressHistory  // capped time-series of progress snapshots
 *   job.isRunning            // true while polling
 */
export interface UseDurableJobOptions {
  /** Unique key per page/section. Used as the JobContext key + sessionStorage suffix. */
  key: string;
  /** Build the GET URL for polling job status from the job_id. */
  pollUrl: (jobId: string) => string;
  /** Poll cadence in ms. Default 2000. */
  pollInterval?: number;
  /** Predicate: has the job finished? (terminal states) */
  isComplete: (data: any) => boolean;
  /**
   * Pull a progress snapshot off each poll response. Return null to skip the
   * snapshot. Snapshots are appended to JobEntry.progressHistory (capped).
   */
  captureProgress?: (data: any) => any;
  /**
   * Equality check between two snapshots — used to skip duplicate appends
   * (e.g. the server hasn't advanced since the last poll). Default uses JSON
   * equality on the snapshot.
   */
  isProgressEqual?: (a: any, b: any) => boolean;
  /** Cap for progressHistory ring buffer. Default 200. */
  historyCap?: number;
  /** Browser notification title shown when the tab is hidden on completion. */
  notificationTitle?: string;
  /**
   * Called once when the server reports a terminal state. The job dict is
   * passed in. Use this for toast notifications, side-effects, etc.
   * Failure path: `data.status === "failed"`.
   */
  onComplete?: (data: any) => void;
  /**
   * Called on every poll tick with the latest job dict. Useful for appending
   * to a parallel log buffer or other side effects that don't belong in
   * JobContext. Fires before the terminal-state check.
   */
  onProgress?: (data: any) => void;
}

export function useDurableJob({
  key,
  pollUrl,
  pollInterval = 2000,
  isComplete,
  captureProgress,
  isProgressEqual,
  historyCap = 200,
  notificationTitle = "Clone-Xs",
  onComplete,
  onProgress,
}: UseDurableJobOptions) {
  const ctx = useJobContext();
  const entry: JobEntry | null = ctx.getJob(key);

  const intervalRef = useRef<number | null>(null);
  const wasBackgrounded = useRef(false);
  const finishedRef = useRef(false);
  const onCompleteRef = useRef(onComplete);
  const onProgressRef = useRef(onProgress);
  const captureProgressRef = useRef(captureProgress);
  const isProgressEqualRef = useRef(isProgressEqual);
  const isCompleteRef = useRef(isComplete);
  const pollUrlRef = useRef(pollUrl);

  // Keep refs in sync — callbacks captured at first render would otherwise
  // close over stale state.
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  useEffect(() => { onProgressRef.current = onProgress; }, [onProgress]);
  useEffect(() => { captureProgressRef.current = captureProgress; }, [captureProgress]);
  useEffect(() => { isProgressEqualRef.current = isProgressEqual; }, [isProgressEqual]);
  useEffect(() => { isCompleteRef.current = isComplete; }, [isComplete]);
  useEffect(() => { pollUrlRef.current = pollUrl; }, [pollUrl]);

  const stopPolling = useCallback(() => {
    if (intervalRef.current != null) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  const poll = useCallback(async () => {
    const current = ctx.getJob(key);
    const jobId = current?.jobId;
    if (!jobId) return;

    try {
      const data = await api.get<any>(pollUrlRef.current(jobId));
      ctx.updateJob(key, { data });
      onProgressRef.current?.(data);

      const snap = captureProgressRef.current?.(data);
      if (snap != null) {
        const last = current?.progressHistory?.[current.progressHistory.length - 1];
        const eq = isProgressEqualRef.current
          ? isProgressEqualRef.current(last, snap)
          : JSON.stringify(last) === JSON.stringify(snap);
        if (!eq) ctx.appendProgress(key, snap, historyCap);
      }

      if (isCompleteRef.current(data)) {
        finishedRef.current = true;
        stopPolling();
        const failed = data.status === "failed";
        if (failed) {
          ctx.failJob(key, data.error || "Job failed");
        } else {
          ctx.completeJob(key, data);
        }
        // Browser notification when tab was backgrounded.
        if (wasBackgrounded.current
          && typeof Notification !== "undefined"
          && Notification.permission === "granted") {
          try {
            new Notification(notificationTitle, {
              body: failed
                ? `Job failed: ${data.error || "Unknown error"}`
                : `Job ${jobId} completed`,
              icon: "/favicon.svg",
            });
          } catch {
            // notifications not supported — ignore
          }
        }
        wasBackgrounded.current = false;
        onCompleteRef.current?.(data);
      }
    } catch {
      // Transient failure — keep polling, the next tick will retry. We never
      // tear down the loop on a single bad response, so a server bounce or
      // brief network blip doesn't kill the tracker.
    }
  }, [key, ctx, stopPolling, historyCap, notificationTitle]);

  /**
   * Submit a new job. `submit` should perform the POST and return the
   * server-issued job_id (string). Anything else returned is treated as a
   * synchronous result and the entry is marked success immediately.
   */
  const start = useCallback(
    async (params: Record<string, any>, submit: () => Promise<string | { job_id?: string } | any>) => {
      ctx.startJob(key, params);
      finishedRef.current = false;
      wasBackgrounded.current = false;
      try {
        const res = await submit();
        const jobId = typeof res === "string"
          ? res
          : (res?.job_id ?? null);
        if (jobId) {
          ctx.updateJob(key, { jobId });
          // Trigger an immediate poll — don't wait for the first interval tick.
          // The polling effect picks it up via the jobId update.
          return jobId as string;
        }
        // No job_id returned — treat the response itself as the result.
        ctx.completeJob(key, res);
        return null;
      } catch (e: any) {
        ctx.failJob(key, e?.message || "Submit failed");
        throw e;
      }
    },
    [key, ctx],
  );

  /**
   * Stop tracking the current job and clear local state. Does NOT cancel on
   * the server — pass a `serverStop` callback if you need that.
   */
  const stop = useCallback(
    async (serverStop?: () => Promise<void>) => {
      stopPolling();
      try {
        if (serverStop) await serverStop();
      } finally {
        ctx.clearJob(key);
      }
    },
    [key, ctx, stopPolling],
  );

  /** Wipe the entry entirely (idle the page). */
  const clear = useCallback(() => {
    stopPolling();
    ctx.clearJob(key);
  }, [key, ctx, stopPolling]);

  // Drive polling. The effect (re)starts whenever the tracked jobId changes.
  // On mount it sees the hydrated entry from sessionStorage — that's how
  // navigation-survival reconnects.
  const trackedJobId = entry?.jobId ?? null;
  const trackedStatus = entry?.status ?? "idle";
  useEffect(() => {
    if (!trackedJobId) return;
    if (trackedStatus !== "loading") return;
    finishedRef.current = false;
    // Immediate poll, then on cadence.
    poll();
    intervalRef.current = window.setInterval(poll, pollInterval) as unknown as number;
    return () => {
      if (intervalRef.current != null) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [trackedJobId, trackedStatus, pollInterval, poll]);

  // Tab visibility — poll immediately when the tab returns, and remember the
  // backgrounded state so we know whether to fire a desktop notification.
  useEffect(() => {
    if (!trackedJobId || trackedStatus !== "loading") return;
    function onVisibilityChange() {
      if (document.hidden) {
        wasBackgrounded.current = true;
      } else {
        poll();
      }
    }
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => document.removeEventListener("visibilitychange", onVisibilityChange);
  }, [trackedJobId, trackedStatus, poll]);

  // One-time notification permission request.
  useEffect(() => {
    if (typeof Notification !== "undefined" && Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  }, []);

  return {
    /** Full JobEntry from JobContext (status / params / data / error / jobId / progressHistory). */
    entry,
    /** Server-side job_id once `start()` has resolved. */
    jobId: trackedJobId,
    /** Latest server response from polling — entry.data shorthand. */
    data: entry?.data ?? null,
    /** Capped progress snapshots — for streaming/throughput charts. */
    progressHistory: entry?.progressHistory ?? [],
    /** True while the job is in flight (loading status). */
    isRunning: trackedStatus === "loading",
    /** True when the job completed successfully. */
    isSuccess: trackedStatus === "success",
    /** True when the job failed. */
    isError: trackedStatus === "error",
    /** Submit a new job — see docs above. */
    start,
    /** Stop tracking. Optionally invoke a server-side stop endpoint first. */
    stop,
    /** Clear the entry entirely. */
    clear,
  };
}
