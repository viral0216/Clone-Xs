// @ts-nocheck
import { useState, useEffect, useRef } from "react";
// Polling-based progress — no WebSocket needed
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useStartClone, useVolumes, useTargetConnections, useTargetCatalogs, useTestTargetConnection, useSnapshots } from "@/hooks/useApi";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import { api } from "@/lib/api-client";
import { useFavorites } from "@/hooks/useFavorites";
import { useActiveJobs } from "@/contexts/ActiveJobsContext";
import {
  Copy, Play, Eye, CheckCircle, XCircle, Loader2,
  ArrowRight, Clock, AlertCircle, Download, ClipboardCopy, Check, ExternalLink,
  Star, Plus, X,
} from "lucide-react";
import StatusBadge from "@/components/StatusBadge";
import LoadingState from "@/components/LoadingState";
import ErrorBoundary from "@/components/ErrorBoundary";
import ScopePicker, { ObjectRef, ScopeMode } from "@/components/ScopePicker";
import PreviewPanel from "@/components/PreviewPanel";
import FieldLabel, { FieldLabelSmall, InfoDot } from "@/components/FieldLabel";
import { toast } from "sonner";

const TTL_PATTERN = /^\d+[hdw]$/;

type Step = "source" | "options" | "preview" | "execute";

function ProgressBar({ value, max, label }: { value: number; max: number; label?: string }) {
  const pct = max > 0 ? Math.round((value / max) * 100) : 0;
  return (
    <div className="space-y-1">
      {label && (
        <div className="flex justify-between text-xs text-gray-500">
          <span>{label}</span>
          <span>{value}/{max} ({pct}%)</span>
        </div>
      )}
      <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500 ease-out bg-[#E8453C]"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

// Cross-workspace clone returns a flat shape (tables_cloned, views_migrated,
// schemas_created, ...). New backends emit canonical aliases too, but older
// completed jobs persisted before that change still have only the flat fields,
// so map them here as a fallback.
//
// Fanout results (`mode: "fanout"`) wrap N per-target cross-workspace results
// in a `per_target` list with rolled-up totals at the top level. Same
// flat-field naming as cross-workspace, so the fallback mapping works.
function normalizeResult(result: any): any {
  if (!result) return result;
  const isFanout = result.mode === "fanout";
  const isCrossWorkspace =
    isFanout || (result.tables_total !== undefined && result.tables === undefined);
  if (!isCrossWorkspace) return result;
  return {
    ...result,
    schemas_processed: result.schemas_processed ?? result.schemas_created ?? 0,
    tables: result.tables ?? {
      success: result.tables_cloned || 0,
      failed: result.tables_failed || 0,
      skipped: result.tables_skipped || 0,
    },
    views: result.views ?? {
      success: result.views_migrated || 0,
      failed: result.views_failed || 0,
      skipped: 0,
    },
    functions: result.functions ?? {
      success: result.functions_migrated || 0,
      failed: result.functions_failed || 0,
      skipped: 0,
    },
    volumes: result.volumes ?? {
      success: result.volumes_migrated || 0,
      failed: result.volumes_failed || 0,
      skipped: 0,
    },
  };
}

function downloadFile(content: string, filename: string, type = "application/json") {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// Render a byte count with the largest sensible unit. Used by the clone
// metrics row (Databricks reports `copied_files_size` in bytes; teams want
// to see GB / MB).
function formatBytes(n: number): string {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

function generateReport(job: any): string {
  const result = normalizeResult(job.result);
  const v = result?.validation;
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const lines = [
    `# Clone Report`,
    ``,
    `**Generated:** ${new Date().toLocaleString()}`,
    `**Job ID:** ${job.job_id}`,
    `**Status:** ${job.status}`,
    ``,
    `## Clone Details`,
    `| Field | Value |`,
    `|-------|-------|`,
    `| Source | ${job.source_catalog} |`,
    `| Destination | ${job.destination_catalog} |`,
    `| Clone Type | ${job.clone_type} |`,
    `| Started | ${job.started_at ? new Date(job.started_at).toLocaleString() : "—"} |`,
    `| Completed | ${job.completed_at ? new Date(job.completed_at).toLocaleString() : "—"} |`,
    `| Duration | ${result?.duration_seconds ? result.duration_seconds.toFixed(1) + "s" : "—"} |`,
    ``,
    `## Objects Processed`,
    `| Type | Success | Failed | Skipped |`,
    `|------|---------|--------|---------|`,
  ];

  for (const key of ["tables", "views", "functions", "volumes"]) {
    const d = result?.[key];
    if (d) {
      lines.push(`| ${key} | ${d.success || 0} | ${d.failed || 0} | ${d.skipped || 0} |`);
    }
  }

  lines.push(`| schemas | ${result?.schemas_processed || 0} | — | — |`);

  if (v) {
    lines.push(``);
    lines.push(`## Validation Summary`);
    lines.push(`| Metric | Count |`);
    lines.push(`|--------|-------|`);
    lines.push(`| Total Tables | ${v.total_tables} |`);
    lines.push(`| Matched | ${v.matched} |`);
    lines.push(`| Mismatched | ${v.mismatched} |`);
    lines.push(`| Errors | ${v.errors} |`);

    if (v.details && v.details.length > 0) {
      lines.push(``);
      lines.push(`## Table Details`);
      lines.push(`| Status | Schema | Table | Source Rows | Dest Rows |`);
      lines.push(`|--------|--------|-------|------------|-----------|`);
      for (const row of v.details) {
        const status = row.match ? "✅" : row.error ? "⚠️" : "❌";
        lines.push(`| ${status} | ${row.schema} | ${row.table} | ${row.source_count?.toLocaleString() ?? "—"} | ${row.dest_count?.toLocaleString() ?? "—"} |`);
      }
    }

    if (v.mismatched_tables && v.mismatched_tables.length > 0) {
      lines.push(``);
      lines.push(`## Mismatched Tables`);
      lines.push(`| Schema | Table | Source | Dest | Diff |`);
      lines.push(`|--------|-------|--------|------|------|`);
      for (const m of v.mismatched_tables) {
        const diff = m.source_count != null && m.dest_count != null ? m.source_count - m.dest_count : "—";
        lines.push(`| ${m.schema} | ${m.table} | ${m.source_count?.toLocaleString() ?? "—"} | ${m.dest_count?.toLocaleString() ?? "—"} | ${diff.toLocaleString?.()} |`);
      }
    }
  }

  if (result?.errors && result.errors.length > 0) {
    lines.push(``);
    lines.push(`## Warnings`);
    for (const err of result.errors) {
      lines.push(`- ${err}`);
    }
  }

  if (job.logs && job.logs.length > 0) {
    lines.push(``);
    lines.push(`## Logs`);
    lines.push("```");
    for (const log of job.logs) {
      lines.push(log);
    }
    lines.push("```");
  }

  return lines.join("\n");
}

type LogFilter = "all" | "errors" | "warnings" | "info";

function groupLogs(logs: string[]): { line: string; count: number }[] {
  const grouped: { line: string; count: number }[] = [];
  for (const line of logs) {
    // Normalize: strip object names to group similar permission/tag errors
    const normalized = line
      .replace(/for \S+:/, "for <object>:")
      .replace(/on \S+:/, "on <object>:");
    const prev = grouped[grouped.length - 1];
    if (prev) {
      const prevNorm = prev.line
        .replace(/for \S+:/, "for <object>:")
        .replace(/on \S+:/, "on <object>:");
      if (prevNorm === normalized) {
        prev.count++;
        prev.line = line; // keep the latest actual line
        continue;
      }
    }
    grouped.push({ line, count: 1 });
  }
  return grouped;
}

function getLogClass(line: string): string {
  if (line.includes("ERROR")) return "text-red-400";
  if (line.includes("WARNING")) return "text-gray-400";
  if (line.includes("OK") || line.includes("completed") || line.includes("success")) return "text-gray-300";
  if (line.includes("Scanning") || line.includes("Cloning") || line.includes("Validating") || line.includes("Schemas")) return "text-[#E8453C]";
  return "";
}

function LogPanel({ logs, jobId, isRunning }: { logs: string[]; jobId: string; isRunning: boolean }) {
  const [copied, setCopied] = useState(false);
  const [filter, setFilter] = useState<LogFilter>("all");
  const [expanded, setExpanded] = useState(false);

  const logText = logs.join("\n");

  const handleCopy = async () => {
    await navigator.clipboard.writeText(logText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    downloadFile(logText, `clone-logs-${jobId}.log`, "text/plain");
  };

  const filteredLogs = logs.filter((line) => {
    if (filter === "all") return true;
    if (filter === "errors") return line.includes("ERROR");
    if (filter === "warnings") return line.includes("WARNING") || line.includes("ERROR");
    return !line.includes("WARNING") && !line.includes("ERROR");
  });

  const errorCount = logs.filter((l) => l.includes("ERROR")).length;
  const warnCount = logs.filter((l) => l.includes("WARNING")).length;
  const grouped = groupLogs(filteredLogs);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center justify-between">
          <span className="flex items-center gap-2">
            {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
            Logs
          </span>
          <div className="flex items-center gap-2">
            {errorCount > 0 && (
              <Badge
                variant={filter === "errors" ? "default" : "outline"}
                className="text-xs cursor-pointer text-red-500 border-red-500/30"
                onClick={() => setFilter(filter === "errors" ? "all" : "errors")}
              >
                {errorCount} errors
              </Badge>
            )}
            {warnCount > 0 && (
              <Badge
                variant={filter === "warnings" ? "default" : "outline"}
                className="text-xs cursor-pointer text-muted-foreground border-border/30"
                onClick={() => setFilter(filter === "warnings" ? "all" : "warnings")}
              >
                {warnCount} warnings
              </Badge>
            )}
            <Badge variant="outline" className="text-xs">{logs.length} lines</Badge>
            <Button variant="ghost" size="sm" className="h-7 px-2" onClick={() => setExpanded(!expanded)}>
              <span className="text-xs">{expanded ? "Collapse" : "Expand"}</span>
            </Button>
            <Button variant="ghost" size="sm" className="h-7 px-2" onClick={handleCopy}>
              {copied ? <Check className="h-3 w-3 text-foreground" /> : <ClipboardCopy className="h-3 w-3" />}
              <span className="ml-1 text-xs">{copied ? "Copied" : "Copy"}</span>
            </Button>
            <Button variant="ghost" size="sm" className="h-7 px-2" onClick={handleDownload}>
              <Download className="h-3 w-3" />
              <span className="ml-1 text-xs">Download</span>
            </Button>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div
          className={`bg-gray-900 text-gray-300 p-3 rounded-lg font-mono text-xs overflow-y-auto ${expanded ? "max-h-[600px]" : "max-h-72"}`}
          ref={(el) => { if (el && isRunning) el.scrollTop = el.scrollHeight; }}
        >
          {grouped.map((entry, i: number) => (
            <div key={i} className={`${getLogClass(entry.line)} leading-5`}>
              {entry.line}
              {entry.count > 1 && (
                <span className="ml-2 text-gray-500 text-[10px]">
                  (repeated {entry.count}x)
                </span>
              )}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function JobProgress({ jobId }: { jobId: string }) {
  const { getJob } = useActiveJobs();
  const [job, setJob] = useState<any>(null);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  // Use global context data as baseline, poll faster (2s) for active view
  useEffect(() => {
    const contextJob = getJob(jobId);
    if (contextJob) setJob(contextJob);
  }, [getJob, jobId]);

  useEffect(() => {
    const controller = new AbortController();
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${jobId}`, { signal: controller.signal });
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch (e: any) {
        if (e?.name === "AbortError") return; // Expected on unmount
        // Transient poll failures are expected during network blips; next tick retries.
      }
    };

    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => { controller.abort(); if (pollRef.current) clearInterval(pollRef.current); };
  }, [jobId]);


  if (!job) {
    return <LoadingState message="Loading job status..." />;
  }

  const statusIcon = {
    queued: <Clock className="h-5 w-5 text-muted-foreground" />,
    running: <Loader2 className="h-5 w-5 text-[#E8453C] animate-spin" />,
    completed: <CheckCircle className="h-5 w-5 text-foreground" />,
    failed: <XCircle className="h-5 w-5 text-red-600" />,
    cancelled: <AlertCircle className="h-5 w-5 text-gray-600" />,
  }[job.status];

  return (
    <div className="space-y-4">
      {/* Status Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {statusIcon}
          <div>
            <p className="font-semibold text-lg">
              {job.source_catalog} <ArrowRight className="inline h-4 w-4 mx-1" /> {job.destination_catalog}
            </p>
            <p className="text-sm text-gray-500">
              Job {jobId} &middot; {job.clone_type}
              {job.run_url && (
                <a
                  href={job.run_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 ml-2 text-[#E8453C] hover:text-[#E8453C] hover:underline"
                >
                  <ExternalLink className="h-3 w-3" />
                  View in Databricks
                </a>
              )}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {job.status === "completed" && (
            <>
              <Button
                variant="outline"
                size="sm"
                onClick={() => downloadFile(generateReport(job), `clone-report-${jobId}.md`, "text/markdown")}
              >
                <Download className="h-3 w-3 mr-1" />
                Report
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => downloadFile(JSON.stringify(job, null, 2), `clone-result-${jobId}.json`)}
              >
                <Download className="h-3 w-3 mr-1" />
                JSON
              </Button>
            </>
          )}
          <StatusBadge status={job.status ?? "unknown"} />
        </div>
      </div>

      {/* Progress */}
      {job.status === "running" && (
        <div className="space-y-3">
          {job.progress ? (
            <>
              <ProgressBar
                value={job.progress.completed_tables || 0}
                max={job.progress.total_tables || 0}
                label="Tables"
              />
              <ProgressBar
                value={job.progress.completed_schemas || 0}
                max={job.progress.total_schemas || 0}
                label="Schemas"
              />
              {job.progress.current_table && (
                <p className="text-xs text-gray-500">
                  Current: {job.progress.current_schema}.{job.progress.current_table}
                </p>
              )}
            </>
          ) : (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <Loader2 className="h-4 w-4 animate-spin" />
                Clone in progress...
              </div>
              <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                <div className="h-full bg-[#E8453C] rounded-full animate-pulse" style={{ width: "60%" }} />
              </div>
            </div>
          )}
        </div>
      )}

      {/* Timing */}
      <div className="flex gap-6 text-xs text-gray-500">
        {job.started_at && (
          <span>Started: {new Date(job.started_at).toLocaleTimeString()}</span>
        )}
        {job.completed_at && (
          <span>Completed: {new Date(job.completed_at).toLocaleTimeString()}</span>
        )}
        {job.started_at && job.completed_at && (
          <span>
            Duration: {Math.round((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 1000)}s
          </span>
        )}
      </div>

      {/* Result */}
      {job.status === "completed" && job.result && (() => {
        const result = normalizeResult(job.result);
        return (
        <div className="space-y-4">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <Card>
              <CardContent className="pt-4 text-center">
                <p className="text-2xl font-bold text-[#E8453C]">{result.schemas_processed || 0}</p>
                <p className="text-xs text-gray-500">Schemas Processed</p>
              </CardContent>
            </Card>
            {["tables", "views", "functions", "volumes"].map((key) => {
              const d = result[key];
              if (!d) return null;
              const total = (d.success || 0) + (d.failed || 0) + (d.skipped || 0);
              return (
                <Card key={key}>
                  <CardContent className="pt-4 text-center">
                    <p className="text-2xl font-bold text-foreground">{d.success || 0}</p>
                    <p className="text-xs text-gray-500">{key.charAt(0).toUpperCase() + key.slice(1)}</p>
                    {(d.failed > 0 || d.skipped > 0) && (
                      <div className="flex justify-center gap-1 mt-1">
                        {d.failed > 0 && <Badge variant="destructive" className="text-xs px-1">{d.failed} failed</Badge>}
                        {d.skipped > 0 && <Badge variant="outline" className="text-xs px-1">{d.skipped} skipped</Badge>}
                      </div>
                    )}
                  </CardContent>
                </Card>
              );
            })}
          </div>

          {/* Per-target rollup for fanout runs (mode === "fanout"). Shows
              one row per target with its status, bytes/tables, and any
              error so users can see at a glance which target(s) failed. */}
          {result.mode === "fanout" && Array.isArray(result.per_target) && (
            <div className="border rounded-md p-3">
              <div className="flex items-center gap-2 mb-2">
                <div className="text-sm font-medium">
                  Fanout: {result.succeeded_targets ?? 0} of {result.target_count ?? 0} targets succeeded
                </div>
                {result.status && (
                  <Badge
                    variant="outline"
                    className={
                      result.status === "success"
                        ? "border-emerald-500 text-emerald-700 dark:text-emerald-400 text-[10px]"
                        : result.status === "partial"
                        ? "border-amber-500 text-amber-700 dark:text-amber-400 text-[10px]"
                        : "border-red-500 text-red-700 dark:text-red-400 text-[10px]"
                    }
                  >
                    {String(result.status).toUpperCase()}
                  </Badge>
                )}
              </div>
              <div className="space-y-1.5">
                {result.per_target.map((t: any, idx: number) => (
                  <div
                    key={t.target_host || idx}
                    className="flex items-start gap-2 bg-muted/30 rounded p-2"
                  >
                    {t.target_status === "success" ? (
                      <CheckCircle className="h-4 w-4 text-emerald-600 mt-0.5 flex-shrink-0" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-600 mt-0.5 flex-shrink-0" />
                    )}
                    <div className="min-w-0 flex-1">
                      <div className="text-xs font-mono truncate">{t.target_host}</div>
                      <div className="text-[11px] text-muted-foreground">
                        {t.target_status === "success"
                          ? `${t.tables_cloned ?? 0} tables · ${formatBytes(t.bytes_copied || 0)} · ${(t.target_duration_seconds ?? 0).toFixed(1)}s`
                          : t.error || "failed"}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Source format mix — same CLONE syntax handles Delta, Parquet, and
              Iceberg sources registered in UC. Surface the breakdown so the
              user knows what they migrated (especially useful for catalogs
              that mix formats during a migration). */}
          {result.formats && Object.keys(result.formats).length > 1 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm text-muted-foreground">Source formats:</span>
              {Object.entries(result.formats as Record<string, number>)
                .sort((a, b) => (b[1] as number) - (a[1] as number))
                .map(([fmt, count]) => (
                  <Badge key={fmt} variant="outline" className="text-xs">
                    {fmt}: {count}
                  </Badge>
                ))}
            </div>
          )}

          {/* Clone metrics (Databricks per-CLONE counters, summed across tables) —
              only render when we actually have data movement to show. */}
          {((result.bytes_copied || 0) > 0 || (result.files_copied || 0) > 0) && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-foreground">{formatBytes(result.bytes_copied || 0)}</p>
                  <p className="text-xs text-gray-500">Bytes Copied</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-foreground">{(result.files_copied || 0).toLocaleString()}</p>
                  <p className="text-xs text-gray-500">Files Copied</p>
                </CardContent>
              </Card>
              {(result.source_table_size || 0) > 0 && (
                <Card>
                  <CardContent className="pt-4 text-center">
                    <p className="text-2xl font-bold text-foreground">{formatBytes(result.source_table_size || 0)}</p>
                    <p className="text-xs text-gray-500">Source Size</p>
                  </CardContent>
                </Card>
              )}
              {(result.source_num_of_files || 0) > 0 && (
                <Card>
                  <CardContent className="pt-4 text-center">
                    <p className="text-2xl font-bold text-foreground">{(result.source_num_of_files || 0).toLocaleString()}</p>
                    <p className="text-xs text-gray-500">Source Files</p>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* Duration */}
          {result.duration_seconds && (
            <p className="text-sm text-gray-500">Clone duration: {result.duration_seconds.toFixed(1)}s</p>
          )}

          {/* Validation Results */}
          {result.validation && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-foreground" />
                  Post-Clone Validation
                  <Badge variant="outline" className="ml-auto">
                    {result.validation.matched}/{result.validation.total_tables} matched
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {/* Validation summary */}
                <div className="grid grid-cols-4 gap-3 mb-4">
                  <div className="text-center p-2 bg-muted/20 rounded">
                    <p className="text-lg font-bold text-foreground">{result.validation.matched}</p>
                    <p className="text-xs text-gray-500">Matched</p>
                  </div>
                  <div className="text-center p-2 bg-red-50 rounded">
                    <p className="text-lg font-bold text-red-700">{result.validation.mismatched}</p>
                    <p className="text-xs text-gray-500">Mismatched</p>
                  </div>
                  <div className="text-center p-2 bg-muted/20 rounded">
                    <p className="text-lg font-bold text-muted-foreground">{result.validation.errors}</p>
                    <p className="text-xs text-gray-500">Errors</p>
                  </div>
                  <div className="text-center p-2 bg-muted/30 rounded">
                    <p className="text-lg font-bold text-[#E8453C]">{result.validation.total_tables}</p>
                    <p className="text-xs text-gray-500">Total Tables</p>
                  </div>
                </div>

                {/* Table details */}
                {result.validation.details && result.validation.details.length > 0 && (
                  <div className="overflow-x-auto max-h-80 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-white">
                        <tr className="border-b bg-gray-50">
                          <th className="text-left py-2 px-3 font-medium">Status</th>
                          <th className="text-left py-2 px-3 font-medium">Schema</th>
                          <th className="text-left py-2 px-3 font-medium">Table</th>
                          <th className="text-right py-2 px-3 font-medium">Source Rows</th>
                          <th className="text-right py-2 px-3 font-medium">Dest Rows</th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.validation.details.map((row: any, i: number) => (
                          <tr key={i} className={`border-b ${row.match ? "" : row.error ? "bg-muted/20" : "bg-red-50"}`}>
                            <td className="py-2 px-3">
                              {row.match ? (
                                <CheckCircle className="h-4 w-4 text-foreground" />
                              ) : row.error ? (
                                <AlertCircle className="h-4 w-4 text-muted-foreground" />
                              ) : (
                                <XCircle className="h-4 w-4 text-red-500" />
                              )}
                            </td>
                            <td className="py-2 px-3 text-gray-600">{row.schema}</td>
                            <td className="py-2 px-3 font-medium">{row.table}</td>
                            <td className="py-2 px-3 text-right">{row.source_count?.toLocaleString() ?? "—"}</td>
                            <td className="py-2 px-3 text-right">{row.dest_count?.toLocaleString() ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Errors */}
          {result.errors && result.errors.length > 0 && (
            <Card className="border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center gap-2 text-foreground">
                  <AlertCircle className="h-5 w-5" />
                  Warnings ({result.errors.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="max-h-40 overflow-y-auto text-sm space-y-1">
                  {result.errors.map((err: string, i: number) => (
                    <div key={i} className="text-muted-foreground font-mono text-xs">{err}</div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
        );
      })()}

      {/* Error */}
      {job.status === "failed" && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="pt-4">
            <div className="flex items-center gap-2 mb-2">
              <XCircle className="h-5 w-5 text-red-600" />
              <span className="font-medium text-red-800">Clone failed</span>
            </div>
            <pre className="bg-white p-3 rounded text-sm text-red-700 overflow-auto max-h-48 whitespace-pre-wrap">
              {job.error}
            </pre>
          </CardContent>
        </Card>
      )}

      {/* Live Logs */}
      {job.logs && job.logs.length > 0 && <LogPanel logs={job.logs} jobId={jobId} isRunning={job.status === "running"} />}

    </div>
  );
}

function SnapshotPicker({
  catalog, value, onChange,
}: { catalog: string; value: string; onChange: (id: string) => void }) {
  const snapshots = useSnapshots(catalog || null);
  const items = snapshots.data || [];
  // Don't render the picker at all when there are no snapshots for this catalog —
  // keeps the Source step clean for the common case. A single help line in
  // /snapshots explains how to create one.
  if (!catalog || !items.length) return null;
  return (
    <div>
      <FieldLabelSmall hint="Clone from a named point-in-time snapshot instead of the catalog's current state. Resolves to as_of_timestamp under the hood.">
        Source snapshot (optional)
      </FieldLabelSmall>
      <select
        className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="">— current state —</option>
        {items.map((s: any) => (
          <option key={s.snapshot_id} value={s.snapshot_id}>
            {s.name} · {new Date(s.captured_at).toLocaleDateString()} · {s.table_count ?? 0} tbl
          </option>
        ))}
      </select>
    </div>
  );
}

function DestinationCatalogPicker({
  value,
  onChange,
  targetConnectionName,
}: {
  value: string;
  onChange: (v: string) => void;
  targetConnectionName?: string | null;
}) {
  const [sourceCatalogs, setSourceCatalogs] = useState<string[]>([]);
  const [isNew, setIsNew] = useState(false);
  const [loading, setLoading] = useState(false);

  // When the user is doing a cross-workspace clone with a saved target picked,
  // the dropdown must list catalogs that exist in the *target* workspace
  // (otherwise we suggest catalogs that don't exist on the destination side).
  const targetCatalogs = useTargetCatalogs(targetConnectionName ?? null);
  const useTarget = !!targetConnectionName;

  useEffect(() => {
    if (useTarget) return; // skip source-side fetch
    setLoading(true);
    api.get<string[]>("/catalogs")
      .then((data) => {
        const list = data || [];
        setSourceCatalogs(list);
        if (list.length === 0) {
          toast.warning("No catalogs visible — check workspace permissions or type a name manually.");
        }
      })
      .catch(() => setSourceCatalogs([]))
      .finally(() => setLoading(false));
  }, [useTarget]);

  const catalogs = useTarget ? (targetCatalogs.data ?? []) : sourceCatalogs;
  const isLoading = useTarget ? targetCatalogs.isLoading : loading;
  const fetchError = useTarget && targetCatalogs.isError;

  const selectClass =
    "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]";

  return (
    <div>
      <label className="text-sm font-medium">
        Destination Catalog
        {useTarget && (
          <span className="ml-2 text-xs font-normal text-muted-foreground">
            (from target '{targetConnectionName}')
          </span>
        )}
      </label>
      {fetchError && (
        <p className="text-xs text-red-500 mt-1">
          Could not list target catalogs — check the saved connection in Settings.
        </p>
      )}
      {isLoading ? (
        <LoadingState message="Loading catalogs..." className="py-2" />
      ) : isNew ? (
        <div className="space-y-2">
          <div className="flex gap-2">
            <input
              className={selectClass}
              value={value}
              onChange={(e) => onChange(e.target.value)}
              placeholder="Enter new catalog name (e.g. my_catalog_clone)"
              autoFocus
            />
            <button
              onClick={() => { setIsNew(false); onChange(""); }}
              className="px-3 py-2 text-sm rounded-lg border border-border hover:bg-muted/50 text-muted-foreground whitespace-nowrap"
            >
              Cancel
            </button>
          </div>
          <p className="text-xs text-muted-foreground">
            This catalog will be created automatically during the clone operation
          </p>
        </div>
      ) : (
        <select
          className={selectClass}
          value={value}
          onChange={(e) => {
            if (e.target.value === "__NEW__") {
              setIsNew(true);
              onChange("");
            } else {
              onChange(e.target.value);
            }
          }}
        >
          <option value="">Select catalog...</option>
          <option value="__NEW__">+ Create New Catalog</option>
          {catalogs.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      )}
    </div>
  );
}

function CrossWorkspaceTogglePanel({
  enabled,
  onEnabledChange,
  connectionName,
  onConnectionChange,
  fanoutMode,
  onFanoutModeChange,
  fanoutNames,
  onFanoutNamesChange,
  fanoutMaxParallel,
  onFanoutMaxParallelChange,
}: {
  enabled: boolean;
  onEnabledChange: (v: boolean) => void;
  connectionName: string;
  onConnectionChange: (v: string) => void;
  // Fanout state — when on, plural multi-target dispatch replaces the
  // single-target one. See `target_workspaces` field on CloneRequest.
  fanoutMode: boolean;
  onFanoutModeChange: (v: boolean) => void;
  fanoutNames: string[];
  onFanoutNamesChange: (v: string[]) => void;
  fanoutMaxParallel: number;
  onFanoutMaxParallelChange: (v: number) => void;
}) {
  const conns = useTargetConnections();
  const test = useTestTargetConnection();
  const list = conns.data ?? [];
  const picked = list.find((c) => c.name === connectionName);

  const runTest = () => {
    if (!connectionName) return;
    test.mutate(connectionName, {
      onSuccess: (data: any) => {
        const extras = [
          typeof data?.catalog_count === "number" ? `${data.catalog_count} catalogs` : "",
          data?.warehouse_state ? `warehouse ${data.warehouse_state}` : "",
        ].filter(Boolean);
        toast.success(`'${connectionName}' connection OK${extras.length ? ` — ${extras.join(", ")}` : ""}`);
        if (data?.warehouse_start_triggered) {
          toast.info(`Warehouse was ${data.warehouse_state} — start triggered, RUNNING in 30–60s.`);
        }
      },
      onError: (e: any) => toast.error(e?.message || `'${connectionName}' test failed`),
    });
  };

  const toggleFanoutTarget = (name: string) => {
    if (fanoutNames.includes(name)) {
      onFanoutNamesChange(fanoutNames.filter((n) => n !== name));
    } else {
      onFanoutNamesChange([...fanoutNames, name]);
    }
  };

  return (
    <div className="space-y-3">
      <label className="flex items-start gap-2 cursor-pointer">
        <input
          type="checkbox"
          className="mt-1"
          checked={enabled}
          onChange={(e) => onEnabledChange(e.target.checked)}
        />
        <div>
          <div className="text-sm font-medium">Clone to a different workspace</div>
          {!enabled && (
            <div className="text-xs text-muted-foreground">
              Cross-workspace / cross-cloud migration via Delta Sharing → DEEP CLONE. Manage saved targets in Settings.
            </div>
          )}
        </div>
      </label>

      {enabled && (
        <div className="space-y-3 border-t pt-3">
          {conns.isLoading && (
            <p className="text-xs text-muted-foreground">Loading saved targets…</p>
          )}
          {!conns.isLoading && list.length === 0 && (
            <div className="flex items-center gap-2">
              <p className="text-sm text-muted-foreground">No saved target workspaces.</p>
              <a href="/settings#target-workspaces" className="text-sm text-[#E8453C] underline">
                + Configure in Settings →
              </a>
            </div>
          )}

          {/* Fanout mode toggle. Off → single-target dropdown (legacy). On →
              multi-pick checkboxes that route to the plural target_workspaces
              field on CloneRequest, dispatching to clone_fanout in parallel. */}
          {list.length > 0 && (
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={fanoutMode}
                onChange={(e) => onFanoutModeChange(e.target.checked)}
              />
              <span className="text-sm font-medium">Fan out to multiple targets (parallel multi-region clone)</span>
            </label>
          )}

          {list.length > 0 && !fanoutMode && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm font-medium">Target connection:</span>
              <select
                className="h-9 px-3 py-1 text-sm bg-background border border-input rounded-md min-w-[200px]"
                value={connectionName}
                onChange={(e) => onConnectionChange(e.target.value)}
              >
                <option value="">— select target —</option>
                {list.map((c) => (
                  <option key={c.name} value={c.name}>{c.name}</option>
                ))}
              </select>
              <Button
                size="sm"
                variant="outline"
                onClick={runTest}
                disabled={!connectionName || test.isPending}
              >
                {test.isPending ? <Loader2 className="h-3 w-3 animate-spin" /> : "Test"}
              </Button>
              <a href="/settings#target-workspaces" className="text-xs text-muted-foreground hover:text-foreground underline">
                Manage in Settings →
              </a>
            </div>
          )}

          {list.length > 0 && fanoutMode && (
            <div className="space-y-2">
              <div className="text-sm font-medium">
                Targets ({fanoutNames.length} of {list.length} selected):
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-1 max-h-48 overflow-y-auto border rounded-md p-2">
                {list.map((c) => {
                  const checked = fanoutNames.includes(c.name);
                  return (
                    <label
                      key={c.name}
                      className="flex items-start gap-2 px-2 py-1 rounded hover:bg-muted/50 cursor-pointer"
                    >
                      <input
                        type="checkbox"
                        className="mt-1"
                        checked={checked}
                        onChange={() => toggleFanoutTarget(c.name)}
                      />
                      <div className="min-w-0 flex-1">
                        <div className="text-sm font-medium truncate">{c.name}</div>
                        <div className="text-[10px] text-muted-foreground font-mono truncate">
                          {c.host} · WH {c.warehouse_id}
                        </div>
                      </div>
                    </label>
                  );
                })}
              </div>
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm">Parallel:</span>
                <input
                  type="number"
                  min={1}
                  max={20}
                  value={fanoutMaxParallel}
                  onChange={(e) => onFanoutMaxParallelChange(Math.max(1, parseInt(e.target.value, 10) || 1))}
                  className="h-8 w-16 px-2 text-sm bg-background border border-input rounded-md"
                />
                <span className="text-xs text-muted-foreground">
                  How many targets clone simultaneously. Cap at 5 for typical bandwidth-limited sources.
                </span>
              </div>
              {fanoutNames.length === 0 && (
                <div className="text-xs text-amber-700 dark:text-amber-400">
                  Pick at least one target to fan out to.
                </div>
              )}
              {fanoutNames.length === 1 && (
                <div className="text-xs text-muted-foreground">
                  Only one target picked — request will be sent as `target_workspaces` (plural) but it&apos;s equivalent to a regular cross-workspace clone.
                </div>
              )}
            </div>
          )}

          {picked && !fanoutMode && (
            <div className="text-xs text-muted-foreground bg-muted/40 rounded-md p-2 font-mono truncate">
              {picked.host} · {picked.auth_method === "pat" ? "PAT" : picked.auth_method === "service_principal" ? "SP" : `Profile ${picked.profile}`} · WH {picked.warehouse_id} · {picked.data_sync_mode}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ClonePageInner() {
  const [step, setStep] = useState<Step>("source");
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const { favorites, addFavorite, removeFavorite } = useFavorites();
  const [showAddFav, setShowAddFav] = useState(false);
  const [favSource, setFavSource] = useState("");
  const [favDest, setFavDest] = useState("");

  // Cross-workspace / cross-cloud migration state
  const [crossWorkspace, setCrossWorkspace] = useState(false);
  const [targetConnectionName, setTargetConnectionName] = useState<string>("");
  // Multi-target fanout state. When `fanoutMode` is on, the singular target
  // dropdown is hidden and multi-pick checkboxes drive the request payload's
  // `target_workspaces` (plural) field — dispatched to the parallel
  // run_cross_workspace_fanout orchestrator on the server side.
  const [fanoutMode, setFanoutMode] = useState(false);
  const [fanoutNames, setFanoutNames] = useState<string[]>([]);
  const [fanoutMaxParallel, setFanoutMaxParallel] = useState<number>(5);
  const targetConnections = useTargetConnections();
  const pickedConnection = (targetConnections.data ?? []).find((c) => c.name === targetConnectionName) ?? null;
  const pickedFanoutConnections = (targetConnections.data ?? []).filter((c) => fanoutNames.includes(c.name));
  // Synthesize a target shape for PreviewPanel's CLI/YAML output. Secrets stay
  // redacted as "***" (the GET endpoint masks them) — correct behavior for
  // commands the user might paste/share.
  const targetForPreview = pickedConnection
    ? {
        host: pickedConnection.host,
        auth_method: pickedConnection.auth_method,
        token: pickedConnection.token ?? "",
        client_id: pickedConnection.client_id ?? "",
        client_secret: pickedConnection.client_secret ?? "",
        profile: pickedConnection.profile ?? "",
        warehouse_id: pickedConnection.warehouse_id,
        keep_share: !!pickedConnection.keep_share,
        data_sync_mode: pickedConnection.data_sync_mode ?? "snapshot_once",
        auto_handle_masks: !!pickedConnection.auto_handle_masks,
      }
    : {
        host: "",
        auth_method: "pat" as const,
        token: "",
        client_id: "",
        client_secret: "",
        profile: "",
        warehouse_id: "",
        keep_share: false,
        data_sync_mode: "snapshot_once" as const,
        auto_handle_masks: false,
      };

  // Scope selection state
  const [scopeMode, setScopeMode] = useState<ScopeMode>("all");
  const [selectedObjects, setSelectedObjects] = useState<ObjectRef[]>([]);

  // Default config
  const defaults = {
    source_catalog: "",
    destination_catalog: "",
    source_snapshot_id: null as string | null,
    clone_type: "DEEP" as "DEEP" | "SHALLOW",
    load_type: "FULL" as "FULL" | "INCREMENTAL",
    dry_run: false,
    max_workers: 4,
    parallel_tables: 1,
    max_parallel_queries: 100,
    max_rps: 0,
    max_duration_min: null as number | null,
    max_tables: null as number | null,
    // Copy options
    copy_permissions: true,
    copy_ownership: true,
    copy_tags: true,
    copy_properties: true,
    copy_security: true,
    copy_constraints: true,
    copy_comments: true,
    // Features
    enable_rollback: true,
    validate_after_clone: false,
    validate_checksum: false,
    force_reclone: false,
    schema_only: false,
    generate_report: false,
    show_progress: true,
    auto_rollback: false,
    rollback_threshold: 5,
    checkpoint: false,
    require_approval: false,
    impact_check: false,
    skip_unused: false,
    verbose: false,
    // Serverless
    serverless: false,
    volume: "",
    // Filtering
    exclude_schemas: ["information_schema", "default"],
    include_schemas: [] as string[],
    include_tables_regex: "",
    exclude_tables_regex: "",
    order_by_size: "" as "" | "asc" | "desc",
    // Time travel
    as_of_timestamp: "",
    as_of_version: "",
    // Advanced
    location: "",
    where_clause: "",
    throttle: "" as "" | "low" | "medium" | "high" | "max",
    ttl: "",
    template: "",
    // Free-form TBLPROPERTIES override block — one `key = value` per line.
    // Parsed at submit-time into clone_tbl_properties dict. Empty = no override.
    clone_tbl_properties_text: "",
  };

  // Apply URL query params from template selection on mount
  const [config, setConfig] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.size === 0) return defaults;

    const merged = { ...defaults };
    // Boolean config keys
    const boolKeys = [
      "copy_permissions", "copy_ownership", "copy_tags", "copy_properties",
      "copy_security", "copy_constraints", "copy_comments", "enable_rollback",
      "validate_after_clone", "validate_checksum", "dry_run", "force_reclone", "schema_only",
      "generate_report", "show_progress", "auto_rollback", "checkpoint",
      "require_approval", "impact_check", "skip_unused", "verbose", "serverless",
    ];
    // Number config keys
    const numKeys = ["max_workers", "parallel_tables", "max_parallel_queries", "max_rps", "rollback_threshold"];

    for (const [key, val] of params.entries()) {
      if (key in merged) {
        if (boolKeys.includes(key)) {
          (merged as any)[key] = val === "true" || val === "True";
        } else if (numKeys.includes(key)) {
          (merged as any)[key] = parseInt(val, 10) || (merged as any)[key];
        } else {
          (merged as any)[key] = val;
        }
      }
    }
    return merged;
  });

  // Load saved config from backend on mount (overrides hardcoded defaults)
  useEffect(() => {
    api.get<any>("/config").then((saved) => {
      if (!saved) return;
      setConfig((prev: any) => ({
        ...prev,
        source_catalog: prev.source_catalog || saved.source_catalog || "",
        destination_catalog: prev.destination_catalog || saved.destination_catalog || "",
        clone_type: saved.clone_type || prev.clone_type,
        load_type: saved.load_type || prev.load_type,
        max_workers: saved.max_workers ?? prev.max_workers,
        parallel_tables: saved.parallel_tables ?? prev.parallel_tables,
        max_parallel_queries: saved.max_parallel_queries ?? prev.max_parallel_queries,
        max_rps: saved.max_rps ?? prev.max_rps,
        copy_permissions: saved.copy_permissions ?? prev.copy_permissions,
        copy_ownership: saved.copy_ownership ?? prev.copy_ownership,
        copy_tags: saved.copy_tags ?? prev.copy_tags,
        copy_properties: saved.copy_properties ?? prev.copy_properties,
        copy_constraints: saved.copy_constraints ?? prev.copy_constraints,
        copy_comments: saved.copy_comments ?? prev.copy_comments,
        exclude_schemas: saved.exclude_schemas?.length ? saved.exclude_schemas : prev.exclude_schemas,
        include_schemas: saved.include_schemas?.length ? saved.include_schemas : prev.include_schemas,
        include_tables_regex: saved.include_tables_regex || prev.include_tables_regex,
        exclude_tables_regex: saved.exclude_tables_regex || prev.exclude_tables_regex,
        location: saved.catalog_location || prev.location,
        ttl: saved.ttl || prev.ttl,
      }));
    }).catch(() => {});
  }, []);

  // Auto-populate storage location from source catalog's storage root
  const [sourceStorageRoot, setSourceStorageRoot] = useState("");
  useEffect(() => {
    if (!config.source_catalog) { setSourceStorageRoot(""); return; }
    api.get(`/catalogs/${config.source_catalog}/info`)
      .then((info: any) => setSourceStorageRoot(info.storage_root || ""))
      .catch(() => setSourceStorageRoot(""));
  }, [config.source_catalog]);

  useEffect(() => {
    if (sourceStorageRoot && config.destination_catalog && config.source_catalog) {
      const m = sourceStorageRoot.match(/^(abfss?:\/\/)([^@]+)@([^/]+)(\/.*)?$/);
      if (m) {
        const protocol = m[1];
        const container = m[2];
        const account = m[3];
        const pathPart = (m[4] || "").replace(/\/+$/, "");
        let newPath = pathPart;
        if (pathPart) {
          const escaped = config.source_catalog.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
          newPath = pathPart.replace(new RegExp(escaped, "g"), config.destination_catalog);
          if (newPath === pathPart) {
            const segs = pathPart.split("/");
            segs[segs.length - 1] = config.destination_catalog;
            newPath = segs.join("/");
          }
        } else {
          newPath = "/" + config.destination_catalog;
        }
        setConfig((prev) => ({ ...prev, location: `${protocol}${container}@${account}${newPath}` }));
      }
    }
  }, [sourceStorageRoot, config.destination_catalog, config.source_catalog]);

  const startClone = useStartClone();
  const volumes = useVolumes();

  const handleClone = (dryRun: boolean) => {
    if (
      !crossWorkspace &&
      config.source_catalog &&
      config.source_catalog === config.destination_catalog
    ) {
      toast.error("Source and destination catalogs must be different.");
      return;
    }
    if (crossWorkspace) {
      if (fanoutMode) {
        if (fanoutNames.length === 0) {
          toast.error("Pick at least one target to fan out to.");
          return;
        }
      } else if (!targetConnectionName) {
        toast.error("Pick a target workspace (or add one in Settings).");
        return;
      }
    }
    for (const field of ["include_tables_regex", "exclude_tables_regex"] as const) {
      const pattern = (config as any)[field];
      if (pattern) {
        try {
          new RegExp(pattern);
        } catch {
          toast.error(`Invalid regex in ${field.replace(/_/g, " ")}: ${pattern}`);
          return;
        }
      }
    }
    if (config.ttl && !TTL_PATTERN.test(config.ttl)) {
      toast.error("TTL must look like 24h, 7d, or 2w.");
      return;
    }
    // Clean up empty strings → null so Pydantic optional fields validate correctly
    const payload: Record<string, unknown> = { ...config, dry_run: dryRun };
    if (!payload.order_by_size) payload.order_by_size = null;
    if (!payload.as_of_version) payload.as_of_version = null;
    else payload.as_of_version = parseInt(payload.as_of_version as string, 10) || null;
    if (!payload.as_of_timestamp) payload.as_of_timestamp = null;
    if (!payload.include_tables_regex) payload.include_tables_regex = null;
    if (!payload.exclude_tables_regex) payload.exclude_tables_regex = null;
    if (!payload.location) payload.location = null;
    if (!payload.volume) payload.volume = null;
    if (!payload.throttle) delete payload.throttle;
    if (!payload.ttl) delete payload.ttl;
    if (!payload.template) delete payload.template;
    if (!payload.where_clause) delete payload.where_clause;
    if (payload.max_duration_min == null) delete payload.max_duration_min;
    if (payload.max_tables == null) delete payload.max_tables;
    if (!payload.source_snapshot_id) delete payload.source_snapshot_id;

    // Parse the free-form TBLPROPERTIES textarea ("key = value" per line) into
    // the dict the backend expects on `clone_tbl_properties`. Skip blank
    // lines and lines without `=`. Strip surrounding quotes from values.
    const tblPropsText = (payload.clone_tbl_properties_text as string) || "";
    delete payload.clone_tbl_properties_text;
    if (tblPropsText.trim()) {
      const props: Record<string, string> = {};
      for (const line of tblPropsText.split("\n")) {
        const eq = line.indexOf("=");
        if (eq < 0) continue;
        const k = line.slice(0, eq).trim();
        let v = line.slice(eq + 1).trim();
        if (!k) continue;
        if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith('"') && v.endsWith('"'))) {
          v = v.slice(1, -1);
        }
        props[k] = v;
      }
      if (Object.keys(props).length > 0) {
        payload.clone_tbl_properties = props;
      }
    }

    if (crossWorkspace && fanoutMode && fanoutNames.length > 0) {
      // Multi-target fanout — server-side `clone_fanout` orchestrator runs N
      // cross-workspace clones in parallel, one per target. Same inline-creds
      // pattern as the single-target path, just one entry per picked target.
      const missing = fanoutNames.filter(
        (n) => !pickedFanoutConnections.some((c) => c.name === n),
      );
      if (missing.length > 0) {
        toast.error(`Target connections not found in browser storage: ${missing.join(", ")}`);
        return;
      }
      payload.target_workspaces = pickedFanoutConnections.map((c) => ({
        host: c.host,
        auth_method: c.auth_method,
        token: c.token,
        client_id: c.client_id,
        client_secret: c.client_secret,
        profile: c.profile,
        warehouse_id: c.warehouse_id,
        keep_share: !!c.keep_share,
        data_sync_mode: c.data_sync_mode || "snapshot_once",
        auto_handle_masks: !!c.auto_handle_masks,
      }));
      payload.fanout_max_parallel = fanoutMaxParallel;
    } else if (crossWorkspace && targetConnectionName) {
      // Resolve localStorage entry → inline target_workspace creds for this
      // single request. Server is stateless w.r.t. saved connections.
      if (!pickedConnection) {
        toast.error(`Target '${targetConnectionName}' not found in browser storage.`);
        return;
      }
      payload.target_workspace = {
        host: pickedConnection.host,
        auth_method: pickedConnection.auth_method,
        token: pickedConnection.token,
        client_id: pickedConnection.client_id,
        client_secret: pickedConnection.client_secret,
        profile: pickedConnection.profile,
        warehouse_id: pickedConnection.warehouse_id,
        keep_share: !!pickedConnection.keep_share,
        data_sync_mode: pickedConnection.data_sync_mode || "snapshot_once",
        auto_handle_masks: !!pickedConnection.auto_handle_masks,
      };
    }

    if (scopeMode === "select" && selectedObjects.length > 0) {
      payload.include_objects = selectedObjects;
    }

    startClone.mutate(payload, {
      onSuccess: (data: any) => {
        setActiveJobId(data.job_id);
        setStep("execute");
      },
    });
    setStep("execute");
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Clone Catalog"
        icon={Copy}
        description="Deep or shallow clone of a Unity Catalog catalog — copies tables, views, permissions, tags, properties, and constraints across schemas. Supports serverless compute, incremental clone, time-travel, and post-clone validation."
        breadcrumbs={["Operations", "Clone Catalog"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-create-table-clone"
        docsLabel="CREATE TABLE CLONE"
      />

      {/* Pinned Catalog Pairs */}
      {(favorites.length > 0 || showAddFav) && (
        <div className="flex items-center gap-2 flex-wrap">
          <Star className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          {favorites.map((fav, i) => (
            <div key={i} className="group flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border border-border bg-muted/30 hover:border-[#E8453C]/50 transition-all cursor-pointer text-xs"
              onClick={() => setConfig(c => ({ ...c, source_catalog: fav.source, destination_catalog: fav.destination }))}
            >
              <span className="font-medium text-foreground">{fav.source}</span>
              <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
              <span className="text-foreground">{fav.destination}</span>
              <button className="ml-0.5 opacity-0 group-hover:opacity-100 transition-opacity" onClick={(e) => { e.stopPropagation(); removeFavorite(fav.source, fav.destination); }}>
                <X className="h-2.5 w-2.5 text-muted-foreground hover:text-red-500" />
              </button>
            </div>
          ))}
          {showAddFav ? (
            <div className="flex items-center gap-1.5">
              <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md text-foreground w-28" placeholder="Source" value={favSource} onChange={(e) => setFavSource(e.target.value)} />
              <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
              <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md text-foreground w-28" placeholder="Destination" value={favDest} onChange={(e) => setFavDest(e.target.value)} />
              <Button size="sm" className="h-6 text-xs px-2" disabled={!favSource.trim() || !favDest.trim()} onClick={() => { addFavorite(favSource.trim(), favDest.trim()); setFavSource(""); setFavDest(""); setShowAddFav(false); }}>Pin</Button>
              <button onClick={() => setShowAddFav(false)} className="text-muted-foreground hover:text-foreground"><X className="h-3 w-3" /></button>
            </div>
          ) : (
            <button onClick={() => setShowAddFav(true)} className="flex items-center gap-1 px-2 py-1 text-xs text-muted-foreground hover:text-foreground rounded-md hover:bg-muted/50 transition-colors">
              <Plus className="h-3 w-3" /> Pin pair
            </button>
          )}
        </div>
      )}

      {/* Step indicators */}
      <div className="flex gap-2">
        {(["source", "options", "preview", "execute"] as Step[]).map((s, i) => (
          <Badge
            key={s}
            variant={step === s ? "default" : "outline"}
            className={`cursor-pointer ${step === s ? "bg-[#E8453C]" : ""}`}
            onClick={() => s !== "execute" && setStep(s)}
          >
            {i + 1}. {s.charAt(0).toUpperCase() + s.slice(1)}
          </Badge>
        ))}
      </div>

      {/* Step 1: Source & Destination */}
      {step === "source" && (
        <Card>
          <CardHeader>
            <CardTitle>Source & Destination</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="text-sm font-medium">Source Catalog</label>
              <CatalogPicker
                catalog={config.source_catalog}
                onCatalogChange={(v) => setConfig({ ...config, source_catalog: v })}
                showSchema={false}
                showTable={false}
              />
            </div>
            <DestinationCatalogPicker
              value={config.destination_catalog}
              onChange={(v) => setConfig({ ...config, destination_catalog: v })}
              targetConnectionName={crossWorkspace ? targetConnectionName : null}
            />
            {config.source_catalog && config.destination_catalog &&
              config.source_catalog === config.destination_catalog && (
              <p className="text-xs text-red-600 dark:text-red-400 -mt-2">
                Source and destination catalogs must be different.
              </p>
            )}
            <SnapshotPicker
              catalog={config.source_catalog}
              value={config.source_snapshot_id || ""}
              onChange={(id) => setConfig({ ...config, source_snapshot_id: id || null })}
            />
            <div>
              <label className="text-sm font-medium">Storage Location (optional)</label>
              <Input
                placeholder="abfss://container@storage.dfs.core.windows.net/path"
                value={config.location}
                onChange={(e) => setConfig({ ...config, location: e.target.value })}
              />
              <p className="text-xs text-gray-400 mt-1">Required if workspace uses Default Storage</p>
            </div>
            <div className="border-t border-b py-4">
              <CrossWorkspaceTogglePanel
                enabled={crossWorkspace}
                onEnabledChange={(v) => {
                  setCrossWorkspace(v);
                  if (!v) {
                    setTargetConnectionName("");
                    setFanoutMode(false);
                    setFanoutNames([]);
                  }
                }}
                connectionName={targetConnectionName}
                onConnectionChange={setTargetConnectionName}
                fanoutMode={fanoutMode}
                onFanoutModeChange={(v) => {
                  setFanoutMode(v);
                  // Switching modes clears the other side so submission state
                  // is unambiguous (matches the Pydantic XOR validator).
                  if (v) setTargetConnectionName("");
                  else setFanoutNames([]);
                }}
                fanoutNames={fanoutNames}
                onFanoutNamesChange={setFanoutNames}
                fanoutMaxParallel={fanoutMaxParallel}
                onFanoutMaxParallelChange={setFanoutMaxParallel}
              />
            </div>
            <ScopePicker
              catalog={config.source_catalog}
              mode={scopeMode}
              onModeChange={setScopeMode}
              selected={selectedObjects}
              onSelectedChange={setSelectedObjects}
            />
            <Button
              onClick={() => setStep("options")}
              disabled={
                !config.source_catalog ||
                !config.destination_catalog ||
                (!crossWorkspace && config.source_catalog === config.destination_catalog) ||
                (crossWorkspace && !targetConnectionName) ||
                (scopeMode === "select" && selectedObjects.length === 0)
              }
            >
              Next: Options
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Step 2: Clone Options */}
      {step === "options" && (
        <Card>
          <CardHeader>
            <CardTitle>Clone Options</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <FieldLabel field="clone_type">Clone Type</FieldLabel>
                <div className="flex gap-2 mt-1">
                  {(["DEEP", "SHALLOW"] as const).map((t) => (
                    <Button
                      key={t}
                      variant={config.clone_type === t ? "default" : "outline"}
                      size="sm"
                      onClick={() => setConfig({ ...config, clone_type: t })}
                    >
                      {t}
                    </Button>
                  ))}
                </div>
              </div>
              <div>
                <FieldLabel field="load_type">Load Type</FieldLabel>
                <div className="flex gap-2 mt-1">
                  {(["FULL", "INCREMENTAL"] as const).map((t) => (
                    <Button
                      key={t}
                      variant={config.load_type === t ? "default" : "outline"}
                      size="sm"
                      onClick={() => setConfig({ ...config, load_type: t })}
                    >
                      {t}
                    </Button>
                  ))}
                </div>
              </div>
            </div>

            {/* Serverless */}
            <div>
              <label className="text-sm font-medium mb-2 block">Compute</label>
              <div className="flex gap-4 items-start">
                <label className="flex items-center gap-2 text-sm cursor-pointer pt-2">
                  <input
                    type="checkbox"
                    checked={config.serverless}
                    onChange={(e) => setConfig({ ...config, serverless: e.target.checked })}
                  />
                  Use Serverless Compute
                  <InfoDot field="serverless" />
                </label>
                {config.serverless && (
                  <div className="flex-1">
                    <FieldLabelSmall field="volume" className="text-xs text-gray-500 mb-1 inline-flex items-center gap-1.5">UC Volume (required for serverless)</FieldLabelSmall>
                    {volumes.isLoading ? (
                      <div className="flex items-center gap-2 text-sm text-gray-400 py-2">
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Loading volumes...
                      </div>
                    ) : volumes.isError ? (
                      <div className="space-y-2">
                        <p className="text-xs text-red-500">Failed to load volumes</p>
                        <Input
                          placeholder="/Volumes/catalog/schema/volume"
                          value={config.volume}
                          onChange={(e) => setConfig({ ...config, volume: e.target.value })}
                        />
                      </div>
                    ) : (
                      <select
                        className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]"
                        value={config.volume}
                        onChange={(e) => setConfig({ ...config, volume: e.target.value })}
                      >
                        <option value="">Select a volume...</option>
                        {(volumes.data || []).map((v) => (
                          <option key={v.path} value={v.path}>
                            {v.path} ({v.type})
                          </option>
                        ))}
                      </select>
                    )}
                  </div>
                )}
              </div>
            </div>

            {/* Performance */}
            <div>
              <label className="text-sm font-medium mb-2 block">Performance</label>
              <div className="grid grid-cols-4 gap-4">
                <div>
                  <FieldLabelSmall field="max_workers">Max Workers (schemas)</FieldLabelSmall>
                  <Input type="number" min={1} max={16} value={config.max_workers}
                    onChange={(e) => setConfig({ ...config, max_workers: parseInt(e.target.value) || 4 })} />
                </div>
                <div>
                  <FieldLabelSmall field="parallel_tables">Parallel Tables</FieldLabelSmall>
                  <Input type="number" min={1} max={8} value={config.parallel_tables}
                    onChange={(e) => setConfig({ ...config, parallel_tables: parseInt(e.target.value) || 1 })} />
                </div>
                <div>
                  <FieldLabelSmall field="max_parallel_queries">Max Parallel Queries</FieldLabelSmall>
                  <Input type="number" min={1} max={200} value={config.max_parallel_queries}
                    onChange={(e) => setConfig({ ...config, max_parallel_queries: parseInt(e.target.value) || 100 })} />
                </div>
                <div>
                  <FieldLabelSmall field="max_rps">Max RPS (0=unlimited)</FieldLabelSmall>
                  <Input type="number" min={0} max={100} value={config.max_rps}
                    onChange={(e) => setConfig({ ...config, max_rps: parseFloat(e.target.value) || 0 })} />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4 mt-3">
                <div>
                  <FieldLabelSmall field="order_by_size">Order by Size</FieldLabelSmall>
                  <div className="flex gap-1 mt-1">
                    {(["", "asc", "desc"] as const).map((v) => (
                      <Button key={v || "none"} size="sm" variant={config.order_by_size === v ? "default" : "outline"}
                        onClick={() => setConfig({ ...config, order_by_size: v })}>
                        {v || "None"}
                      </Button>
                    ))}
                  </div>
                </div>
                <div>
                  <FieldLabelSmall field="throttle">Throttle Profile</FieldLabelSmall>
                  <div className="flex gap-1 mt-1">
                    {(["", "low", "medium", "high", "max"] as const).map((v) => (
                      <Button key={v || "none"} size="sm" variant={config.throttle === v ? "default" : "outline"}
                        onClick={() => setConfig({ ...config, throttle: v })}>
                        {v || "None"}
                      </Button>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            {/* Copy Options */}
            <div>
              <label className="text-sm font-medium mb-2 block">Copy Options</label>
              <div className="grid grid-cols-4 gap-2">
                {([
                  ["copy_permissions", "Permissions"],
                  ["copy_ownership", "Ownership"],
                  ["copy_tags", "Tags"],
                  ["copy_properties", "Properties"],
                  ["copy_security", "Security"],
                  ["copy_constraints", "Constraints"],
                  ["copy_comments", "Comments"],
                ] as const).map(([key, label]) => (
                  <label key={key} className="flex items-center gap-2 text-sm">
                    <input type="checkbox" checked={config[key] as boolean}
                      onChange={(e) => setConfig({ ...config, [key]: e.target.checked })} />
                    {label}
                    <InfoDot field={key} />
                  </label>
                ))}
              </div>
            </div>

            {/* Features */}
            <div>
              <label className="text-sm font-medium mb-2 block">Features</label>
              <div className="grid grid-cols-4 gap-2">
                {([
                  ["enable_rollback", "Enable Rollback"],
                  ["auto_rollback", "Auto Rollback on Fail"],
                  ["validate_after_clone", "Validate After Clone"],
                  ["validate_checksum", "Checksum Validation"],
                  ["force_reclone", "Force Re-clone"],
                  ["schema_only", "Schema Only (empty tables)"],
                  ["generate_report", "Generate Report"],
                  ["show_progress", "Show Progress"],
                  ["checkpoint", "Enable Checkpoint"],
                  ["require_approval", "Require Approval"],
                  ["impact_check", "Impact Check"],
                  ["skip_unused", "Skip Unused Tables"],
                  ["verbose", "Verbose Logging"],
                ] as const).map(([key, label]) => (
                  <label key={key} className="flex items-center gap-2 text-sm">
                    <input type="checkbox" checked={config[key] as boolean}
                      onChange={(e) => setConfig({ ...config, [key]: e.target.checked })} />
                    {label}
                    <InfoDot field={key} />
                  </label>
                ))}
              </div>
            </div>

            {/* Auto Rollback Threshold */}
            {config.auto_rollback && (
              <div className="w-48">
                <FieldLabelSmall field="rollback_threshold">Rollback Threshold (%)</FieldLabelSmall>
                <Input type="number" min={0} max={100} value={config.rollback_threshold}
                  onChange={(e) => setConfig({ ...config, rollback_threshold: parseFloat(e.target.value) || 5 })} />
              </div>
            )}

            {/* Filtering */}
            <div>
              <label className="text-sm font-medium mb-2 block">Filtering</label>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <FieldLabelSmall field="include_schemas">Include Schemas (comma-separated)</FieldLabelSmall>
                  <Input placeholder="e.g. bronze,silver,gold" value={config.include_schemas.join(",")}
                    onChange={(e) => setConfig({ ...config, include_schemas: e.target.value ? e.target.value.split(",").map(s => s.trim()) : [] })} />
                </div>
                <div>
                  <FieldLabelSmall field="exclude_schemas">Exclude Schemas (comma-separated)</FieldLabelSmall>
                  <Input value={config.exclude_schemas.join(",")}
                    onChange={(e) => setConfig({ ...config, exclude_schemas: e.target.value ? e.target.value.split(",").map(s => s.trim()) : [] })} />
                </div>
                <div>
                  <FieldLabelSmall field="include_tables_regex">Include Tables Regex</FieldLabelSmall>
                  <Input placeholder="e.g. ^fact_.*" value={config.include_tables_regex}
                    onChange={(e) => setConfig({ ...config, include_tables_regex: e.target.value })} />
                </div>
                <div>
                  <FieldLabelSmall field="exclude_tables_regex">Exclude Tables Regex</FieldLabelSmall>
                  <Input placeholder="e.g. _tmp$|_backup$" value={config.exclude_tables_regex}
                    onChange={(e) => setConfig({ ...config, exclude_tables_regex: e.target.value })} />
                </div>
              </div>
            </div>

            {/* Time Travel */}
            <div>
              <label className="text-sm font-medium mb-2 block">Time Travel (optional)</label>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <FieldLabelSmall field="as_of_timestamp">As-of Timestamp</FieldLabelSmall>
                  <Input type="datetime-local" value={config.as_of_timestamp}
                    onChange={(e) => setConfig({ ...config, as_of_timestamp: e.target.value })} />
                </div>
                <div>
                  <FieldLabelSmall field="as_of_version">As-of Version</FieldLabelSmall>
                  <Input type="number" min={0} placeholder="e.g. 5" value={config.as_of_version}
                    onChange={(e) => setConfig({ ...config, as_of_version: e.target.value })} />
                </div>
              </div>
            </div>

            {/* Advanced */}
            <div>
              <label className="text-sm font-medium mb-2 block">Advanced</label>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <FieldLabelSmall field="where_clause">WHERE Clause (deep clone only)</FieldLabelSmall>
                  <Input placeholder="e.g. created_date > '2024-01-01'" value={config.where_clause}
                    onChange={(e) => setConfig({ ...config, where_clause: e.target.value })} />
                </div>
                <div>
                  <FieldLabelSmall field="ttl">TTL (e.g. 7d, 30d, 2w)</FieldLabelSmall>
                  <Input placeholder="e.g. 7d" value={config.ttl}
                    onChange={(e) => setConfig({ ...config, ttl: e.target.value })} />
                </div>
                <div>
                  <FieldLabelSmall field="template">Template</FieldLabelSmall>
                  <Input placeholder="e.g. dev-refresh, dr-replica" value={config.template}
                    onChange={(e) => setConfig({ ...config, template: e.target.value })} />
                </div>
                <div>
                  <FieldLabelSmall hint="Runtime guardrail: abort the clone if it takes longer than this (minutes). Leave blank for no limit.">
                    Max duration (min)
                  </FieldLabelSmall>
                  <Input
                    type="number"
                    min={1}
                    placeholder="e.g. 60"
                    value={config.max_duration_min ?? ""}
                    onChange={(e) =>
                      setConfig({
                        ...config,
                        max_duration_min: e.target.value ? parseInt(e.target.value, 10) : null,
                      })
                    }
                  />
                </div>
                <div>
                  <FieldLabelSmall hint="Runtime guardrail: abort the clone after this many tables have been touched. Safety net against runaway scope changes.">
                    Max tables
                  </FieldLabelSmall>
                  <Input
                    type="number"
                    min={1}
                    placeholder="e.g. 500"
                    value={config.max_tables ?? ""}
                    onChange={(e) =>
                      setConfig({
                        ...config,
                        max_tables: e.target.value ? parseInt(e.target.value, 10) : null,
                      })
                    }
                  />
                </div>
              </div>

              <div className="mt-3">
                <FieldLabelSmall hint="Optional Databricks TBLPROPERTIES applied INLINE on every per-table CLONE statement. One `key = value` per line. Primary use case: archival retention (e.g. delta.logRetentionDuration = '3650 days'). Setting these via ALTER TABLE post-clone is too late for the first VACUUM window.">
                  TBLPROPERTIES override (advanced — for archival use)
                </FieldLabelSmall>
                <textarea
                  className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]"
                  rows={3}
                  placeholder={"delta.logRetentionDuration = '3650 days'\ndelta.deletedFileRetentionDuration = '3650 days'"}
                  value={config.clone_tbl_properties_text}
                  onChange={(e) => setConfig({ ...config, clone_tbl_properties_text: e.target.value })}
                />
              </div>
            </div>

            <div className="flex gap-2">
              <Button variant="outline" onClick={() => setStep("source")}>Back</Button>
              <Button onClick={() => setStep("preview")}>Next: Preview</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Step 3: Preview */}
      {step === "preview" && (
        <PreviewPanel
          config={config}
          scopeMode={scopeMode}
          selectedObjects={selectedObjects}
          crossWorkspace={crossWorkspace}
          target={targetForPreview}
          fanoutMode={fanoutMode}
          fanoutTargets={pickedFanoutConnections.map((c) => ({
            name: c.name,
            host: c.host,
            warehouse_id: c.warehouse_id,
          }))}
          fanoutMaxParallel={fanoutMaxParallel}
          onBack={() => setStep("options")}
          onDryRun={() => handleClone(true)}
          onExecute={() => handleClone(false)}
          isStartingClone={startClone.isPending}
          dryRunResult={startClone.isSuccess && config.dry_run ? startClone.data : null}
        />
      )}

      {/* Step 4: Execution with Live Progress */}
      {step === "execute" && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Copy className="h-5 w-5" />
              Clone Execution
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Config Summary */}
            <div className="flex flex-wrap gap-1.5">
              <Badge variant="outline" className="text-xs">{config.clone_type}</Badge>
              <Badge variant="outline" className="text-xs">{config.load_type}</Badge>
              <Badge variant="outline" className="text-xs">W:{config.max_workers} T:{config.parallel_tables} Q:{config.max_parallel_queries}</Badge>
              {config.max_rps > 0 && <Badge variant="outline" className="text-xs">RPS:{config.max_rps}</Badge>}
              {config.order_by_size && <Badge variant="outline" className="text-xs">Size:{config.order_by_size}</Badge>}
              {config.throttle && <Badge variant="outline" className="text-xs">Throttle:{config.throttle}</Badge>}
              {config.copy_permissions && <Badge variant="outline" className="text-xs">Permissions</Badge>}
              {config.copy_tags && <Badge variant="outline" className="text-xs">Tags</Badge>}
              {config.copy_security && <Badge variant="outline" className="text-xs">Security</Badge>}
              {config.copy_constraints && <Badge variant="outline" className="text-xs">Constraints</Badge>}
              {config.copy_properties && <Badge variant="outline" className="text-xs">Properties</Badge>}
              {config.copy_comments && <Badge variant="outline" className="text-xs">Comments</Badge>}
              {config.enable_rollback && <Badge variant="outline" className="text-xs">Rollback</Badge>}
              {config.auto_rollback && <Badge variant="outline" className="text-xs">Auto-Rollback ({config.rollback_threshold}%)</Badge>}
              {config.validate_after_clone && <Badge variant="outline" className="text-xs">Validate</Badge>}
              {config.validate_checksum && <Badge variant="outline" className="text-xs">Checksum</Badge>}
              {config.generate_report && <Badge variant="outline" className="text-xs">Report</Badge>}
              {config.checkpoint && <Badge variant="outline" className="text-xs">Checkpoint</Badge>}
              {config.force_reclone && <Badge variant="outline" className="text-xs">Force Re-clone</Badge>}
              {config.schema_only && <Badge variant="outline" className="text-xs">Schema Only</Badge>}
              {config.skip_unused && <Badge variant="outline" className="text-xs">Skip Unused</Badge>}
              {config.impact_check && <Badge variant="outline" className="text-xs">Impact Check</Badge>}
              {config.require_approval && <Badge variant="outline" className="text-xs">Approval Required</Badge>}
              {config.dry_run && <Badge variant="outline" className="text-xs">Dry Run</Badge>}
              {config.verbose && <Badge variant="outline" className="text-xs">Verbose</Badge>}
              {config.serverless && <Badge variant="outline" className="text-xs">Serverless</Badge>}
              {!config.copy_permissions && <Badge variant="outline" className="text-xs border-red-400 text-red-600 dark:text-red-400">No Permissions</Badge>}
              {!config.copy_ownership && <Badge variant="outline" className="text-xs border-red-400 text-red-600 dark:text-red-400">No Ownership</Badge>}
              {config.include_schemas.length > 0 && <Badge variant="outline" className="text-xs">Schemas: {config.include_schemas.join(",")}</Badge>}
              {config.include_tables_regex && <Badge variant="outline" className="text-xs">Include: {config.include_tables_regex}</Badge>}
              {config.exclude_tables_regex && <Badge variant="outline" className="text-xs">Exclude: {config.exclude_tables_regex}</Badge>}
              {config.as_of_timestamp && <Badge variant="outline" className="text-xs">@{config.as_of_timestamp}</Badge>}
              {config.as_of_version && <Badge variant="outline" className="text-xs">v{config.as_of_version}</Badge>}
              {config.where_clause && <Badge variant="outline" className="text-xs">WHERE</Badge>}
              {config.ttl && <Badge variant="outline" className="text-xs">TTL:{config.ttl}</Badge>}
              {config.template && <Badge variant="outline" className="text-xs">Template:{config.template}</Badge>}
              {config.location && <Badge variant="outline" className="text-xs">Location Set</Badge>}
            </div>

            {startClone.isPending && !activeJobId && (
              <div className="flex items-center gap-2 text-gray-500">
                <Loader2 className="h-4 w-4 animate-spin" />
                Submitting clone job...
              </div>
            )}

            {activeJobId && <JobProgress jobId={activeJobId} />}

            {startClone.isError && !activeJobId && (
              <div className="flex items-center gap-2 text-red-600">
                <XCircle className="h-5 w-5" />
                <span>Error: {startClone.error instanceof Error ? startClone.error.message : String(startClone.error)}</span>
              </div>
            )}

            <div className="mt-6 flex gap-2">
              <Button
                variant="outline"
                onClick={() => {
                  setActiveJobId(null);
                  setStep("source");
                }}
              >
                New Clone
              </Button>
              <Button
                variant="outline"
                onClick={() => setStep("preview")}
              >
                Back to Preview
              </Button>
              <a
                href={`/preview?source=${encodeURIComponent(config.source_catalog)}&dest=${encodeURIComponent(config.destination_catalog)}`}
                className="inline-flex items-center gap-2 px-4 py-2 rounded-md border border-border text-[#E8453C] hover:bg-muted/30 dark:hover:bg-white/5 text-sm font-medium"
              >
                Compare Data →
              </a>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

export default function ClonePage() {
  return (
    <ErrorBoundary fallbackTitle="Clone page failed to render">
      <ClonePageInner />
    </ErrorBoundary>
  );
}
