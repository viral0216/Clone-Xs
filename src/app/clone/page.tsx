// @ts-nocheck
import { useState, useEffect, useRef } from "react";
// Polling-based progress — no WebSocket needed
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useStartClone } from "@/hooks/useApi";
import { api } from "@/lib/api-client";
import {
  Copy, Play, Eye, CheckCircle, XCircle, Loader2,
  ArrowRight, Clock, AlertCircle, Download, ClipboardCopy, Check,
} from "lucide-react";

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
          className="h-full rounded-full transition-all duration-500 ease-out bg-blue-600"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
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

function generateReport(job: any): string {
  const v = job.result?.validation;
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
    `| Duration | ${job.result?.duration_seconds ? job.result.duration_seconds.toFixed(1) + "s" : "—"} |`,
    ``,
    `## Objects Processed`,
    `| Type | Success | Failed | Skipped |`,
    `|------|---------|--------|---------|`,
  ];

  for (const key of ["tables", "views", "functions", "volumes"]) {
    const d = job.result?.[key];
    if (d) {
      lines.push(`| ${key} | ${d.success || 0} | ${d.failed || 0} | ${d.skipped || 0} |`);
    }
  }

  lines.push(`| schemas | ${job.result?.schemas_processed || 0} | — | — |`);

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

  if (job.result?.errors && job.result.errors.length > 0) {
    lines.push(``);
    lines.push(`## Warnings`);
    for (const err of job.result.errors) {
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

function LogPanel({ logs, jobId, isRunning }: { logs: string[]; jobId: string; isRunning: boolean }) {
  const [copied, setCopied] = useState(false);

  const logText = logs.join("\n");

  const handleCopy = async () => {
    await navigator.clipboard.writeText(logText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    downloadFile(logText, `clone-logs-${jobId}.log`, "text/plain");
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center justify-between">
          <span className="flex items-center gap-2">
            {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
            Logs
          </span>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">{logs.length} lines</Badge>
            <Button variant="ghost" size="sm" className="h-7 px-2" onClick={handleCopy}>
              {copied ? <Check className="h-3 w-3 text-green-500" /> : <ClipboardCopy className="h-3 w-3" />}
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
          className="bg-gray-900 text-gray-300 p-3 rounded-lg font-mono text-xs max-h-64 overflow-y-auto"
          ref={(el) => { if (el && isRunning) el.scrollTop = el.scrollHeight; }}
        >
          {logs.map((line: string, i: number) => (
            <div
              key={i}
              className={
                line.includes("ERROR") ? "text-red-400" :
                line.includes("WARNING") ? "text-yellow-400" :
                line.includes("OK") || line.includes("completed") || line.includes("success") ? "text-green-400" :
                line.includes("Scanning") || line.includes("Cloning") || line.includes("Validating") || line.includes("Schemas") ? "text-blue-400" :
                ""
              }
            >
              {line}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function JobProgress({ jobId }: { jobId: string }) {
  const [job, setJob] = useState<any>(null);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  // Poll job status
  useEffect(() => {
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${jobId}`);
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch (e) { /* ignore */ }
    };

    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [jobId]);

  // Polling handles progress updates (every 2s via the effect above)


  if (!job) {
    return (
      <div className="flex items-center gap-2 text-gray-500">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading job status...
      </div>
    );
  }

  const statusColor = {
    queued: "bg-yellow-100 text-yellow-800",
    running: "bg-blue-100 text-blue-800",
    completed: "bg-green-100 text-green-800",
    failed: "bg-red-100 text-red-800",
    cancelled: "bg-gray-100 text-gray-800",
  }[job.status] || "bg-gray-100 text-gray-800";

  const statusIcon = {
    queued: <Clock className="h-5 w-5 text-yellow-600" />,
    running: <Loader2 className="h-5 w-5 text-blue-600 animate-spin" />,
    completed: <CheckCircle className="h-5 w-5 text-green-600" />,
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
          <Badge className={statusColor}>
            {job.status.toUpperCase()}
          </Badge>
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
                <div className="h-full bg-blue-600 rounded-full animate-pulse" style={{ width: "60%" }} />
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
      {job.status === "completed" && job.result && (
        <div className="space-y-4">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <Card>
              <CardContent className="pt-4 text-center">
                <p className="text-2xl font-bold text-blue-700">{job.result.schemas_processed || 0}</p>
                <p className="text-xs text-gray-500">Schemas Processed</p>
              </CardContent>
            </Card>
            {["tables", "views", "functions", "volumes"].map((key) => {
              const d = job.result[key];
              if (!d) return null;
              const total = (d.success || 0) + (d.failed || 0) + (d.skipped || 0);
              return (
                <Card key={key}>
                  <CardContent className="pt-4 text-center">
                    <p className="text-2xl font-bold text-green-700">{d.success || 0}</p>
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

          {/* Duration */}
          {job.result.duration_seconds && (
            <p className="text-sm text-gray-500">Clone duration: {job.result.duration_seconds.toFixed(1)}s</p>
          )}

          {/* Validation Results */}
          {job.result.validation && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-green-600" />
                  Post-Clone Validation
                  <Badge className="ml-auto bg-green-100 text-green-800">
                    {job.result.validation.matched}/{job.result.validation.total_tables} matched
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {/* Validation summary */}
                <div className="grid grid-cols-4 gap-3 mb-4">
                  <div className="text-center p-2 bg-green-50 rounded">
                    <p className="text-lg font-bold text-green-700">{job.result.validation.matched}</p>
                    <p className="text-xs text-gray-500">Matched</p>
                  </div>
                  <div className="text-center p-2 bg-red-50 rounded">
                    <p className="text-lg font-bold text-red-700">{job.result.validation.mismatched}</p>
                    <p className="text-xs text-gray-500">Mismatched</p>
                  </div>
                  <div className="text-center p-2 bg-yellow-50 rounded">
                    <p className="text-lg font-bold text-yellow-700">{job.result.validation.errors}</p>
                    <p className="text-xs text-gray-500">Errors</p>
                  </div>
                  <div className="text-center p-2 bg-blue-50 rounded">
                    <p className="text-lg font-bold text-blue-700">{job.result.validation.total_tables}</p>
                    <p className="text-xs text-gray-500">Total Tables</p>
                  </div>
                </div>

                {/* Table details */}
                {job.result.validation.details && job.result.validation.details.length > 0 && (
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
                        {job.result.validation.details.map((row: any, i: number) => (
                          <tr key={i} className={`border-b ${row.match ? "" : row.error ? "bg-yellow-50" : "bg-red-50"}`}>
                            <td className="py-2 px-3">
                              {row.match ? (
                                <CheckCircle className="h-4 w-4 text-green-500" />
                              ) : row.error ? (
                                <AlertCircle className="h-4 w-4 text-yellow-500" />
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
          {job.result.errors && job.result.errors.length > 0 && (
            <Card className="border-yellow-200">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center gap-2 text-yellow-800">
                  <AlertCircle className="h-5 w-5" />
                  Warnings ({job.result.errors.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="max-h-40 overflow-y-auto text-sm space-y-1">
                  {job.result.errors.map((err: string, i: number) => (
                    <div key={i} className="text-yellow-700 font-mono text-xs">{err}</div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}

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

export default function ClonePage() {
  const [step, setStep] = useState<Step>("source");
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [config, setConfig] = useState({
    source_catalog: "",
    destination_catalog: "",
    clone_type: "DEEP" as "DEEP" | "SHALLOW",
    load_type: "FULL" as "FULL" | "INCREMENTAL",
    dry_run: false,
    max_workers: 4,
    parallel_tables: 1,
    max_parallel_queries: 10,
    max_rps: 0,
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
    generate_report: false,
    show_progress: true,
    auto_rollback: false,
    rollback_threshold: 5,
    checkpoint: false,
    require_approval: false,
    impact_check: false,
    skip_unused: false,
    verbose: false,
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
  });

  const startClone = useStartClone();

  const handleClone = (dryRun: boolean) => {
    startClone.mutate(
      { ...config, dry_run: dryRun },
      {
        onSuccess: (data: any) => {
          setActiveJobId(data.job_id);
          setStep("execute");
        },
      }
    );
    setStep("execute");
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Clone Catalog</h1>
        <p className="text-gray-500 mt-1">Clone a Unity Catalog catalog with full control</p>
      </div>

      {/* Step indicators */}
      <div className="flex gap-2">
        {(["source", "options", "preview", "execute"] as Step[]).map((s, i) => (
          <Badge
            key={s}
            variant={step === s ? "default" : "outline"}
            className={`cursor-pointer ${step === s ? "bg-blue-600" : ""}`}
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
              <Input
                placeholder="e.g. production"
                value={config.source_catalog}
                onChange={(e) => setConfig({ ...config, source_catalog: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input
                placeholder="e.g. staging"
                value={config.destination_catalog}
                onChange={(e) => setConfig({ ...config, destination_catalog: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Storage Location (optional)</label>
              <Input
                placeholder="abfss://container@storage.dfs.core.windows.net/path"
                value={config.location}
                onChange={(e) => setConfig({ ...config, location: e.target.value })}
              />
              <p className="text-xs text-gray-400 mt-1">Required if workspace uses Default Storage</p>
            </div>
            <Button onClick={() => setStep("options")} disabled={!config.source_catalog || !config.destination_catalog}>
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
          <CardContent className="space-y-6">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium">Clone Type</label>
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
                <label className="text-sm font-medium">Load Type</label>
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

            {/* Performance */}
            <div>
              <label className="text-sm font-medium mb-2 block">Performance</label>
              <div className="grid grid-cols-4 gap-4">
                <div>
                  <label className="text-xs text-gray-500">Max Workers (schemas)</label>
                  <Input type="number" min={1} max={16} value={config.max_workers}
                    onChange={(e) => setConfig({ ...config, max_workers: parseInt(e.target.value) || 4 })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Parallel Tables</label>
                  <Input type="number" min={1} max={8} value={config.parallel_tables}
                    onChange={(e) => setConfig({ ...config, parallel_tables: parseInt(e.target.value) || 1 })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Max Parallel Queries</label>
                  <Input type="number" min={1} max={50} value={config.max_parallel_queries}
                    onChange={(e) => setConfig({ ...config, max_parallel_queries: parseInt(e.target.value) || 10 })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Max RPS (0=unlimited)</label>
                  <Input type="number" min={0} max={100} value={config.max_rps}
                    onChange={(e) => setConfig({ ...config, max_rps: parseFloat(e.target.value) || 0 })} />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4 mt-3">
                <div>
                  <label className="text-xs text-gray-500">Order by Size</label>
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
                  <label className="text-xs text-gray-500">Throttle Profile</label>
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
                  </label>
                ))}
              </div>
            </div>

            {/* Auto Rollback Threshold */}
            {config.auto_rollback && (
              <div className="w-48">
                <label className="text-xs text-gray-500">Rollback Threshold (%)</label>
                <Input type="number" min={0} max={100} value={config.rollback_threshold}
                  onChange={(e) => setConfig({ ...config, rollback_threshold: parseFloat(e.target.value) || 5 })} />
              </div>
            )}

            {/* Filtering */}
            <div>
              <label className="text-sm font-medium mb-2 block">Filtering</label>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-xs text-gray-500">Include Schemas (comma-separated)</label>
                  <Input placeholder="e.g. bronze,silver,gold" value={config.include_schemas.join(",")}
                    onChange={(e) => setConfig({ ...config, include_schemas: e.target.value ? e.target.value.split(",").map(s => s.trim()) : [] })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Exclude Schemas (comma-separated)</label>
                  <Input value={config.exclude_schemas.join(",")}
                    onChange={(e) => setConfig({ ...config, exclude_schemas: e.target.value ? e.target.value.split(",").map(s => s.trim()) : [] })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Include Tables Regex</label>
                  <Input placeholder="e.g. ^fact_.*" value={config.include_tables_regex}
                    onChange={(e) => setConfig({ ...config, include_tables_regex: e.target.value })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Exclude Tables Regex</label>
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
                  <label className="text-xs text-gray-500">As-of Timestamp</label>
                  <Input type="datetime-local" value={config.as_of_timestamp}
                    onChange={(e) => setConfig({ ...config, as_of_timestamp: e.target.value })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">As-of Version</label>
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
                  <label className="text-xs text-gray-500">WHERE Clause (deep clone only)</label>
                  <Input placeholder="e.g. created_date > '2024-01-01'" value={config.where_clause}
                    onChange={(e) => setConfig({ ...config, where_clause: e.target.value })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">TTL (e.g. 7d, 30d, 2w)</label>
                  <Input placeholder="e.g. 7d" value={config.ttl}
                    onChange={(e) => setConfig({ ...config, ttl: e.target.value })} />
                </div>
                <div>
                  <label className="text-xs text-gray-500">Template</label>
                  <Input placeholder="e.g. dev-refresh, dr-replica" value={config.template}
                    onChange={(e) => setConfig({ ...config, template: e.target.value })} />
                </div>
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
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Eye className="h-5 w-5" />
              Preview Clone Configuration
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm space-y-1">
              <p>clone-catalog clone \</p>
              <p>  --source {config.source_catalog} --dest {config.destination_catalog} \</p>
              <p>  --clone-type {config.clone_type} --load-type {config.load_type} \</p>
              <p>  --max-workers {config.max_workers} --parallel-tables {config.parallel_tables} \</p>
              {!config.copy_permissions && <p>  --no-permissions \</p>}
              {!config.copy_tags && <p>  --no-tags \</p>}
              {!config.copy_security && <p>  --no-security \</p>}
              {config.enable_rollback && <p>  --enable-rollback \</p>}
              {config.validate_after_clone && <p>  --validate \</p>}
              {config.force_reclone && <p>  --force-reclone \</p>}
              {config.location && <p>  --location &quot;{config.location}&quot; \</p>}
              <p>  --progress</p>
            </div>

            <div className="flex gap-2">
              <Button variant="outline" onClick={() => setStep("options")}>Back</Button>
              <Button variant="outline" onClick={() => handleClone(true)} disabled={startClone.isPending}>
                {startClone.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Eye className="h-4 w-4 mr-2" />}
                Dry Run
              </Button>
              <Button onClick={() => handleClone(false)} className="bg-blue-600 hover:bg-blue-700" disabled={startClone.isPending}>
                {startClone.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
                Execute Clone
              </Button>
            </div>
          </CardContent>
        </Card>
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
              {config.copy_permissions && <Badge className="bg-green-100 text-green-800 text-xs">Permissions</Badge>}
              {config.copy_tags && <Badge className="bg-green-100 text-green-800 text-xs">Tags</Badge>}
              {config.copy_security && <Badge className="bg-green-100 text-green-800 text-xs">Security</Badge>}
              {config.copy_constraints && <Badge className="bg-green-100 text-green-800 text-xs">Constraints</Badge>}
              {config.copy_properties && <Badge className="bg-green-100 text-green-800 text-xs">Properties</Badge>}
              {config.copy_comments && <Badge className="bg-green-100 text-green-800 text-xs">Comments</Badge>}
              {config.enable_rollback && <Badge className="bg-blue-100 text-blue-800 text-xs">Rollback</Badge>}
              {config.auto_rollback && <Badge className="bg-blue-100 text-blue-800 text-xs">Auto-Rollback ({config.rollback_threshold}%)</Badge>}
              {config.validate_after_clone && <Badge className="bg-blue-100 text-blue-800 text-xs">Validate</Badge>}
              {config.validate_checksum && <Badge className="bg-blue-100 text-blue-800 text-xs">Checksum</Badge>}
              {config.generate_report && <Badge className="bg-blue-100 text-blue-800 text-xs">Report</Badge>}
              {config.checkpoint && <Badge className="bg-blue-100 text-blue-800 text-xs">Checkpoint</Badge>}
              {config.force_reclone && <Badge className="bg-orange-100 text-orange-800 text-xs">Force Re-clone</Badge>}
              {config.skip_unused && <Badge className="bg-orange-100 text-orange-800 text-xs">Skip Unused</Badge>}
              {config.impact_check && <Badge className="bg-purple-100 text-purple-800 text-xs">Impact Check</Badge>}
              {config.require_approval && <Badge className="bg-purple-100 text-purple-800 text-xs">Approval Required</Badge>}
              {config.dry_run && <Badge className="bg-yellow-100 text-yellow-800 text-xs">Dry Run</Badge>}
              {config.verbose && <Badge className="bg-gray-100 text-gray-800 text-xs">Verbose</Badge>}
              {!config.copy_permissions && <Badge className="bg-red-100 text-red-800 text-xs">No Permissions</Badge>}
              {!config.copy_ownership && <Badge className="bg-red-100 text-red-800 text-xs">No Ownership</Badge>}
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
                <span>Error: {(startClone.error as Error).message}</span>
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
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
