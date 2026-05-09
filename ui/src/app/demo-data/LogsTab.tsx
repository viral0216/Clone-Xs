// @ts-nocheck
//
// Logs tab on /demo-data — sibling of DocumentsTab/MediaTab/KnowledgeTab.
//
// Generates a corpus of realistic log files (NGINX access, JSON app
// logs, syslog, OpenTelemetry traces) into either a UC Volume, a
// Volume + per-file catalog table, or a direct (one-row-per-LINE)
// Delta table.
//
// Distinct from the other unstructured tabs:
//   1. Per-line direct_table — operators query log analytics by
//      level/timestamp, not by file. The direct table has columns
//      (log_id, log_type, service, ts TIMESTAMP, level, message,
//      attrs MAP<STRING,STRING>).
//   2. Each "count" is a number of FILES; lines_per_file controls
//      the per-file density (default 1000).
//   3. Files spread across the last N UTC days (`days_back`) so the
//      corpus has a multi-day shape with realistic peak-hour
//      timestamp clustering.
//   4. Per-type cap is 1000 files (not 10000) — at 1000 lines/file
//      that's already 1M lines per type.
//
// Pairs with backend:
//   - GET    /api/generate/demo-logs/types    → registry
//   - POST   /api/generate/demo-logs/preview  → estimate
//   - POST   /api/generate/demo-logs          → submit job
//   - GET    /api/clone/{job_id}              → poll progress

import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useDurableJob } from "@/hooks/useDurableJob";
import { toast } from "sonner";
import CatalogSchemaVolumePicker from "@/components/CatalogSchemaVolumePicker";
import {
  AlertTriangle,
  CheckCircle2,
  Loader2,
  Play,
  ScrollText,
} from "lucide-react";

type LogDestination = "volume" | "volume_with_catalog" | "direct_table";

interface LogTypeInfo {
  type: string;
  category: string;
  label: string;
  extension: string;
}

interface PerTypePreview {
  type: string;
  category: string;
  label: string;
  count: number;
  line_count: number;
  estimated_bytes: number;
  estimated_seconds: number;
}

interface PreviewResponse {
  per_type: PerTypePreview[];
  total_files: number;
  total_lines: number;
  total_bytes: number;
  estimated_seconds: number;
  unknown_types: string[];
}

interface TypesResponse {
  types: LogTypeInfo[];
  available: boolean;
  unavailable_reason: string | null;
}

const INDUSTRIES = [
  "healthcare", "financial", "retail", "telecom", "manufacturing",
  "energy", "education", "real_estate", "logistics", "insurance",
] as const;

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatNumber(n: number): string {
  return n.toLocaleString();
}

export default function LogsTab() {
  const [destination, setDestination] = useState<LogDestination>("volume_with_catalog");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [linesPerFile, setLinesPerFile] = useState(1000);
  const [daysBack, setDaysBack] = useState(7);

  const [selectedTypes, setSelectedTypes] = useState<Record<string, boolean>>({});
  const [counts, setCounts] = useState<Record<string, number>>({});

  const [typeRegistry, setTypeRegistry] = useState<LogTypeInfo[]>([]);
  const [available, setAvailable] = useState<boolean>(true);
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [registryLoading, setRegistryLoading] = useState(false);

  const [preview, setPreview] = useState<PreviewResponse | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const logsJob = useDurableJob({
    key: "demo-logs",
    pollUrl: (id) => `/clone/${id}`,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
  });

  // Fetch type registry on mount.
  useEffect(() => {
    setRegistryLoading(true);
    api
      .get<TypesResponse>("/generate/demo-logs/types")
      .then((res) => {
        setTypeRegistry(res.types || []);
        setAvailable(res.available);
        setUnavailableReason(res.unavailable_reason);
        const initialCounts: Record<string, number> = {};
        for (const t of res.types) initialCounts[t.type] = 5;
        setCounts(initialCounts);
      })
      .catch(() => {
        setTypeRegistry([]);
        setAvailable(false);
        setUnavailableReason("Could not load log types from the API.");
      })
      .finally(() => setRegistryLoading(false));
  }, []);

  const activeTypes = useMemo(
    () => Object.keys(selectedTypes).filter((k) => selectedTypes[k]),
    [selectedTypes],
  );

  const groupedTypes = useMemo(() => {
    const out: Record<string, LogTypeInfo[]> = {};
    for (const t of typeRegistry) {
      (out[t.category] ??= []).push(t);
    }
    return out;
  }, [typeRegistry]);

  // Live preview — debounced. Re-runs when lines_per_file changes
  // because it scales the line + byte estimates.
  useEffect(() => {
    if (activeTypes.length === 0) {
      setPreview(null);
      return;
    }
    const handle = setTimeout(() => {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 5;
      api
        .post<PreviewResponse>("/generate/demo-logs/preview", {
          types: activeTypes,
          counts: activeCounts,
          lines_per_file: linesPerFile,
        })
        .then(setPreview)
        .catch(() => setPreview(null));
    }, 200);
    return () => clearTimeout(handle);
  }, [activeTypes, counts, linesPerFile]);

  const volumeRequired = destination !== "direct_table";
  const canSubmit =
    available &&
    !submitting &&
    catalog.trim() &&
    schema.trim() &&
    (!volumeRequired || volume.trim()) &&
    activeTypes.length > 0 &&
    linesPerFile > 0;

  const submit = async () => {
    setSubmitting(true);
    setSubmitError("");
    try {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 5;
      const res = await api.post<{ job_id: string; status: string }>(
        "/generate/demo-logs",
        {
          catalog: catalog.trim(),
          schema: schema.trim(),
          volume: volumeRequired ? volume.trim() : undefined,
          destination,
          types: activeTypes,
          counts: activeCounts,
          industry,
          lines_per_file: linesPerFile,
          days_back: daysBack,
        },
      );
      logsJob.start({}, async () => res.job_id);
      toast.success(`Job ${res.job_id} submitted`);
    } catch (e: any) {
      const msg = e?.message || "Submission failed";
      setSubmitError(msg);
    } finally {
      setSubmitting(false);
    }
  };

  if (registryLoading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading log types…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {!available && (
        <div className="border border-amber-500/60 bg-amber-500/10 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-amber-700 dark:text-amber-200">
              Logs generator unavailable
            </p>
            <p className="text-amber-700 dark:text-amber-100 mt-1">
              {unavailableReason || "Internal error loading the generator."}
            </p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Form column — spans 2 of 3 */}
        <div className="lg:col-span-2 space-y-5">
          {/* Destination */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <ScrollText className="h-4 w-4" />
                Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                {[
                  {
                    value: "volume_with_catalog",
                    label: "Volume + per-file catalog (Recommended)",
                    desc: "Files in the Volume + a Delta table indexing them with per-file metadata (line_count, error_rate, day, service).",
                  },
                  {
                    value: "volume",
                    label: "Volume only",
                    desc: "Log files only — no Delta table. Inspect via the Volume browser. Best when the customer's pipeline already reads from a Volume path.",
                  },
                  {
                    value: "direct_table",
                    label: "Direct table (one row per LINE)",
                    desc: "Each log line lands as its own row with parsed columns (ts TIMESTAMP, level, message, attrs MAP). Natural shape for SQL log analytics: SELECT count(*) FROM demo_logs WHERE level='ERROR'. No Volume writes.",
                  },
                ].map(({ value, label, desc }) => (
                  <label
                    key={value}
                    className="flex gap-2 items-start cursor-pointer p-2 hover:bg-muted/50 rounded"
                  >
                    <input
                      type="radio"
                      name="logs-destination"
                      value={value}
                      checked={destination === value}
                      onChange={() => setDestination(value as LogDestination)}
                      className="mt-1"
                    />
                    <div className="text-sm">
                      <div className="font-medium">{label}</div>
                      <div className="text-xs text-muted-foreground">{desc}</div>
                    </div>
                  </label>
                ))}
              </div>

              <div className="pt-2">
                <CatalogSchemaVolumePicker
                  catalog={catalog}
                  setCatalog={setCatalog}
                  schema={schema}
                  setSchema={setSchema}
                  volume={volume}
                  setVolume={setVolume}
                  volumeEnabled={volumeRequired}
                  defaultVolumeName="demo_unstructured"
                />
              </div>
              {volumeRequired && (
                <p className="text-xs text-muted-foreground">
                  Volume is auto-created (<code className="px-1 bg-muted rounded">CREATE VOLUME IF NOT EXISTS</code>) if it doesn&apos;t exist. Files land in <code className="px-1 bg-muted rounded">/&lt;type&gt;/&lt;service&gt;/&lt;day&gt;/&lt;file&gt;</code> sub-paths.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Industry + per-file density + days_back */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Content options</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="logs-industry">
                  Industry context
                </label>
                <select
                  id="logs-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Drives the service-name pool and (for nginx_access) the URL path templates.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="text-xs font-medium mb-1 block">
                    Lines per file
                  </label>
                  <Input
                    type="number"
                    value={linesPerFile}
                    onChange={(e) =>
                      setLinesPerFile(Math.max(1, Math.min(100_000, parseInt(e.target.value) || 1)))
                    }
                    min={1}
                    max={100_000}
                    className="h-8 text-sm"
                  />
                  <p className="text-xs text-muted-foreground mt-1">
                    Default 1000. Capped at 100,000.
                  </p>
                </div>
                <div>
                  <label className="text-xs font-medium mb-1 block">
                    Days back (UTC)
                  </label>
                  <Input
                    type="number"
                    value={daysBack}
                    onChange={(e) =>
                      setDaysBack(Math.max(1, Math.min(365, parseInt(e.target.value) || 1)))
                    }
                    min={1}
                    max={365}
                    className="h-8 text-sm"
                  />
                  <p className="text-xs text-muted-foreground mt-1">
                    Files spread across last N days for partition demos.
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Log types */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Log types</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {Object.entries(groupedTypes).map(([category, types]) => (
                <div key={category}>
                  <div className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-1.5">
                    {category}
                  </div>
                  <div className="space-y-1.5">
                    {types.map((t) => (
                      <div key={t.type} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={!!selectedTypes[t.type]}
                          onChange={(e) =>
                            setSelectedTypes({
                              ...selectedTypes,
                              [t.type]: e.target.checked,
                            })
                          }
                        />
                        <span className="flex-1">
                          {t.label}
                          <span className="text-muted-foreground ml-1.5 font-mono text-xs">
                            .{t.extension}
                          </span>
                        </span>
                        <Input
                          type="number"
                          value={counts[t.type] ?? 5}
                          onChange={(e) =>
                            setCounts({
                              ...counts,
                              [t.type]: Math.max(0, Math.min(1000, parseInt(e.target.value) || 0)),
                            })
                          }
                          disabled={!selectedTypes[t.type]}
                          min={0}
                          max={1000}
                          className="w-20 h-7 text-xs"
                        />
                      </div>
                    ))}
                  </div>
                </div>
              ))}
              {activeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic">
                  Pick at least one log type to enable submit. Each count is a number of files; total lines = files × lines-per-file.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Submit */}
          <div className="flex items-center gap-3">
            <Button onClick={submit} disabled={!canSubmit} size="lg">
              {submitting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Submitting…
                </>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-2" />
                  Generate logs
                </>
              )}
            </Button>
            {submitError && (
              <span className="text-sm text-red-500">{submitError}</span>
            )}
          </div>
        </div>

        {/* Live preview + progress column */}
        <div className="space-y-5">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Estimate</CardTitle>
            </CardHeader>
            <CardContent>
              {preview ? (
                <div className="space-y-2 text-sm">
                  <div className="flex items-baseline justify-between border-b pb-2">
                    <span className="font-medium">{preview.total_files} files</span>
                    <span className="font-mono text-xs text-muted-foreground">
                      {formatBytes(preview.total_bytes)}
                    </span>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {formatNumber(preview.total_lines)} log lines · est {preview.estimated_seconds.toFixed(1)}s
                  </div>
                  <div className="space-y-0.5 pt-2">
                    {preview.per_type.map((p) => (
                      <div key={p.type} className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground">{p.label}</span>
                        <span className="font-mono">
                          {p.count}f · {formatNumber(p.line_count)}L · {formatBytes(p.estimated_bytes)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground italic">
                  Pick types above to see an estimate.
                </p>
              )}
            </CardContent>
          </Card>

          {logsJob.jobId && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  {logsJob.data?.status === "completed" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                  {logsJob.data?.status === "running" && <Loader2 className="h-4 w-4 animate-spin" />}
                  {logsJob.data?.status === "failed" && <AlertTriangle className="h-4 w-4 text-red-500" />}
                  Job {logsJob.jobId}
                  <Badge variant="outline" className="text-xs">
                    {logsJob.data?.status ?? "queued"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {logsJob.data?.progress && (
                  <>
                    <div className="flex items-baseline justify-between border-b pb-2">
                      <span className="font-medium">
                        {logsJob.data.progress.files_written ?? 0} files
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatBytes(logsJob.data.progress.total_bytes ?? 0)}
                      </span>
                    </div>
                    {(logsJob.data.progress.lines_written ?? 0) > 0 && (
                      <div className="text-xs text-muted-foreground">
                        {formatNumber(logsJob.data.progress.lines_written ?? 0)} log lines written
                      </div>
                    )}
                    {logsJob.data.progress.per_type && (
                      <div className="space-y-0.5 pt-1">
                        {Object.entries(logsJob.data.progress.per_type).map(([type, n]) => (
                          <div key={type} className="flex items-center justify-between text-xs">
                            <span className="text-muted-foreground">{type}</span>
                            <span className="font-mono">{n as number}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {logsJob.data.progress.current_path && (
                      <div className="text-xs text-muted-foreground font-mono break-all">
                        Current: {logsJob.data.progress.current_path}
                      </div>
                    )}
                  </>
                )}
                {logsJob.data?.result && (
                  <div className="border-t pt-2 space-y-1">
                    {logsJob.data.result.table_fqn && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Table: </span>
                        <code className="px-1 bg-muted rounded font-mono">
                          {logsJob.data.result.table_fqn}
                        </code>
                      </div>
                    )}
                    {logsJob.data.result.volume_path && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Volume: </span>
                        <code className="px-1 bg-muted rounded font-mono break-all">
                          {logsJob.data.result.volume_path}
                        </code>
                      </div>
                    )}
                  </div>
                )}
                {logsJob.data?.error && (
                  <div className="text-xs text-red-500 border-t pt-2">
                    {logsJob.data.error}
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
