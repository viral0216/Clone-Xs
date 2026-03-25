// @ts-nocheck
import React, { useState, useEffect, useRef, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useVolumes } from "@/hooks/useApi";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  RefreshCw, Loader2, CheckCircle, XCircle, ArrowRight,
  AlertTriangle, Database, Table2, Clock, ChevronDown, ChevronRight,
  Download, ClipboardCopy, Check, GitCompareArrows, History,
  CalendarClock, ExternalLink, Zap,
} from "lucide-react";


/* ── Reusable progress bar ────────────────────────────── */
function ProgressBar({ value, max, label }: { value: number; max: number; label?: string }) {
  const pct = max > 0 ? Math.round((value / max) * 100) : 0;
  return (
    <div className="space-y-1">
      {label && (
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>{label}</span>
          <span>{value}/{max} ({pct}%)</span>
        </div>
      )}
      <div className="w-full bg-muted rounded-full h-2.5 overflow-hidden">
        <div
          className="h-full bg-[#E8453C] rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}


/* ── Log panel with color coding ─────────────────────── */
function LogPanel({ logs, jobId, isRunning }: { logs: string[]; jobId: string; isRunning: boolean }) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (isRunning && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs.length, isRunning]);

  function logColor(line: string) {
    if (/error|ERROR|FAILED|failed/i.test(line)) return "text-red-400";
    if (/warn|WARNING/i.test(line)) return "text-gray-400";
    if (/OK|success|cloned|completed|matched/i.test(line)) return "text-gray-300";
    if (/progress|running|scanning|cloning/i.test(line)) return "text-[#E8453C]";
    return "text-gray-300";
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <p className="text-sm font-medium text-muted-foreground flex items-center gap-2">
          {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
          Logs
          <Badge variant="outline" className="text-xs">{logs.length} lines</Badge>
        </p>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => {
            navigator.clipboard.writeText(logs.join("\n"));
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
          }}
        >
          {copied ? <Check className="h-3 w-3 mr-1" /> : <ClipboardCopy className="h-3 w-3 mr-1" />}
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <div className="bg-[#0d1117] rounded-lg p-3 max-h-60 overflow-y-auto font-mono text-xs">
        {logs.map((line, i) => (
          <div key={i} className={logColor(line)}>{line}</div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}


/* ── Per-schema job progress tracker ─────────────────── */
function SyncJobProgress({ jobId, schema }: { jobId: string; schema: string }) {
  const [job, setJob] = useState<any>(null);
  const [expanded, setExpanded] = useState(true);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${jobId}`);
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch {}
    };
    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [jobId]);

  const statusColor: Record<string, string> = {
    queued: "bg-muted/200/20 text-gray-400 border-border/30",
    running: "bg-muted/300/20 text-[#E8453C] border-[#E8453C]/30",
    completed: "bg-muted/200/20 text-gray-300 border-border/30",
    failed: "bg-red-500/20 text-red-400 border-red-500/30",
  };

  const statusIcon: Record<string, React.ReactNode> = {
    queued: <Clock className="h-4 w-4 text-muted-foreground" />,
    running: <Loader2 className="h-4 w-4 text-[#E8453C] animate-spin" />,
    completed: <CheckCircle className="h-4 w-4 text-foreground" />,
    failed: <XCircle className="h-4 w-4 text-red-500" />,
  };

  if (!job) {
    return (
      <Card className="bg-card border-border">
        <CardContent className="pt-4">
          <div className="flex items-center gap-3">
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
            <span className="text-sm text-muted-foreground">{schema}: Loading job {jobId}...</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  const result = job.result || {};
  const synced = result.synced ?? result.tables?.success ?? result.tables?.cloned ?? 0;
  const failed = result.failed ?? result.tables?.failed ?? 0;

  return (
    <Card className={`bg-card border-border transition-all ${
      job.status === "running" ? "ring-1 ring-[#E8453C]/40" : ""
    }`}>
      {/* Header — always visible */}
      <div
        className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-muted/30"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-3">
          {expanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
          {statusIcon[job.status] || statusIcon.queued}
          <div>
            <span className="font-medium text-foreground">{schema}</span>
            <span className="text-xs text-muted-foreground ml-2">Job {jobId}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {job.status === "completed" && (
            <span className="text-xs text-muted-foreground">
              {synced} synced{failed > 0 ? `, ${failed} failed` : ""}
            </span>
          )}
          {job.started_at && job.completed_at && (
            <span className="text-xs text-muted-foreground">
              {Math.round((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 1000)}s
            </span>
          )}
          <Badge className={statusColor[job.status] || statusColor.queued}>
            {job.status.toUpperCase()}
          </Badge>
        </div>
      </div>

      {/* Expanded details */}
      {expanded && (
        <CardContent className="pt-0 space-y-3">
          {/* Progress */}
          {job.status === "running" && (
            <div className="space-y-2">
              {job.progress ? (
                <>
                  <ProgressBar value={job.progress.completed_tables || 0} max={job.progress.total_tables || 0} label="Tables" />
                  {job.progress.current_table && (
                    <p className="text-xs text-muted-foreground">
                      Current: {job.progress.current_schema}.{job.progress.current_table}
                    </p>
                  )}
                </>
              ) : (
                <div className="space-y-1">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-3 w-3 animate-spin" /> Sync in progress...
                  </div>
                  <div className="w-full bg-muted rounded-full h-2 overflow-hidden">
                    <div className="h-full bg-[#E8453C] rounded-full animate-pulse" style={{ width: "60%" }} />
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Timing */}
          <div className="flex gap-4 text-xs text-muted-foreground">
            {job.started_at && <span>Started: {new Date(job.started_at).toLocaleTimeString()}</span>}
            {job.completed_at && <span>Completed: {new Date(job.completed_at).toLocaleTimeString()}</span>}
          </div>

          {/* Result summary */}
          {job.status === "completed" && (
            <div className="grid grid-cols-3 gap-2">
              <div className="text-center p-2 rounded bg-muted/50">
                <p className="text-lg font-bold text-foreground">{synced}</p>
                <p className="text-xs text-muted-foreground">Synced</p>
              </div>
              <div className="text-center p-2 rounded bg-muted/50">
                <p className="text-lg font-bold text-red-500">{failed}</p>
                <p className="text-xs text-muted-foreground">Failed</p>
              </div>
              <div className="text-center p-2 rounded bg-muted/50">
                <p className="text-lg font-bold text-foreground">{result.tables_checked || result.schemas || 0}</p>
                <p className="text-xs text-muted-foreground">Checked</p>
              </div>
            </div>
          )}

          {/* Error */}
          {job.status === "failed" && job.error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded p-3">
              <div className="flex items-center gap-2 mb-1">
                <XCircle className="h-4 w-4 text-red-500" />
                <span className="text-sm font-medium text-red-400">Sync failed</span>
              </div>
              <pre className="text-xs text-red-300 whitespace-pre-wrap">{job.error}</pre>
            </div>
          )}

          {/* Logs */}
          {job.logs && job.logs.length > 0 && (
            <LogPanel logs={job.logs} jobId={jobId} isRunning={job.status === "running"} />
          )}
        </CardContent>
      )}
    </Card>
  );
}

interface ChangedTable {
  table_name: string;
  reason: string;
  last_synced_version: number | null;
  current_version?: number;
  changes_since_sync?: number;
  operations?: string[];
}

interface SchemaCheck {
  schema: string;
  tables_needing_sync: number;
  tables: ChangedTable[];
  loading?: boolean;
  error?: string;
}

export default function IncrementalSyncPage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState("");
  const [schemaResults, setSchemaResults] = useState<SchemaCheck[]>([]);
  const [syncResult, setSyncResult] = useState<any>(null);
  const [serverless, setServerless] = useState(false);
  const [volume, setVolume] = useState("");
  const volumes = useVolumes();
  // Track selected tables as "schema.table_name" keys
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  // Sync history
  const [syncHistory, setSyncHistory] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  // Schedule form
  const [showSchedule, setShowSchedule] = useState(false);
  const [schedCron, setSchedCron] = useState("0 0 6 * * ?");
  const [schedEmail, setSchedEmail] = useState("");
  const [schedResult, setSchedResult] = useState<any>(null);
  const [scheduling, setScheduling] = useState(false);
  // Expanded table rows for diff preview
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  function tableKey(schema: string, table: string) { return `${schema}.${table}`; }

  // Load sync history when both catalogs are set
  useEffect(() => {
    if (sourceCatalog && destCatalog) {
      setHistoryLoading(true);
      api.get(`/audit/sync-history?source=${sourceCatalog}&dest=${destCatalog}&limit=5`)
        .then((res) => setSyncHistory(Array.isArray(res) ? res : []))
        .catch(() => setSyncHistory([]))
        .finally(() => setHistoryLoading(false));
    } else {
      setSyncHistory([]);
    }
  }, [sourceCatalog, destCatalog]);

  // Fetch schemas for source catalog to iterate
  async function checkChanges() {
    setLoading(true);
    setError("");
    setSchemaResults([]);
    setSyncResult(null);
    setSelectedTables(new Set());

    try {
      if (sourceSchema) {
        // Single schema mode
        const data = await api.post("/incremental/check", {
          source_catalog: sourceCatalog,
          destination_catalog: destCatalog,
          schema_name: sourceSchema,
        });
        const result = { ...data, schema: sourceSchema };
        setSchemaResults([result]);
        // Auto-select all changed tables
        const keys = new Set((result.tables || []).map((t: ChangedTable) => tableKey(sourceSchema, t.table_name)));
        setSelectedTables(keys);
      } else {
        // All schemas mode — get schema list then check each
        const schemas = await api.get<string[]>(`/catalogs/${sourceCatalog}/schemas`);
        const filtered = (schemas || []).filter(
          (s) => s !== "information_schema" && s !== "default"
        );

        // Initialize with loading states
        const initial = filtered.map((s) => ({ schema: s, tables_needing_sync: 0, tables: [] as ChangedTable[], loading: true }));
        setSchemaResults(initial);

        // Fire all checks in parallel — each updates state as it resolves
        const promises = filtered.map(async (s, idx) => {
          try {
            const data = await api.post("/incremental/check", {
              source_catalog: sourceCatalog,
              destination_catalog: destCatalog,
              schema_name: s,
            });
            const result: SchemaCheck = { ...data, schema: s, loading: false };

            // Update this schema's result in-place
            setSchemaResults((prev) => prev.map((r, i) => i === idx ? result : r));

            // Auto-select changed tables as they come in
            if (result.tables?.length) {
              setSelectedTables((prev) => {
                const next = new Set(prev);
                result.tables.forEach((t: ChangedTable) => next.add(tableKey(s, t.table_name)));
                return next;
              });
            }
          } catch (err: any) {
            setSchemaResults((prev) =>
              prev.map((r, i) => i === idx ? { ...r, loading: false, error: err.message } : r)
            );
          }
        });

        await Promise.all(promises);
      }
    } catch (e: any) {
      setError(e.message || "Failed to check changes");
    } finally {
      setLoading(false);
    }
  }

  async function runSync() {
    setSyncing(true);
    setError("");
    setSyncResult(null);

    // Group selected tables by schema
    const bySchema = new Map<string, string[]>();
    selectedTables.forEach((key) => {
      const [schema, ...rest] = key.split(".");
      const tbl = rest.join(".");
      if (!bySchema.has(schema)) bySchema.set(schema, []);
      bySchema.get(schema)!.push(tbl);
    });

    const results: any[] = [];
    try {
      for (const [schema, tables] of bySchema.entries()) {
        const data = await api.post("/incremental/sync", {
          source_catalog: sourceCatalog,
          destination_catalog: destCatalog,
          schema_name: schema,
          serverless,
          volume: serverless ? volume : null,
        });
        results.push({ schema, tables: tables.length, ...data });
      }
      setSyncResult({ schemas: results, total: selectedTables.size });
    } catch (e: any) {
      setError(e.message || "Sync failed");
    } finally {
      setSyncing(false);
    }
  }

  // Toggle a single table
  function toggleTable(schema: string, table: string) {
    const key = tableKey(schema, table);
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  // Toggle all tables in a schema
  function toggleSchema(schema: string, tables: ChangedTable[]) {
    const keys = tables.map((t) => tableKey(schema, t.table_name));
    const allSelected = keys.every((k) => selectedTables.has(k));
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (allSelected) {
        keys.forEach((k) => next.delete(k));
      } else {
        keys.forEach((k) => next.add(k));
      }
      return next;
    });
  }

  function selectAll() {
    const allKeys = new Set<string>();
    schemaResults.forEach((r) => {
      (r.tables || []).forEach((t) => allKeys.add(tableKey(r.schema, t.table_name)));
    });
    setSelectedTables(allKeys);
  }

  function deselectAll() {
    setSelectedTables(new Set());
  }

  // Schedule recurring sync
  async function createSchedule() {
    setScheduling(true);
    setSchedResult(null);
    try {
      const res = await api.post("/create-job", {
        source_catalog: sourceCatalog,
        destination_catalog: destCatalog,
        load_type: "INCREMENTAL",
        clone_type: "DEEP",
        schedule: schedCron,
        job_name: `incremental-sync-${sourceCatalog}-to-${destCatalog}`,
        notification_emails: schedEmail ? schedEmail.split(",").map(e => e.trim()).filter(Boolean) : [],
        include_schemas: sourceSchema ? [sourceSchema] : [],
        tags: { created_by: "clone-xs", type: "incremental-sync" },
      });
      setSchedResult(res);
    } catch (e: any) {
      setSchedResult({ error: e.message });
    }
    setScheduling(false);
  }

  // Operation type stats
  const opBreakdown = useMemo(() => {
    const counts: Record<string, number> = {};
    schemaResults.forEach(sr => {
      (sr.tables || []).forEach(t => {
        (t.operations || []).forEach(op => {
          counts[op] = (counts[op] || 0) + 1;
        });
      });
    });
    return counts;
  }, [schemaResults]);

  const totalChanges = schemaResults.reduce((sum, r) => sum + r.tables_needing_sync, 0);
  const selectedCount = selectedTables.size;

  // How many tables selected in a given schema
  function schemaSelectedCount(schema: string, tables: ChangedTable[]) {
    return tables.filter((t) => selectedTables.has(tableKey(schema, t.table_name))).length;
  }

  function isSchemaAllSelected(schema: string, tables: ChangedTable[]) {
    return tables.length > 0 && tables.every((t) => selectedTables.has(tableKey(schema, t.table_name)));
  }

  function isSchemaSomeSelected(schema: string, tables: ChangedTable[]) {
    const count = schemaSelectedCount(schema, tables);
    return count > 0 && count < tables.length;
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Incremental Sync"
        icon={GitCompareArrows}
        breadcrumbs={["Operations", "Incremental Sync"]}
        description="Sync only tables that have changed since the last operation using Delta table version history. Dramatically faster than full sync for large catalogs."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/delta/history"
        docsLabel="Delta table history"
      />

      {/* Summary Stats — shown after check */}
      {schemaResults.length > 0 && !schemaResults.every(r => r.loading) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Schemas Checked</span>
                <Database className="h-4 w-4 text-[#E8453C]" />
              </div>
              <p className="text-2xl font-bold text-foreground">{schemaResults.filter(r => !r.loading).length}</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Tables Changed</span>
                <AlertTriangle className="h-4 w-4 text-muted-foreground" />
              </div>
              <p className="text-2xl font-bold text-foreground">{totalChanges}</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Up to Date</span>
                <CheckCircle className="h-4 w-4 text-foreground" />
              </div>
              <p className="text-2xl font-bold text-foreground">{schemaResults.filter(r => !r.loading && r.tables_needing_sync === 0).length}</p>
              <p className="text-xs text-muted-foreground mt-0.5">schemas in sync</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Operations</span>
                <Zap className="h-4 w-4 text-muted-foreground" />
              </div>
              <div className="flex flex-wrap gap-1 mt-1">
                {Object.entries(opBreakdown).length === 0 ? (
                  <span className="text-sm text-muted-foreground">—</span>
                ) : Object.entries(opBreakdown).map(([op, count]) => (
                  <Badge key={op} variant="outline" className="text-[10px]">{op}: {count}</Badge>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Source & Destination Selection */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto_1fr] gap-4 items-end">
            {/* Source */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Source</p>
              <CatalogPicker
                catalog={sourceCatalog}
                schema={sourceSchema}
                onCatalogChange={(c) => { setSourceCatalog(c); setSourceSchema(""); }}
                onSchemaChange={setSourceSchema}
                showTable={false}
              />
            </div>

            <div className="flex items-center justify-center pb-2">
              <ArrowRight className="h-5 w-5 text-muted-foreground" />
            </div>

            {/* Destination */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Destination</p>
              <CatalogPicker
                catalog={destCatalog}
                onCatalogChange={setDestCatalog}
                showSchema={false}
                showTable={false}
              />
            </div>
          </div>

          {/* Serverless toggle */}
          <div className="flex gap-4 items-start">
            <label className="flex items-center gap-2 text-sm cursor-pointer pt-1">
              <input
                type="checkbox"
                checked={serverless}
                onChange={(e) => setServerless(e.target.checked)}
                className="h-4 w-4 rounded border-border"
              />
              Use Serverless Compute
            </label>
            {serverless && (
              <div className="flex-1 max-w-md">
                <label className="text-xs text-muted-foreground mb-1 block">UC Volume (required for serverless)</label>
                {volumes.isLoading ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                    <Loader2 className="h-4 w-4 animate-spin" /> Loading volumes...
                  </div>
                ) : volumes.isError ? (
                  <Input
                    placeholder="/Volumes/catalog/schema/volume"
                    value={volume}
                    onChange={(e) => setVolume(e.target.value)}
                  />
                ) : (
                  <select
                    className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]"
                    value={volume}
                    onChange={(e) => setVolume(e.target.value)}
                  >
                    <option value="">Select a volume...</option>
                    {(volumes.data || []).map((v: any) => (
                      <option key={v.path} value={v.path}>
                        {v.path} ({v.type})
                      </option>
                    ))}
                  </select>
                )}
              </div>
            )}
          </div>

          <div className="flex items-center gap-3 pt-2">
            <Button onClick={checkChanges} disabled={!sourceCatalog || !destCatalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {sourceSchema ? `Check ${sourceSchema}` : "Check All Schemas"}
            </Button>
            {!sourceSchema && sourceCatalog && (
              <span className="text-xs text-muted-foreground">
                Leave schema empty to scan all schemas
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Error */}
      {error && (
        <Card className="border-red-500/30 bg-red-500/5">
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-red-500">
              <XCircle className="h-5 w-5" /> {error}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Sync Job Progress — live tracking per schema */}
      {syncResult && syncResult.schemas.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
            <RefreshCw className="h-4 w-4" />
            Sync Progress — {syncResult.schemas.length} schema(s)
          </div>
          {syncResult.schemas.map((s: any) => (
            <SyncJobProgress key={s.job_id} jobId={s.job_id} schema={s.schema} />
          ))}
        </div>
      )}

      {/* Summary Bar */}
      {schemaResults.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <Database className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm text-foreground font-medium">
                    {schemaResults.filter((r) => !r.loading).length}/{schemaResults.length} schemas checked
                  </span>
                  {schemaResults.some((r) => r.loading) && (
                    <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
                  )}
                </div>
                <Badge variant={totalChanges > 0 ? "destructive" : "secondary"}>
                  {totalChanges} tables need sync
                </Badge>
                {selectedCount > 0 && (
                  <Badge variant="outline">
                    {selectedCount} table{selectedCount !== 1 ? "s" : ""} selected
                  </Badge>
                )}
              </div>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="sm" onClick={selectAll}>Select all</Button>
                <Button variant="ghost" size="sm" onClick={deselectAll}>Clear</Button>
                <Button
                  onClick={runSync}
                  disabled={selectedCount === 0 || syncing || (serverless && !volume)}
                >
                  {syncing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                  {serverless ? "Serverless " : ""}Sync {selectedCount} Table{selectedCount !== 1 ? "s" : ""}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Sync History + Schedule */}
      {sourceCatalog && destCatalog && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Sync History */}
          <Card className="bg-card border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <History className="h-4 w-4" /> Sync History
              </CardTitle>
            </CardHeader>
            <CardContent>
              {historyLoading ? (
                <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground"><Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading...</div>
              ) : syncHistory.length === 0 ? (
                <p className="text-sm text-muted-foreground py-2">No previous syncs found for this pair</p>
              ) : (
                <div className="space-y-2">
                  {syncHistory.map((h, i) => (
                    <div key={i} className="flex items-center justify-between px-2 py-1.5 rounded hover:bg-muted/30">
                      <div className="flex items-center gap-2 min-w-0">
                        {h.status === "success" || h.status === "completed" ? (
                          <CheckCircle className="h-3.5 w-3.5 text-foreground shrink-0" />
                        ) : h.status === "failed" ? (
                          <XCircle className="h-3.5 w-3.5 text-red-500 shrink-0" />
                        ) : (
                          <Clock className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        )}
                        <span className="text-xs text-foreground truncate">
                          {h.tables_cloned || 0} tables synced{h.tables_failed > 0 ? `, ${h.tables_failed} failed` : ""}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 shrink-0 text-xs text-muted-foreground">
                        {h.duration_seconds && <span>{Math.round(h.duration_seconds)}s</span>}
                        <span>{h.started_at ? new Date(h.started_at).toLocaleDateString() : ""}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Schedule Recurring Sync */}
          <Card className="bg-card border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <CalendarClock className="h-4 w-4" /> Schedule Recurring Sync
              </CardTitle>
            </CardHeader>
            <CardContent>
              {schedResult?.job_url ? (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-foreground">
                    <CheckCircle className="h-4 w-4" /> Job created successfully
                  </div>
                  <a href={schedResult.job_url} target="_blank" rel="noopener noreferrer" className="text-sm text-[#E8453C] hover:underline flex items-center gap-1">
                    Open in Databricks <ExternalLink className="h-3 w-3" />
                  </a>
                  <p className="text-xs text-muted-foreground">Job: {schedResult.job_name}</p>
                </div>
              ) : schedResult?.error ? (
                <div className="text-sm text-red-500 flex items-center gap-2">
                  <XCircle className="h-4 w-4" /> {schedResult.error}
                </div>
              ) : (
                <div className="space-y-3">
                  <div>
                    <label className="text-xs font-medium text-muted-foreground block mb-1">Schedule (Quartz Cron)</label>
                    <div className="flex gap-2">
                      <select
                        className="flex-1 h-9 px-3 rounded-md border border-border bg-background text-sm text-foreground"
                        value={schedCron}
                        onChange={(e) => setSchedCron(e.target.value)}
                      >
                        <option value="0 0 */6 * * ?">Every 6 hours</option>
                        <option value="0 0 6 * * ?">Daily at 6:00 AM</option>
                        <option value="0 0 0 * * ?">Daily at midnight</option>
                        <option value="0 0 6 ? * MON-FRI">Weekdays at 6:00 AM</option>
                        <option value="0 0 2 ? * SUN">Weekly Sunday 2:00 AM</option>
                        <option value="0 0 */1 * * ?">Every hour</option>
                      </select>
                    </div>
                  </div>
                  <div>
                    <label className="text-xs font-medium text-muted-foreground block mb-1">Notification Email (optional)</label>
                    <Input
                      placeholder="team@company.com"
                      value={schedEmail}
                      onChange={(e) => setSchedEmail(e.target.value)}
                      className="text-sm"
                    />
                  </div>
                  <Button onClick={createSchedule} disabled={scheduling || !sourceCatalog || !destCatalog} size="sm" className="w-full">
                    {scheduling ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <CalendarClock className="h-3.5 w-3.5 mr-1.5" />}
                    {scheduling ? "Creating..." : "Create Scheduled Job"}
                  </Button>
                  <p className="text-[10px] text-muted-foreground">
                    Creates a persistent Databricks Job with incremental load type. Appears in your workspace Jobs list.
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* Per-Schema Results */}
      {schemaResults.length > 0 && (
        <div className="space-y-3">
          {schemaResults.map((sr) => (
            <Card
              key={sr.schema}
              className={`bg-card border-border transition-all ${
                schemaSelectedCount(sr.schema, sr.tables || []) > 0 ? "ring-1 ring-[#1A73E8]/40" : ""
              }`}
            >
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    {sr.tables_needing_sync > 0 && (
                      <input
                        type="checkbox"
                        checked={isSchemaAllSelected(sr.schema, sr.tables || [])}
                        ref={(el) => { if (el) el.indeterminate = isSchemaSomeSelected(sr.schema, sr.tables || []); }}
                        onChange={() => toggleSchema(sr.schema, sr.tables || [])}
                        className="h-4 w-4 rounded border-border"
                        title="Select/deselect all tables in this schema"
                      />
                    )}
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <CardTitle className="text-base">{sr.schema}</CardTitle>
                    {sr.tables_needing_sync > 0 && schemaSelectedCount(sr.schema, sr.tables || []) > 0 && (
                      <span className="text-xs text-muted-foreground">
                        ({schemaSelectedCount(sr.schema, sr.tables || [])}/{sr.tables_needing_sync} selected)
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {sr.loading ? (
                      <Badge variant="secondary"><Loader2 className="h-3 w-3 animate-spin mr-1" /> Checking...</Badge>
                    ) : sr.error ? (
                      <Badge variant="destructive">Error</Badge>
                    ) : sr.tables_needing_sync === 0 ? (
                      <Badge variant="secondary" className="text-foreground">
                        <CheckCircle className="h-3 w-3 mr-1" /> Up to date
                      </Badge>
                    ) : (
                      <Badge variant="destructive">
                        <AlertTriangle className="h-3 w-3 mr-1" /> {sr.tables_needing_sync} changed
                      </Badge>
                    )}
                  </div>
                </div>
              </CardHeader>

              {sr.tables && sr.tables.length > 0 && (
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-left text-muted-foreground">
                          <th className="py-2 px-3 w-8"></th>
                          <th className="py-2 px-3">Table</th>
                          <th className="py-2 px-3">Status</th>
                          <th className="py-2 px-3">Last Synced</th>
                          <th className="py-2 px-3">Current</th>
                          <th className="py-2 px-3">Changes</th>
                          <th className="py-2 px-3">Operations</th>
                        </tr>
                      </thead>
                      <tbody>
                        {sr.tables.map((t) => (
                          <React.Fragment key={t.table_name}>
                          <tr
                            className={`border-b border-border/50 hover:bg-muted/50 cursor-pointer ${
                              selectedTables.has(tableKey(sr.schema, t.table_name)) ? "bg-[#1A73E8]/5" : ""
                            }`}
                            onClick={() => toggleTable(sr.schema, t.table_name)}
                          >
                            <td className="py-2 px-3">
                              <input
                                type="checkbox"
                                checked={selectedTables.has(tableKey(sr.schema, t.table_name))}
                                onChange={() => toggleTable(sr.schema, t.table_name)}
                                onClick={(e) => e.stopPropagation()}
                                className="h-4 w-4 rounded border-border"
                              />
                            </td>
                            <td className="py-2 px-3 font-mono text-foreground flex items-center gap-2">
                              <Table2 className="h-3 w-3 text-muted-foreground" /> {t.table_name}
                            </td>
                            <td className="py-2 px-3">
                              <Badge variant={t.reason === "never_synced" ? "destructive" : "secondary"} className="text-xs">
                                {t.reason === "never_synced" ? "New" : "Changed"}
                              </Badge>
                            </td>
                            <td className="py-2 px-3 text-muted-foreground font-mono text-xs">
                              {t.last_synced_version != null ? `v${t.last_synced_version}` : "—"}
                            </td>
                            <td className="py-2 px-3 text-muted-foreground font-mono text-xs">
                              {t.current_version != null ? `v${t.current_version}` : "—"}
                            </td>
                            <td className="py-2 px-3 text-muted-foreground">
                              {t.changes_since_sync != null ? t.changes_since_sync : "—"}
                            </td>
                            <td className="py-2 px-3">
                              <div className="flex items-center gap-1">
                                {t.operations?.length ? (
                                  <div className="flex flex-wrap gap-1">
                                    {t.operations.slice(0, 3).map((op, i) => (
                                      <Badge key={i} variant="outline" className="text-xs">{op}</Badge>
                                    ))}
                                    {t.operations.length > 3 && (
                                      <Badge variant="outline" className="text-xs">+{t.operations.length - 3}</Badge>
                                    )}
                                  </div>
                                ) : "—"}
                                <button
                                  className="ml-1 text-[#E8453C] hover:text-[#E8453C]"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    const key = tableKey(sr.schema, t.table_name);
                                    setExpandedTable(expandedTable === key ? null : key);
                                  }}
                                  title="Preview changes"
                                >
                                  {expandedTable === tableKey(sr.schema, t.table_name) ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                                </button>
                              </div>
                            </td>
                          </tr>
                          {/* Diff Preview Row */}
                          {expandedTable === tableKey(sr.schema, t.table_name) && (
                            <tr className="bg-muted/20">
                              <td colSpan={7} className="px-3 py-3">
                                <div className="space-y-2">
                                  <p className="text-xs font-medium text-muted-foreground">Change Preview</p>
                                  <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                                    <div className="px-3 py-2 bg-background rounded border border-border">
                                      <p className="text-[10px] text-muted-foreground">Version Range</p>
                                      <p className="text-sm font-mono text-foreground">
                                        {t.last_synced_version != null ? `v${t.last_synced_version}` : "new"} → v{t.current_version ?? "?"}
                                      </p>
                                    </div>
                                    <div className="px-3 py-2 bg-background rounded border border-border">
                                      <p className="text-[10px] text-muted-foreground">Transactions</p>
                                      <p className="text-sm font-bold text-foreground">{t.changes_since_sync ?? "—"}</p>
                                    </div>
                                    <div className="px-3 py-2 bg-background rounded border border-border">
                                      <p className="text-[10px] text-muted-foreground">Operation Types</p>
                                      <div className="flex flex-wrap gap-1 mt-0.5">
                                        {(t.operations || []).map((op, i) => (
                                          <Badge key={i} variant="outline" className={`text-[10px] ${
                                            op === "WRITE" ? "border-border/30 text-foreground" :
                                            op === "DELETE" ? "border-red-500/30 text-red-500" :
                                            op === "UPDATE" || op === "MERGE" ? "border-border/30 text-muted-foreground" :
                                            ""
                                          }`}>{op}</Badge>
                                        ))}
                                      </div>
                                    </div>
                                    <div className="px-3 py-2 bg-background rounded border border-border">
                                      <p className="text-[10px] text-muted-foreground">Sync Action</p>
                                      <p className="text-sm text-foreground">
                                        {t.reason === "never_synced" ? "Full clone (new table)" : "Re-clone with latest data"}
                                      </p>
                                    </div>
                                  </div>
                                  <p className="text-[10px] text-muted-foreground italic">
                                    Row-level diff (inserts/updates/deletes) requires Delta Change Data Feed to be enabled on the source table.
                                  </p>
                                </div>
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              )}
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
