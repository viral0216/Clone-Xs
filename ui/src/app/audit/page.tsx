// @ts-nocheck
import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import {
  History, RefreshCw, Filter, ChevronDown, ChevronRight, Copy,
  CheckCircle, XCircle, Clock, AlertTriangle, ArrowRight,
  Database, User, Server, FileText, Download, Loader2,
  TrendingUp, BarChart3, Settings2, Timer, Layers,
} from "lucide-react";
import PageHeader from "@/components/PageHeader";
import StatusBadge from "@/components/StatusBadge";
import LoadingState from "@/components/LoadingState";
import EmptyState from "@/components/EmptyState";
import ErrorCard from "@/components/ErrorCard";

interface AuditEntry {
  job_id?: string;
  operation_id?: string;
  job_type?: string;
  operation_type?: string;
  source_catalog: string;
  destination_catalog: string;
  clone_type?: string;
  status: string;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  user_name?: string;
  host?: string;
  error_message?: string;
  tables_cloned?: number;
  tables_failed?: number;
  tables_skipped?: number;
  views_cloned?: number;
  volumes_cloned?: number;
  total_size_bytes?: number;
  clone_mode?: string;
  trigger?: string;
  log_line_count?: number;
}

function formatDuration(seconds?: number) {
  if (!seconds || seconds === 0) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatBytes(bytes?: number) {
  if (!bytes || bytes === 0) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 1 ? 1 : 0)} ${units[i]}`;
}

/* statusBadge replaced by <StatusBadge /> component */

function statusIcon(status: string) {
  const s = status?.toLowerCase();
  if (s === "success" || s === "completed") return <CheckCircle className="h-4 w-4 text-foreground" />;
  if (s === "failed") return <XCircle className="h-4 w-4 text-red-500" />;
  if (s === "running") return <Loader2 className="h-4 w-4 text-[#E8453C] animate-spin" />;
  return <AlertTriangle className="h-4 w-4 text-muted-foreground" />;
}

function LogDetailPanel({ jobId }: { jobId: string }) {
  const [detail, setDetail] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [showConfig, setShowConfig] = useState(false);
  const [showRawJson, setShowRawJson] = useState(false);

  useEffect(() => {
    api.get(`/audit/${jobId}/logs`)
      .then((res) => setDetail(res))
      .catch(() => setDetail(null))
      .finally(() => setLoading(false));
  }, [jobId]);

  if (loading) return <LoadingState message="Loading logs..." />;
  if (!detail || detail.error) return <div className="py-4 text-center text-muted-foreground text-sm">No detailed logs available</div>;

  // log_lines can be: array, JSON string, or comma-separated string from Delta
  let logLines: string[] = [];
  if (Array.isArray(detail.log_lines)) {
    logLines = detail.log_lines;
  } else if (typeof detail.log_lines === "string") {
    try {
      const parsed = JSON.parse(detail.log_lines);
      logLines = Array.isArray(parsed) ? parsed : (detail.log_lines.trim() ? [detail.log_lines] : []);
    } catch {
      logLines = detail.log_lines.split("\n").filter(Boolean);
    }
  }

  let resultJson: any = null;
  if (detail.result_json) {
    try {
      resultJson = typeof detail.result_json === "string" ? JSON.parse(detail.result_json) : detail.result_json;
    } catch {
      resultJson = null;
    }
  }

  let configJson: any = null;
  if (detail.config_json) {
    try {
      configJson = typeof detail.config_json === "string" ? JSON.parse(detail.config_json) : detail.config_json;
    } catch {
      configJson = null;
    }
  }

  // Schema durations from result
  const schemaDurations: Record<string, number> = resultJson?.schema_durations || {};
  const hasSchemaDurations = Object.keys(schemaDurations).length > 0;
  const maxSchemaDuration = hasSchemaDurations ? Math.max(...Object.values(schemaDurations)) : 0;

  // Config display: group into meaningful sections
  const configHighlights = configJson ? [
    { label: "Clone Type", value: configJson.clone_type },
    { label: "Load Type", value: configJson.load_type },
    { label: "Warehouse", value: configJson.sql_warehouse_id },
    { label: "Workers", value: configJson.max_workers },
    { label: "Parallel Tables", value: configJson.parallel_tables },
    { label: "Dry Run", value: configJson.dry_run ? "Yes" : "No" },
  ].filter(c => c.value != null) : [];

  const configFlags = configJson ? [
    { label: "Permissions", enabled: configJson.copy_permissions },
    { label: "Ownership", enabled: configJson.copy_ownership },
    { label: "Tags", enabled: configJson.copy_tags },
    { label: "Properties", enabled: configJson.copy_properties },
    { label: "Security", enabled: configJson.copy_security },
    { label: "Constraints", enabled: configJson.copy_constraints },
    { label: "Comments", enabled: configJson.copy_comments },
    { label: "Rollback", enabled: configJson.enable_rollback },
    { label: "Validate", enabled: configJson.validate_after_clone },
    { label: "Checksum", enabled: configJson.validate_checksum },
    { label: "Schema Only", enabled: configJson.schema_only },
  ] : [];

  return (
    <div className="space-y-3">
      {/* Result summary */}
      {resultJson && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
          {resultJson.schemas_processed != null && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Schemas</p>
              <p className="text-sm font-semibold">{resultJson.schemas_processed}</p>
            </div>
          )}
          {resultJson.tables && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Tables</p>
              <p className="text-sm font-semibold">{resultJson.tables?.success || resultJson.tables?.cloned || 0} ok / {resultJson.tables?.failed || 0} fail</p>
            </div>
          )}
          {resultJson.views && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Views</p>
              <p className="text-sm font-semibold">{resultJson.views?.success || resultJson.views?.cloned || 0}</p>
            </div>
          )}
          {resultJson.functions && (resultJson.functions.success > 0 || resultJson.functions.failed > 0) && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Functions</p>
              <p className="text-sm font-semibold">{resultJson.functions?.success || 0}</p>
            </div>
          )}
          {resultJson.volumes && (resultJson.volumes.success > 0 || resultJson.volumes.failed > 0) && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Volumes</p>
              <p className="text-sm font-semibold">{resultJson.volumes?.success || 0}</p>
            </div>
          )}
          {resultJson.duration_seconds != null && (
            <div className="px-3 py-2 bg-muted/50 rounded-lg">
              <p className="text-xs text-muted-foreground">Duration</p>
              <p className="text-sm font-semibold">{formatDuration(resultJson.duration_seconds)}</p>
            </div>
          )}
        </div>
      )}

      {/* Schema durations — horizontal bar chart */}
      {hasSchemaDurations && (
        <div>
          <p className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1.5">
            <Timer className="h-3 w-3" /> Per-Schema Duration
          </p>
          <div className="space-y-1.5">
            {Object.entries(schemaDurations)
              .sort(([, a], [, b]) => b - a)
              .map(([schema, duration]) => (
                <div key={schema} className="flex items-center gap-2">
                  <span className="text-xs text-foreground w-28 truncate font-mono">{schema}</span>
                  <div className="flex-1 h-5 bg-muted/30 rounded overflow-hidden">
                    <div
                      className="h-full bg-[#E8453C]/60 rounded flex items-center px-2"
                      style={{ width: `${Math.max((duration / maxSchemaDuration) * 100, 4)}%` }}
                    >
                      <span className="text-[10px] text-white font-medium whitespace-nowrap">
                        {formatDuration(duration)}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Errors from result */}
      {resultJson?.errors && resultJson.errors.length > 0 && (
        <div className="px-3 py-2 bg-red-500/5 border border-red-500/20 rounded-lg">
          <p className="text-xs font-medium text-red-500 mb-1">Errors ({resultJson.errors.length})</p>
          <div className="space-y-1">
            {resultJson.errors.map((err: string, i: number) => (
              <p key={i} className="text-xs text-red-400 font-mono">{err}</p>
            ))}
          </div>
        </div>
      )}

      {/* Clone configuration */}
      {configJson && (
        <div>
          <button
            onClick={() => setShowConfig(!showConfig)}
            className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
          >
            {showConfig ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            <Settings2 className="h-3 w-3" />
            Clone Configuration
          </button>
          {showConfig && (
            <div className="mt-2 space-y-2">
              {/* Key settings */}
              <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
                {configHighlights.map((c) => (
                  <div key={c.label} className="px-2.5 py-1.5 bg-muted/50 rounded-lg">
                    <p className="text-[10px] text-muted-foreground">{c.label}</p>
                    <p className="text-xs font-semibold text-foreground font-mono">{String(c.value)}</p>
                  </div>
                ))}
              </div>
              {/* Copy flags */}
              <div className="flex flex-wrap gap-1.5">
                {configFlags.map((f) => (
                  <Badge
                    key={f.label}
                    variant="outline"
                    className={`text-[10px] ${f.enabled ? "border-foreground/20 text-foreground" : "border-border/30 text-muted-foreground line-through opacity-50"}`}
                  >
                    {f.enabled ? "✓" : "✗"} {f.label}
                  </Badge>
                ))}
              </div>
              {/* Exclude/include schemas */}
              {configJson.exclude_schemas?.length > 0 && (
                <p className="text-xs text-muted-foreground">
                  Excluded schemas: <span className="font-mono text-foreground">{configJson.exclude_schemas.join(", ")}</span>
                </p>
              )}
              {configJson.include_schemas?.length > 0 && (
                <p className="text-xs text-muted-foreground">
                  Include schemas: <span className="font-mono text-foreground">{configJson.include_schemas.join(", ")}</span>
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {/* Log lines */}
      {logLines.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-1">
            <p className="text-xs font-medium text-muted-foreground">Execution Log ({logLines.length} lines)</p>
            <Button variant="ghost" size="sm" className="h-6 px-2" onClick={async () => {
              await navigator.clipboard.writeText(logLines.join("\n"));
            }}>
              <Copy className="h-3 w-3 mr-1" />
              <span className="text-xs">Copy</span>
            </Button>
          </div>
          <div className="bg-black/80 rounded-lg p-3 max-h-64 overflow-y-auto font-mono text-xs leading-relaxed">
            {logLines.map((line: string, i: number) => (
              <div key={i} className={`${line.includes("ERROR") || line.includes("FAIL") ? "text-red-400" : line.includes("WARN") ? "text-gray-400" : "text-gray-300"}`}>
                {line}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Action buttons */}
      <div className="flex gap-2">
        <Button
          variant="outline"
          size="sm"
          onClick={() => setShowRawJson(!showRawJson)}
        >
          <FileText className="h-3.5 w-3.5 mr-1.5" />
          {showRawJson ? "Hide" : "View"} Raw JSON
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            const blob = new Blob([JSON.stringify(detail, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `audit-${jobId}.json`;
            a.click();
            URL.revokeObjectURL(url);
          }}
        >
          <Download className="h-3.5 w-3.5 mr-1.5" />
          Download Full Log
        </Button>
      </div>

      {/* Raw JSON viewer */}
      {showRawJson && (
        <div>
          <div className="flex items-center justify-between mb-1">
            <p className="text-xs font-medium text-muted-foreground">Raw Audit Record</p>
            <Button variant="ghost" size="sm" className="h-6 px-2" onClick={async () => {
              await navigator.clipboard.writeText(JSON.stringify(detail, null, 2));
            }}>
              <Copy className="h-3 w-3 mr-1" />
              <span className="text-xs">Copy</span>
            </Button>
          </div>
          <div className="bg-black/80 rounded-lg p-3 max-h-80 overflow-y-auto font-mono text-xs leading-relaxed text-gray-300">
            <pre className="whitespace-pre-wrap break-words">{JSON.stringify(detail, null, 2)}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

export default function AuditPage() {
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [opFilter, setOpFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  const loadAudit = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<AuditEntry[]>("/audit");
      setEntries(Array.isArray(res) ? res : []);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { loadAudit(); }, []);

  const filtered = useMemo(() => entries.filter((e) => {
    const op = e.job_type || e.operation_type || "";
    if (opFilter && !op.toLowerCase().includes(opFilter.toLowerCase())) return false;
    if (statusFilter && e.status?.toLowerCase() !== statusFilter.toLowerCase()) return false;
    if (catalogFilter) {
      const match = catalogFilter.toLowerCase();
      if (!e.source_catalog?.toLowerCase().includes(match) && !e.destination_catalog?.toLowerCase().includes(match)) return false;
    }
    const ts = (e.started_at || "").slice(0, 10);
    if (dateFrom && ts < dateFrom) return false;
    if (dateTo && ts > dateTo) return false;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      const searchable = [e.source_catalog, e.destination_catalog, e.user_name, e.status, op, e.error_message].join(" ").toLowerCase();
      if (!searchable.includes(q)) return false;
    }
    return true;
  }), [entries, opFilter, statusFilter, catalogFilter, dateFrom, dateTo, searchQuery]);

  // Summary stats
  const stats = useMemo(() => {
    const total = filtered.length;
    const succeeded = filtered.filter(e => ["success", "completed"].includes(e.status?.toLowerCase())).length;
    const failed = filtered.filter(e => e.status?.toLowerCase() === "failed").length;
    const avgDuration = filtered.reduce((sum, e) => sum + (e.duration_seconds || 0), 0) / (total || 1);
    return { total, succeeded, failed, avgDuration };
  }, [filtered]);

  // Unique status values for filter
  const statusOptions = useMemo(() => [...new Set(entries.map(e => e.status).filter(Boolean))], [entries]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Audit Trail"
        icon={History}
        breadcrumbs={["Overview", "Audit Trail"]}
        description="Complete history of all clone operations — who ran what, when, results, and full execution logs."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/delta/"
        docsLabel="Delta Lake docs"
        actions={
          <Button onClick={loadAudit} disabled={loading} variant="outline" size="sm">
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        }
      />

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Operations</span>
              <BarChart3 className="h-4 w-4 text-[#E8453C]" />
            </div>
            <p className="text-2xl font-bold text-foreground">{stats.total}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Succeeded</span>
              <CheckCircle className="h-4 w-4 text-foreground" />
            </div>
            <p className="text-2xl font-bold text-foreground">{stats.succeeded}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Failed</span>
              <XCircle className="h-4 w-4 text-red-500" />
            </div>
            <p className="text-2xl font-bold text-foreground">{stats.failed}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Avg Duration</span>
              <Clock className="h-4 w-4 text-muted-foreground" />
            </div>
            <p className="text-2xl font-bold text-foreground">{formatDuration(stats.avgDuration)}</p>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm flex items-center gap-2 text-muted-foreground">
            <Filter className="h-3.5 w-3.5" /> Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-3 flex-wrap">
            <div className="flex-1 min-w-[180px]">
              <label className="text-xs font-medium text-muted-foreground">Search</label>
              <Input placeholder="Search catalogs, users, errors..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">From</label>
              <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">To</label>
              <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Status</label>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="mt-1 w-full h-9 px-3 text-sm bg-background border border-border rounded-md text-foreground"
              >
                <option value="">All statuses</option>
                {statusOptions.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Operation</label>
              <Input placeholder="clone, sync..." value={opFilter} onChange={(e) => setOpFilter(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Catalog</label>
              <Input placeholder="Filter catalog..." value={catalogFilter} onChange={(e) => setCatalogFilter(e.target.value)} className="mt-1" />
            </div>
          </div>
          {(searchQuery || dateFrom || dateTo || statusFilter || opFilter || catalogFilter) && (
            <div className="mt-2 flex items-center gap-2">
              <span className="text-xs text-muted-foreground">{filtered.length} of {entries.length} entries</span>
              <Button variant="ghost" size="sm" className="text-xs h-6" onClick={() => { setSearchQuery(""); setDateFrom(""); setDateTo(""); setStatusFilter(""); setOpFilter(""); setCatalogFilter(""); }}>
                Clear all
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {error && (
        <ErrorCard error={error} onRetry={loadAudit} />
      )}

      {/* Entries List */}
      {loading ? (
        <div className="space-y-2">
          {[1, 2, 3, 4, 5].map(i => (
            <div key={i} className="h-16 bg-muted/30 rounded-lg animate-pulse" />
          ))}
        </div>
      ) : filtered.length === 0 ? (
        <Card className="bg-card border-border">
          <CardContent className="py-4">
            <EmptyState
              icon={History}
              title="No audit entries found"
              description="Operations are logged automatically when you run clones, syncs, or other operations."
            />
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-2">
          {filtered.map((entry, i) => {
            const id = entry.job_id || entry.operation_id || `entry-${i}`;
            const isExpanded = expandedId === id;
            const op = entry.job_type || entry.operation_type || "clone";

            return (
              <Card key={id} className={`bg-card border-border transition-all ${isExpanded ? "ring-1 ring-[#E8453C]/30" : ""}`}>
                <CardContent className="py-3 px-4">
                  {/* Main row */}
                  <div
                    className="flex items-center gap-3 cursor-pointer"
                    onClick={() => setExpandedId(isExpanded ? null : id)}
                  >
                    {/* Expand icon */}
                    {isExpanded
                      ? <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
                      : <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />}

                    {/* Status icon */}
                    {statusIcon(entry.status)}

                    {/* Operation badge */}
                    <Badge variant="secondary" className="text-[10px] font-mono shrink-0">{op}</Badge>

                    {/* Source → Dest */}
                    <div className="flex items-center gap-1.5 min-w-0 flex-1">
                      <span className="text-sm font-medium text-foreground truncate">{entry.source_catalog}</span>
                      <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                      <span className="text-sm text-foreground truncate">{entry.destination_catalog}</span>
                    </div>

                    {/* Clone type */}
                    <Badge variant="outline" className={`text-[10px] shrink-0 ${
                      entry.clone_type === "DEEP" ? "border-[#E8453C]/30 text-[#E8453C]" : "border-border/30 text-muted-foreground"
                    }`}>
                      {entry.clone_type || "—"}
                    </Badge>

                    {/* Status */}
                    <StatusBadge status={entry.status} />

                    {/* Duration */}
                    <span className="text-xs text-muted-foreground shrink-0 w-16 text-right">
                      {formatDuration(entry.duration_seconds)}
                    </span>

                    {/* Timestamp */}
                    <span className="text-xs text-muted-foreground shrink-0 hidden md:block">
                      {entry.started_at ? new Date(entry.started_at).toLocaleString() : "—"}
                    </span>
                  </div>

                  {/* Expanded detail */}
                  {isExpanded && (
                    <div className="mt-4 pt-4 border-t border-border space-y-4">
                      {/* Detail cards */}
                      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                        <div className="space-y-1">
                          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">User</p>
                          <div className="flex items-center gap-1.5">
                            <User className="h-3 w-3 text-muted-foreground" />
                            <span className="text-sm text-foreground">{entry.user_name || "—"}</span>
                          </div>
                        </div>
                        <div className="space-y-1">
                          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Host</p>
                          <div className="flex items-center gap-1.5">
                            <Server className="h-3 w-3 text-muted-foreground" />
                            <span className="text-xs text-foreground truncate">{entry.host || "—"}</span>
                          </div>
                        </div>
                        <div className="space-y-1">
                          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Started</p>
                          <span className="text-xs text-foreground">{entry.started_at ? new Date(entry.started_at).toLocaleString() : "—"}</span>
                        </div>
                        <div className="space-y-1">
                          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Completed</p>
                          <span className="text-xs text-foreground">{entry.completed_at ? new Date(entry.completed_at).toLocaleString() : "—"}</span>
                        </div>
                        {entry.tables_cloned != null && (
                          <div className="space-y-1">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Tables Cloned</p>
                            <span className="text-sm font-semibold text-foreground">{entry.tables_cloned}</span>
                          </div>
                        )}
                        {entry.tables_failed != null && entry.tables_failed > 0 && (
                          <div className="space-y-1">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Tables Failed</p>
                            <span className="text-sm font-semibold text-red-500">{entry.tables_failed}</span>
                          </div>
                        )}
                        {entry.total_size_bytes != null && entry.total_size_bytes > 0 && (
                          <div className="space-y-1">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Data Size</p>
                            <span className="text-sm text-foreground">{formatBytes(entry.total_size_bytes)}</span>
                          </div>
                        )}
                        {entry.clone_mode && (
                          <div className="space-y-1">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Mode</p>
                            <Badge variant="outline" className="text-[10px]">{entry.clone_mode}</Badge>
                          </div>
                        )}
                        {entry.trigger && (
                          <div className="space-y-1">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Trigger</p>
                            <Badge variant="outline" className="text-[10px]">{entry.trigger}</Badge>
                          </div>
                        )}
                      </div>

                      {/* Error message */}
                      {entry.error_message && (
                        <div className="px-3 py-2 bg-red-500/5 border border-red-500/20 rounded-lg">
                          <p className="text-xs font-medium text-red-500 mb-1">Error</p>
                          <p className="text-xs text-red-400 font-mono whitespace-pre-wrap">{entry.error_message}</p>
                        </div>
                      )}

                      {/* Log detail panel — fetches from /audit/{job_id}/logs */}
                      <LogDetailPanel jobId={id} />
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
