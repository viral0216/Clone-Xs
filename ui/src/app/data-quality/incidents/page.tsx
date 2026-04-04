// @ts-nocheck
import { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import {
  Bell, Loader2, XCircle, AlertCircle, Info, CheckCircle, CheckCircle2,
  RefreshCw, Search, Database, ShieldAlert, Activity, GitBranch, Eye,
  Clock, Timer, ExternalLink,
} from "lucide-react";

/* ── Types & Constants ─────────────────────────────────── */

interface Incident {
  id: string;
  title: string;
  description: string;
  source: string;
  severity: "critical" | "warning" | "info";
  status: "open" | "resolved";
  detected_at: string;
  resolved_at?: string;
  related_link?: string;
  table_fqn?: string;
}

const SOURCE_LABELS: Record<string, string> = {
  dq_rule: "DQ Rule",
  dq: "DQ Rule",
  freshness: "Freshness",
  anomaly: "Anomaly",
  reconciliation: "Reconciliation",
  reconciliation_mismatch: "Reconciliation",
  pii: "PII",
  sla: "SLA",
};

const SOURCE_LINKS: Record<string, string> = {
  dq_rule: "/data-quality/rules",
  dq: "/data-quality/rules",
  freshness: "/data-quality/freshness",
  anomaly: "/data-quality/anomalies",
  reconciliation: "/data-quality/reconciliation/row-level",
  reconciliation_mismatch: "/data-quality/reconciliation/row-level",
  pii: "/data-quality/pii",
  sla: "/data-quality/configuration",
};

const SOURCE_ICONS: Record<string, any> = {
  dq_rule: ShieldAlert,
  dq: ShieldAlert,
  freshness: Clock,
  anomaly: Activity,
  reconciliation: GitBranch,
  reconciliation_mismatch: GitBranch,
  pii: Eye,
  sla: Timer,
};

const SEVERITY_STYLES: Record<string, { text: string; border: string; bg: string; icon: any }> = {
  critical: {
    text: "text-red-600 dark:text-red-400",
    border: "border-l-red-500",
    bg: "bg-red-50 dark:bg-red-950/20",
    icon: XCircle,
  },
  warning: {
    text: "text-amber-600 dark:text-amber-400",
    border: "border-l-amber-500",
    bg: "bg-amber-50 dark:bg-amber-950/20",
    icon: AlertCircle,
  },
  info: {
    text: "text-sky-600 dark:text-sky-400",
    border: "border-l-sky-500",
    bg: "bg-sky-50 dark:bg-sky-950/20",
    icon: Info,
  },
};

const SOURCE_BADGE_COLORS: Record<string, string> = {
  dq_rule: "text-purple-600 border-purple-300 dark:text-purple-400 dark:border-purple-700",
  dq: "text-purple-600 border-purple-300 dark:text-purple-400 dark:border-purple-700",
  freshness: "text-cyan-600 border-cyan-300 dark:text-cyan-400 dark:border-cyan-700",
  anomaly: "text-amber-600 border-amber-300 dark:text-amber-400 dark:border-amber-700",
  reconciliation: "text-indigo-600 border-indigo-300 dark:text-indigo-400 dark:border-indigo-700",
  reconciliation_mismatch: "text-indigo-600 border-indigo-300 dark:text-indigo-400 dark:border-indigo-700",
  pii: "text-pink-600 border-pink-300 dark:text-pink-400 dark:border-pink-700",
  sla: "text-orange-600 border-orange-300 dark:text-orange-400 dark:border-orange-700",
};

/* ── Component ─────────────────────────────────────────── */

export default function IncidentsPage() {
  const [loading, setLoading] = useState(true);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [filter, setFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState<string>("all");
  const [sourceFilter, setSourceFilter] = useState<string>("all");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function loadIncidents() {
    setLoading(true);
    try {
      const data = await api.get<{ incidents?: any[]; [k: string]: any }>("/data-quality/incidents");
      const raw = Array.isArray(data) ? data : (data?.incidents ?? []);
      const mapped: Incident[] = raw.map((inc: any, idx: number) => ({
        id: inc.id ?? `inc-${idx}`,
        title: inc.title ?? "",
        description: inc.description ?? "",
        source: inc.source ?? inc.type ?? "anomaly",
        severity: inc.severity ?? "warning",
        status: inc.status ?? (inc.severity === "normal" ? "resolved" : "open"),
        detected_at: inc.detected_at ?? inc.timestamp ?? "",
        resolved_at: inc.resolved_at,
        related_link: inc.related_link,
        table_fqn: inc.table_fqn,
      }));
      setIncidents(mapped);
    } catch (err: any) {
      toast.error(err?.message || "Failed to load incidents.");
      setIncidents([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadIncidents(); }, []);

  // Auto-refresh
  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(loadIncidents, 60_000);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh]);

  const filtered = incidents.filter((inc) => {
    if (severityFilter !== "all" && inc.severity !== severityFilter) return false;
    if (sourceFilter !== "all" && inc.source !== sourceFilter) return false;
    if (filter && !JSON.stringify(inc).toLowerCase().includes(filter.toLowerCase())) return false;
    return true;
  });

  const openCount = incidents.filter((i) => i.status === "open").length;
  const resolvedCount = incidents.filter((i) => i.status === "resolved").length;
  const criticalCount = incidents.filter((i) => i.severity === "critical" && i.status === "open").length;
  const warningCount = incidents.filter((i) => i.severity === "warning" && i.status === "open").length;
  const infoCount = incidents.filter((i) => i.severity === "info").length;

  // Source breakdown
  const sourceCounts: Record<string, number> = {};
  incidents.forEach((i) => {
    const key = SOURCE_LABELS[i.source] || i.source;
    sourceCounts[key] = (sourceCounts[key] || 0) + 1;
  });

  // Unique source types for filter dropdown
  const activeSources = [...new Set(incidents.map((i) => i.source))];

  // Group by date for timeline
  const groupedByDate: Record<string, Incident[]> = {};
  filtered.forEach((inc) => {
    const date = String(inc.detected_at || "").slice(0, 10) || "Unknown";
    if (!groupedByDate[date]) groupedByDate[date] = [];
    groupedByDate[date].push(inc);
  });
  const dateGroups = Object.entries(groupedByDate).sort(([a], [b]) => b.localeCompare(a));

  return (
    <div className="space-y-4">
      <PageHeader
        title="Incidents"
        description="Unified timeline of data quality incidents from all sources — anomalies, DQ rule failures, stale tables, reconciliation mismatches, and SLA violations."
        icon={Bell}
        breadcrumbs={["Data Quality", "Monitoring", "Incidents"]}
      />

      {/* Summary KPI cards */}
      <div className="grid grid-cols-3 md:grid-cols-5 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-amber-500">{openCount}</p>
            <p className="text-[10px] text-muted-foreground uppercase mt-1">Open</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-green-500">{resolvedCount}</p>
            <p className="text-[10px] text-muted-foreground uppercase mt-1">Resolved</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-red-500">{criticalCount}</p>
            <p className="text-[10px] text-muted-foreground uppercase mt-1">Critical</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-amber-500">{warningCount}</p>
            <p className="text-[10px] text-muted-foreground uppercase mt-1">Warning</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-sky-500">{infoCount}</p>
            <p className="text-[10px] text-muted-foreground uppercase mt-1">Info</p>
          </CardContent>
        </Card>
      </div>

      {/* Source breakdown badges */}
      {Object.keys(sourceCounts).length > 0 && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground">By source:</span>
          {Object.entries(sourceCounts)
            .sort(([, a], [, b]) => b - a)
            .map(([src, count]) => (
              <Badge key={src} variant="outline" className="text-[10px] gap-1">
                {src} <span className="font-bold">{count}</span>
              </Badge>
            ))}
        </div>
      )}

      {/* Filters */}
      <Card className="bg-card border-border">
        <CardContent className="pt-5 pb-4">
          <div className="flex flex-wrap items-end gap-4">
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
              <Input placeholder="Search incidents..." value={filter} onChange={(e) => setFilter(e.target.value)} className="pl-8" />
            </div>
            <div className="w-32">
              <label className="text-xs text-muted-foreground mb-1 block">Severity</label>
              <select
                value={severityFilter}
                onChange={(e) => setSeverityFilter(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
              >
                <option value="all">All</option>
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
            <div className="w-44">
              <label className="text-xs text-muted-foreground mb-1 block">Source</label>
              <select
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
              >
                <option value="all">All Sources</option>
                {activeSources.map((src) => (
                  <option key={src} value={src}>{SOURCE_LABELS[src] || src}</option>
                ))}
              </select>
            </div>
            <Button variant="outline" onClick={loadIncidents} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
            <Button
              variant={autoRefresh ? "default" : "outline"}
              size="sm"
              onClick={() => setAutoRefresh(!autoRefresh)}
              className="h-9 text-xs gap-1.5"
            >
              <Timer className="h-3.5 w-3.5" />
              {autoRefresh ? "Auto: ON" : "Auto: OFF"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Timeline */}
      {loading ? (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground mt-3">Loading incidents...</p>
          </CardContent>
        </Card>
      ) : filtered.length === 0 ? (
        <Card className="bg-card border-border">
          <CardContent className="py-10 text-center">
            {incidents.length === 0 ? (
              <>
                <CheckCircle2 className="h-8 w-8 mx-auto text-green-500 mb-2" />
                <p className="text-foreground font-medium">All clear — no incidents detected</p>
                <p className="text-sm text-muted-foreground mt-1">
                  No data quality issues found across anomalies, DQ rules, freshness, and reconciliation sources.
                </p>
              </>
            ) : (
              <>
                <Search className="h-6 w-6 mx-auto text-muted-foreground mb-2" />
                <p className="text-sm text-muted-foreground">No incidents match your current filters.</p>
              </>
            )}
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-5">
          {dateGroups.map(([date, items]) => (
            <div key={date}>
              <div className="flex items-center gap-2 mb-2">
                <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider sticky top-0 bg-background py-1">
                  {date}
                </h3>
                <Badge variant="outline" className="text-[9px]">{items.length}</Badge>
              </div>
              <div className="space-y-2">
                {items.map((inc) => {
                  const sev = SEVERITY_STYLES[inc.severity] || SEVERITY_STYLES.info;
                  const SevIcon = sev.icon;
                  const SourceIcon = SOURCE_ICONS[inc.source] || Bell;
                  return (
                    <Card key={inc.id} className={`border-l-2 ${sev.border} bg-card`}>
                      <CardContent className="py-3 px-4">
                        <div className="flex items-start gap-3">
                          <SevIcon className={`h-4 w-4 mt-0.5 shrink-0 ${sev.text}`} />
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 flex-wrap">
                              <p className="text-sm font-medium text-foreground">{inc.title}</p>
                              <Badge variant="outline" className={`text-[10px] ${sev.text} border-current/30`}>
                                {inc.severity}
                              </Badge>
                              <Badge variant="outline" className={`text-[10px] gap-0.5 ${SOURCE_BADGE_COLORS[inc.source] || ""}`}>
                                <SourceIcon className="h-2.5 w-2.5" />
                                {SOURCE_LABELS[inc.source] || inc.source}
                              </Badge>
                              {inc.status === "resolved" && (
                                <Badge variant="outline" className="text-[10px] text-green-600 border-green-300 dark:text-green-400 dark:border-green-700 gap-0.5">
                                  <CheckCircle className="h-2.5 w-2.5" /> Resolved
                                </Badge>
                              )}
                            </div>
                            <p className="text-xs text-muted-foreground mt-1">{inc.description}</p>
                            {inc.table_fqn && (
                              <p className="text-[10px] font-mono text-muted-foreground mt-0.5">{inc.table_fqn}</p>
                            )}
                            <div className="flex items-center gap-3 mt-2">
                              <span className="text-[10px] text-muted-foreground">
                                {String(inc.detected_at || "").slice(11, 19)}
                              </span>
                              <Link
                                to={inc.related_link || SOURCE_LINKS[inc.source] || "/data-quality"}
                                className="text-[10px] text-[#E8453C] hover:underline inline-flex items-center gap-0.5"
                              >
                                <ExternalLink className="h-2.5 w-2.5" /> View Source
                              </Link>
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
