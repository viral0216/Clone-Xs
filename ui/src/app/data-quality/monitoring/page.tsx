// @ts-nocheck
import React, { useState, useEffect } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  Settings, Loader2, Search, Trash2, RefreshCw, Play, Plus,
  Eye, EyeOff, Clock, Activity, Pause, CheckCircle,
  Timer, Power, PowerOff, Zap, History, ChevronLeft, ChevronRight, ChevronDown,
} from "lucide-react";

interface MonitoringConfig {
  config_id: string;
  table_fqn: string;
  metrics: string[];
  frequency: string;
  auto_baseline: boolean;
  baseline_days: number;
  enabled: boolean;
  baseline_status: string;
  created_at: string;
  updated_at: string;
}

const ALL_METRICS = ["row_count", "null_rate", "distinct_count", "min", "max", "mean"];

const METRIC_LABELS: Record<string, string> = {
  row_count: "Row Count",
  null_rate: "Null Rate",
  distinct_count: "Distinct Count",
  min: "Min",
  max: "Max",
  mean: "Mean",
};

const FREQUENCY_OPTIONS = [
  { value: "5min", label: "Every 5 min" },
  { value: "15min", label: "Every 15 min" },
  { value: "30min", label: "Every 30 min" },
  { value: "hourly", label: "Hourly" },
  { value: "4hours", label: "Every 4 hours" },
  { value: "daily", label: "Daily" },
  { value: "weekly", label: "Weekly" },
];

export default function MonitoringConfigPage() {
  const [configs, setConfigs] = useState<MonitoringConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");

  // Discovery state
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [discoveredTables, setDiscoveredTables] = useState<string[]>([]);
  const [discovering, setDiscovering] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [bulkMetrics, setBulkMetrics] = useState<Set<string>>(new Set(["row_count", "null_rate", "distinct_count"]));
  const [bulkFrequency, setBulkFrequency] = useState("daily");
  const [adding, setAdding] = useState(false);

  // Bulk delete state
  const [selectedConfigs, setSelectedConfigs] = useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = useState(false);

  // Run monitoring
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = usePersistedState<any>("dq-monitoring-runResult", null);

  // Scheduler state
  const [scheduler, setScheduler] = useState<any>(null);
  const [schedulerLoading, setSchedulerLoading] = useState(false);
  const [expandedRun, setExpandedRun] = useState<number | null>(null);
  const [historyPage, setHistoryPage] = useState(0);
  const HISTORY_PAGE_SIZE = 10;

  useEffect(() => { loadConfigs(); loadScheduler(); }, []);

  // Auto-refresh scheduler status every 30 seconds when enabled
  useEffect(() => {
    if (!scheduler?.enabled) return;
    const controller = new AbortController();
    const poll = async () => {
      try {
        const data = await api.get("/data-quality/monitoring/scheduler", { signal: controller.signal });
        setScheduler(data);
      } catch { /* ignore aborted */ }
    };
    const interval = setInterval(poll, 30_000);
    return () => { controller.abort(); clearInterval(interval); };
  }, [scheduler?.enabled]);

  async function loadScheduler() {
    try {
      const data = await api.get("/data-quality/monitoring/scheduler");
      setScheduler(data);
      if (data?.last_run_result && !runResult) {
        setRunResult(data.last_run_result);
      }
    } catch { /* scheduler not available */ }
  }

  async function toggleScheduler() {
    setSchedulerLoading(true);
    try {
      if (scheduler?.enabled) {
        const data = await api.post("/data-quality/monitoring/scheduler/disable", {});
        setScheduler(data);
        toast.success("Auto-monitoring disabled.");
      } else {
        // Always use 1-minute heartbeat — per-table frequency controls actual collection
        const data = await api.post("/data-quality/monitoring/scheduler/enable?frequency_minutes=1", {});
        setScheduler(data);
        toast.success("Auto-monitoring enabled. Tables will be checked based on their individual frequency.");
      }
    } catch (e: any) {
      toast.error(e?.message || "Failed to toggle auto-monitoring.");
    } finally {
      setSchedulerLoading(false);
    }
  }

  async function triggerNow() {
    setSchedulerLoading(true);
    try {
      const data = await api.post("/data-quality/monitoring/scheduler/run-now", {});
      setScheduler(data);
      toast.success("Monitoring run triggered.");
    } catch (e: any) {
      toast.error(e?.message || "Failed to trigger run.");
    } finally {
      setSchedulerLoading(false);
    }
  }

  async function loadConfigs() {
    setLoading(true);
    try {
      const data = await api.get("/data-quality/monitoring/configs");
      setConfigs(Array.isArray(data.configs) ? data.configs : []);
    } catch {
      setConfigs([]);
    } finally {
      setLoading(false);
    }
  }

  async function discoverTables() {
    if (!catalog) { toast.error("Select a catalog first."); return; }
    setDiscovering(true);
    setDiscoveredTables([]);
    setSelected(new Set());
    try {
      const params = schema ? `?schema=${encodeURIComponent(schema)}` : "";
      const data = await api.get(`/data-quality/monitoring/discover/${encodeURIComponent(catalog)}${params}`);
      setDiscoveredTables(data.tables || []);
      if ((data.tables || []).length === 0) toast.info("No tables found.");
    } catch (e: any) {
      toast.error(e?.message || "Discovery failed.");
    } finally {
      setDiscovering(false);
    }
  }

  function toggleSelectAll() {
    if (selected.size === discoveredTables.length) setSelected(new Set());
    else setSelected(new Set(discoveredTables));
  }

  function toggleSelect(fqn: string) {
    const next = new Set(selected);
    next.has(fqn) ? next.delete(fqn) : next.add(fqn);
    setSelected(next);
  }

  function toggleBulkMetric(m: string) {
    const next = new Set(bulkMetrics);
    next.has(m) ? next.delete(m) : next.add(m);
    setBulkMetrics(next);
  }

  async function bulkAdd() {
    if (selected.size === 0) return;
    if (bulkMetrics.size === 0) { toast.error("Select at least one metric."); return; }
    setAdding(true);
    try {
      await api.post("/data-quality/monitoring/bulk-add", {
        table_fqns: [...selected],
        metrics: [...bulkMetrics],
        frequency: bulkFrequency,
      });
      toast.success(`Added ${selected.size} table(s) for monitoring.`);
      setSelected(new Set());
      setDiscoveredTables([]);
      loadConfigs();
    } catch (e: any) {
      toast.error(e?.message || "Failed to add tables.");
    } finally {
      setAdding(false);
    }
  }

  async function toggleConfig(configId: string) {
    try {
      await api.post(`/data-quality/monitoring/configs/${configId}/toggle`, {});
      setConfigs(prev => prev.map(c => c.config_id === configId ? { ...c, enabled: !c.enabled } : c));
    } catch (e: any) {
      toast.error(e?.message || "Toggle failed.");
    }
  }

  async function deleteConfig(configId: string) {
    try {
      await api.delete(`/data-quality/monitoring/configs/${configId}`);
      setConfigs(prev => prev.filter(c => c.config_id !== configId));
      toast.success("Monitoring config removed.");
    } catch (e: any) {
      toast.error(e?.message || "Delete failed.");
    }
  }

  async function bulkDeleteConfigs() {
    if (selectedConfigs.size === 0) return;
    setBulkDeleting(true);
    try {
      const result = await api.post("/data-quality/monitoring/bulk-delete", {
        config_ids: [...selectedConfigs],
      });
      setConfigs(prev => prev.filter(c => !selectedConfigs.has(c.config_id)));
      setSelectedConfigs(new Set());
      toast.success(`Deleted ${result.deleted} monitoring config(s).`);
    } catch (e: any) {
      toast.error(e?.message || "Failed to delete configs.");
    } finally {
      setBulkDeleting(false);
    }
  }

  function toggleSelectConfig(configId: string) {
    const next = new Set(selectedConfigs);
    next.has(configId) ? next.delete(configId) : next.add(configId);
    setSelectedConfigs(next);
  }

  function toggleSelectAll() {
    if (selectedConfigs.size === filtered.length) {
      setSelectedConfigs(new Set());
    } else {
      setSelectedConfigs(new Set(filtered.map(c => c.config_id)));
    }
  }

  async function runMonitoring() {
    setRunning(true);
    setRunResult(null);
    try {
      const result = await api.post("/data-quality/monitoring/run", {});
      setRunResult(result);
      toast.success(`Monitoring complete: ${result.metrics_recorded} metrics recorded, ${result.anomalies_found} anomalies.`);
    } catch (e: any) {
      toast.error(e?.message || "Monitoring run failed.");
    } finally {
      setRunning(false);
    }
  }

  const filtered = configs.filter(c => {
    if (!searchQuery) return true;
    return c.table_fqn.toLowerCase().includes(searchQuery.toLowerCase());
  });

  const activeCount = configs.filter(c => c.enabled).length;
  const pausedCount = configs.filter(c => !c.enabled).length;
  const baselinePending = configs.filter(c => c.baseline_status === "pending").length;

  // Filter out already-monitored tables from discovery
  const monitoredSet = new Set(configs.map(c => c.table_fqn));
  const availableTables = discoveredTables.filter(t => !monitoredSet.has(t));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Monitoring Configuration"
        description="Select tables, choose metrics, and set monitoring frequency for anomaly detection."
        icon={Settings}
        breadcrumbs={["Data Quality", "Monitoring", "Configuration"]}
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Total Monitored</p>
            <p className="text-2xl font-bold mt-1">{configs.length}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Active</p>
            <p className="text-2xl font-bold mt-1 text-green-500">{activeCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Paused</p>
            <p className="text-2xl font-bold mt-1 text-amber-500">{pausedCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Baseline Pending</p>
            <p className="text-2xl font-bold mt-1 text-blue-500">{baselinePending}</p>
          </CardContent>
        </Card>
      </div>

      {/* Auto-Monitoring Toggle */}
      <Card className="bg-card border-border">
        <CardContent className="py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Timer className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm font-medium text-foreground">Auto-Monitoring</p>
                <p className="text-xs text-muted-foreground">
                  {scheduler?.enabled
                    ? "Scheduler checks every minute and collects metrics based on each table's frequency."
                    : "Enable to automatically collect metrics based on each table's frequency setting."}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3">
              {scheduler?.enabled && (
                <>
                  {scheduler.running ? (
                    <Badge variant="outline" className="text-[10px] text-amber-500 border-amber-500/30 gap-1">
                      <Loader2 className="h-2.5 w-2.5 animate-spin" /> Running...
                    </Badge>
                  ) : (() => {
                    const lastMeaningful = (scheduler.run_history || []).find((r: any) => (r.tables_processed ?? 0) > 0 || r.status === "error");
                    return lastMeaningful ? (
                      <span className="text-[10px] text-muted-foreground">
                        Last collection: <span className="text-foreground font-medium">{new Date(lastMeaningful.timestamp).toLocaleTimeString()}</span>
                        <span className="text-green-500 ml-1">
                          ({lastMeaningful.tables_processed}t / {lastMeaningful.metrics_recorded}m / {lastMeaningful.anomalies_found}a)
                        </span>
                      </span>
                    ) : (
                      <span className="text-[10px] text-muted-foreground">Waiting for first collection...</span>
                    );
                  })()}
                  <Button variant="outline" size="sm" onClick={triggerNow} disabled={schedulerLoading || scheduler?.running} className="h-8 text-xs gap-1">
                    <Zap className="h-3 w-3" /> Run Now
                  </Button>
                </>
              )}
              <Button
                variant={scheduler?.enabled ? "destructive" : "default"}
                size="sm"
                onClick={toggleScheduler}
                disabled={schedulerLoading}
                className="h-8 gap-1.5"
              >
                {schedulerLoading ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : scheduler?.enabled ? (
                  <PowerOff className="h-3.5 w-3.5" />
                ) : (
                  <Power className="h-3.5 w-3.5" />
                )}
                {scheduler?.enabled ? "Disable" : "Enable"}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Controls */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Discover Tables</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-end gap-3">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              onCatalogChange={(v) => { setCatalog(v); setSchema(""); }}
              onSchemaChange={setSchema}
              showTable={false}
              schemaLabel="Schema (optional)"
            />
            <Button onClick={discoverTables} disabled={discovering || !catalog}>
              {discovering ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}
              Discover Tables
            </Button>
            <div className="flex-1" />
            <Button variant="outline" onClick={runMonitoring} disabled={running || configs.length === 0}>
              {running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
              Run Monitoring
            </Button>
            <Button variant="outline" onClick={loadConfigs} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Run Result */}
      {runResult && (
        <Card className="border-green-500/30">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-6 text-sm">
                <div><span className="text-muted-foreground">Tables processed:</span> <strong>{runResult.tables_processed}</strong></div>
                <div><span className="text-muted-foreground">Metrics recorded:</span> <strong>{runResult.metrics_recorded}</strong></div>
                <div><span className="text-muted-foreground">Anomalies found:</span> <strong className={runResult.anomalies_found > 0 ? "text-red-500" : "text-green-500"}>{runResult.anomalies_found}</strong></div>
                <div><span className="text-muted-foreground">Errors:</span> <strong className={runResult.errors > 0 ? "text-red-500" : ""}>{runResult.errors}</strong></div>
              </div>
              <div className="flex items-center gap-2">
                <Link to="/data-quality/anomalies">
                  <Button variant="outline" size="sm">
                    <Activity className="h-3 w-3 mr-1" /> View Anomalies
                  </Button>
                </Link>
                <Link to="/data-quality/volume">
                  <Button variant="outline" size="sm">
                    <Search className="h-3 w-3 mr-1" /> View Volume
                  </Button>
                </Link>
                <Link to="/data-quality/dashboard">
                  <Button variant="outline" size="sm">
                    <CheckCircle className="h-3 w-3 mr-1" /> DQ Dashboard
                  </Button>
                </Link>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Run History — only show runs that actually processed tables */}
      {scheduler?.run_history?.some((r: any) => (r.tables_processed ?? 0) > 0 || r.status === "error") && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base flex items-center gap-2">
                <History className="h-4 w-4" /> Run History ({scheduler.run_history.filter((r: any) => (r.tables_processed ?? 0) > 0 || r.status === "error").length})
              </CardTitle>
              <div className="flex items-center gap-2">
                <Link to="/data-quality/anomalies">
                  <Button variant="outline" size="sm" className="h-7 text-xs gap-1">
                    <Activity className="h-3 w-3" /> Anomalies
                  </Button>
                </Link>
                <Link to="/data-quality/trends">
                  <Button variant="outline" size="sm" className="h-7 text-xs gap-1">
                    <Clock className="h-3 w-3" /> Trends
                  </Button>
                </Link>
                <Link to="/data-quality/incidents">
                  <Button variant="outline" size="sm" className="h-7 text-xs gap-1">
                    <Eye className="h-3 w-3" /> Incidents
                  </Button>
                </Link>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {(() => {
              const allRuns = scheduler.run_history.filter((r: any) => (r.tables_processed ?? 0) > 0 || r.status === "error");
              const totalPages = Math.ceil(allRuns.length / HISTORY_PAGE_SIZE);
              const pageRuns = allRuns.slice(historyPage * HISTORY_PAGE_SIZE, (historyPage + 1) * HISTORY_PAGE_SIZE);
              return (
                <>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-muted-foreground">
                          <th className="py-2 px-3 text-left font-medium">Status</th>
                          <th className="py-2 px-3 text-left font-medium">Timestamp</th>
                          <th className="py-2 px-3 text-right font-medium">Tables</th>
                          <th className="py-2 px-3 text-right font-medium">Metrics</th>
                          <th className="py-2 px-3 text-right font-medium">Anomalies</th>
                          <th className="py-2 px-3 text-right font-medium">Errors</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pageRuns.map((run: any, i: number) => {
                          const globalIdx = historyPage * HISTORY_PAGE_SIZE + i;
                          const isExpanded = expandedRun === globalIdx;
                          const details = run.details || [];
                          return (
                            <React.Fragment key={i}>
                              <tr
                                className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer ${isExpanded ? "bg-muted/20" : ""}`}
                                onClick={() => setExpandedRun(isExpanded ? null : globalIdx)}
                              >
                                <td className="py-1.5 px-3">
                                  <div className="flex items-center gap-1">
                                    {isExpanded ? <ChevronDown className="h-3 w-3 text-muted-foreground" /> : <ChevronRight className="h-3 w-3 text-muted-foreground" />}
                                    {run.status === "error" ? (
                                      <Badge variant="outline" className="text-[10px] text-red-500 border-red-500/30">Failed</Badge>
                                    ) : (
                                      <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">
                                        <CheckCircle className="h-2.5 w-2.5 mr-0.5" /> OK
                                      </Badge>
                                    )}
                                  </div>
                                </td>
                                <td className="py-1.5 px-3 text-xs text-muted-foreground">{new Date(run.timestamp).toLocaleString()}</td>
                                <td className="py-1.5 px-3 text-right text-xs">{run.tables_processed ?? "—"}</td>
                                <td className="py-1.5 px-3 text-right text-xs">{run.metrics_recorded ?? "—"}</td>
                                <td className="py-1.5 px-3 text-right text-xs">
                                  {(run.anomalies_found ?? 0) > 0 ? (
                                    <Link to="/data-quality/anomalies" className="text-red-500 font-medium hover:underline" onClick={e => e.stopPropagation()}>{run.anomalies_found}</Link>
                                  ) : (
                                    <span>{run.anomalies_found ?? "—"}</span>
                                  )}
                                </td>
                                <td className="py-1.5 px-3 text-right text-xs">
                                  {run.error ? (
                                    <span className="text-red-500 truncate max-w-[200px] inline-block" title={run.error}>{run.error}</span>
                                  ) : (
                                    <span className={run.errors > 0 ? "text-red-500" : ""}>{run.errors ?? 0}</span>
                                  )}
                                </td>
                              </tr>
                              {isExpanded && details.length > 0 && (
                                <tr>
                                  <td colSpan={6} className="p-0">
                                    <div className="bg-muted/10 border-b border-border px-6 py-3">
                                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-2 font-semibold">Tables Collected</p>
                                      <div className="grid gap-2">
                                        {details.map((d: any) => (
                                          <div key={d.table_fqn} className="flex items-center gap-4 text-xs bg-background rounded-md px-3 py-2 border border-border/50">
                                            <span className="font-mono font-medium text-foreground min-w-[250px] truncate" title={d.table_fqn}>{d.table_fqn}</span>
                                            <div className="flex flex-wrap gap-2">
                                              {Object.entries(d.metrics || {}).map(([metric, value]: [string, any]) => (
                                                <span key={metric} className="inline-flex items-center gap-1">
                                                  <span className="text-muted-foreground">{metric}:</span>
                                                  <span className="font-mono font-medium">{typeof value === "number" ? value.toLocaleString() : value}</span>
                                                </span>
                                              ))}
                                            </div>
                                            {(d.anomalies || []).length > 0 && (
                                              <div className="flex gap-1 ml-auto">
                                                {d.anomalies.map((a: any, ai: number) => (
                                                  <Badge key={ai} variant="outline" className={`text-[9px] ${a.severity === "critical" ? "text-red-500 border-red-500/30" : "text-amber-500 border-amber-500/30"}`}>
                                                    {a.metric} z={a.z_score}
                                                  </Badge>
                                                ))}
                                              </div>
                                            )}
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  </td>
                                </tr>
                              )}
                              {isExpanded && details.length === 0 && run.status !== "error" && (
                                <tr>
                                  <td colSpan={6} className="px-6 py-3 text-xs text-muted-foreground bg-muted/10 border-b border-border">
                                    No per-table details available for this run.
                                  </td>
                                </tr>
                              )}
                            </React.Fragment>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  {totalPages > 1 && (
                    <div className="flex items-center justify-between pt-3 border-t border-border/50 mt-2">
                      <span className="text-xs text-muted-foreground">
                        Showing {historyPage * HISTORY_PAGE_SIZE + 1}–{Math.min((historyPage + 1) * HISTORY_PAGE_SIZE, allRuns.length)} of {allRuns.length}
                      </span>
                      <div className="flex items-center gap-1">
                        <Button variant="outline" size="sm" className="h-7 w-7 p-0"
                          disabled={historyPage === 0}
                          onClick={() => setHistoryPage(p => p - 1)}>
                          <ChevronLeft className="h-3.5 w-3.5" />
                        </Button>
                        {Array.from({ length: totalPages }, (_, i) => (
                          <Button key={i} variant={historyPage === i ? "default" : "outline"} size="sm"
                            className="h-7 w-7 p-0 text-xs"
                            onClick={() => setHistoryPage(i)}>
                            {i + 1}
                          </Button>
                        ))}
                        <Button variant="outline" size="sm" className="h-7 w-7 p-0"
                          disabled={historyPage >= totalPages - 1}
                          onClick={() => setHistoryPage(p => p + 1)}>
                          <ChevronRight className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </div>
                  )}
                </>
              );
            })()}
          </CardContent>
        </Card>
      )}

      {/* Discovery Results */}
      {availableTables.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">Available Tables ({availableTables.length})</CardTitle>
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm" onClick={toggleSelectAll}>
                  {selected.size === availableTables.length ? "Deselect All" : "Select All"}
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Metric + Frequency selection for bulk add */}
            <div className="flex flex-wrap items-center gap-3 p-3 bg-muted/30 rounded-lg">
              <span className="text-xs text-muted-foreground font-medium">Metrics:</span>
              {ALL_METRICS.map(m => (
                <button key={m} onClick={() => toggleBulkMetric(m)}
                  className={`px-2 py-0.5 rounded text-xs border transition-colors ${
                    bulkMetrics.has(m)
                      ? "bg-primary/10 border-primary/30 text-primary font-medium"
                      : "border-border text-muted-foreground hover:bg-muted"
                  }`}>
                  {METRIC_LABELS[m] || m}
                </button>
              ))}
              <span className="text-xs text-muted-foreground font-medium ml-2">Frequency:</span>
              <select value={bulkFrequency} onChange={e => setBulkFrequency(e.target.value)}
                className="h-8 rounded-md border border-input bg-background px-2 text-xs font-medium min-w-[130px]">
                {FREQUENCY_OPTIONS.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
              </select>
            </div>

            {/* Table list with checkboxes */}
            <div className="max-h-64 overflow-y-auto border rounded-lg">
              {availableTables.map(fqn => (
                <label key={fqn} className={`flex items-center gap-2 px-3 py-1.5 border-b border-border/50 cursor-pointer text-sm font-mono transition-colors ${
                  selected.has(fqn) ? "bg-muted/50" : "hover:bg-muted/30"
                }`}>
                  <input type="checkbox" checked={selected.has(fqn)} onChange={() => toggleSelect(fqn)} className="rounded border-border" />
                  {fqn}
                </label>
              ))}
            </div>

            {selected.size > 0 && (
              <Button onClick={bulkAdd} disabled={adding}>
                {adding ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Plus className="h-4 w-4 mr-2" />}
                Add {selected.size} Table(s) for Monitoring
              </Button>
            )}
          </CardContent>
        </Card>
      )}

      {/* Existing Configs Table */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <CardTitle className="text-base">Monitored Tables ({configs.length})</CardTitle>
              {selectedConfigs.size > 0 && (
                <Button variant="destructive" size="sm" onClick={bulkDeleteConfigs} disabled={bulkDeleting}>
                  {bulkDeleting ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Trash2 className="h-3 w-3 mr-1" />}
                  Delete {selectedConfigs.size} Selected
                </Button>
              )}
            </div>
            <div className="relative">
              <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground" />
              <Input placeholder="Filter tables..." className="pl-8 h-8 w-56 text-xs"
                value={searchQuery} onChange={e => setSearchQuery(e.target.value)} />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading configurations...
            </div>
          ) : filtered.length === 0 ? (
            <div className="text-center py-12 space-y-3">
              <Activity className="h-12 w-12 mx-auto text-muted-foreground/50" />
              <p className="text-muted-foreground text-sm">No monitoring configurations found.</p>
              <p className="text-xs text-muted-foreground">Discover and add tables above to start monitoring.</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="py-2 px-2 w-8">
                      <input type="checkbox"
                        checked={selectedConfigs.size === filtered.length && filtered.length > 0}
                        onChange={toggleSelectAll}
                        className="h-3.5 w-3.5 rounded border-border cursor-pointer" />
                    </th>
                    <th className="py-2 px-3 text-left font-medium">Table</th>
                    <th className="py-2 px-3 text-left font-medium">Metrics</th>
                    <th className="py-2 px-3 text-center font-medium">Frequency</th>
                    <th className="py-2 px-3 text-center font-medium">Baseline</th>
                    <th className="py-2 px-3 text-center font-medium">Status</th>
                    <th className="py-2 px-3 text-right font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(c => (
                    <tr key={c.config_id} className={`border-b border-border/50 hover:bg-muted/30 transition-colors ${!c.enabled ? "opacity-50" : ""} ${selectedConfigs.has(c.config_id) ? "bg-[#E8453C]/5" : ""}`}>
                      <td className="py-1.5 px-2">
                        <input type="checkbox"
                          checked={selectedConfigs.has(c.config_id)}
                          onChange={() => toggleSelectConfig(c.config_id)}
                          className="h-3.5 w-3.5 rounded border-border cursor-pointer" />
                      </td>
                      <td className="py-1.5 px-3 font-mono text-xs">{c.table_fqn}</td>
                      <td className="py-1.5 px-3">
                        <div className="flex flex-wrap gap-1">
                          {c.metrics.map(m => (
                            <Badge key={m} variant="outline" className="text-[10px]">
                              {METRIC_LABELS[m] || m}
                            </Badge>
                          ))}
                        </div>
                      </td>
                      <td className="py-1.5 px-3 text-center">
                        <select
                          value={c.frequency}
                          onChange={async (e) => {
                            const newFreq = e.target.value;
                            try {
                              await api.put(`/data-quality/monitoring/configs/${c.config_id}`, {
                                ...c,
                                frequency: newFreq,
                              });
                              setConfigs(prev => prev.map(x => x.config_id === c.config_id ? { ...x, frequency: newFreq } : x));
                              toast.success(`Frequency updated to ${newFreq}`);
                            } catch (err: any) { toast.error(err.message || "Failed to update"); }
                          }}
                          className="h-8 rounded-md border border-input bg-background px-2 text-xs font-medium cursor-pointer min-w-[130px]"
                        >
                          {FREQUENCY_OPTIONS.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                        </select>
                      </td>
                      <td className="py-1.5 px-3 text-center">
                        {c.baseline_status === "ready" ? (
                          <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">
                            <CheckCircle className="h-2.5 w-2.5 mr-1" /> Ready
                          </Badge>
                        ) : (
                          <Badge variant="outline" className="text-[10px] text-blue-500 border-blue-500/30">
                            <Clock className="h-2.5 w-2.5 mr-1" /> Pending
                          </Badge>
                        )}
                      </td>
                      <td className="py-1.5 px-3 text-center">
                        <button onClick={() => toggleConfig(c.config_id)}
                          className="inline-flex items-center gap-1 text-xs hover:underline"
                          title={c.enabled ? "Pause monitoring" : "Resume monitoring"}>
                          {c.enabled ? (
                            <><Eye className="h-3.5 w-3.5 text-green-500" /> <span className="text-green-500">Active</span></>
                          ) : (
                            <><EyeOff className="h-3.5 w-3.5 text-amber-500" /> <span className="text-amber-500">Paused</span></>
                          )}
                        </button>
                      </td>
                      <td className="py-1.5 px-3 text-right">
                        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => deleteConfig(c.config_id)}>
                          <Trash2 className="h-3.5 w-3.5 text-destructive" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
