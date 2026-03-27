// @ts-nocheck
import { useState, useEffect } from "react";
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

const FREQUENCY_OPTIONS = ["hourly", "daily", "weekly"];

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

  // Run monitoring
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState<any>(null);

  useEffect(() => { loadConfigs(); }, []);

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

      {/* Controls */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Discover Tables</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-end gap-3">
            <div className="min-w-[200px]">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog</label>
              <CatalogPicker value={catalog} onChange={setCatalog} placeholder="Select catalog..." />
            </div>
            <div className="min-w-[160px]">
              <label className="text-xs text-muted-foreground mb-1 block">Schema (optional)</label>
              <Input value={schema} onChange={e => setSchema(e.target.value)} placeholder="e.g. gold" />
            </div>
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
            <div className="flex items-center gap-6 text-sm">
              <div><span className="text-muted-foreground">Tables processed:</span> <strong>{runResult.tables_processed}</strong></div>
              <div><span className="text-muted-foreground">Metrics recorded:</span> <strong>{runResult.metrics_recorded}</strong></div>
              <div><span className="text-muted-foreground">Anomalies found:</span> <strong className={runResult.anomalies_found > 0 ? "text-red-500" : "text-green-500"}>{runResult.anomalies_found}</strong></div>
              <div><span className="text-muted-foreground">Errors:</span> <strong className={runResult.errors > 0 ? "text-red-500" : ""}>{runResult.errors}</strong></div>
            </div>
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
                className="h-7 rounded border border-input bg-background px-2 text-xs">
                {FREQUENCY_OPTIONS.map(f => <option key={f} value={f}>{f}</option>)}
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
            <CardTitle className="text-base">Monitored Tables ({configs.length})</CardTitle>
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
                    <tr key={c.config_id} className={`border-b border-border/50 hover:bg-muted/30 transition-colors ${!c.enabled ? "opacity-50" : ""}`}>
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
                        <Badge variant="outline" className="text-[10px]">
                          <Clock className="h-2.5 w-2.5 mr-1" />
                          {c.frequency}
                        </Badge>
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
