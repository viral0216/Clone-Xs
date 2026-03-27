// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import {
  AlertTriangle, Loader2, XCircle, AlertCircle, Info, RefreshCw, X,
  Play, Database, BarChart3, Activity,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  ReferenceArea,
} from "recharts";

interface Anomaly {
  id?: string;
  table_fqn: string;
  column_name: string;
  metric_name: string;
  value: number;
  baseline_mean: number;
  baseline_stddev?: number;
  z_score: number;
  severity: "critical" | "warning" | "info";
  measured_at: string;
  history?: { timestamp: string; value: number; baseline_low: number; baseline_high: number }[];
}

function severityColor(severity: string) {
  if (severity === "critical") return "text-red-500 border-red-500/30 bg-red-500/5";
  if (severity === "warning") return "text-amber-500 border-amber-500/30 bg-amber-500/5";
  return "text-blue-500 border-blue-500/30 bg-blue-500/5";
}

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === "critical") return <XCircle className="h-3.5 w-3.5 text-red-500" />;
  if (severity === "warning") return <AlertCircle className="h-3.5 w-3.5 text-amber-500" />;
  return <Info className="h-3.5 w-3.5 text-blue-500" />;
}

export default function AnomaliesPage() {
  const [loading, setLoading] = useState(true);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [selectedAnomaly, setSelectedAnomaly] = useState<Anomaly | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  // Metric collection
  const [collectCatalog, setCollectCatalog] = useState("");
  const [collectSchema, setCollectSchema] = useState("");
  const [collecting, setCollecting] = useState(false);
  const [collectResult, setCollectResult] = useState<any>(null);

  // All recent measurements (not just anomalies)
  const [allMetrics, setAllMetrics] = useState<any[]>([]);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsHint, setMetricsHint] = useState<string | null>(null);

  async function loadAnomalies() {
    setLoading(true);
    try {
      const data = await api.get("/data-quality/anomalies");
      const list = Array.isArray(data) ? data : Array.isArray(data?.anomalies) ? data.anomalies : [];
      setAnomalies(list);
    } catch (err: any) {
      toast.error(err?.message || "Failed to load anomalies.");
      setAnomalies([]);
    } finally {
      setLoading(false);
    }
  }

  async function loadAllMetrics() {
    setMetricsLoading(true);
    setMetricsHint(null);
    try {
      const data = await api.get("/data-quality/metrics/recent?limit=50");
      const list = Array.isArray(data) ? data : Array.isArray(data?.metrics) ? data.metrics : [];
      setAllMetrics(list);
      if (data?.hint) setMetricsHint(data.hint);
    } catch {
      setAllMetrics([]);
    } finally {
      setMetricsLoading(false);
    }
  }

  async function collectMetrics() {
    if (!collectCatalog) { toast.error("Select a catalog"); return; }
    setCollecting(true);
    setCollectResult(null);
    try {
      const data = await api.post("/data-quality/volume/snapshot", {
        catalog: collectCatalog,
        schema_name: collectSchema || "",
      });
      setCollectResult(data);
      toast.success(`Recorded ${data.tables_recorded || 0} table metrics. ${data.errors || 0} errors.`);
      // Reload anomalies after collection
      loadAnomalies();
      loadAllMetrics();
    } catch (e: any) {
      toast.error(e.message || "Failed to collect metrics");
    } finally {
      setCollecting(false);
    }
  }

  useEffect(() => { loadAnomalies(); loadAllMetrics(); }, []);

  async function viewHistory(anomaly: Anomaly) {
    setSelectedAnomaly(anomaly);
    if (anomaly.history) return;
    setHistoryLoading(true);
    try {
      const data = await api.get(
        `/data-quality/anomalies/metrics/${encodeURIComponent(anomaly.table_fqn)}?metric_name=${encodeURIComponent(anomaly.metric_name)}`
      );
      // Transform API response to chart-friendly format
      const rawHistory = Array.isArray(data) ? data : Array.isArray(data?.history) ? data.history : [];
      const history = rawHistory.map((h: any) => ({
        timestamp: h.measured_at || h.timestamp || "",
        value: h.value,
        baseline_low: (h.baseline_mean || 0) - 2 * (h.baseline_stddev || 0),
        baseline_high: (h.baseline_mean || 0) + 2 * (h.baseline_stddev || 0),
      }));
      setSelectedAnomaly({ ...anomaly, history });
    } catch {
      // keep anomaly selected without history
    } finally {
      setHistoryLoading(false);
    }
  }

  const critical = anomalies.filter((a) => a.severity === "critical").length;
  const warning = anomalies.filter((a) => a.severity === "warning").length;
  const info = anomalies.filter((a) => a.severity === "info").length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Anomaly Detection"
        description="Statistical anomalies detected across your data assets."
        icon={AlertTriangle}
        breadcrumbs={["Data Quality", "Monitoring", "Anomalies"]}
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Total Anomalies</p>
            <p className="text-2xl font-bold mt-1">{anomalies.length}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Critical</p>
            <p className="text-2xl font-bold mt-1 text-red-500">{critical}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Warning</p>
            <p className="text-2xl font-bold mt-1 text-amber-500">{warning}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Info</p>
            <p className="text-2xl font-bold mt-1 text-blue-500">{info}</p>
          </CardContent>
        </Card>
      </div>

      {/* ── Collect Metrics ─────────────────────────────────────── */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Activity className="h-4 w-4" /> Collect Metrics
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-xs text-muted-foreground mb-3">
            Record row counts for tables in a catalog. Run multiple times to build baselines — anomalies are detected when values deviate &gt;2 standard deviations from the rolling average.
          </p>
          <div className="flex flex-wrap items-end gap-3">
            <div className="flex-1 min-w-[280px]">
              <CatalogPicker
                catalog={collectCatalog}
                schema={collectSchema}
                onCatalogChange={setCollectCatalog}
                onSchemaChange={setCollectSchema}
                showTable={false}
                idPrefix="anomaly"
              />
            </div>
            <Button onClick={collectMetrics} disabled={collecting || !collectCatalog}>
              {collecting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Play className="h-4 w-4 mr-2" />}
              {collecting ? "Collecting..." : "Collect Metrics"}
            </Button>
          </div>
          {collectResult && (
            <div className="mt-3 p-3 rounded-lg bg-muted/30 text-sm">
              Recorded <span className="font-medium">{collectResult.tables_recorded}</span> table metrics
              {collectResult.errors > 0 && <span className="text-red-500 ml-1">({collectResult.errors} errors)</span>}
              {" "}for <span className="font-mono">{collectResult.catalog}</span>
              {collectResult.schema_name && <span className="font-mono">.{collectResult.schema_name}</span>}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Metric History Chart (selected anomaly) */}
      {selectedAnomaly && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">
                Metric History: {selectedAnomaly.table_fqn}.{selectedAnomaly.column_name} — {selectedAnomaly.metric_name}
              </CardTitle>
              <Button variant="ghost" size="sm" onClick={() => setSelectedAnomaly(null)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {historyLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading history...
              </div>
            ) : selectedAnomaly.history && selectedAnomaly.history.length > 0 ? (
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={selectedAnomaly.history}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="timestamp" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                  <YAxis tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                  <Tooltip />
                  <ReferenceArea
                    y1={selectedAnomaly.history[0]?.baseline_low}
                    y2={selectedAnomaly.history[0]?.baseline_high}
                    fill="#E8453C"
                    fillOpacity={0.05}
                    label="Baseline Band"
                  />
                  <Line type="monotone" dataKey="value" stroke="#E8453C" strokeWidth={2} dot={{ r: 3 }} name="Value" />
                  <Line type="monotone" dataKey="baseline_low" stroke="var(--muted-foreground)" strokeDasharray="4 4" strokeWidth={1} dot={false} name="Baseline Low" />
                  <Line type="monotone" dataKey="baseline_high" stroke="var(--muted-foreground)" strokeDasharray="4 4" strokeWidth={1} dot={false} name="Baseline High" />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-sm text-muted-foreground py-4">No history data available for this metric.</p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Anomalies Table */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">Detected Anomalies ({anomalies.length})</CardTitle>
            <Button variant="outline" size="sm" onClick={loadAnomalies} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading anomalies...
            </div>
          ) : anomalies.length === 0 ? (
            <div className="py-6 text-center">
              <AlertTriangle className="h-8 w-8 mx-auto text-muted-foreground/30 mb-3" />
              <p className="text-sm font-medium">No anomalies detected</p>
              <p className="text-xs text-muted-foreground mt-1 max-w-md mx-auto">
                Anomalies appear when a metric value deviates more than 2 standard deviations from its baseline.
                Use "Collect Metrics" above to record table row counts, then run it again later — anomalies will be flagged when values change significantly.
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="py-2 px-3 text-left font-medium">Table</th>
                    <th className="py-2 px-3 text-left font-medium">Column</th>
                    <th className="py-2 px-3 text-left font-medium">Metric</th>
                    <th className="py-2 px-3 text-right font-medium">Value</th>
                    <th className="py-2 px-3 text-right font-medium">Baseline</th>
                    <th className="py-2 px-3 text-right font-medium">Z-Score</th>
                    <th className="py-2 px-3 text-center font-medium">Severity</th>
                    <th className="py-2 px-3 text-left font-medium">Detected At</th>
                  </tr>
                </thead>
                <tbody>
                  {anomalies.map((a, i) => (
                    <tr
                      key={a.id || i}
                      className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer ${selectedAnomaly?.id === a.id && selectedAnomaly?.table_fqn === a.table_fqn ? "bg-[#E8453C]/5" : ""}`}
                      onClick={() => viewHistory(a)}
                    >
                      <td className="py-1.5 px-3 font-mono text-xs">{a.table_fqn}</td>
                      <td className="py-1.5 px-3 text-xs">{a.column_name}</td>
                      <td className="py-1.5 px-3 text-xs">{a.metric_name}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums">{a.value?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground">{a.baseline_mean?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums font-medium">{a.z_score?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <SeverityIcon severity={a.severity} />
                          <Badge variant="outline" className={`text-[10px] ${severityColor(a.severity)}`}>
                            {a.severity}
                          </Badge>
                        </div>
                      </td>
                      <td className="py-1.5 px-3 text-xs">{String(a.measured_at || "").slice(0, 19)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Recent Measurements ───────────────────────────────────── */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <BarChart3 className="h-4 w-4" /> Recent Measurements ({allMetrics.length})
            </CardTitle>
            <Button variant="ghost" size="sm" onClick={loadAllMetrics} disabled={metricsLoading}>
              <RefreshCw className={`h-3.5 w-3.5 ${metricsLoading ? "animate-spin" : ""}`} />
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {metricsLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : allMetrics.length === 0 ? (
            <div className="py-4 text-center">
              <Database className="h-6 w-6 mx-auto text-muted-foreground/30 mb-2" />
              <p className="text-xs text-muted-foreground">
                {metricsHint || "No measurements recorded yet. Use \"Collect Metrics\" above to start."}
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="py-1.5 px-2 text-left font-medium">Table</th>
                    <th className="py-1.5 px-2 text-left font-medium">Metric</th>
                    <th className="py-1.5 px-2 text-right font-medium">Value</th>
                    <th className="py-1.5 px-2 text-right font-medium">Baseline</th>
                    <th className="py-1.5 px-2 text-right font-medium">Z-Score</th>
                    <th className="py-1.5 px-2 text-center font-medium">Status</th>
                    <th className="py-1.5 px-2 text-left font-medium">Measured At</th>
                  </tr>
                </thead>
                <tbody>
                  {allMetrics.map((m, i) => (
                    <tr key={m.id || i} className={`border-b border-border/30 ${m.is_anomaly === true || m.is_anomaly === "true" ? "bg-red-500/5" : ""}`}>
                      <td className="py-1 px-2 font-mono">{m.table_fqn}</td>
                      <td className="py-1 px-2">{m.metric_name}</td>
                      <td className="py-1 px-2 text-right tabular-nums">{Number(m.value || 0).toLocaleString()}</td>
                      <td className="py-1 px-2 text-right tabular-nums text-muted-foreground">{Number(m.baseline_mean || 0).toFixed(1)}</td>
                      <td className="py-1 px-2 text-right tabular-nums">{Number(m.z_score || 0).toFixed(2)}</td>
                      <td className="py-1 px-2 text-center">
                        {m.is_anomaly === true || m.is_anomaly === "true" ? (
                          <Badge variant="outline" className={`text-[9px] ${severityColor(m.severity)}`}>{m.severity}</Badge>
                        ) : (
                          <span className="text-muted-foreground">normal</span>
                        )}
                      </td>
                      <td className="py-1 px-2">{String(m.measured_at || "").slice(0, 19)}</td>
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
