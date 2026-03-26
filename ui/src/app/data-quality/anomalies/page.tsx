// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import {
  AlertTriangle, Loader2, XCircle, AlertCircle, Info, RefreshCw, X,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  ReferenceArea,
} from "recharts";

interface Anomaly {
  id?: string;
  table_name: string;
  column_name: string;
  metric: string;
  value: number;
  baseline: number;
  z_score: number;
  severity: "critical" | "warning" | "info";
  detected_at: string;
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

  async function loadAnomalies() {
    setLoading(true);
    try {
      const data = await api.get("/data-quality/anomalies");
      setAnomalies(Array.isArray(data) ? data : []);
    } catch (err: any) {
      toast.error(err?.message || "Failed to load anomalies.");
      setAnomalies([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadAnomalies(); }, []);

  async function viewHistory(anomaly: Anomaly) {
    setSelectedAnomaly(anomaly);
    if (anomaly.history) return;
    setHistoryLoading(true);
    try {
      const data = await api.get(
        `/data-quality/anomalies/history?table=${encodeURIComponent(anomaly.table_name)}&column=${encodeURIComponent(anomaly.column_name)}&metric=${encodeURIComponent(anomaly.metric)}`
      );
      setSelectedAnomaly({ ...anomaly, history: Array.isArray(data) ? data : [] });
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

      {/* Metric History Chart (selected anomaly) */}
      {selectedAnomaly && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">
                Metric History: {selectedAnomaly.table_name}.{selectedAnomaly.column_name} — {selectedAnomaly.metric}
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
            <p className="text-sm text-muted-foreground py-4">No anomalies detected.</p>
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
                      className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer ${selectedAnomaly?.id === a.id && selectedAnomaly?.table_name === a.table_name ? "bg-[#E8453C]/5" : ""}`}
                      onClick={() => viewHistory(a)}
                    >
                      <td className="py-1.5 px-3 font-mono text-xs">{a.table_name}</td>
                      <td className="py-1.5 px-3 text-xs">{a.column_name}</td>
                      <td className="py-1.5 px-3 text-xs">{a.metric}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums">{a.value?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground">{a.baseline?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-right tabular-nums font-medium">{a.z_score?.toFixed(2)}</td>
                      <td className="py-1.5 px-3 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <SeverityIcon severity={a.severity} />
                          <Badge variant="outline" className={`text-[10px] ${severityColor(a.severity)}`}>
                            {a.severity}
                          </Badge>
                        </div>
                      </td>
                      <td className="py-1.5 px-3 text-xs">{String(a.detected_at || "").slice(0, 19)}</td>
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
