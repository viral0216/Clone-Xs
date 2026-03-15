// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { BarChart3, RefreshCw, Activity, Clock, Zap } from "lucide-react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts";

interface Metrics {
  total_clones?: number;
  success_rate?: number;
  avg_duration?: string;
  tables_per_hour?: number;
  by_status?: { status: string; count: number }[];
}

const STATUS_COLORS: Record<string, string> = {
  success: "#22c55e",
  failed: "#ef4444",
  running: "#3b82f6",
  pending: "#eab308",
};

export default function MetricsPage() {
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<Metrics>("/monitor/metrics");
      setMetrics(res);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const summaryCards = [
    { label: "Total Clones", value: metrics?.total_clones ?? "—", icon: BarChart3, color: "text-blue-600" },
    { label: "Success Rate", value: metrics?.success_rate != null ? `${metrics.success_rate}%` : "—", icon: Activity, color: "text-green-600" },
    { label: "Avg Duration", value: metrics?.avg_duration ?? "—", icon: Clock, color: "text-orange-600" },
    { label: "Tables/Hour", value: metrics?.tables_per_hour ?? "—", icon: Zap, color: "text-purple-600" },
  ];

  const chartData = metrics?.by_status ?? [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Metrics Dashboard</h1>
          <p className="text-muted-foreground mt-1">Clone operation metrics and analytics</p>
        </div>
        <Button onClick={load} disabled={loading} variant="outline">
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      {!metrics && !loading && !error && (
        <div className="text-center py-12 text-muted-foreground">
          <BarChart3 className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>No metrics data available</p>
        </div>
      )}

      {metrics && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {summaryCards.map((c) => (
              <Card key={c.label}>
                <CardContent className="pt-6 text-center">
                  <c.icon className={`h-6 w-6 mx-auto mb-2 ${c.color}`} />
                  <p className="text-2xl font-bold text-foreground">{c.value}</p>
                  <p className="text-xs text-muted-foreground mt-1">{c.label}</p>
                </CardContent>
              </Card>
            ))}
          </div>

          {chartData.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Clones by Status</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis dataKey="status" />
                    <YAxis allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                      {chartData.map((entry, i) => (
                        <Cell key={i} fill={STATUS_COLORS[entry.status] ?? "#6b7280"} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
