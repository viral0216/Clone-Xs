// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  DollarSign, Loader2, RefreshCw, TrendingDown, AlertTriangle,
  Database, Settings, Calculator,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, Cell,
} from "recharts";

const CATEGORY_COLORS: Record<string, string> = {
  dq_failure: "#ef4444",
  sla_breach: "#f59e0b",
  reprocessing: "#8b5cf6",
  manual_fix: "#3b82f6",
  downstream_impact: "#ec4899",
  data_loss: "#dc2626",
  other: "#6b7280",
};

function categoryLabel(cat: string) {
  return cat.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatDollars(amount: number) {
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `$${(amount / 1_000).toFixed(1)}K`;
  return `$${amount.toFixed(0)}`;
}

export default function COPQPage() {
  const [loading, setLoading] = useState(false);
  const [computing, setComputing] = useState(false);
  const [summary, setSummary] = useState<any>(null);
  const [byTable, setByTable] = useState<any[]>([]);
  const [trends, setTrends] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showConfig, setShowConfig] = useState(false);
  const [config, setConfig] = useState({
    hourly_rate: "150",
    rerun_cost: "50",
    sla_penalty: "500",
    downstream_multiplier: "2.5",
  });

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const [sumData, tableData, trendData] = await Promise.all([
        api.get("/copq/summary"),
        api.get("/copq/by-table"),
        api.get("/copq/trends"),
      ]);
      setSummary(sumData);
      setByTable(Array.isArray(tableData) ? tableData : tableData?.tables || []);
      setTrends(Array.isArray(trendData) ? trendData : trendData?.weeks || []);
    } catch (e: any) {
      setError(e.message || "Failed to load COPQ data.");
    }
    setLoading(false);
  }

  async function computeCOPQ() {
    setComputing(true);
    setError(null);
    try {
      await api.post("/copq/compute", {
        hourly_rate: parseFloat(config.hourly_rate) || 150,
        rerun_cost: parseFloat(config.rerun_cost) || 50,
        sla_penalty: parseFloat(config.sla_penalty) || 500,
        downstream_multiplier: parseFloat(config.downstream_multiplier) || 2.5,
      });
      await loadData();
    } catch (e: any) {
      setError(e.message || "Failed to compute COPQ.");
    }
    setComputing(false);
  }

  useEffect(() => {
    loadData();
  }, []);

  const totalCost = summary?.total_cost ?? 0;
  const breakdown = summary?.by_category ?? {};
  const breakdownEntries = Object.entries(breakdown).sort(
    ([, a]: any, [, b]: any) => (b as number) - (a as number)
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost of Poor Data Quality"
        icon={DollarSign}
        description="Quantify DQ failures in dollars"
        breadcrumbs={["FinOps", "COPQ"]}
      />

      {/* Actions */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <Button onClick={loadData} disabled={loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Refresh
            </Button>
            <Button onClick={computeCOPQ} disabled={computing} variant="secondary">
              {computing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Calculator className="h-4 w-4 mr-2" />}
              Compute COPQ
            </Button>
            <Button variant="outline" onClick={() => setShowConfig(!showConfig)}>
              <Settings className="h-4 w-4 mr-2" />
              Cost Config
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Config Section */}
      {showConfig && (
        <Card className="bg-card border-border border-blue-500/30">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Settings className="h-5 w-5 text-blue-400" /> Cost Assumptions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { key: "hourly_rate", label: "Hourly Rate ($)", placeholder: "150" },
                { key: "rerun_cost", label: "Rerun Cost ($)", placeholder: "50" },
                { key: "sla_penalty", label: "SLA Penalty ($)", placeholder: "500" },
                { key: "downstream_multiplier", label: "Downstream Multiplier", placeholder: "2.5" },
              ].map((field) => (
                <div key={field.key} className="flex flex-col gap-1">
                  <label className="text-sm text-muted-foreground">{field.label}</label>
                  <input
                    className="px-3 py-2 rounded-md border border-border bg-background text-sm"
                    value={config[field.key]}
                    onChange={(e) => setConfig({ ...config, [field.key]: e.target.value })}
                    placeholder={field.placeholder}
                  />
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {error && (
        <div className="text-red-400 text-sm bg-red-500/10 border border-red-500/30 rounded-md p-3">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <>
          {/* Big Total Card */}
          <Card className="bg-card border-border border-red-500/30">
            <CardContent className="pt-6 pb-6 text-center">
              <p className="text-sm text-muted-foreground mb-1">Total Cost of Poor Data Quality</p>
              <p className="text-5xl font-bold text-red-400">{formatDollars(totalCost)}</p>
              {summary?.period && (
                <p className="text-xs text-muted-foreground mt-2">Period: {summary.period}</p>
              )}
            </CardContent>
          </Card>

          {/* Breakdown Cards */}
          {breakdownEntries.length > 0 && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {breakdownEntries.map(([cat, cost]: any) => (
                <Card key={cat} className="bg-card border-border">
                  <CardContent className="pt-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm text-muted-foreground">{categoryLabel(cat)}</p>
                        <p className="text-2xl font-bold mt-1">{formatDollars(cost)}</p>
                      </div>
                      <div
                        className="h-10 w-10 rounded-full flex items-center justify-center"
                        style={{ backgroundColor: `${CATEGORY_COLORS[cat] || "#6b7280"}20` }}
                      >
                        <DollarSign
                          className="h-5 w-5"
                          style={{ color: CATEGORY_COLORS[cat] || "#6b7280" }}
                        />
                      </div>
                    </div>
                    <div className="mt-2 h-1.5 bg-muted rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full"
                        style={{
                          width: `${totalCost ? (cost / totalCost) * 100 : 0}%`,
                          backgroundColor: CATEGORY_COLORS[cat] || "#6b7280",
                        }}
                      />
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {/* Weekly Trends Chart */}
          {trends.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <TrendingDown className="h-5 w-5" /> Weekly COPQ Trends
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={trends}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="week" tick={{ fontSize: 12 }} stroke="#888" />
                    <YAxis
                      tick={{ fontSize: 12 }}
                      stroke="#888"
                      tickFormatter={(v) => formatDollars(v)}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: "#1c1c1c", border: "1px solid #333" }}
                      formatter={(value: number) => [formatDollars(value), "Cost"]}
                    />
                    <Legend />
                    <Bar dataKey="total_cost" name="Total Cost" fill="#ef4444" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="dq_failure" name="DQ Failures" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="sla_breach" name="SLA Breaches" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {/* Most Expensive Tables */}
          {byTable.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <AlertTriangle className="h-5 w-5 text-amber-400" /> Most Expensive Tables
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="text-left py-2 px-3">Rank</th>
                        <th className="text-left py-2 px-3">Table</th>
                        <th className="text-right py-2 px-3">Cost</th>
                        <th className="text-right py-2 px-3">Incidents</th>
                        <th className="text-left py-2 px-3">Primary Category</th>
                      </tr>
                    </thead>
                    <tbody>
                      {byTable
                        .sort((a, b) => (b.total_cost ?? 0) - (a.total_cost ?? 0))
                        .slice(0, 20)
                        .map((row, i) => (
                          <tr key={i} className="border-b border-border/50 hover:bg-muted/30 transition-colors">
                            <td className="py-2 px-3">
                              <Badge variant="outline" className="text-xs">#{i + 1}</Badge>
                            </td>
                            <td className="py-2 px-3 font-medium flex items-center gap-2">
                              <Database className="h-4 w-4 text-muted-foreground" />
                              {row.table_name}
                            </td>
                            <td className="py-2 px-3 text-right font-mono text-red-400">
                              {formatDollars(row.total_cost ?? 0)}
                            </td>
                            <td className="py-2 px-3 text-right">{row.incident_count ?? "—"}</td>
                            <td className="py-2 px-3">
                              {row.primary_category ? (
                                <Badge
                                  style={{
                                    backgroundColor: `${CATEGORY_COLORS[row.primary_category] || "#6b7280"}20`,
                                    color: CATEGORY_COLORS[row.primary_category] || "#6b7280",
                                  }}
                                >
                                  {categoryLabel(row.primary_category)}
                                </Badge>
                              ) : (
                                "—"
                              )}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
