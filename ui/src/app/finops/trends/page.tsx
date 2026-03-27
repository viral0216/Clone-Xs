// @ts-nocheck
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useAzureCosts, useBillingData } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import {
  TrendingUp, Loader2, TrendingDown, AlertTriangle, Calendar, DollarSign,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, ReferenceDot,
  ScatterChart, Scatter, ComposedChart,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatCost(value: number | null, currency: string = "USD"): string {
  if (value == null) return "\u2014";
  const symbols: Record<string, string> = { USD: "$", EUR: "\u20AC", GBP: "\u00A3", INR: "\u20B9", AUD: "A$", CAD: "C$", JPY: "\u00A5" };
  const sym = symbols[currency] || "$";
  if (value >= 1_000_000) return `${sym}${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${sym}${(value / 1_000).toFixed(1)}K`;
  return `${sym}${value.toFixed(2)}`;
}

function formatNumber(n: number | null): string {
  if (n == null) return "\u2014";
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function SummaryCard({ label, value, sub, color }: { label: string; value: string | number; sub?: string; color?: string }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
        {sub && <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>}
      </CardContent>
    </Card>
  );
}

// ── Component ────────────────────────────────────────────────────────

export default function CostTrendsPage() {
  const [catalog, setCatalog] = useState("");
  const [period, setPeriod] = useState(30);

  const azureQuery = useAzureCosts(period);
  const billingQuery = useBillingData(catalog, period);

  const loading = azureQuery.isLoading || billingQuery.isLoading;

  const azureCosts = azureQuery.data ?? null;
  const azureConfigured = !!azureCosts;
  const billingRaw = billingQuery.data;
  const billingData = Array.isArray(billingRaw) ? billingRaw : [];

  // ── Daily trend data ───────────────────────────────────────────────
  const dailyTrend = azureCosts?.daily_trend || [];
  const currency = azureCosts?.currency || "USD";

  // ── DBU aggregation by day ─────────────────────────────────────────
  const dbuByDay: Record<string, number> = {};
  billingData.forEach((r) => {
    const d = r.date?.slice(0, 10);
    if (d) dbuByDay[d] = (dbuByDay[d] || 0) + (Number(r.usage_quantity) || 0);
  });

  // ── Combined chart data ────────────────────────────────────────────
  const chartData = useMemo(() => {
    const dayMap: Record<string, any> = {};
    dailyTrend.forEach((d: any) => {
      const date = d.date?.slice(0, 10);
      if (date) dayMap[date] = { ...dayMap[date], date, cost: d.cost || 0 };
    });
    Object.entries(dbuByDay).forEach(([date, dbus]) => {
      dayMap[date] = { ...dayMap[date], date, dbus };
    });
    return Object.values(dayMap).sort((a: any, b: any) => a.date.localeCompare(b.date));
  }, [dailyTrend, dbuByDay]);

  // ── KPI calculations ───────────────────────────────────────────────
  const totalCost = dailyTrend.reduce((s: number, d: any) => s + (d.cost || 0), 0);
  const avgDaily = dailyTrend.length > 0 ? totalCost / dailyTrend.length : 0;
  const projectedMonthly = avgDaily * 30;

  // ── MoM calculation ────────────────────────────────────────────────
  const momDelta = useMemo(() => {
    if (period < 31 || dailyTrend.length < 31) return null;
    const sorted = [...dailyTrend].sort((a: any, b: any) => a.date.localeCompare(b.date));
    const midpoint = sorted.length - 30;
    if (midpoint <= 0) return null;
    const current30 = sorted.slice(midpoint).reduce((s: number, d: any) => s + (d.cost || 0), 0);
    const previous30 = sorted.slice(Math.max(0, midpoint - 30), midpoint).reduce((s: number, d: any) => s + (d.cost || 0), 0);
    if (previous30 === 0) return null;
    return ((current30 - previous30) / previous30) * 100;
  }, [dailyTrend, period]);

  // ── Anomaly detection: days where cost > 2x average ────────────────
  const anomalies = useMemo(() => {
    if (avgDaily <= 0) return [];
    return chartData.filter((d: any) => d.cost > avgDaily * 2);
  }, [chartData, avgDaily]);

  // Mark anomalies on the chart data
  const chartDataWithAnomalies = useMemo(() => {
    const anomalyDates = new Set(anomalies.map((a: any) => a.date));
    return chartData.map((d: any) => ({
      ...d,
      anomaly: anomalyDates.has(d.date) ? d.cost : null,
    }));
  }, [chartData, anomalies]);

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost Trends"
        description="Track cost trends over time, identify anomalies, and compare month-over-month spending."
        icon={TrendingUp}
        breadcrumbs={["FinOps", "Budgets & Alerts", "Trends"]}
      />

      {/* ── Filters ─────────────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row gap-4 items-end">
            <div className="flex-1">
              <CatalogPicker
                catalog={catalog}
                onCatalogChange={setCatalog}
                showSchema={false}
                showTable={false}
              />
            </div>
            <div className="flex gap-2">
              {[30, 60, 90].map((d) => (
                <Button
                  key={d}
                  variant={period === d ? "default" : "outline"}
                  size="sm"
                  onClick={() => setPeriod(d)}
                >
                  {d}d
                </Button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* ── Loading ─────────────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading trend data...
        </div>
      ) : chartData.length === 0 && !azureConfigured ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <TrendingUp className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">No trend data available. Configure Azure Cost Management or select a catalog.</p>
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard
              label={`Total Cost (${period}d)`}
              value={formatCost(totalCost, currency)}
            />
            <SummaryCard
              label="Avg Daily"
              value={formatCost(avgDaily, currency)}
            />
            <SummaryCard
              label="Projected Monthly"
              value={formatCost(projectedMonthly, currency)}
              sub="Based on avg daily"
            />
            <SummaryCard
              label="MoM Change"
              value={momDelta != null ? `${momDelta > 0 ? "+" : ""}${momDelta.toFixed(1)}%` : "\u2014"}
              sub={momDelta != null ? "Current 30d vs previous 30d" : `Need >${period > 30 ? "" : "30"}d data`}
              color={momDelta != null ? (momDelta > 10 ? "red" : momDelta > 0 ? "amber" : "green") : undefined}
            />
          </div>

          {/* ── Cost Trend Chart ─────────────────────────────────────── */}
          {chartDataWithAnomalies.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">Daily Cost Trend</CardTitle>
                  {anomalies.length > 0 && (
                    <Badge variant="outline" className="text-red-500 border-red-500/30">
                      <AlertTriangle className="h-3 w-3 mr-1" />
                      {anomalies.length} anomal{anomalies.length === 1 ? "y" : "ies"}
                    </Badge>
                  )}
                </div>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={320}>
                  <ComposedChart data={chartDataWithAnomalies}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis dataKey="date" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <YAxis yAxisId="left" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    {Object.keys(dbuByDay).length > 0 && (
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    )}
                    <Tooltip formatter={(v: number, name: string) => name === "DBUs" ? formatNumber(v) : formatCost(v, currency)} />
                    <Legend />
                    <Line yAxisId="left" type="monotone" dataKey="cost" stroke="#E8453C" strokeWidth={2} dot={{ r: 1.5 }} name="Cost" />
                    <Scatter yAxisId="left" dataKey="anomaly" fill="#dc2626" name="Anomaly (>2x avg)" shape="circle" />
                    {Object.keys(dbuByDay).length > 0 && (
                      <Line yAxisId="right" type="monotone" dataKey="dbus" stroke="#3b82f6" strokeWidth={1.5} dot={false} name="DBUs" strokeDasharray="4 2" />
                    )}
                  </ComposedChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {/* ── MoM Comparison ──────────────────────────────────────── */}
          {momDelta != null && period > 30 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Month-over-Month Comparison</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div className="text-center">
                    <p className="text-xs text-muted-foreground uppercase">Previous 30 days</p>
                    <p className="text-xl font-bold mt-1">
                      {(() => {
                        const sorted = [...dailyTrend].sort((a: any, b: any) => a.date.localeCompare(b.date));
                        const midpoint = sorted.length - 30;
                        const prev = sorted.slice(Math.max(0, midpoint - 30), midpoint).reduce((s: number, d: any) => s + (d.cost || 0), 0);
                        return formatCost(prev, currency);
                      })()}
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-xs text-muted-foreground uppercase">Current 30 days</p>
                    <p className="text-xl font-bold mt-1">
                      {(() => {
                        const sorted = [...dailyTrend].sort((a: any, b: any) => a.date.localeCompare(b.date));
                        const current = sorted.slice(-30).reduce((s: number, d: any) => s + (d.cost || 0), 0);
                        return formatCost(current, currency);
                      })()}
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-xs text-muted-foreground uppercase">Change</p>
                    <p className={`text-xl font-bold mt-1 flex items-center justify-center gap-1 ${momDelta > 10 ? "text-red-500" : momDelta > 0 ? "text-amber-500" : "text-green-500"}`}>
                      {momDelta > 0 ? <TrendingUp className="h-4 w-4" /> : <TrendingDown className="h-4 w-4" />}
                      {momDelta > 0 ? "+" : ""}{momDelta.toFixed(1)}%
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Anomaly List ────────────────────────────────────────── */}
          {anomalies.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Cost Anomalies (Days &gt; 2x Average)</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Date</th>
                        <th className="py-2 pr-4 text-right">Cost</th>
                        <th className="py-2 pr-4 text-right">Avg Daily</th>
                        <th className="py-2 text-right">Multiplier</th>
                      </tr>
                    </thead>
                    <tbody>
                      {anomalies.map((a: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium">{a.date}</td>
                          <td className="py-2 pr-4 text-right font-mono text-red-500">{formatCost(a.cost, currency)}</td>
                          <td className="py-2 pr-4 text-right font-mono text-muted-foreground">{formatCost(avgDaily, currency)}</td>
                          <td className="py-2 text-right font-mono">
                            <Badge variant="outline" className="text-red-500 border-red-500/30">
                              {(a.cost / avgDaily).toFixed(1)}x
                            </Badge>
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
