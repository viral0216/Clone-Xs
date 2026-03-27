// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import { useBillingData, useAzureCosts } from "@/hooks/useApi";
import {
  Receipt, Loader2, DollarSign, TrendingUp, BarChart3, Zap,
} from "lucide-react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
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

const CHART_COLORS = ["#E8453C", "#3b82f6", "#22c55e", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"];

// ── Component ────────────────────────────────────────────────────────

export default function BillingPage() {
  const [catalog, setCatalog] = useState("");
  const [days, setDays] = useState(30);

  const billingQuery = useBillingData(catalog, days);
  const azureQuery = useAzureCosts(days);

  const loading = billingQuery.isLoading;
  const billingData = Array.isArray(billingQuery.data) ? billingQuery.data : billingQuery.data?.usage || [];
  const azureCosts = azureQuery.data || null;
  const azureConfigured = !!azureCosts;

  // ── Derived metrics ────────────────────────────────────────────────
  const totalDBUs = billingData.reduce((sum, r) => sum + (Number(r.usage_quantity) || 0), 0);
  const totalAzureCost = azureCosts?.total_cost ?? null;
  const currency = azureCosts?.currency || "USD";
  const avgDailyCost = azureCosts?.daily_trend?.length
    ? azureCosts.daily_trend.reduce((s: number, d: any) => s + (d.cost || 0), 0) / azureCosts.daily_trend.length
    : null;
  const databricksCost = azureCosts?.databricks_costs?.total ?? null;
  const dbxPct = totalAzureCost && databricksCost ? ((databricksCost / totalAzureCost) * 100).toFixed(1) : null;

  // ── Daily DBU aggregation ──────────────────────────────────────────
  const dailyMap: Record<string, number> = {};
  billingData.forEach((r) => {
    const d = r.date?.slice(0, 10);
    if (d) dailyMap[d] = (dailyMap[d] || 0) + (Number(r.usage_quantity) || 0);
  });
  const dailyDBU = Object.entries(dailyMap)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, dbus]) => ({ date, dbus }));

  // Merge Azure daily trend
  const chartData = dailyDBU.map((d) => {
    const azureDay = azureCosts?.daily_trend?.find((a: any) => a.date?.slice(0, 10) === d.date);
    return { ...d, azure_cost: azureDay?.cost ?? null };
  });

  // ── SKU breakdown ──────────────────────────────────────────────────
  const skuMap: Record<string, number> = {};
  billingData.forEach((r) => {
    const sku = r.sku || "Unknown";
    skuMap[sku] = (skuMap[sku] || 0) + (Number(r.usage_quantity) || 0);
  });
  const skuBreakdown = Object.entries(skuMap)
    .sort(([, a], [, b]) => b - a)
    .map(([sku, quantity]) => ({ sku, quantity }));

  // ── Azure service breakdown ────────────────────────────────────────
  const serviceBreakdown = azureCosts?.service_breakdown
    ? Object.entries(azureCosts.service_breakdown)
        .map(([service, cost]) => ({ service, cost: cost as number }))
        .sort((a, b) => b.cost - a.cost)
    : [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Billing & DBU Usage"
        description="Track Databricks DBU consumption and Azure cost breakdowns."
        icon={Receipt}
        breadcrumbs={["FinOps", "Cost Analysis", "Billing"]}
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
                  variant={days === d ? "default" : "outline"}
                  size="sm"
                  onClick={() => setDays(d)}
                >
                  {d}d
                </Button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* ── Loading / Empty ─────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading billing data...
        </div>
      ) : !catalog ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <Receipt className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">Select a catalog to view billing data.</p>
        </div>
      ) : billingData.length === 0 && !azureCosts ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <Receipt className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">No billing data available for the selected period.</p>
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total DBUs" value={formatNumber(totalDBUs)} sub={`Last ${days} days`} />
            <SummaryCard
              label="Total Azure Cost"
              value={totalAzureCost != null ? formatCost(totalAzureCost, currency) : "\u2014"}
              sub={azureConfigured ? `${days}d period` : "Azure not configured"}
            />
            <SummaryCard
              label="Avg Daily Cost"
              value={avgDailyCost != null ? formatCost(avgDailyCost, currency) : "\u2014"}
              sub="Per day average"
            />
            <SummaryCard
              label="Databricks % of Total"
              value={dbxPct != null ? `${dbxPct}%` : "\u2014"}
              sub="Databricks vs total Azure"
              color={dbxPct && parseFloat(dbxPct) > 80 ? "red" : dbxPct && parseFloat(dbxPct) > 50 ? "amber" : "green"}
            />
          </div>

          {/* ── Daily DBU Usage Chart ───────────────────────────────── */}
          {chartData.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Daily DBU Usage</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={280}>
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis dataKey="date" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <YAxis yAxisId="left" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    {azureConfigured && (
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    )}
                    <Tooltip />
                    <Legend />
                    <Line yAxisId="left" type="monotone" dataKey="dbus" stroke="#E8453C" strokeWidth={2} dot={{ r: 2 }} name="DBUs" />
                    {azureConfigured && (
                      <Line yAxisId="right" type="monotone" dataKey="azure_cost" stroke="#3b82f6" strokeWidth={2} dot={{ r: 2 }} name="Azure Cost" />
                    )}
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {/* ── SKU Breakdown Table ─────────────────────────────────── */}
          {skuBreakdown.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">SKU Breakdown</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">SKU</th>
                        <th className="py-2 pr-4 text-right">Usage (DBUs)</th>
                        <th className="py-2 text-right">% of Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {skuBreakdown.map((row, i) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium">{row.sku}</td>
                          <td className="py-2 pr-4 text-right font-mono">{formatNumber(row.quantity)}</td>
                          <td className="py-2 text-right font-mono">
                            {totalDBUs > 0 ? ((row.quantity / totalDBUs) * 100).toFixed(1) : 0}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Azure Service Breakdown Bar Chart ───────────────────── */}
          {serviceBreakdown.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Azure Service Breakdown</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={Math.max(200, serviceBreakdown.length * 36)}>
                  <BarChart data={serviceBreakdown} layout="vertical" margin={{ left: 120 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <YAxis type="category" dataKey="service" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" width={110} />
                    <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                    <Bar dataKey="cost" fill="#E8453C" radius={[0, 4, 4, 0]} />
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
