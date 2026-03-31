// @ts-nocheck
// useState no longer needed — billing data is catalog-independent
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useBillingCost } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
// CatalogPicker no longer needed — billing data is catalog-independent
import {
  PieChart as PieChartIcon, Loader2, DollarSign, HardDrive, Server, TrendingUp, RefreshCw,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatCost(value: number | string | null, currency: string = "USD"): string {
  if (value == null) return "\u2014";
  const n = Number(value);
  if (isNaN(n)) return "\u2014";
  const symbols: Record<string, string> = { USD: "$", EUR: "\u20AC", GBP: "\u00A3", INR: "\u20B9", AUD: "A$", CAD: "C$", JPY: "\u00A5" };
  const sym = symbols[currency] || "$";
  if (n >= 1_000_000) return `${sym}${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${sym}${(n / 1_000).toFixed(1)}K`;
  return `${sym}${n.toFixed(2)}`;
}

function formatNumber(n: number | string | null): string {
  if (n == null) return "\u2014";
  const v = Number(n);
  if (isNaN(v)) return "\u2014";
  return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
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

const CHART_COLORS = ["#E8453C", "#374151", "#9CA3AF", "#6B7280", "#D1D5DB", "#B91C1C", "#1F2937", "#4B5563"];

// ── Component ────────────────────────────────────────────────────────

export default function CostBreakdownPage() {
  const billingQuery = useBillingCost(30);

  const loading = billingQuery.isLoading;

  const billingResult = billingQuery.data ?? null;

  // ── Derived metrics ────────────────────────────────────────────────
  const currency = billingResult?.currency || "USD";
  const totalCost = billingResult?.total_cost != null ? Number(billingResult.total_cost) : null;
  const totalDBUs = billingResult?.total_dbus != null ? Number(billingResult.total_dbus) : null;
  const avgDailyCost = billingResult?.avg_daily_cost != null ? Number(billingResult.avg_daily_cost) : null;
  const projectedMonthly = avgDailyCost != null ? avgDailyCost * 30 : null;

  // ── Product breakdown pie (by_product) ─────────────────────────────
  const byProduct = Array.isArray(billingResult?.by_product) ? billingResult.by_product : [];
  const productBreakdown = byProduct
    .map((p: any) => ({ name: p.product || p.name || "Unknown", value: Number(p.cost ?? p.total ?? 0) }))
    .sort((a: any, b: any) => b.value - a.value);

  // ── SKU breakdown ──────────────────────────────────────────────────
  const bySku = Array.isArray(billingResult?.by_sku) ? billingResult.by_sku : [];
  const skuBreakdown = bySku
    .map((s: any) => ({ resource_group: s.sku || s.sku_name || "Unknown", cost: Number(s.cost ?? s.total ?? 0) }))
    .sort((a: any, b: any) => b.cost - a.cost);

  // ── By warehouse ───────────────────────────────────────────────────
  const byWarehouse = Array.isArray(billingResult?.by_warehouse) ? billingResult.by_warehouse : [];

  // ── By user ────────────────────────────────────────────────────────
  const byUser = Array.isArray(billingResult?.by_user) ? billingResult.by_user : [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost Breakdown"
        description="Detailed breakdown of Azure and Databricks costs by service, resource group, and resource."
        icon={PieChartIcon}
        breadcrumbs={["FinOps", "Cost Analysis", "Breakdown"]}
      />

      {/* ── Controls ────────────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex justify-end">
            <Button variant="outline" size="sm" onClick={() => { billingQuery.refetch(); }} disabled={billingQuery.isRefetching}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${billingQuery.isRefetching ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* ── Loading ─────────────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading cost breakdown...
        </div>
      ) : !billingResult ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <PieChartIcon className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">No billing data available. System tables may not be accessible.</p>
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard
              label="Total Cost (30d)"
              value={totalCost != null ? formatCost(totalCost, currency) : "\u2014"}
              sub="From billing system tables"
            />
            <SummaryCard
              label="Total DBUs"
              value={totalDBUs != null ? formatNumber(totalDBUs) : "\u2014"}
              sub="30d period"
            />
            <SummaryCard
              label="Avg Daily Cost"
              value={avgDailyCost != null ? formatCost(avgDailyCost, currency) : "\u2014"}
              sub="30d average"
            />
            <SummaryCard
              label="Projected Monthly"
              value={projectedMonthly != null ? formatCost(projectedMonthly, currency) : "\u2014"}
              sub="Based on avg daily"
            />
          </div>

          {/* ── Charts Row ──────────────────────────────────────────── */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Product Breakdown Pie */}
            {productBreakdown.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Cost by Product</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={productBreakdown.slice(0, 8)}
                        cx="50%"
                        cy="50%"
                        outerRadius={100}
                        dataKey="value"
                        nameKey="name"
                        label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                      >
                        {productBreakdown.slice(0, 8).map((_: any, i: number) => (
                          <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}

            {/* SKU Breakdown */}
            {skuBreakdown.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Cost by SKU</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={Math.max(200, skuBreakdown.length * 36)}>
                    <BarChart data={skuBreakdown.slice(0, 10)} layout="vertical" margin={{ left: 120 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                      <YAxis type="category" dataKey="resource_group" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" width={110} />
                      <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                      <Bar dataKey="cost" fill="#374151" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>

          {/* ── By Warehouse Table ──────────────────────────────────── */}
          {byWarehouse.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Cost by Warehouse</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Warehouse</th>
                        <th className="py-2 text-right">Cost</th>
                      </tr>
                    </thead>
                    <tbody>
                      {byWarehouse.map((w: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium truncate max-w-[350px]">{w.warehouse_id || w.warehouse || w.name || "\u2014"}</td>
                          <td className="py-2 text-right font-mono">{formatCost(Number(w.cost ?? w.total ?? 0), currency)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── By User Table ───────────────────────────────────────── */}
          {byUser.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Cost by User</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">User</th>
                        <th className="py-2 text-right">Cost</th>
                      </tr>
                    </thead>
                    <tbody>
                      {byUser.map((u: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium truncate max-w-[350px]">{u.user || u.user_name || u.identity || "\u2014"}</td>
                          <td className="py-2 text-right font-mono">{formatCost(Number(u.cost ?? u.total ?? 0), currency)}</td>
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
