// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useAzureCosts, useCostEstimate } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import {
  PieChart as PieChartIcon, Loader2, DollarSign, HardDrive, Server, TrendingUp,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend,
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

export default function CostBreakdownPage() {
  const [catalog, setCatalog] = useState("");

  const azureQuery = useAzureCosts(30);
  const estimateQuery = useCostEstimate(catalog);

  const loading = azureQuery.isLoading || estimateQuery.isLoading;

  const azureCosts = azureQuery.data ?? null;
  const azureConfigured = !!azureCosts;
  const storageEstimate = estimateQuery.data ?? null;

  // ── Derived metrics ────────────────────────────────────────────────
  const currency = azureCosts?.currency || "USD";
  const totalAzureCost = azureCosts?.total_cost ?? null;
  const databricksCost = azureCosts?.databricks_costs?.total ?? null;
  const storageCostEst = storageEstimate?.monthly_cost_usd ?? null;
  const projectedMonthly = totalAzureCost != null ? totalAzureCost : storageCostEst;

  // ── Service breakdown pie ──────────────────────────────────────────
  const serviceBreakdown = azureCosts?.service_breakdown
    ? Object.entries(azureCosts.service_breakdown)
        .map(([service, cost]) => ({ name: service, value: cost as number }))
        .sort((a, b) => b.value - a.value)
    : [];

  // ── Resource group breakdown ───────────────────────────────────────
  const rgBreakdown = azureCosts?.resource_group_breakdown
    ? Object.entries(azureCosts.resource_group_breakdown)
        .map(([rg, cost]) => ({ resource_group: rg, cost: cost as number }))
        .sort((a, b) => b.cost - a.cost)
    : [];

  // ── Top resources ──────────────────────────────────────────────────
  const topResources = azureCosts?.top_resources || [];

  // ── Storage estimate top tables ────────────────────────────────────
  const topTables = storageEstimate?.top_tables || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost Breakdown"
        description="Detailed breakdown of Azure and Databricks costs by service, resource group, and resource."
        icon={PieChartIcon}
        breadcrumbs={["FinOps", "Cost Analysis", "Breakdown"]}
      />

      {/* ── Catalog Picker ──────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <CatalogPicker
            catalog={catalog}
            onCatalogChange={setCatalog}
            showSchema={false}
            showTable={false}
          />
        </CardContent>
      </Card>

      {/* ── Loading ─────────────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading cost breakdown...
        </div>
      ) : !azureConfigured && !storageEstimate ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <PieChartIcon className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">No cost data available. Configure Azure Cost Management or select a catalog for storage estimates.</p>
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard
              label="Total Azure Cost"
              value={totalAzureCost != null ? formatCost(totalAzureCost, currency) : "\u2014"}
              sub={azureConfigured ? "30d period" : "Not configured"}
            />
            <SummaryCard
              label="Storage Cost"
              value={storageCostEst != null ? formatCost(storageCostEst, "USD") : "\u2014"}
              sub="Estimated monthly"
            />
            <SummaryCard
              label="Databricks Cost"
              value={databricksCost != null ? formatCost(databricksCost, currency) : "\u2014"}
              sub={azureConfigured ? "30d period" : "Not configured"}
            />
            <SummaryCard
              label="Projected Monthly"
              value={projectedMonthly != null ? formatCost(projectedMonthly, currency) : "\u2014"}
              sub="Based on current usage"
            />
          </div>

          {/* ── Charts Row ──────────────────────────────────────────── */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Service Breakdown Pie */}
            {serviceBreakdown.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Service Breakdown</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={serviceBreakdown.slice(0, 8)}
                        cx="50%"
                        cy="50%"
                        outerRadius={100}
                        dataKey="value"
                        nameKey="name"
                        label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                      >
                        {serviceBreakdown.slice(0, 8).map((_, i) => (
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

            {/* Resource Group Breakdown */}
            {rgBreakdown.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Resource Group Breakdown</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={Math.max(200, rgBreakdown.length * 36)}>
                    <BarChart data={rgBreakdown.slice(0, 10)} layout="vertical" margin={{ left: 120 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                      <YAxis type="category" dataKey="resource_group" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" width={110} />
                      <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                      <Bar dataKey="cost" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>

          {/* ── Top Resources Table ─────────────────────────────────── */}
          {topResources.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Top 20 Resources by Cost</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Resource</th>
                        <th className="py-2 pr-4">Service</th>
                        <th className="py-2 text-right">Cost</th>
                      </tr>
                    </thead>
                    <tbody>
                      {topResources.slice(0, 20).map((r: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium truncate max-w-[350px]">{r.resource_name || r.name}</td>
                          <td className="py-2 pr-4 text-muted-foreground">{r.service || r.meter_category || "\u2014"}</td>
                          <td className="py-2 text-right font-mono">{formatCost(r.cost, currency)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Storage Estimate (fallback when no Azure) ───────────── */}
          {!azureConfigured && storageEstimate && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Storage Cost Estimate</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-center gap-4 text-sm">
                    <span className="text-muted-foreground">Total Size:</span>
                    <span className="font-mono font-medium">{storageEstimate.total_gb?.toFixed(1)} GB</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span className="text-muted-foreground">Monthly Cost:</span>
                    <span className="font-mono font-medium">{formatCost(storageEstimate.monthly_cost_usd, "USD")}</span>
                  </div>
                  {topTables.length > 0 && (
                    <>
                      <p className="text-xs text-muted-foreground uppercase tracking-wider mt-4">Top Tables</p>
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b text-left text-muted-foreground">
                              <th className="py-2 pr-4">Table</th>
                              <th className="py-2 text-right">Size (GB)</th>
                            </tr>
                          </thead>
                          <tbody>
                            {topTables.map((t: any, i: number) => (
                              <tr key={i} className="border-b border-border/50">
                                <td className="py-2 pr-4 font-medium">{t.table_name || t.name}</td>
                                <td className="py-2 text-right font-mono">{(t.size_gb ?? t.total_gb ?? 0).toFixed(2)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  )}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
