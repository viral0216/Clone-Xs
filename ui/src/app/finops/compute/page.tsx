// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { useFinOpsClusters, useFinOpsQueryStats, useAzureCosts } from "@/hooks/useApi";
import {
  Zap, Loader2, Server, Clock, Activity, BarChart3, RefreshCw,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
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

function formatDuration(ms: number | null): string {
  if (ms == null) return "\u2014";
  if (ms >= 60_000) return `${(ms / 60_000).toFixed(1)}m`;
  if (ms >= 1_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${Math.round(ms)}ms`;
}

function formatBytes(bytes: number | null): string {
  if (bytes == null || bytes === 0) return "\u2014";
  if (bytes >= 1_099_511_627_776) return `${(bytes / 1_099_511_627_776).toFixed(2)} TB`;
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
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

export default function ComputeCostsPage() {
  const clusterQuery = useFinOpsClusters();
  const queryStatsQuery = useFinOpsQueryStats(30);
  const azureQuery = useAzureCosts(30);

  const loading = clusterQuery.isLoading || queryStatsQuery.isLoading;
  const clusterData = clusterQuery.data || null;
  const queryStats = queryStatsQuery.data || null;
  const azureCosts = azureQuery.data || null;
  const azureConfigured = !!azureCosts;

  // ── Derived metrics ────────────────────────────────────────────────
  const clusters = clusterData?.clusters || [];
  const runningClusters = clusters.filter((c: any) => c.state === "RUNNING" || c.state === "RESIZING").length;

  const querySummary = queryStats?.summary || {};
  const totalQueries = Number(querySummary.total_queries ?? 0);
  const p95Duration = querySummary.p95_duration_ms != null ? Number(querySummary.p95_duration_ms) : null;

  const byWarehouse = queryStats?.by_warehouse || [];
  const byUser = queryStats?.by_user || [];
  const slowestQueries = queryStats?.slowest || [];

  const databricksCost = azureCosts?.databricks_costs?.total ?? null;
  const currency = azureCosts?.currency || "USD";

  // ── Sub-category cost breakdown ────────────────────────────────────
  const subCategories = azureCosts?.databricks_costs?.sub_categories
    ? Object.entries(azureCosts.databricks_costs.sub_categories)
        .map(([category, cost]) => ({ category, cost: cost as number }))
        .sort((a, b) => b.cost - a.cost)
    : [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Compute Costs"
        description="Monitor cluster usage, query performance, and compute-related costs."
        icon={Zap}
        breadcrumbs={["FinOps", "Cost Analysis", "Compute"]}
      />

      {/* ── Controls ────────────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex justify-end">
            <Button variant="outline" size="sm" onClick={() => { clusterQuery.refetch(); queryStatsQuery.refetch(); azureQuery.refetch(); }} disabled={clusterQuery.isRefetching || queryStatsQuery.isRefetching || azureQuery.isRefetching}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${clusterQuery.isRefetching || queryStatsQuery.isRefetching || azureQuery.isRefetching ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* ── Loading ─────────────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading compute data...
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard
              label="Running Clusters"
              value={runningClusters}
              sub={`${clusters.length} total`}
              color={runningClusters > 5 ? "amber" : "green"}
            />
            <SummaryCard label="Total Queries (30d)" value={formatNumber(totalQueries)} />
            <SummaryCard label="P95 Query Duration" value={formatDuration(p95Duration)} color={p95Duration && p95Duration > 60000 ? "amber" : "green"} />
            <SummaryCard
              label="Databricks Compute Cost"
              value={databricksCost != null ? formatCost(databricksCost, currency) : "\u2014"}
              sub={azureConfigured ? "30d period" : "Azure not configured"}
            />
          </div>

          {/* ── Cluster Table ───────────────────────────────────────── */}
          {clusters.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Clusters</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Name</th>
                        <th className="py-2 pr-4">State</th>
                        <th className="py-2 pr-4">Node Type</th>
                        <th className="py-2 pr-4 text-right">Workers</th>
                        <th className="py-2">Spark Version</th>
                      </tr>
                    </thead>
                    <tbody>
                      {clusters.map((c: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium truncate max-w-[250px]">{c.cluster_name || "\u2014"}</td>
                          <td className="py-2 pr-4">
                            <Badge
                              variant="outline"
                              className={
                                c.state === "RUNNING" ? "text-green-500 border-green-500/30" :
                                c.state === "TERMINATED" ? "text-muted-foreground border-border" :
                                "text-amber-500 border-amber-500/30"
                              }
                            >
                              {c.state || "\u2014"}
                            </Badge>
                          </td>
                          <td className="py-2 pr-4 font-mono text-xs">{c.worker_node_type || "\u2014"}</td>
                          <td className="py-2 pr-4 text-right">{c.worker_count ?? "\u2014"}</td>
                          <td className="py-2 font-mono text-xs truncate max-w-[200px]">{c.dbr_version || "\u2014"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Per-Warehouse Query Breakdown ───────────────────────── */}
          {byWarehouse.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Query Performance by Warehouse</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Warehouse</th>
                        <th className="py-2 pr-4 text-right">Queries</th>
                        <th className="py-2 pr-4 text-right">Avg Duration</th>
                        <th className="py-2 pr-4 text-right">P95</th>
                        <th className="py-2 text-right">Total Read</th>
                      </tr>
                    </thead>
                    <tbody>
                      {byWarehouse.map((w: any, i: number) => (
                        <tr key={i} className="border-b border-border/50">
                          <td className="py-2 pr-4 font-medium truncate max-w-[200px]">{w.warehouse_name || w.warehouse_id || w.warehouse}</td>
                          <td className="py-2 pr-4 text-right font-mono">{formatNumber(Number(w.query_count ?? w.queries ?? 0))}</td>
                          <td className="py-2 pr-4 text-right font-mono">{formatDuration(w.avg_duration_ms != null ? Number(w.avg_duration_ms) : null)}</td>
                          <td className="py-2 pr-4 text-right font-mono">{formatDuration(w.p95_duration_ms != null ? Number(w.p95_duration_ms) : null)}</td>
                          <td className="py-2 text-right font-mono">{formatBytes(w.total_read_bytes != null ? Number(w.total_read_bytes) : null)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Databricks Sub-Category Cost Bar Chart ──────────────── */}
          {subCategories.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Databricks Compute Cost Breakdown</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={Math.max(200, subCategories.length * 40)}>
                  <BarChart data={subCategories} layout="vertical" margin={{ left: 140 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <YAxis type="category" dataKey="category" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" width={130} />
                    <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                    <Bar dataKey="cost" fill="#E8453C" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {/* ── Longest Running Queries ───────────────────────────────── */}
          {slowestQueries.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Longest Running Queries (Top 50)</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="py-2 px-2 text-left font-medium">Query</th>
                        <th className="py-2 px-2 text-left font-medium">User</th>
                        <th className="py-2 px-2 text-left font-medium">Warehouse</th>
                        <th className="py-2 px-2 text-right font-medium">Duration</th>
                        <th className="py-2 px-2 text-right font-medium">Read</th>
                        <th className="py-2 px-2 text-left font-medium">Started</th>
                      </tr>
                    </thead>
                    <tbody>
                      {slowestQueries.slice(0, 20).map((q: any, i: number) => (
                        <tr key={q.query_id || i} className="border-b border-border/30 hover:bg-muted/30">
                          <td className="py-1.5 px-2 max-w-[300px] truncate font-mono text-[10px]" title={q.query_text}>
                            {q.query_text || "—"}
                          </td>
                          <td className="py-1.5 px-2 text-muted-foreground">{q.user_name || "—"}</td>
                          <td className="py-1.5 px-2 font-mono text-muted-foreground">{(q.warehouse_id || "").slice(0, 8)}</td>
                          <td className="py-1.5 px-2 text-right font-mono font-medium text-red-500">
                            {formatDuration(q.total_duration_ms != null ? Number(q.total_duration_ms) : null)}
                          </td>
                          <td className="py-1.5 px-2 text-right font-mono text-muted-foreground">
                            {q.read_bytes != null ? formatBytes(Number(q.read_bytes)) : "—"}
                          </td>
                          <td className="py-1.5 px-2 text-muted-foreground">{String(q.start_time || "").slice(0, 16)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* ── Empty state ─────────────────────────────────────────── */}
          {clusters.length === 0 && byWarehouse.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
              <Zap className="h-10 w-10 mb-3 opacity-40" />
              <p className="text-sm">No compute data available. Check your Databricks connection.</p>
            </div>
          )}
        </>
      )}
    </div>
  );
}
