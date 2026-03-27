// @ts-nocheck
import { useState, useMemo } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  useBillingData, useStorageMetrics, useCostEstimate,
  useWarehouseInsights, useQueryPerformance, useFinOpsConfig,
} from "@/hooks/useApi";
import {
  DollarSign, Loader2, RefreshCw, HardDrive, Zap, TrendingDown,
  AlertTriangle, CheckCircle, XCircle, Server, Database, Lightbulb,
  BarChart3, Target,
} from "lucide-react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatCost(value: number | null, currency: string = "USD"): string {
  if (value == null) return "—";
  const symbols: Record<string, string> = { USD: "$", EUR: "\u20AC", GBP: "\u00A3", INR: "\u20B9", AUD: "A$", CAD: "C$", JPY: "\u00A5" };
  const sym = symbols[currency] || "$";
  if (value >= 1_000_000) return `${sym}${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${sym}${(value / 1_000).toFixed(1)}K`;
  return `${sym}${value.toFixed(2)}`;
}

function formatBytes(bytes: number | null): string {
  if (bytes == null) return "—";
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

function formatNumber(n: number | null): string {
  if (n == null) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

const CHART_COLORS = ["#E8453C", "#3b82f6", "#22c55e", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"];

// ── Component ────────────────────────────────────────────────────────

export default function FinOpsPage() {
  const [catalog, setCatalog] = useState("");
  const queryClient = useQueryClient();

  // Cached config
  const { data: configData } = useFinOpsConfig();
  const currency = configData?.currency ?? "USD";
  const pricePerGb = configData?.price_per_gb ?? 0.023;

  // Cached data hooks — all share query keys so switching pages doesn't re-fetch
  const billingQuery = useBillingData(catalog);
  const storageQuery = useStorageMetrics(catalog);
  const estimateQuery = useCostEstimate(catalog);
  const warehouseQuery = useWarehouseInsights(catalog);
  const queryPerfQuery = useQueryPerformance(catalog);

  const loading = billingQuery.isLoading || storageQuery.isLoading || estimateQuery.isLoading;
  const billing = billingQuery.data?.usage || billingQuery.data || [];
  const storageData = storageQuery.data || null;
  const costEstimate = estimateQuery.data || null;
  const warehouses = warehouseQuery.data?.warehouses || warehouseQuery.data || [];
  const queryPerf = queryPerfQuery.data || null;

  // Budget (localStorage)
  const [monthlyBudget, setMonthlyBudget] = useState(() => {
    try { return Number(localStorage.getItem("clxs-finops-budget")) || 0; } catch { return 0; }
  });

  function saveBudget(val: number) {
    setMonthlyBudget(val);
    try { localStorage.setItem("clxs-finops-budget", String(val)); } catch {}
  }

  function loadAll() {
    queryClient.invalidateQueries({ queryKey: ["finops-billing"] });
    queryClient.invalidateQueries({ queryKey: ["finops-storage"] });
    queryClient.invalidateQueries({ queryKey: ["finops-estimate"] });
    queryClient.invalidateQueries({ queryKey: ["finops-warehouses"] });
    queryClient.invalidateQueries({ queryKey: ["finops-query-perf"] });
  }

  // ── Derived metrics ────────────────────────────────────────────────

  const totalDbus = useMemo(() =>
    billing.reduce((sum, r) => sum + (Number(r.dbus) || Number(r.usage_quantity) || 0), 0), [billing]);

  const totalDbCost = useMemo(() =>
    billing.reduce((sum, r) => sum + (Number(r.cost) || Number(r.dollar_amount) || 0), 0), [billing]);

  const monthlyCost = costEstimate?.monthly_cost || (storageData ? (storageData.total_bytes || 0) / 1_073_741_824 * pricePerGb : 0);
  const totalStorageGb = storageData ? (storageData.total_bytes || 0) / 1_073_741_824 : costEstimate?.total_gb || 0;
  const vacuumableBytes = storageData?.vacuumable_bytes || storageData?.tables?.reduce((s: number, t: any) => s + (Number(t.vacuumable_bytes) || 0), 0) || 0;
  const vacuumablePct = storageData?.total_bytes > 0 ? (vacuumableBytes / storageData.total_bytes * 100) : 0;
  const vacuumSavings = (vacuumableBytes / 1_073_741_824) * pricePerGb;

  const estimatedMonthlyCost = monthlyCost + totalDbCost;

  // Cost trend (daily)
  const costTrend = useMemo(() => {
    const byDate: Record<string, number> = {};
    for (const r of billing) {
      const date = String(r.date || r.usage_date || "").slice(0, 10);
      if (date) byDate[date] = (byDate[date] || 0) + (Number(r.cost) || Number(r.dollar_amount) || 0);
    }
    return Object.entries(byDate).sort(([a], [b]) => a.localeCompare(b)).map(([date, cost]) => ({ date, cost: Math.round(cost * 100) / 100 }));
  }, [billing]);

  // SKU breakdown
  const skuBreakdown = useMemo(() => {
    const bySku: Record<string, number> = {};
    for (const r of billing) {
      const sku = r.sku || r.sku_name || r.billing_origin_product || "Unknown";
      bySku[sku] = (bySku[sku] || 0) + (Number(r.cost) || Number(r.dollar_amount) || 0);
    }
    return Object.entries(bySku)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 8)
      .map(([sku, cost], i) => ({ sku: sku.length > 25 ? sku.slice(0, 22) + "..." : sku, cost: Math.round(cost * 100) / 100, fill: CHART_COLORS[i % CHART_COLORS.length] }));
  }, [billing]);

  // Top tables by size
  const topTables = useMemo(() => {
    const tables = storageData?.tables || costEstimate?.tables || [];
    return [...tables]
      .sort((a: any, b: any) => (Number(b.total_bytes) || Number(b.size_bytes) || 0) - (Number(a.total_bytes) || Number(a.size_bytes) || 0))
      .slice(0, 10);
  }, [storageData, costEstimate]);

  // Recommendations
  const recommendations = useMemo(() => {
    const recs: { severity: string; message: string; icon: any }[] = [];
    const noAutoStop = warehouses.filter((w: any) => !w.auto_stop_mins && w.auto_stop_mins !== 0 && w.state === "RUNNING");
    if (noAutoStop.length > 0) recs.push({ severity: "warning", message: `${noAutoStop.length} warehouse(s) don't have auto-stop enabled — they may incur idle costs`, icon: Server });
    if (vacuumableBytes > 1_073_741_824) recs.push({ severity: "info", message: `${formatBytes(vacuumableBytes)} of storage can be reclaimed via VACUUM (saves ~${formatCost(vacuumSavings, currency)}/mo)`, icon: HardDrive });
    const highTtTables = (storageData?.tables || []).filter((t: any) => {
      const total = Number(t.total_bytes) || 0;
      const tt = Number(t.time_travel_bytes) || 0;
      return total > 0 && tt / total > 0.5;
    });
    if (highTtTables.length > 0) recs.push({ severity: "warning", message: `${highTtTables.length} table(s) have >50% time-travel overhead — consider reducing retention`, icon: Database });
    if (monthlyBudget > 0 && estimatedMonthlyCost > monthlyBudget * 0.9) recs.push({ severity: "critical", message: `Estimated cost (${formatCost(estimatedMonthlyCost, currency)}) is approaching budget (${formatCost(monthlyBudget, currency)})`, icon: AlertTriangle });
    const slowQueries = queryPerf?.summary?.p95_duration_ms;
    if (slowQueries && slowQueries > 30000) recs.push({ severity: "info", message: `P95 query duration is ${(slowQueries / 1000).toFixed(1)}s — consider optimizing slow queries or scaling warehouse`, icon: Zap });
    if (recs.length === 0) recs.push({ severity: "ok", message: "No issues detected — your FinOps posture looks good", icon: CheckCircle });
    return recs;
  }, [warehouses, vacuumableBytes, vacuumSavings, storageData, monthlyBudget, estimatedMonthlyCost, queryPerf, currency]);

  const hasData = billing.length > 0 || storageData || costEstimate;

  return (
    <div className="space-y-6">
      <PageHeader
        title="FinOps Dashboard"
        description="Unified view of costs, storage, compute usage, and optimization opportunities."
        icon={DollarSign}
        breadcrumbs={["FinOps", "Dashboard"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="min-w-[240px]">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog</label>
              <CatalogPicker value={catalog} onChange={setCatalog} placeholder="Select catalog..." />
            </div>
            <Button onClick={loadAll} disabled={loading || !catalog}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {loading ? "Loading..." : "Refresh"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {!hasData && !loading && (
        <div className="text-center py-16">
          <DollarSign className="h-12 w-12 mx-auto text-muted-foreground/30 mb-3" />
          <p className="text-sm text-muted-foreground">Select a catalog and click Refresh to load FinOps data.</p>
        </div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}

      {hasData && !loading && (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <DollarSign className="h-4 w-4 text-[#E8453C]" />
                  <p className="text-xs text-muted-foreground uppercase">Est. Monthly Cost</p>
                </div>
                <p className="text-2xl font-bold mt-1">{formatCost(estimatedMonthlyCost, currency)}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">Storage + Compute</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <HardDrive className="h-4 w-4 text-blue-500" />
                  <p className="text-xs text-muted-foreground uppercase">Total Storage</p>
                </div>
                <p className="text-2xl font-bold mt-1">{formatNumber(totalStorageGb)} GB</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{formatCost(totalStorageGb * pricePerGb, currency)}/mo</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <Zap className="h-4 w-4 text-amber-500" />
                  <p className="text-xs text-muted-foreground uppercase">DBU Usage (30d)</p>
                </div>
                <p className="text-2xl font-bold mt-1">{formatNumber(totalDbus)}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{formatCost(totalDbCost, currency)} compute cost</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <TrendingDown className="h-4 w-4 text-green-500" />
                  <p className="text-xs text-muted-foreground uppercase">Optimization Savings</p>
                </div>
                <p className="text-2xl font-bold mt-1 text-green-500">{formatCost(vacuumSavings, currency)}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{vacuumablePct.toFixed(1)}% reclaimable via VACUUM</p>
              </CardContent>
            </Card>
          </div>

          {/* Budget Tracker */}
          <Card>
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-base flex items-center gap-2">
                  <Target className="h-4 w-4" /> Budget Tracker
                </CardTitle>
                <div className="flex items-center gap-2">
                  <label className="text-xs text-muted-foreground">Monthly Budget:</label>
                  <Input
                    type="number" min={0} step={100}
                    value={monthlyBudget || ""}
                    onChange={e => saveBudget(Number(e.target.value) || 0)}
                    placeholder="Set budget..."
                    className="w-32 h-7 text-xs"
                  />
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {monthlyBudget > 0 ? (
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span>{formatCost(estimatedMonthlyCost, currency)} of {formatCost(monthlyBudget, currency)}</span>
                    <span className={estimatedMonthlyCost > monthlyBudget ? "text-red-500 font-medium" : estimatedMonthlyCost > monthlyBudget * 0.8 ? "text-amber-500" : "text-green-500"}>
                      {((estimatedMonthlyCost / monthlyBudget) * 100).toFixed(0)}%
                    </span>
                  </div>
                  <div className="w-full bg-muted rounded-full h-3">
                    <div
                      className={`h-3 rounded-full transition-all ${estimatedMonthlyCost > monthlyBudget ? "bg-red-500" : estimatedMonthlyCost > monthlyBudget * 0.8 ? "bg-amber-500" : "bg-green-500"}`}
                      style={{ width: `${Math.min(100, (estimatedMonthlyCost / monthlyBudget) * 100)}%` }}
                    />
                  </div>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground">Set a monthly budget to track spending against your target.</p>
              )}
            </CardContent>
          </Card>

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Cost Trend */}
            {costTrend.length > 1 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Daily Compute Cost (30d)</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={220}>
                    <LineChart data={costTrend}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis dataKey="date" tick={{ fontSize: 9 }} stroke="var(--muted-foreground)" />
                      <YAxis tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                      <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                      <Line type="monotone" dataKey="cost" stroke="#E8453C" strokeWidth={2} dot={{ r: 2 }} name="Cost" />
                    </LineChart>
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
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={skuBreakdown} layout="vertical">
                      <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                      <YAxis type="category" dataKey="sku" tick={{ fontSize: 9 }} stroke="var(--muted-foreground)" width={120} />
                      <Tooltip formatter={(v: number) => formatCost(v, currency)} />
                      <Bar dataKey="cost" name="Cost" radius={[0, 4, 4, 0]}>
                        {skuBreakdown.map((d, i) => <Cell key={i} fill={d.fill} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Storage Optimization */}
          {storageData && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <HardDrive className="h-4 w-4" /> Storage Optimization
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div>
                    <p className="text-xs text-muted-foreground uppercase mb-1">Total Storage</p>
                    <p className="text-xl font-bold">{formatBytes(storageData.total_bytes)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase mb-1">Reclaimable (VACUUM)</p>
                    <p className="text-xl font-bold text-green-500">{formatBytes(vacuumableBytes)}</p>
                    <p className="text-[10px] text-muted-foreground">{vacuumablePct.toFixed(1)}% of total</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase mb-1">Est. Monthly Savings</p>
                    <p className="text-xl font-bold text-green-500">{formatCost(vacuumSavings, currency)}</p>
                  </div>
                </div>
                <div className="mt-3 w-full bg-muted rounded-full h-2">
                  <div className="h-2 rounded-full bg-blue-500" style={{ width: `${100 - vacuumablePct}%` }} />
                </div>
                <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
                  <span>Active data</span>
                  <span>Reclaimable</span>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Top Tables + Warehouse Efficiency */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Top Tables by Cost */}
            {topTables.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Top 10 Tables by Size</CardTitle>
                </CardHeader>
                <CardContent>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="py-1.5 px-2 text-left font-medium">Table</th>
                        <th className="py-1.5 px-2 text-right font-medium">Size</th>
                        <th className="py-1.5 px-2 text-right font-medium">Est. Cost/mo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {topTables.map((t: any, i: number) => {
                        const bytes = Number(t.total_bytes) || Number(t.size_bytes) || 0;
                        const gb = bytes / 1_073_741_824;
                        const name = t.table_fqn || t.table_name || t.name || "—";
                        const short = name.split(".").slice(-2).join(".");
                        return (
                          <tr key={i} className="border-b border-border/30 hover:bg-muted/30">
                            <td className="py-1 px-2 font-mono truncate max-w-[200px]" title={name}>{short}</td>
                            <td className="py-1 px-2 text-right tabular-nums">{formatBytes(bytes)}</td>
                            <td className="py-1 px-2 text-right tabular-nums">{formatCost(gb * pricePerGb, currency)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </CardContent>
              </Card>
            )}

            {/* Warehouse Efficiency */}
            {warehouses.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Warehouse Efficiency</CardTitle>
                </CardHeader>
                <CardContent>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="py-1.5 px-2 text-left font-medium">Warehouse</th>
                        <th className="py-1.5 px-2 text-center font-medium">State</th>
                        <th className="py-1.5 px-2 text-center font-medium">Size</th>
                        <th className="py-1.5 px-2 text-center font-medium">Auto-Stop</th>
                      </tr>
                    </thead>
                    <tbody>
                      {warehouses.map((w: any, i: number) => {
                        const hasAutoStop = w.auto_stop_mins > 0 || w.auto_stop_minutes > 0;
                        return (
                          <tr key={i} className="border-b border-border/30 hover:bg-muted/30">
                            <td className="py-1 px-2 font-mono truncate max-w-[160px]">{w.name || w.id}</td>
                            <td className="py-1 px-2 text-center">
                              <Badge variant="outline" className={`text-[9px] ${w.state === "RUNNING" ? "text-green-500 border-green-500/30" : w.state === "STOPPED" ? "text-muted-foreground" : "text-amber-500 border-amber-500/30"}`}>
                                {w.state}
                              </Badge>
                            </td>
                            <td className="py-1 px-2 text-center text-muted-foreground">{w.cluster_size || w.size || "—"}</td>
                            <td className="py-1 px-2 text-center">
                              {hasAutoStop ? (
                                <span className="text-green-500">{w.auto_stop_mins || w.auto_stop_minutes}m</span>
                              ) : (
                                <span className="text-red-500">Off</span>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Recommendations */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <Lightbulb className="h-4 w-4" /> Recommendations
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {recommendations.map((rec, i) => {
                  const Icon = rec.icon;
                  const colors: Record<string, string> = {
                    critical: "border-l-red-500 bg-red-500/5",
                    warning: "border-l-amber-500 bg-amber-500/5",
                    info: "border-l-blue-500 bg-blue-500/5",
                    ok: "border-l-green-500 bg-green-500/5",
                  };
                  const iconColors: Record<string, string> = {
                    critical: "text-red-500",
                    warning: "text-amber-500",
                    info: "text-blue-500",
                    ok: "text-green-500",
                  };
                  return (
                    <div key={i} className={`flex items-start gap-3 p-3 rounded-lg border-l-2 ${colors[rec.severity] || ""}`}>
                      <Icon className={`h-4 w-4 mt-0.5 shrink-0 ${iconColors[rec.severity] || ""}`} />
                      <p className="text-sm">{rec.message}</p>
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
