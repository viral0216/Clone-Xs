// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import { useStorageMetrics, useFinOpsConfig } from "@/hooks/useApi";
import {
  HardDrive, Loader2, Database, Trash2, Clock, DollarSign,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatBytes(bytes: number | null): string {
  if (bytes == null || bytes === 0) return "\u2014";
  if (bytes >= 1_099_511_627_776) return `${(bytes / 1_099_511_627_776).toFixed(2)} TB`;
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

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

export default function StorageCostsPage() {
  const [catalog, setCatalog] = useState("");

  const { data: configData } = useFinOpsConfig();
  const pricePerGb = configData?.price_per_gb ?? 0.023;
  const currency = configData?.currency ?? "USD";

  const storageQuery = useStorageMetrics(catalog);
  const loading = storageQuery.isLoading;
  const storageData = storageQuery.data || null;

  // ── Derived metrics ────────────────────────────────────────────────
  const totalBytes = storageData?.total_bytes ?? 0;
  const activeBytes = storageData?.active_bytes ?? 0;
  const vacuumableBytes = storageData?.vacuumable_bytes ?? 0;
  const timeTravelBytes = storageData?.time_travel_bytes ?? 0;
  const totalGb = totalBytes / 1_073_741_824;
  const estMonthlyCost = totalGb * pricePerGb;

  // ── Pie chart data ─────────────────────────────────────────────────
  const pieData = [
    { name: "Active", value: activeBytes },
    { name: "Vacuumable", value: vacuumableBytes },
    { name: "Time Travel", value: timeTravelBytes },
  ].filter((d) => d.value > 0);

  const PIE_COLORS = ["#22c55e", "#E8453C", "#3b82f6"];

  // ── Schema breakdown ───────────────────────────────────────────────
  const schemaSummaries = storageData?.schema_summaries
    ? Object.entries(storageData.schema_summaries)
        .map(([schema, data]: [string, any]) => ({
          schema,
          total_bytes: data?.total_bytes ?? data ?? 0,
        }))
        .sort((a, b) => b.total_bytes - a.total_bytes)
    : [];

  // ── Table data ─────────────────────────────────────────────────────
  const tables = storageData?.top_tables_by_total || storageData?.tables || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Storage Costs"
        description="Analyze storage usage across schemas and tables, identify reclaimable space."
        icon={HardDrive}
        breadcrumbs={["FinOps", "Cost Analysis", "Storage"]}
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

      {/* ── Loading / Empty ─────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading storage metrics...
        </div>
      ) : !catalog ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <HardDrive className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">Select a catalog to view storage costs.</p>
        </div>
      ) : !storageData ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <HardDrive className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">No storage data available for this catalog.</p>
        </div>
      ) : (
        <>
          {/* ── KPI Cards ───────────────────────────────────────────── */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Storage" value={formatBytes(totalBytes)} />
            <SummaryCard label="Active Data" value={formatBytes(activeBytes)} color="green" />
            <SummaryCard label="Reclaimable" value={formatBytes(vacuumableBytes)} color={vacuumableBytes > 0 ? "amber" : "green"} sub="Via VACUUM" />
            <SummaryCard label="Est. Monthly Cost" value={formatCost(estMonthlyCost, currency)} sub={`@ ${formatCost(pricePerGb, currency)}/GB`} />
          </div>

          {/* ── Charts Row ──────────────────────────────────────────── */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Pie: Storage Breakdown */}
            {pieData.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Storage Breakdown</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie
                        data={pieData}
                        cx="50%"
                        cy="50%"
                        outerRadius={90}
                        dataKey="value"
                        nameKey="name"
                        label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                      >
                        {pieData.map((_, i) => (
                          <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip formatter={(v: number) => formatBytes(v)} />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}

            {/* Bar: Schema Breakdown */}
            {schemaSummaries.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Schema Breakdown</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={schemaSummaries.slice(0, 15)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis dataKey="schema" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" angle={-30} textAnchor="end" height={60} />
                      <YAxis tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" tickFormatter={(v) => formatBytes(v)} />
                      <Tooltip formatter={(v: number) => formatBytes(v)} />
                      <Bar dataKey="total_bytes" fill="#E8453C" radius={[4, 4, 0, 0]} name="Storage" />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>

          {/* ── Per-Table Table ──────────────────────────────────────── */}
          {tables.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Table Storage Details</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="py-2 pr-4">Table</th>
                        <th className="py-2 pr-4 text-right">Total Size</th>
                        <th className="py-2 pr-4 text-right">Active %</th>
                        <th className="py-2 pr-4 text-right">Vacuumable %</th>
                        <th className="py-2 text-right">Cost/mo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tables.map((t: any, i: number) => {
                        const tTotal = t.total_bytes || 0;
                        const tActive = t.active_bytes || 0;
                        const tVac = t.vacuumable_bytes || 0;
                        const activePct = tTotal > 0 ? ((tActive / tTotal) * 100).toFixed(1) : "0.0";
                        const vacPct = tTotal > 0 ? ((tVac / tTotal) * 100).toFixed(1) : "0.0";
                        const tableCost = (tTotal / 1_073_741_824) * pricePerGb;
                        const tableName = t.table_name || t.name || `${t.schema || ""}.${t.table || ""}`;
                        return (
                          <tr key={i} className="border-b border-border/50">
                            <td className="py-2 pr-4 font-medium truncate max-w-[300px]">{tableName}</td>
                            <td className="py-2 pr-4 text-right font-mono">{formatBytes(tTotal)}</td>
                            <td className="py-2 pr-4 text-right font-mono text-green-500">{activePct}%</td>
                            <td className="py-2 pr-4 text-right font-mono text-amber-500">{vacPct}%</td>
                            <td className="py-2 text-right font-mono">{formatCost(tableCost, currency)}</td>
                          </tr>
                        );
                      })}
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
