// @ts-nocheck
import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import {
  BarChart, Bar, PieChart, Pie, Cell, LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import {
  Database, Loader2, TrendingUp, TrendingDown, Minus, Camera, AlertTriangle,
  Search, ArrowUpDown, ArrowUp, ArrowDown, Download, ChevronDown, ChevronRight,
  BarChart3, Table2, Filter, HardDrive, Clock, Activity, Gauge,
} from "lucide-react";

// ── Types ──────────────────────────────────────────────────────────────────

interface VolumeRow {
  table_name: string;
  current_rows: number | null;
  previous_rows: number | null;
  change_pct: number | null;
  size_bytes: number | null;
  last_modified: string | null;
}

interface TrendPoint { date: string; table_count: number; total_rows: number }
interface PerTableHistory { [fqn: string]: { date: string; rows: number }[] }

type SortKey = "table_name" | "current_rows" | "previous_rows" | "change_pct" | "size_bytes";
type SortDir = "asc" | "desc";
type FilterPreset = "all" | "empty" | "anomalous" | "growing" | "shrinking" | "largest" | "smallest";

// ── Helpers ────────────────────────────────────────────────────────────────

function trendIcon(pct: number | null) {
  if (pct == null) return <Minus className="h-3.5 w-3.5 text-muted-foreground" />;
  if (pct > 0) return <TrendingUp className="h-3.5 w-3.5 text-green-500" />;
  if (pct < 0) return <TrendingDown className="h-3.5 w-3.5 text-red-500" />;
  return <Minus className="h-3.5 w-3.5 text-muted-foreground" />;
}

function changeColor(pct: number | null) {
  if (pct == null) return "text-muted-foreground";
  if (Math.abs(pct) > 10) return "text-red-500";
  if (Math.abs(pct) > 5) return "text-amber-500";
  return "text-green-500";
}

function formatNumber(n: number | null) {
  if (n == null) return "—";
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function formatBytes(bytes: number | null) {
  if (bytes == null) return "—";
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

function shortTableName(fqn: string) {
  const parts = fqn.split(".");
  return parts.length >= 3 ? `${parts[1]}.${parts[2]}` : fqn;
}

function healthScore(r: VolumeRow): { score: number; label: string; color: string } {
  let score = 100;
  if (r.current_rows === 0) score -= 40;
  if (r.change_pct != null && Math.abs(r.change_pct) > 10) score -= 30;
  if (r.change_pct != null && Math.abs(r.change_pct) > 5) score -= 15;
  if (r.current_rows == null) score -= 20;
  score = Math.max(0, score);
  if (score >= 80) return { score, label: "Healthy", color: "text-green-500" };
  if (score >= 50) return { score, label: "Warning", color: "text-amber-500" };
  return { score, label: "Critical", color: "text-red-500" };
}

function heatmapBg(current: number | null, maxRows: number): string {
  if (current == null || maxRows <= 0) return "";
  const ratio = Math.min(current / maxRows, 1);
  if (ratio > 0.7) return "bg-[#E8453C]/10";
  if (ratio > 0.4) return "bg-amber-500/8";
  if (ratio > 0.1) return "bg-green-500/5";
  return "";
}

const CHART_COLORS = ["#E8453C", "#374151", "#9CA3AF", "#6B7280", "#D1D5DB", "#F59E0B", "#10B981", "#6366F1"];

const SCHEMA_COLORS = ["#E8453C", "#3B82F6", "#10B981", "#F59E0B", "#8B5CF6", "#EC4899", "#06B6D4", "#84CC16", "#F97316", "#6366F1"];
function schemaColor(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = ((hash << 5) - hash + name.charCodeAt(i)) | 0;
  return SCHEMA_COLORS[Math.abs(hash) % SCHEMA_COLORS.length];
}

function relativeTimeAgo(dateStr: string): string {
  const d = new Date(dateStr);
  const diff = Date.now() - d.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

const FILTER_PRESETS: { key: FilterPreset; label: string; icon: any }[] = [
  { key: "all", label: "All", icon: Table2 },
  { key: "empty", label: "Empty", icon: AlertTriangle },
  { key: "anomalous", label: "Anomalous", icon: Activity },
  { key: "growing", label: "Growing", icon: TrendingUp },
  { key: "shrinking", label: "Shrinking", icon: TrendingDown },
  { key: "largest", label: "Top 20", icon: BarChart3 },
  { key: "smallest", label: "Bottom 20", icon: Minus },
];

// ── Component ──────────────────────────────────────────────────────────────

export default function VolumeMonitorPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [loading, setLoading] = useState(false);
  const [snapshotting, setSnapshotting] = useState(false);
  const [results, setResults] = useState<VolumeRow[]>([]);
  const [hasData, setHasData] = useState(false);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("current_rows");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [viewMode, setViewMode] = useState<"table" | "schema">("table");
  const [collapsedSchemas, setCollapsedSchemas] = useState<Set<string>>(new Set());
  const [filterPreset, setFilterPreset] = useState<FilterPreset>("all");
  const [filterSchema, setFilterSchema] = useState("");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [trend, setTrend] = useState<TrendPoint[]>([]);
  const [perTableHistory, setPerTableHistory] = useState<PerTableHistory>({});
  const [showCharts, setShowCharts] = useState(true);

  // ── Data loading ──

  async function loadVolume() {
    if (!catalog) return;
    setLoading(true);
    try {
      const params = schema ? `?schema=${encodeURIComponent(schema)}` : "";
      const data = await api.get<{ tables?: any[]; [k: string]: any }>(
        `/data-quality/volume/${encodeURIComponent(catalog)}${params}`
      );
      const raw = Array.isArray(data) ? data : (data?.tables ?? []);
      const mapped: VolumeRow[] = raw.map((t: any) => ({
        table_name: t.table_name ?? t.table_fqn ?? "",
        current_rows: t.current_rows ?? t.row_count ?? null,
        previous_rows: t.previous_rows ?? null,
        change_pct: t.change_pct ?? null,
        size_bytes: t.size_bytes ?? null,
        last_modified: t.last_modified ?? null,
      }));
      setResults(mapped);
      setHasData(true);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  }

  async function loadHistory() {
    if (!catalog) return;
    try {
      const data = await api.get<{ trend?: TrendPoint[]; per_table?: PerTableHistory }>(
        `/data-quality/volume/${encodeURIComponent(catalog)}/history?days=30`
      );
      setTrend(data?.trend ?? []);
      setPerTableHistory(data?.per_table ?? {});
    } catch {
      setTrend([]);
      setPerTableHistory({});
    }
  }

  async function takeSnapshot() {
    if (!catalog) { toast.error("Please select a catalog first."); return; }
    setSnapshotting(true);
    try {
      await api.post("/data-quality/volume/snapshot", { catalog, schema_name: schema || undefined });
      toast.success("Volume snapshot captured successfully.");
      await Promise.all([loadVolume(), loadHistory()]);
    } catch (err: any) {
      toast.error(err?.message || "Failed to take volume snapshot.");
    } finally {
      setSnapshotting(false);
    }
  }

  function exportCsv() {
    if (!filtered.length) return;
    const header = "table_name,current_rows,previous_rows,change_pct,size_bytes,last_modified,health_score\n";
    const rows = filtered.map((r) => {
      const h = healthScore(r);
      return `"${r.table_name}",${r.current_rows ?? ""},${r.previous_rows ?? ""},${r.change_pct ?? ""},${r.size_bytes ?? ""},${r.last_modified ?? ""},${h.score}`;
    }).join("\n");
    const blob = new Blob([header + rows], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `volume-${catalog}${schema ? `-${schema}` : ""}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  useEffect(() => {
    if (catalog) { loadVolume(); loadHistory(); }
  }, [catalog, schema]);

  // ── Sorting ──

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir(key === "table_name" ? "asc" : "desc"); }
  }

  // ── Filtering ──

  // Extract unique schemas from results
  const availableSchemas = useMemo(() => {
    const schemas = new Set<string>();
    for (const r of results) {
      const parts = r.table_name.split(".");
      if (parts.length >= 3) schemas.add(parts[1]);
    }
    return [...schemas].sort();
  }, [results]);

  const filtered = useMemo(() => {
    let list = results;

    // Schema filter
    if (filterSchema) {
      list = list.filter((r) => {
        const parts = r.table_name.split(".");
        return parts.length >= 3 && parts[1] === filterSchema;
      });
    }

    // Preset filters
    switch (filterPreset) {
      case "empty": list = list.filter((r) => r.current_rows === 0); break;
      case "anomalous": list = list.filter((r) => r.change_pct != null && Math.abs(r.change_pct) > 10); break;
      case "growing": list = list.filter((r) => r.change_pct != null && r.change_pct > 0); break;
      case "shrinking": list = list.filter((r) => r.change_pct != null && r.change_pct < 0); break;
      case "largest": list = [...list].sort((a, b) => (b.current_rows || 0) - (a.current_rows || 0)).slice(0, 20); break;
      case "smallest": list = [...list].sort((a, b) => (a.current_rows || 0) - (b.current_rows || 0)).slice(0, 20); break;
    }

    // Text search
    if (search) {
      const q = search.toLowerCase();
      list = list.filter((r) => r.table_name.toLowerCase().includes(q));
    }

    // Sort
    list = [...list].sort((a, b) => {
      const av = a[sortKey]; const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string" && typeof bv === "string")
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      return sortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });
    return list;
  }, [results, search, sortKey, sortDir, filterPreset, filterSchema]);

  // Reset page when filters change
  useEffect(() => { setPage(0); }, [search, filterPreset, filterSchema, sortKey, sortDir, catalog, schema]);

  const totalPages = Math.ceil(filtered.length / pageSize);
  const paginated = filtered.slice(page * pageSize, (page + 1) * pageSize);

  // ── Schema groups ──

  const schemaGroups = useMemo(() => {
    const groups: Record<string, { tables: VolumeRow[]; totalRows: number; prevRows: number; totalSize: number; tableCount: number }> = {};
    for (const r of filtered) {
      const parts = r.table_name.split(".");
      const sk = parts.length >= 3 ? parts[1] : "default";
      if (!groups[sk]) groups[sk] = { tables: [], totalRows: 0, prevRows: 0, totalSize: 0, tableCount: 0 };
      groups[sk].tables.push(r);
      groups[sk].totalRows += r.current_rows || 0;
      groups[sk].prevRows += r.previous_rows || 0;
      groups[sk].totalSize += r.size_bytes || 0;
      groups[sk].tableCount++;
    }
    return Object.entries(groups).sort(([, a], [, b]) => b.totalRows - a.totalRows);
  }, [filtered]);

  function toggleSchema(s: string) {
    setCollapsedSchemas((prev) => { const n = new Set(prev); n.has(s) ? n.delete(s) : n.add(s); return n; });
  }

  // ── Aggregates ──

  const totalRows = results.reduce((sum, r) => sum + (r.current_rows || 0), 0);
  const totalSize = results.reduce((sum, r) => sum + (r.size_bytes || 0), 0);
  const emptyTables = results.filter((r) => r.current_rows === 0).length;
  const tablesWithChanges = results.filter((r) => r.change_pct != null && r.change_pct !== 0).length;
  const anomalous = results.filter((r) => r.change_pct != null && Math.abs(r.change_pct) > 10).length;
  const growing = results.filter((r) => r.change_pct != null && r.change_pct > 0).length;
  const shrinking = results.filter((r) => r.change_pct != null && r.change_pct < 0).length;
  const maxRows = Math.max(...results.map((r) => r.current_rows || 0), 1);

  // Chart data
  const topMovers = useMemo(() => {
    return [...results]
      .filter((r) => r.change_pct != null && r.change_pct !== 0)
      .sort((a, b) => Math.abs(b.change_pct!) - Math.abs(a.change_pct!))
      .slice(0, 10)
      .map((r) => ({
        name: r.table_name.split(".").pop() || r.table_name,
        change: r.change_pct!,
        fill: r.change_pct! > 0 ? "#10B981" : "#E8453C",
      }));
  }, [results]);

  const schemaDistribution = useMemo(() => {
    const groups: Record<string, number> = {};
    for (const r of results) {
      const s = r.table_name.split(".")[1] || "other";
      groups[s] = (groups[s] || 0) + (r.current_rows || 0);
    }
    const sorted = Object.entries(groups).sort(([, a], [, b]) => b - a);
    const top5 = sorted.slice(0, 5).map(([name, rows], i) => ({ name, rows, fill: CHART_COLORS[i % CHART_COLORS.length] }));
    const otherRows = sorted.slice(5).reduce((s, [, v]) => s + v, 0);
    if (otherRows > 0) top5.push({ name: "Other", rows: otherRows, fill: "#D1D5DB" });
    return top5;
  }, [results]);

  // Top 10 tables by size (for horizontal bar chart)
  const topTablesBySize = useMemo(() => {
    return [...results]
      .filter((r) => r.size_bytes != null && r.size_bytes > 0)
      .sort((a, b) => (b.size_bytes || 0) - (a.size_bytes || 0))
      .slice(0, 10)
      .map((r) => ({
        name: r.table_name.split(".").pop() || r.table_name,
        schema: r.table_name.split(".")[1] || "",
        size: r.size_bytes!,
        fill: schemaColor(r.table_name.split(".")[1] || ""),
      }));
  }, [results]);

  // Filter badge counts
  const filterCounts: Record<FilterPreset, number> = {
    all: results.length,
    empty: emptyTables,
    anomalous: anomalous,
    growing: growing,
    shrinking: shrinking,
    largest: Math.min(results.length, 20),
    smallest: Math.min(results.length, 20),
  };

  // ── Subcomponents ──

  function SortHeader({ label, field, align = "right" }: { label: string; field: SortKey; align?: string }) {
    const active = sortKey === field;
    return (
      <th
        className={`py-2 px-3 font-medium cursor-pointer select-none hover:text-foreground transition-colors ${align === "left" ? "text-left" : "text-right"}`}
        onClick={() => toggleSort(field)}
      >
        <span className="inline-flex items-center gap-1">
          {label}
          {active ? (sortDir === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />) : <ArrowUpDown className="h-3 w-3 opacity-30" />}
        </span>
      </th>
    );
  }

  function HealthBadge({ row }: { row: VolumeRow }) {
    const h = healthScore(row);
    return (
      <span className={`text-[10px] font-medium ${h.color}`} title={`Health: ${h.score}/100`}>
        {h.score}
      </span>
    );
  }

  // ── Render ──

  return (
    <div className="space-y-6">
      <PageHeader
        title="Volume Monitor"
        description="Track table row counts over time and detect volume anomalies."
        icon={Database}
        breadcrumbs={["Data Quality", "Monitoring", "Volume"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="min-w-[500px]">
              <CatalogPicker
                catalog={catalog} schema={schema}
                onCatalogChange={(c) => { setCatalog(c); setSchema(""); }}
                onSchemaChange={setSchema} showTable={false}
              />
            </div>
            <Button onClick={takeSnapshot} disabled={snapshotting || !catalog}>
              {snapshotting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Camera className="h-4 w-4 mr-2" />}
              Take Snapshot
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Loading state */}
      {loading && (
        <Card>
          <CardContent className="py-12 flex flex-col items-center gap-3">
            <Loader2 className="h-8 w-8 animate-spin text-[#E8453C]" />
            <p className="text-sm text-muted-foreground">Counting rows across tables...</p>
            <p className="text-xs text-muted-foreground/60">This may take a minute for large catalogs</p>
          </CardContent>
        </Card>
      )}

      {hasData && !loading && (
        <>
          {/* ── Summary Cards ── */}
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
            {[
              { label: "Total Tables", value: results.length, fmt: false },
              { label: "Total Rows", value: totalRows, fmt: true, sub: totalRows.toLocaleString() },
              { label: "Total Size", value: totalSize, fmt: false, display: formatBytes(totalSize) },
              { label: "Avg Table Size", value: results.length > 0 ? Math.round(totalSize / results.length) : 0, fmt: false, display: formatBytes(results.length > 0 ? Math.round(totalSize / results.length) : 0) },
              { label: "Empty Tables", value: emptyTables, fmt: false, color: emptyTables > 0 ? "text-amber-500" : "text-green-500", click: "empty" as FilterPreset },
              { label: "With Changes", value: tablesWithChanges, fmt: false, color: tablesWithChanges > 0 ? "text-amber-500" : "", click: "growing" as FilterPreset },
              { label: "Anomalous (>10%)", value: anomalous, fmt: false, color: anomalous > 0 ? "text-red-500" : "", click: "anomalous" as FilterPreset },
              { label: "Last Snapshot", value: 0, fmt: false, display: trend.length > 0 ? relativeTimeAgo(trend[trend.length - 1].date) : "Never" },
            ].map((c) => (
              <Card
                key={c.label}
                className={c.click ? "cursor-pointer hover:border-[#E8453C]/30 transition-colors" : ""}
                onClick={c.click ? () => setFilterPreset(c.click!) : undefined}
              >
                <CardContent className="pt-4 pb-3">
                  <p className="text-[10px] text-muted-foreground uppercase tracking-wide">{c.label}</p>
                  <p className={`text-xl font-bold mt-0.5 ${c.color || ""}`}>
                    {c.display ?? (c.fmt ? formatNumber(c.value) : c.value)}
                  </p>
                  {c.sub && <p className="text-[10px] text-muted-foreground">{c.sub}</p>}
                </CardContent>
              </Card>
            ))}
          </div>

          {/* ── Charts Section ── */}
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-medium text-muted-foreground">Analytics</h3>
            <button onClick={() => setShowCharts(!showCharts)} className="text-xs text-muted-foreground hover:text-foreground">
              {showCharts ? "Hide charts" : "Show charts"}
            </button>
          </div>

          {showCharts && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Volume History Trend */}
              <Card>
                <CardHeader className="pb-1">
                  <CardTitle className="text-sm flex items-center gap-2"><Activity className="h-4 w-4" /> Volume Trend (30d)</CardTitle>
                </CardHeader>
                <CardContent>
                  {trend.length >= 2 ? (
                    <ResponsiveContainer width="100%" height={200}>
                      <AreaChart data={trend}>
                        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                        <XAxis dataKey="date" tick={{ fontSize: 10 }} tickFormatter={(v) => v.slice(5)} />
                        <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => formatNumber(v)} />
                        <Tooltip
                          contentStyle={{ fontSize: 12, background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8 }}
                          formatter={(v: number) => [v.toLocaleString(), "Total Rows"]}
                        />
                        <Area type="monotone" dataKey="total_rows" stroke="#E8453C" fill="#E8453C" fillOpacity={0.1} strokeWidth={2} />
                      </AreaChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="flex flex-col items-center justify-center h-[200px] text-center">
                      <Camera className="h-8 w-8 text-muted-foreground/30 mb-3" />
                      <p className="text-sm text-muted-foreground">
                        {trend.length === 1 ? "1 snapshot taken" : "No snapshots yet"}
                      </p>
                      <p className="text-xs text-muted-foreground/60 mt-1 max-w-[200px]">
                        Take snapshots over multiple days to see volume trends over time
                      </p>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Schema Distribution (Rows) */}
              {schemaDistribution.length > 0 && (
                <Card>
                  <CardHeader className="pb-1">
                    <CardTitle className="text-sm flex items-center gap-2"><Database className="h-4 w-4" /> Rows by Schema</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="flex items-center gap-4">
                      <ResponsiveContainer width="45%" height={180}>
                        <PieChart>
                          <Pie data={schemaDistribution} dataKey="rows" nameKey="name" cx="50%" cy="50%" outerRadius={65} innerRadius={30} paddingAngle={2}>
                            {schemaDistribution.map((e, i) => <Cell key={i} fill={e.fill} />)}
                          </Pie>
                          <Tooltip
                            contentStyle={{ fontSize: 12, background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8 }}
                            formatter={(v: number) => [v.toLocaleString(), "Rows"]}
                          />
                        </PieChart>
                      </ResponsiveContainer>
                      <div className="flex-1 space-y-1.5 overflow-hidden">
                        {schemaDistribution.map((s, i) => {
                          const pct = totalRows > 0 ? ((s.rows / totalRows) * 100) : 0;
                          return (
                            <div key={i} className="flex items-center gap-2 text-xs">
                              <span className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ background: s.fill }} />
                              <span className="truncate flex-1 text-muted-foreground">{s.name}</span>
                              <span className="tabular-nums font-medium min-w-[36px] text-right">{pct < 0.1 ? "<0.1" : pct.toFixed(pct < 1 ? 1 : 0)}%</span>
                              <span className="tabular-nums text-muted-foreground min-w-[50px] text-right">{formatNumber(s.rows)}</span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Top 10 Tables by Size */}
              {topTablesBySize.length > 0 && (
                <Card>
                  <CardHeader className="pb-1">
                    <CardTitle className="text-sm flex items-center gap-2"><HardDrive className="h-4 w-4" /> Top 10 by Size</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-1.5">
                      {topTablesBySize.map((t, i) => {
                        const maxSize = topTablesBySize[0]?.size || 1;
                        const pct = (t.size / maxSize) * 100;
                        return (
                          <div key={i} className="flex items-center gap-2 text-xs">
                            <span className="w-[110px] truncate font-mono text-muted-foreground" title={t.name}>{t.name}</span>
                            <div className="flex-1 h-4 rounded-sm bg-muted/30 overflow-hidden relative">
                              <div className="h-full rounded-sm transition-all" style={{ width: `${pct}%`, background: t.fill, opacity: 0.7 }} />
                            </div>
                            <span className="tabular-nums font-medium min-w-[55px] text-right">{formatBytes(t.size)}</span>
                          </div>
                        );
                      })}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Top Movers (only when there are changes) */}
              {topMovers.length > 0 && (
                <Card className="lg:col-span-3">
                  <CardHeader className="pb-1">
                    <CardTitle className="text-sm flex items-center gap-2"><TrendingUp className="h-4 w-4" /> Top Movers</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <ResponsiveContainer width="100%" height={180}>
                      <BarChart data={topMovers} layout="vertical" margin={{ left: 100 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                        <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={(v) => `${v > 0 ? "+" : ""}${v}%`} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize: 10 }} width={100} />
                        <Tooltip
                          contentStyle={{ fontSize: 12, background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8 }}
                          formatter={(v: number) => [`${v > 0 ? "+" : ""}${v.toFixed(1)}%`, "Change"]}
                        />
                        <Bar dataKey="change" radius={[0, 4, 4, 0]}>
                          {topMovers.map((e, i) => <Cell key={i} fill={e.fill} />)}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* ── Filter Presets ── */}
          <div className="flex flex-wrap items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground" />
            {availableSchemas.length > 1 && (
              <select
                value={filterSchema}
                onChange={(e) => setFilterSchema(e.target.value)}
                className={`h-9 rounded-lg border px-3 pr-8 text-sm font-medium transition-colors appearance-none bg-no-repeat bg-[length:16px] bg-[right_8px_center] cursor-pointer ${
                  filterSchema
                    ? "bg-[#E8453C] text-white border-[#E8453C]"
                    : "bg-card border-border text-foreground shadow-sm hover:border-[#E8453C]/50"
                }`}
                style={{
                  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='${filterSchema ? 'white' : '%236b7280'}' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
                }}
              >
                <option value="">All Schemas ({availableSchemas.length})</option>
                {availableSchemas.map((s) => {
                  const count = results.filter(r => r.table_name.split(".")[1] === s).length;
                  return <option key={s} value={s}>{s} ({count})</option>;
                })}
              </select>
            )}
            {FILTER_PRESETS.map(({ key, label, icon: Icon }) => {
              const count = filterCounts[key];
              const active = filterPreset === key;
              const disabled = key !== "all" && key !== "largest" && key !== "smallest" && count === 0;
              return (
                <button
                  key={key}
                  onClick={() => setFilterPreset(active ? "all" : key)}
                  disabled={disabled}
                  className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border transition-colors ${
                    active
                      ? "bg-[#E8453C] text-white border-[#E8453C]"
                      : disabled
                        ? "border-border text-muted-foreground/40 cursor-not-allowed"
                        : "border-border text-muted-foreground hover:border-[#E8453C]/50 hover:text-foreground"
                  }`}
                >
                  <Icon className="h-3 w-3" />
                  {label}
                  {key !== "all" && <span className={`${active ? "text-white/70" : "text-muted-foreground/60"}`}>({count})</span>}
                </button>
              );
            })}
          </div>

          {/* ── Data Table ── */}
          <Card>
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between flex-wrap gap-3">
                <CardTitle className="text-base">
                  Volume Data ({filtered.length}{filtered.length !== results.length ? ` / ${results.length}` : ""})
                  {filterPreset !== "all" && (
                    <Badge variant="outline" className="ml-2 text-[10px] text-[#E8453C] border-[#E8453C]/30">{filterPreset}</Badge>
                  )}
                </CardTitle>
                <div className="flex items-center gap-2">
                  <div className="relative">
                    <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                    <Input className="pl-8 h-8 w-[220px] text-xs" placeholder="Filter tables..." value={search} onChange={(e) => setSearch(e.target.value)} />
                  </div>
                  <div className="flex border border-border rounded-md overflow-hidden">
                    <button className={`px-2.5 py-1.5 text-xs ${viewMode === "table" ? "bg-muted font-medium" : "hover:bg-muted/50"}`} onClick={() => setViewMode("table")} title="Flat view"><Table2 className="h-3.5 w-3.5" /></button>
                    <button className={`px-2.5 py-1.5 text-xs border-l border-border ${viewMode === "schema" ? "bg-muted font-medium" : "hover:bg-muted/50"}`} onClick={() => setViewMode("schema")} title="Group by schema"><Database className="h-3.5 w-3.5" /></button>
                  </div>
                  <Button variant="outline" size="sm" className="h-8 text-xs" onClick={exportCsv} disabled={filtered.length === 0}>
                    <Download className="h-3.5 w-3.5 mr-1.5" /> CSV
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {results.length === 0 ? (
                <p className="text-sm text-muted-foreground py-4">No volume data available. Take a snapshot first.</p>
              ) : filtered.length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-sm text-muted-foreground">No tables match the current filter.</p>
                  <button onClick={() => { setFilterPreset("all"); setSearch(""); }} className="text-xs text-[#E8453C] mt-2 hover:underline">Clear filters</button>
                </div>
              ) : viewMode === "table" ? (
                /* ── Flat table view ── */
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <SortHeader label="Table Name" field="table_name" align="left" />
                        <SortHeader label="Current Rows" field="current_rows" />
                        <SortHeader label="Previous Rows" field="previous_rows" />
                        <SortHeader label="Change %" field="change_pct" />
                        <SortHeader label="Size" field="size_bytes" />
                        <th className="py-2 px-3 text-center font-medium">Health</th>
                        <th className="py-2 px-3 text-center font-medium">Trend</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paginated.map((r) => {
                        const isAnomalous = r.change_pct != null && Math.abs(r.change_pct) > 10;
                        const isEmpty = r.current_rows === 0;
                        return (
                          <tr
                            key={r.table_name}
                            className={`border-b border-border/50 hover:bg-muted/30 ${heatmapBg(r.current_rows, maxRows)} ${isAnomalous ? "!bg-red-500/5" : isEmpty ? "!bg-amber-500/5" : ""}`}
                          >
                            <td className="py-1.5 px-3 font-mono text-xs">
                              <span className="inline-flex items-center gap-1.5">
                                <span className="w-2 h-2 rounded-full shrink-0" style={{ background: schemaColor(r.table_name.split(".")[1] || "") }} />
                                <span title={r.table_name}>{shortTableName(r.table_name)}</span>
                              </span>
                              {isAnomalous && <AlertTriangle className="h-3 w-3 text-red-500 inline ml-1.5" title="Volume anomaly >10%" />}
                              {isEmpty && <Badge variant="outline" className="ml-1.5 text-[9px] px-1 py-0 text-amber-500 border-amber-500/30">EMPTY</Badge>}
                            </td>
                            <td className="py-1.5 px-3 text-right tabular-nums">
                              {r.current_rows != null ? r.current_rows.toLocaleString() : "—"}
                            </td>
                            <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground">
                              {r.previous_rows != null ? r.previous_rows.toLocaleString() : "—"}
                            </td>
                            <td className={`py-1.5 px-3 text-right tabular-nums ${changeColor(r.change_pct)}`}>
                              {r.change_pct != null ? `${r.change_pct > 0 ? "+" : ""}${r.change_pct.toFixed(1)}%` : "—"}
                            </td>
                            <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground">
                              <div className="relative inline-flex items-center gap-1.5 w-full justify-end">
                                {r.size_bytes != null && r.size_bytes > 0 && (
                                  <div className="absolute left-0 top-0 bottom-0 rounded-sm opacity-15" style={{ width: `${Math.min((r.size_bytes / (topTablesBySize[0]?.size || 1)) * 100, 100)}%`, background: schemaColor(r.table_name.split(".")[1] || "") }} />
                                )}
                                <span className="relative">{formatBytes(r.size_bytes)}</span>
                              </div>
                            </td>
                            <td className="py-1.5 px-3 text-center">
                              <HealthBadge row={r} />
                            </td>
                            <td className="py-1.5 px-3 text-center">
                              {trendIcon(r.change_pct)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  {/* Pagination */}
                  {totalPages > 1 && (
                    <div className="flex items-center justify-between pt-4 border-t border-border mt-2">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <span>Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, filtered.length)} of {filtered.length}</span>
                        <span className="text-muted-foreground/40">|</span>
                        <span>Rows per page:</span>
                        {[25, 50, 100].map((size) => (
                          <button
                            key={size}
                            onClick={() => { setPageSize(size); setPage(0); }}
                            className={`px-1.5 py-0.5 rounded ${pageSize === size ? "bg-muted font-medium text-foreground" : "hover:bg-muted/50"}`}
                          >
                            {size}
                          </button>
                        ))}
                      </div>
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => setPage(0)}
                          disabled={page === 0}
                          className="px-2 py-1 rounded text-xs border border-border hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          First
                        </button>
                        <button
                          onClick={() => setPage(page - 1)}
                          disabled={page === 0}
                          className="px-2 py-1 rounded text-xs border border-border hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          Prev
                        </button>
                        {Array.from({ length: Math.min(totalPages, 5) }, (_, i) => {
                          let p: number;
                          if (totalPages <= 5) p = i;
                          else if (page < 3) p = i;
                          else if (page > totalPages - 4) p = totalPages - 5 + i;
                          else p = page - 2 + i;
                          return (
                            <button
                              key={p}
                              onClick={() => setPage(p)}
                              className={`px-2.5 py-1 rounded text-xs border transition-colors ${
                                p === page
                                  ? "bg-[#E8453C] text-white border-[#E8453C]"
                                  : "border-border hover:bg-muted"
                              }`}
                            >
                              {p + 1}
                            </button>
                          );
                        })}
                        <button
                          onClick={() => setPage(page + 1)}
                          disabled={page >= totalPages - 1}
                          className="px-2 py-1 rounded text-xs border border-border hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          Next
                        </button>
                        <button
                          onClick={() => setPage(totalPages - 1)}
                          disabled={page >= totalPages - 1}
                          className="px-2 py-1 rounded text-xs border border-border hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          Last
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                /* ── Schema-grouped view ── */
                <div className="space-y-1">
                  {schemaGroups.map(([schemaName, group]) => {
                    const collapsed = collapsedSchemas.has(schemaName);
                    const schemaPct = group.prevRows > 0 ? ((group.totalRows - group.prevRows) / group.prevRows * 100) : null;
                    return (
                      <div key={schemaName} className="border border-border/50 rounded-lg overflow-hidden">
                        <button className="w-full flex items-center gap-3 px-4 py-2.5 text-left hover:bg-muted/30 transition-colors" onClick={() => toggleSchema(schemaName)}>
                          {collapsed ? <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" /> : <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />}
                          <span className="font-mono text-sm font-medium flex-1">{schemaName}</span>
                          <span className="text-xs text-muted-foreground">{group.tableCount} tables</span>
                          <span className="text-xs font-medium tabular-nums min-w-[90px] text-right">{formatNumber(group.totalRows)} rows</span>
                          <span className="text-xs text-muted-foreground tabular-nums min-w-[70px] text-right">{formatBytes(group.totalSize)}</span>
                          {schemaPct != null && (
                            <span className={`text-xs tabular-nums min-w-[60px] text-right ${changeColor(schemaPct)}`}>
                              {schemaPct > 0 ? "+" : ""}{schemaPct.toFixed(1)}%
                            </span>
                          )}
                        </button>
                        {!collapsed && (
                          <div className="border-t border-border/30">
                            <table className="w-full text-sm">
                              <tbody>
                                {group.tables.map((r) => {
                                  const isAnomalous = r.change_pct != null && Math.abs(r.change_pct) > 10;
                                  const isEmpty = r.current_rows === 0;
                                  const tblName = r.table_name.split(".").pop() || r.table_name;
                                  return (
                                    <tr key={r.table_name} className={`border-b border-border/30 last:border-0 hover:bg-muted/20 ${isAnomalous ? "bg-red-500/5" : isEmpty ? "bg-amber-500/5" : ""}`}>
                                      <td className="py-1.5 px-4 pl-12 font-mono text-xs">
                                        {tblName}
                                        {isAnomalous && <AlertTriangle className="h-3 w-3 text-red-500 inline ml-1.5" />}
                                        {isEmpty && <Badge variant="outline" className="ml-1.5 text-[9px] px-1 py-0 text-amber-500 border-amber-500/30">EMPTY</Badge>}
                                      </td>
                                      <td className="py-1.5 px-3 text-right tabular-nums w-[120px]">{r.current_rows != null ? r.current_rows.toLocaleString() : "—"}</td>
                                      <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground w-[120px]">{r.previous_rows != null ? r.previous_rows.toLocaleString() : "—"}</td>
                                      <td className={`py-1.5 px-3 text-right tabular-nums w-[80px] ${changeColor(r.change_pct)}`}>
                                        {r.change_pct != null ? `${r.change_pct > 0 ? "+" : ""}${r.change_pct.toFixed(1)}%` : "—"}
                                      </td>
                                      <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground w-[80px]">{formatBytes(r.size_bytes)}</td>
                                      <td className="py-1.5 px-3 text-center w-[50px]"><HealthBadge row={r} /></td>
                                      <td className="py-1.5 px-3 text-center w-[50px]">{trendIcon(r.change_pct)}</td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
