// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  History, Loader2, CheckCircle, XCircle, AlertTriangle,
  Search, RefreshCw, GitCompare, ChevronDown, ChevronUp,
  Download, Clock, TrendingUp, Activity,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  BarChart, Bar,
} from "recharts";

// ── Export helpers ──────────────────────────────────────────────────────────

function exportHistoryCsv(runs: any[], computeMatchRate: (r: any) => number) {
  const header = "run_id,run_type,source_catalog,destination_catalog,total_tables,matched,mismatched,errors,match_rate,duration_seconds,execution_mode,executed_at\n";
  const rows = runs.map((r) =>
    [r.run_id, r.run_type, r.source_catalog, r.destination_catalog,
     r.total_tables, r.matched, r.mismatched, r.errors || 0,
     computeMatchRate(r), r.duration_seconds || "",
     r.execution_mode || "", r.executed_at || "",
    ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(",")
  ).join("\n");
  const blob = new Blob([header + rows], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `reconciliation-history-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function exportHistoryJson(runs: any[]) {
  const blob = new Blob([JSON.stringify(runs, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `reconciliation-history-${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Run type badge colors ──────────────────────────────────────────────────

function runTypeBadge(type: string) {
  if (type === "row-level" || type === "row-level-batch")
    return <Badge variant="outline" className="text-[10px] text-blue-500 border-blue-500/30">{type}</Badge>;
  if (type === "column-level")
    return <Badge variant="outline" className="text-[10px] text-purple-500 border-purple-500/30">{type}</Badge>;
  if (type === "deep")
    return <Badge variant="outline" className="text-[10px] text-amber-500 border-amber-500/30">{type}</Badge>;
  return <Badge variant="outline" className="text-[10px]">{type || "—"}</Badge>;
}

// ── Format duration ────────────────────────────────────────────────────────

function fmtDuration(seconds: number | null | undefined): string {
  if (seconds == null || seconds === 0) return "—";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}m ${s}s`;
}

export default function ReconciliationHistoryPage() {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("");
  const [limit, setLimit] = useState(30);

  // Structured filters
  const [filterType, setFilterType] = useState("");
  const [filterCatalog, setFilterCatalog] = useState("");
  const [filterDateFrom, setFilterDateFrom] = useState("");
  const [filterDateTo, setFilterDateTo] = useState("");

  // Compare mode
  const [compareA, setCompareA] = useState<string | null>(null);
  const [compareB, setCompareB] = useState<string | null>(null);
  const [comparison, setComparison] = useState<any>(null);
  const [comparing, setComparing] = useState(false);

  // Drill-down
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);
  const [runDetails, setRunDetails] = useState<Record<string, any[]>>({});
  const [detailsLoading, setDetailsLoading] = useState<string | null>(null);

  async function toggleRunDetails(runId: string) {
    if (expandedRunId === runId) { setExpandedRunId(null); return; }
    setExpandedRunId(runId);
    if (runDetails[runId]) return;
    setDetailsLoading(runId);
    try {
      const data = await api.get(`/reconciliation/history/${runId}/details`);
      setRunDetails(prev => ({ ...prev, [runId]: data.details || [] }));
    } catch {
      setRunDetails(prev => ({ ...prev, [runId]: [] }));
    } finally {
      setDetailsLoading(null);
    }
  }

  async function loadHistory() {
    setLoading(true);
    try {
      const data = await api.get(`/reconciliation/history?limit=${limit}`);
      setRuns(Array.isArray(data) ? data : Array.isArray(data?.runs) ? data.runs : []);
    } catch {
      setRuns([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadHistory(); }, [limit]);

  async function compareRuns() {
    if (!compareA || !compareB) return;
    setComparing(true);
    try {
      const data = await api.post("/reconciliation/compare-runs", { run_id_a: compareA, run_id_b: compareB });
      setComparison(data);
    } catch {}
    setComparing(false);
  }

  function toggleCompare(runId: string) {
    if (compareA === runId) { setCompareA(null); return; }
    if (compareB === runId) { setCompareB(null); return; }
    if (!compareA) setCompareA(runId);
    else if (!compareB) setCompareB(runId);
    else { setCompareA(compareB); setCompareB(runId); }
  }

  // ── Match rate computation ─────────────────────────────────────────────

  function computeMatchRate(r: any): number {
    const type = r.run_type || "";
    const matched = Number(r.matched || 0);
    const mismatched = Number(r.mismatched || 0);
    const total = Number(r.total_tables || 0);
    if (type === "row-level-batch" || type === "row-level") {
      return total > 0 ? Math.round((matched / total) * 100) : 100;
    }
    if (total <= 0) return 100;
    return mismatched === 0 ? 100 : Math.round(((total - mismatched) / total) * 100);
  }

  // ── Derived filter values ──────────────────────────────────────────────

  const runTypes = [...new Set(runs.map(r => r.run_type).filter(Boolean))];
  const catalogs = [...new Set(runs.map(r => r.source_catalog).filter(Boolean))];

  // ── Multi-predicate filtering ──────────────────────────────────────────

  const filtered = runs.filter(r => {
    if (filter && !JSON.stringify(r).toLowerCase().includes(filter.toLowerCase())) return false;
    if (filterType && r.run_type !== filterType) return false;
    if (filterCatalog && r.source_catalog !== filterCatalog) return false;
    if (filterDateFrom && (r.executed_at || "") < filterDateFrom) return false;
    if (filterDateTo && (r.executed_at || "").slice(0, 10) > filterDateTo) return false;
    return true;
  });

  // ── Trend data ─────────────────────────────────────────────────────────

  const trendData = [...filtered].reverse().map((r) => ({
    date: String(r.executed_at || "").slice(0, 16).replace("T", " "),
    match_rate: computeMatchRate(r),
    tables: r.total_tables || 0,
  }));

  // ── Anomaly detection ──────────────────────────────────────────────────

  const anomalies = new Set<string>();
  for (let i = 1; i < trendData.length; i++) {
    if (trendData[i - 1].match_rate - trendData[i].match_rate > 5) {
      anomalies.add(filtered[filtered.length - 1 - i]?.run_id);
    }
  }

  // ── Summary stats ──────────────────────────────────────────────────────

  const avgMatchRate = filtered.length > 0
    ? Math.round(filtered.reduce((sum, r) => sum + computeMatchRate(r), 0) / filtered.length)
    : 0;
  const avgDuration = filtered.length > 0
    ? filtered.reduce((sum, r) => sum + (Number(r.duration_seconds) || 0), 0) / filtered.length
    : 0;
  const anomalyCount = anomalies.size;

  // ── Match rate distribution ────────────────────────────────────────────

  const distribution = [
    { range: "100%", count: filtered.filter(r => computeMatchRate(r) === 100).length, fill: "#22c55e" },
    { range: "95-99%", count: filtered.filter(r => { const m = computeMatchRate(r); return m >= 95 && m < 100; }).length, fill: "#84cc16" },
    { range: "80-94%", count: filtered.filter(r => { const m = computeMatchRate(r); return m >= 80 && m < 95; }).length, fill: "#f59e0b" },
    { range: "<80%", count: filtered.filter(r => computeMatchRate(r) < 80).length, fill: "#ef4444" },
  ];

  // ── Comparison helpers ─────────────────────────────────────────────────

  function buildComparisonTableDiff() {
    if (!comparison?.run_a_details || !comparison?.run_b_details) return [];
    const mapA = new Map((comparison.run_a_details || []).map((d: any) => [`${d.schema_name}.${d.table_name}`, d]));
    const mapB = new Map((comparison.run_b_details || []).map((d: any) => [`${d.schema_name}.${d.table_name}`, d]));
    const allKeys = new Set([...mapA.keys(), ...mapB.keys()]);
    const diffs: any[] = [];
    allKeys.forEach(key => {
      const a = mapA.get(key);
      const b = mapB.get(key);
      const matchA = a ? (a.match === true || a.match === "true") : null;
      const matchB = b ? (b.match === true || b.match === "true") : null;
      if (matchA !== matchB || !a || !b) {
        diffs.push({ table: key, matchA, matchB, deltaA: a?.delta_count, deltaB: b?.delta_count });
      }
    });
    return diffs;
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Reconciliation History"
        description="View past reconciliation runs, trends, and compare results."
        icon={History}
        breadcrumbs={["Data Quality", "Reconciliation", "History"]}
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Total Runs</p>
            <p className="text-2xl font-bold mt-1">{filtered.length}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Avg Match Rate</p>
            <p className={`text-2xl font-bold mt-1 ${avgMatchRate >= 95 ? "text-green-500" : avgMatchRate >= 80 ? "text-amber-500" : "text-red-500"}`}>
              {avgMatchRate}%
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Avg Duration</p>
            <p className="text-2xl font-bold mt-1">{fmtDuration(avgDuration)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Anomalies</p>
            <p className={`text-2xl font-bold mt-1 ${anomalyCount > 0 ? "text-red-500" : "text-green-500"}`}>{anomalyCount}</p>
          </CardContent>
        </Card>
      </div>

      {/* Controls + Filters */}
      <Card>
        <CardContent className="pt-6 space-y-3">
          <div className="flex flex-wrap items-end gap-3">
            <div className="relative flex-1 min-w-[160px]">
              <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
              <Input placeholder="Search runs..." value={filter} onChange={(e) => setFilter(e.target.value)} className="pl-8" />
            </div>
            <div className="w-28">
              <label className="text-xs text-muted-foreground mb-1 block">Run Type</label>
              <select value={filterType} onChange={e => setFilterType(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-2 text-sm">
                <option value="">All</option>
                {runTypes.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div className="w-32">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog</label>
              <select value={filterCatalog} onChange={e => setFilterCatalog(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-2 text-sm">
                <option value="">All</option>
                {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div className="w-32">
              <label className="text-xs text-muted-foreground mb-1 block">From</label>
              <Input type="date" value={filterDateFrom} onChange={e => setFilterDateFrom(e.target.value)} />
            </div>
            <div className="w-32">
              <label className="text-xs text-muted-foreground mb-1 block">To</label>
              <Input type="date" value={filterDateTo} onChange={e => setFilterDateTo(e.target.value)} />
            </div>
            <div className="w-20">
              <label className="text-xs text-muted-foreground mb-1 block">Limit</label>
              <Input type="number" min={5} max={200} value={limit} onChange={(e) => setLimit(Number(e.target.value))} />
            </div>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <Button variant="outline" size="sm" onClick={loadHistory} disabled={loading}>
              <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
            <Button variant="outline" size="sm" onClick={() => exportHistoryCsv(filtered, computeMatchRate)} disabled={filtered.length === 0}>
              <Download className="h-3.5 w-3.5 mr-1.5" /> CSV
            </Button>
            <Button variant="outline" size="sm" onClick={() => exportHistoryJson(filtered)} disabled={filtered.length === 0}>
              <Download className="h-3.5 w-3.5 mr-1.5" /> JSON
            </Button>
            {compareA && compareB && (
              <Button size="sm" onClick={compareRuns} disabled={comparing}>
                <GitCompare className="h-3.5 w-3.5 mr-1.5" /> Compare Selected
              </Button>
            )}
            {(compareA || compareB) && (
              <span className="text-xs text-muted-foreground ml-2">
                Comparing: {compareA?.slice(0, 8) || "..."} vs {compareB?.slice(0, 8) || "(select second)"}
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Charts: Trend + Distribution */}
      {trendData.length > 1 && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <Card className="lg:col-span-2">
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Match Rate Trend</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={trendData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="date" tick={{ fontSize: 9 }} stroke="var(--muted-foreground)" />
                  <YAxis yAxisId="left" domain={[0, 100]} tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" unit="%" />
                  <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                  <Tooltip />
                  <Line yAxisId="left" type="monotone" dataKey="match_rate" stroke="#E8453C" strokeWidth={2} dot={{ r: 3 }} name="Match Rate %" />
                  <Line yAxisId="right" type="monotone" dataKey="tables" stroke="#3b82f6" strokeWidth={1} strokeDasharray="5 5" dot={false} name="Tables" />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={distribution} layout="vertical">
                  <XAxis type="number" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" allowDecimals={false} />
                  <YAxis type="category" dataKey="range" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" width={50} />
                  <Tooltip />
                  <Bar dataKey="count" name="Runs" radius={[0, 4, 4, 0]}>
                    {distribution.map((d, i) => (
                      <rect key={i} fill={d.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Comparison Result */}
      {comparison && (
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-base">Run Comparison</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {/* Side-by-side metrics */}
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div className="bg-muted/30 rounded-lg p-3 space-y-1">
                <p className="text-xs text-muted-foreground font-medium">Run A: {comparison.run_a?.run_id?.slice(0, 8)}</p>
                <p className="text-xs">{comparison.run_a?.executed_at?.slice(0, 19)}</p>
                <div className="flex items-center gap-3 mt-2">
                  <span className="text-green-500 font-bold">{Number(comparison.run_a?.matched || 0).toLocaleString()}</span>
                  <span className="text-xs text-muted-foreground">matched</span>
                  <span className="text-red-500 font-bold">{comparison.run_a?.mismatched || 0}</span>
                  <span className="text-xs text-muted-foreground">mismatched</span>
                </div>
                <p className="text-xs text-muted-foreground">{fmtDuration(comparison.run_a?.duration_seconds)} · {comparison.run_a?.execution_mode || "sql"}</p>
              </div>
              <div className="flex flex-col items-center justify-center gap-1">
                {["matched", "mismatched", "errors"].map(field => {
                  const a = Number(comparison.run_a?.[field] || 0);
                  const b = Number(comparison.run_b?.[field] || 0);
                  const delta = b - a;
                  if (delta === 0) return null;
                  const positive = field === "matched" ? delta > 0 : delta < 0;
                  return (
                    <span key={field} className={`text-xs ${positive ? "text-green-500" : "text-red-500"}`}>
                      {delta > 0 ? "+" : ""}{delta.toLocaleString()} {field}
                    </span>
                  );
                })}
              </div>
              <div className="bg-muted/30 rounded-lg p-3 space-y-1">
                <p className="text-xs text-muted-foreground font-medium">Run B: {comparison.run_b?.run_id?.slice(0, 8)}</p>
                <p className="text-xs">{comparison.run_b?.executed_at?.slice(0, 19)}</p>
                <div className="flex items-center gap-3 mt-2">
                  <span className="text-green-500 font-bold">{Number(comparison.run_b?.matched || 0).toLocaleString()}</span>
                  <span className="text-xs text-muted-foreground">matched</span>
                  <span className="text-red-500 font-bold">{comparison.run_b?.mismatched || 0}</span>
                  <span className="text-xs text-muted-foreground">mismatched</span>
                </div>
                <p className="text-xs text-muted-foreground">{fmtDuration(comparison.run_b?.duration_seconds)} · {comparison.run_b?.execution_mode || "sql"}</p>
              </div>
            </div>
            {/* Per-table diff */}
            {(() => {
              const diffs = buildComparisonTableDiff();
              if (diffs.length === 0) return (
                <p className="text-xs text-muted-foreground text-center py-2">All tables have the same match status between runs.</p>
              );
              return (
                <div>
                  <p className="text-xs text-muted-foreground mb-2">{diffs.length} table(s) with changed status:</p>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border/50 text-muted-foreground">
                        <th className="py-1 px-2 text-left font-medium">Table</th>
                        <th className="py-1 px-2 text-center font-medium">Run A</th>
                        <th className="py-1 px-2 text-center font-medium">Run B</th>
                      </tr>
                    </thead>
                    <tbody>
                      {diffs.map((d, i) => (
                        <tr key={i} className="border-b border-border/30">
                          <td className="py-1 px-2 font-mono">{d.table}</td>
                          <td className="py-1 px-2 text-center">
                            {d.matchA == null ? <span className="text-muted-foreground">—</span>
                              : d.matchA ? <CheckCircle className="h-3.5 w-3.5 text-green-500 inline" />
                              : <XCircle className="h-3.5 w-3.5 text-red-500 inline" />}
                          </td>
                          <td className="py-1 px-2 text-center">
                            {d.matchB == null ? <span className="text-muted-foreground">—</span>
                              : d.matchB ? <CheckCircle className="h-3.5 w-3.5 text-green-500 inline" />
                              : <XCircle className="h-3.5 w-3.5 text-red-500 inline" />}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
          </CardContent>
        </Card>
      )}

      {/* Run History Table */}
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-base">Past Runs ({filtered.length})</CardTitle></CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-4"><Loader2 className="h-4 w-4 animate-spin" /> Loading...</div>
          ) : filtered.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">No reconciliation runs found.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="py-2 px-2 text-left font-medium w-8">Cmp</th>
                    <th className="py-2 px-3 text-left font-medium">Run ID</th>
                    <th className="py-2 px-3 text-left font-medium">Type</th>
                    <th className="py-2 px-3 text-left font-medium">Source → Dest</th>
                    <th className="py-2 px-3 text-right font-medium">Tables</th>
                    <th className="py-2 px-3 text-right font-medium">Matched</th>
                    <th className="py-2 px-3 text-right font-medium">Mismatched</th>
                    <th className="py-2 px-3 text-right font-medium">Duration</th>
                    <th className="py-2 px-3 text-left font-medium">Executed</th>
                    <th className="py-2 px-3 text-center font-medium">Status</th>
                    <th className="w-8"></th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((r) => {
                    const isAnomaly = anomalies.has(r.run_id);
                    const isSelected = r.run_id === compareA || r.run_id === compareB;
                    const isExpanded = expandedRunId === r.run_id;
                    const matchRate = computeMatchRate(r);
                    const details = runDetails[r.run_id] || [];
                    return (
                      <>
                        <tr
                          key={r.run_id}
                          className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer ${isAnomaly ? "bg-red-500/5" : ""} ${isSelected ? "bg-[#E8453C]/5" : ""}`}
                          onClick={() => toggleRunDetails(r.run_id)}
                        >
                          <td className="py-1.5 px-2" onClick={(e) => e.stopPropagation()}>
                            <input type="checkbox" checked={isSelected} onChange={() => toggleCompare(r.run_id)} className="rounded" />
                          </td>
                          <td className="py-1.5 px-3 font-mono text-xs" title={r.run_id}>{r.run_id?.slice(0, 8)}…</td>
                          <td className="py-1.5 px-3">{runTypeBadge(r.run_type)}</td>
                          <td className="py-1.5 px-3 text-xs">{r.source_catalog} → {r.destination_catalog}</td>
                          <td className="py-1.5 px-3 text-right tabular-nums">{r.total_tables}</td>
                          <td className="py-1.5 px-3 text-right tabular-nums text-green-500">{Number(r.matched || 0).toLocaleString()}</td>
                          <td className="py-1.5 px-3 text-right tabular-nums text-red-500">{Number(r.mismatched || 0).toLocaleString()}</td>
                          <td className="py-1.5 px-3 text-right tabular-nums">{fmtDuration(r.duration_seconds)}</td>
                          <td className="py-1.5 px-3 text-xs">{String(r.executed_at || "").slice(0, 19)}</td>
                          <td className="py-1.5 px-3 text-center">
                            {isAnomaly && <AlertTriangle className="h-3.5 w-3.5 text-amber-500 inline mr-1" title="Match rate dropped >5%" />}
                            <Badge variant="outline" className={`text-[10px] ${matchRate >= 95 ? "text-green-500 border-green-500/30" : matchRate >= 80 ? "text-amber-500 border-amber-500/30" : "text-red-500 border-red-500/30"}`}>
                              {matchRate}%
                            </Badge>
                          </td>
                          <td className="py-1.5 px-2">
                            {isExpanded ? <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
                          </td>
                        </tr>
                        {isExpanded && (
                          <tr key={`${r.run_id}-details`}>
                            <td colSpan={11} className="p-0">
                              <div className="bg-muted/20 border-b border-border px-6 py-3">
                                {detailsLoading === r.run_id ? (
                                  <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading details...</div>
                                ) : details.length === 0 ? (
                                  <p className="text-sm text-muted-foreground">No per-table details available for this run.</p>
                                ) : (
                                  <>
                                    <p className="text-xs text-muted-foreground mb-2">
                                      {details.filter((d: any) => d.match === true || d.match === "true").length} of {details.length} table(s) matched
                                    </p>
                                    <table className="w-full text-xs">
                                      <thead>
                                        <tr className="border-b border-border/50 text-muted-foreground">
                                          <th className="py-1.5 px-2 text-left font-medium">Table</th>
                                          <th className="py-1.5 px-2 text-right font-medium">Source Rows</th>
                                          <th className="py-1.5 px-2 text-right font-medium">Dest Rows</th>
                                          <th className="py-1.5 px-2 text-right font-medium">Delta</th>
                                          <th className="py-1.5 px-2 text-center font-medium">Status</th>
                                          <th className="py-1.5 px-2 text-left font-medium">Error</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {details.map((d: any, i: number) => {
                                          const src = Number(d.source_count) || 0;
                                          const dst = Number(d.dest_count) || 0;
                                          const delta = src - dst;
                                          const deltaPct = src > 0 ? Math.abs(delta) / src * 100 : 0;
                                          const matched = d.match === true || d.match === "true";
                                          return (
                                            <tr key={i} className={`border-b border-border/30 ${d.error ? "bg-amber-500/5" : !matched ? "bg-red-500/5" : "bg-green-500/3"}`}>
                                              <td className="py-1 px-2 font-mono">{d.schema_name}.{d.table_name}</td>
                                              <td className="py-1 px-2 text-right tabular-nums">{d.source_count != null ? Number(d.source_count).toLocaleString() : "—"}</td>
                                              <td className="py-1 px-2 text-right tabular-nums">{d.dest_count != null ? Number(d.dest_count).toLocaleString() : "—"}</td>
                                              <td className="py-1 px-2 text-right">
                                                <div className="relative inline-flex items-center gap-1">
                                                  {delta !== 0 && (
                                                    <div
                                                      className="absolute inset-y-0 right-0 bg-red-500/10 rounded-sm"
                                                      style={{ width: `${Math.min(100, deltaPct)}%` }}
                                                    />
                                                  )}
                                                  <span className={`relative tabular-nums ${delta !== 0 ? "text-red-500 font-medium" : "text-muted-foreground"}`}>
                                                    {delta > 0 ? `+${delta.toLocaleString()}` : delta.toLocaleString()}
                                                  </span>
                                                </div>
                                              </td>
                                              <td className="py-1 px-2 text-center">
                                                {d.error ? <Badge variant="outline" className="text-amber-500 border-amber-500/30 text-[9px]">ERROR</Badge>
                                                  : matched ? <CheckCircle className="h-3.5 w-3.5 text-green-500 inline" />
                                                  : <XCircle className="h-3.5 w-3.5 text-red-500 inline" />}
                                              </td>
                                              <td className="py-1 px-2 text-muted-foreground truncate max-w-[200px]">{d.error || "—"}</td>
                                            </tr>
                                          );
                                        })}
                                      </tbody>
                                    </table>
                                  </>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
