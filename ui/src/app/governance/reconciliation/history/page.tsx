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
  Search, RefreshCw, GitCompare,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";

export default function ReconciliationHistoryPage() {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("");
  const [limit, setLimit] = useState(30);

  // Compare mode
  const [compareA, setCompareA] = useState<string | null>(null);
  const [compareB, setCompareB] = useState<string | null>(null);
  const [comparison, setComparison] = useState<any>(null);
  const [comparing, setComparing] = useState(false);

  async function loadHistory() {
    setLoading(true);
    try {
      const data = await api.get(`/reconciliation/history?limit=${limit}`);
      setRuns(Array.isArray(data) ? data : []);
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

  const filtered = filter
    ? runs.filter((r) => JSON.stringify(r).toLowerCase().includes(filter.toLowerCase()))
    : runs;

  // Trend data for chart
  const trendData = [...filtered].reverse().map((r) => ({
    date: String(r.executed_at || "").slice(0, 10),
    match_rate: r.matched && r.total_tables ? Math.round((r.matched / Math.max(r.total_tables, 1)) * 100) : 0,
    total: r.total_tables || 0,
  }));

  // Detect anomalies (>5% drop from previous)
  const anomalies = new Set<string>();
  for (let i = 1; i < trendData.length; i++) {
    if (trendData[i - 1].match_rate - trendData[i].match_rate > 5) {
      anomalies.add(filtered[filtered.length - 1 - i]?.run_id);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Reconciliation History"
        description="View past reconciliation runs, trends, and compare results."
        icon={History}
        breadcrumbs={["Data Quality", "Reconciliation", "History"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
              <Input placeholder="Filter runs..." value={filter} onChange={(e) => setFilter(e.target.value)} className="pl-8" />
            </div>
            <div className="w-24">
              <label className="text-xs text-muted-foreground mb-1 block">Limit</label>
              <Input type="number" min={5} max={100} value={limit} onChange={(e) => setLimit(Number(e.target.value))} />
            </div>
            <Button variant="outline" onClick={loadHistory} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
            {compareA && compareB && (
              <Button onClick={compareRuns} disabled={comparing}>
                <GitCompare className="h-4 w-4 mr-2" /> Compare Selected
              </Button>
            )}
          </div>
          {(compareA || compareB) && (
            <p className="text-xs text-muted-foreground mt-2">
              Comparing: {compareA || "..."} vs {compareB || "(select second run)"}
            </p>
          )}
        </CardContent>
      </Card>

      {/* Trend Chart */}
      {trendData.length > 1 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Match Rate Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="date" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                <Tooltip />
                <Line type="monotone" dataKey="match_rate" stroke="#E8453C" strokeWidth={2} dot={{ r: 3 }} name="Match Rate %" />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {/* Comparison Result */}
      {comparison && (
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-base">Run Comparison</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Run A: {comparison.run_a?.run_id}</p>
                <p className="font-bold">{comparison.run_a?.matched ?? 0} matched / {comparison.run_a?.total_tables ?? 0} tables</p>
                <p className="text-xs">{comparison.run_a?.executed_at}</p>
              </div>
              <div className="flex flex-col items-center justify-center">
                <p className={`text-lg font-bold ${(comparison.delta?.matched ?? 0) > 0 ? "text-green-500" : (comparison.delta?.matched ?? 0) < 0 ? "text-red-500" : "text-muted-foreground"}`}>
                  {(comparison.delta?.matched ?? 0) > 0 ? "+" : ""}{comparison.delta?.matched ?? 0}
                </p>
                <p className="text-xs text-muted-foreground">matched delta</p>
              </div>
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Run B: {comparison.run_b?.run_id}</p>
                <p className="font-bold">{comparison.run_b?.matched ?? 0} matched / {comparison.run_b?.total_tables ?? 0} tables</p>
                <p className="text-xs">{comparison.run_b?.executed_at}</p>
              </div>
            </div>
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
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((r) => {
                    const isAnomaly = anomalies.has(r.run_id);
                    const isSelected = r.run_id === compareA || r.run_id === compareB;
                    const matchRate = r.total_tables > 0 ? Math.round((r.matched / r.total_tables) * 100) : 100;
                    return (
                      <tr
                        key={r.run_id}
                        className={`border-b border-border/50 hover:bg-muted/30 ${isAnomaly ? "bg-red-500/5" : ""} ${isSelected ? "bg-[#E8453C]/5" : ""}`}
                      >
                        <td className="py-1.5 px-2">
                          <input type="checkbox" checked={isSelected} onChange={() => toggleCompare(r.run_id)} className="rounded" />
                        </td>
                        <td className="py-1.5 px-3 font-mono text-xs">{r.run_id}</td>
                        <td className="py-1.5 px-3"><Badge variant="outline" className="text-[10px]">{r.run_type || "—"}</Badge></td>
                        <td className="py-1.5 px-3 text-xs">{r.source_catalog} → {r.destination_catalog}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.total_tables}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums text-green-500">{r.matched}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums text-red-500">{r.mismatched || 0}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.duration_seconds ? `${r.duration_seconds}s` : "—"}</td>
                        <td className="py-1.5 px-3 text-xs">{String(r.executed_at || "").slice(0, 19)}</td>
                        <td className="py-1.5 px-3 text-center">
                          {isAnomaly && <AlertTriangle className="h-3.5 w-3.5 text-amber-500 inline mr-1" title="Match rate dropped >5%" />}
                          <Badge variant="outline" className={`text-[10px] ${matchRate >= 95 ? "text-green-500 border-green-500/30" : matchRate >= 80 ? "text-amber-500 border-amber-500/30" : "text-red-500 border-red-500/30"}`}>
                            {matchRate}%
                          </Badge>
                        </td>
                      </tr>
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
