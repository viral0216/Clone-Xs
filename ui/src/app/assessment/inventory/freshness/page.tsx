// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Leaf, Loader2, Clock, Database, Table2, Search, AlertTriangle, CheckCircle2, Skull } from "lucide-react";

const STALE_OPTIONS = [
  { label: "7 days", value: 7 },
  { label: "30 days", value: 30 },
  { label: "60 days", value: 60 },
  { label: "90 days", value: 90 },
];

function StatCard({ label, value, icon: Icon, color }) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className={`text-2xl font-bold mt-0.5 ${color}`}>{value}</p>
          </div>
          <Icon className={`h-8 w-8 opacity-20 ${color}`} />
        </div>
      </CardContent>
    </Card>
  );
}

function FreshnessBadge({ days, staleDays }) {
  if (days === null || days === undefined) {
    return <Badge className="text-[10px] bg-slate-500 text-white">Never Written</Badge>;
  }
  const n = Number(days);
  if (n > staleDays * 3) {
    return <Badge className="text-[10px] bg-red-600 text-white">Dead</Badge>;
  }
  if (n > staleDays) {
    return <Badge className="text-[10px] bg-amber-500 text-white">Stale</Badge>;
  }
  return <Badge className="text-[10px] bg-green-600 text-white">Fresh</Badge>;
}

function formatDate(val) {
  if (!val) return <span className="text-muted-foreground/40">—</span>;
  try {
    return new Date(val).toLocaleDateString();
  } catch {
    return val;
  }
}

function getColIdx(columns, name) {
  return columns.indexOf(name);
}

export default function FreshnessTrackerPage() {
  const navigate = useNavigate();
  const [catalogs, setCatalogs] = useState([]);
  const [selectedCatalog, setSelectedCatalog] = useState("");
  const [staleDays, setStaleDays] = useState(30);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");

  // Fetch available catalogs on mount
  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => {
        const cats = (d?.catalogs ?? []).map(c => c.name).filter(Boolean);
        setCatalogs(cats);
      })
      .catch(() => {});
  }, []);

  async function loadFreshness() {
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const params = new URLSearchParams({ stale_days: String(staleDays) });
      if (selectedCatalog) params.set("catalog", selectedCatalog);
      const data = await api.get(`/assessment/freshness/tables?${params.toString()}`);
      setResult(data);
    } catch (e) {
      setError(e?.message ?? "Failed to load freshness data. Make sure your SQL Warehouse ID is configured in Settings.");
    } finally {
      setLoading(false);
    }
  }

  const columns = result?.columns ?? [];
  const rows = result?.rows ?? [];

  const catIdx   = getColIdx(columns, "table_catalog");
  const schIdx   = getColIdx(columns, "table_schema");
  const tblIdx   = getColIdx(columns, "table_name");
  const typeIdx  = getColIdx(columns, "table_type");
  const altIdx   = getColIdx(columns, "last_altered");
  const daysIdx  = getColIdx(columns, "days_since_update");

  const filteredRows = useMemo(() => {
    if (!search) return rows;
    const q = search.toLowerCase();
    return rows.filter(r =>
      (r[catIdx] ?? "").toLowerCase().includes(q) ||
      (r[schIdx] ?? "").toLowerCase().includes(q) ||
      (r[tblIdx] ?? "").toLowerCase().includes(q)
    );
  }, [rows, search, catIdx, schIdx, tblIdx]);

  const totalCount       = rows.length;
  const freshCount       = useMemo(() => rows.filter(r => { const d = r[daysIdx]; return d !== null && d !== undefined && Number(d) <= staleDays; }).length, [rows, daysIdx, staleDays]);
  const staleCount       = useMemo(() => rows.filter(r => { const d = r[daysIdx]; return d !== null && d !== undefined && Number(d) > staleDays; }).length, [rows, daysIdx, staleDays]);
  const neverWrittenCount = useMemo(() => rows.filter(r => r[daysIdx] === null || r[daysIdx] === undefined || r[altIdx] === null || r[altIdx] === undefined).length, [rows, daysIdx, altIdx]);

  function openLineage(row) {
    const fqn = `${row[catIdx]}.${row[schIdx]}.${row[tblIdx]}`;
    navigate(`/assessment/inventory/lineage?table=${encodeURIComponent(fqn)}`);
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Data Freshness Tracker"
        icon={Leaf}
        breadcrumbs={["Assessment", "UC Inventory", "Freshness"]}
        description="Track when Unity Catalog tables were last updated and identify stale or never-written tables."
      />

      {/* Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Freshness Configuration</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-3 items-end">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Catalog</label>
            <select
              value={selectedCatalog}
              onChange={e => setSelectedCatalog(e.target.value)}
              className="px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring min-w-[180px]"
            >
              <option value="">All catalogs</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Stale threshold</label>
            <div className="flex gap-1">
              {STALE_OPTIONS.map(opt => (
                <button
                  key={opt.value}
                  onClick={() => setStaleDays(opt.value)}
                  className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${
                    staleDays === opt.value
                      ? "bg-primary text-primary-foreground border-primary"
                      : "border-input bg-background text-muted-foreground hover:text-foreground hover:bg-muted"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>
          <Button onClick={loadFreshness} disabled={loading} className="flex items-center gap-2">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Leaf className="h-4 w-4" />}
            {loading ? "Loading…" : "Load Tables"}
          </Button>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-destructive/20">
          <CardContent className="pt-4 pb-3">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {loading && (
        <div className="text-center py-16 text-muted-foreground">
          <Loader2 className="h-10 w-10 mx-auto animate-spin opacity-30 mb-3" />
          <p className="text-sm">Loading table freshness data…</p>
        </div>
      )}

      {/* Summary cards */}
      {result && !loading && (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatCard label="Total Tables" value={totalCount} icon={Table2} color="text-foreground" />
            <StatCard label={`Fresh (≤${staleDays}d)`} value={freshCount} icon={CheckCircle2} color="text-green-600" />
            <StatCard label={`Stale (>${staleDays}d)`} value={staleCount} icon={AlertTriangle} color="text-amber-600" />
            <StatCard label="Never Written" value={neverWrittenCount} icon={Skull} color="text-red-600" />
          </div>

          {/* Table */}
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between flex-wrap gap-2">
                <CardTitle className="text-sm font-medium">
                  Tables ({rows.length})
                </CardTitle>
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                  <input
                    type="text"
                    placeholder="Search tables…"
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    className="pl-8 pr-3 py-1.5 text-xs border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring w-48"
                  />
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {filteredRows.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <Leaf className="h-10 w-10 mx-auto mb-2 opacity-20" />
                  <p className="text-sm">No tables found.</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border bg-muted/30">
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Table FQN</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Type</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Last Updated</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Days Stale</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((row, i) => {
                        const catalog = row[catIdx] ?? "—";
                        const schema  = row[schIdx] ?? "—";
                        const table   = row[tblIdx] ?? "—";
                        const type    = row[typeIdx] ?? "—";
                        const altered = row[altIdx];
                        const days    = row[daysIdx];
                        const daysNum = days !== null && days !== undefined ? Number(days) : null;

                        return (
                          <tr
                            key={i}
                            onClick={() => openLineage(row)}
                            className="border-b border-border last:border-0 hover:bg-muted/30 cursor-pointer"
                          >
                            <td className="px-4 py-2.5">
                              <span className="font-mono text-[11px] text-muted-foreground">
                                {catalog}.{schema}.
                              </span>
                              <span className="font-mono text-[11px] font-medium">{table}</span>
                            </td>
                            <td className="px-4 py-2.5">
                              <Badge variant="outline" className="text-[10px]">{type}</Badge>
                            </td>
                            <td className="px-4 py-2.5 text-muted-foreground">{formatDate(altered)}</td>
                            <td className="px-4 py-2.5">
                              {daysNum !== null
                                ? <span className={daysNum > staleDays ? "text-amber-600 font-semibold" : "text-green-600"}>{daysNum}</span>
                                : <span className="text-muted-foreground/40">—</span>
                              }
                            </td>
                            <td className="px-4 py-2.5">
                              <FreshnessBadge days={days} staleDays={staleDays} />
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
        </>
      )}

      {!result && !loading && !error && (
        <div className="text-center py-20 text-muted-foreground">
          <Leaf className="h-14 w-14 mx-auto mb-3 opacity-15" />
          <p className="text-sm font-medium">No freshness data loaded</p>
          <p className="text-xs mt-1 opacity-70">
            Select a catalog and staleness threshold, then click "Load Tables".
          </p>
        </div>
      )}
    </div>
  );
}
