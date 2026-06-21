// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ShieldAlert, Loader2, Search, Database, Table2, Layers, RefreshCw } from "lucide-react";

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

function SensitivityBadge({ row, columns }) {
  const tagNameIdx = columns.indexOf("tag_name");
  const tagVal = tagNameIdx >= 0 ? row[tagNameIdx] : null;
  if (tagVal && ["pii", "sensitive", "classified", "phi", "pci"].includes(tagVal?.toLowerCase())) {
    return <Badge className="text-[10px] bg-red-500 text-white">Tagged PII</Badge>;
  }
  return <Badge className="text-[10px] bg-amber-500 text-white">Name Match</Badge>;
}

function getColIdx(columns, name) {
  return columns.indexOf(name);
}

export default function PiiScannerPage() {
  const [catalogs, setCatalogs] = useState([]);
  const [selectedCatalog, setSelectedCatalog] = useState("");
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

  async function runScan() {
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const params = new URLSearchParams({ limit: "200" });
      if (selectedCatalog) params.set("catalog", selectedCatalog);
      const data = await api.get(`/assessment/pii/scan?${params.toString()}`);
      setResult(data);
    } catch (e) {
      setError(e?.message ?? "Failed to scan for PII columns. Make sure your SQL Warehouse ID is configured in Settings.");
    } finally {
      setLoading(false);
    }
  }

  const columns = result?.columns ?? [];
  const rows = result?.rows ?? [];

  const catIdx    = getColIdx(columns, "table_catalog");
  const schIdx    = getColIdx(columns, "table_schema");
  const tblIdx    = getColIdx(columns, "table_name");
  const colIdx    = getColIdx(columns, "column_name");
  const typeIdx   = getColIdx(columns, "data_type");
  const tagNameIdx = getColIdx(columns, "tag_name");

  const filteredRows = useMemo(() => {
    if (!search) return rows;
    const q = search.toLowerCase();
    return rows.filter(r =>
      (r[catIdx] ?? "").toLowerCase().includes(q) ||
      (r[schIdx] ?? "").toLowerCase().includes(q) ||
      (r[tblIdx] ?? "").toLowerCase().includes(q) ||
      (r[colIdx] ?? "").toLowerCase().includes(q)
    );
  }, [rows, search, catIdx, schIdx, tblIdx, colIdx]);

  const uniqueTables  = useMemo(() => new Set(rows.map(r => `${r[catIdx]}.${r[schIdx]}.${r[tblIdx]}`)).size, [rows, catIdx, schIdx, tblIdx]);
  const uniqueSchemas = useMemo(() => new Set(rows.map(r => `${r[catIdx]}.${r[schIdx]}`)).size, [rows, catIdx, schIdx]);
  const taggedCount   = useMemo(() => rows.filter(r => {
    const tv = r[tagNameIdx];
    return tv && ["pii","sensitive","classified","phi","pci"].includes(tv.toLowerCase());
  }).length, [rows, tagNameIdx]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="PII & Sensitive Data Scanner"
        icon={ShieldAlert}
        breadcrumbs={["Assessment", "UC Inventory", "PII Scanner"]}
        description="Scan Unity Catalog for columns with PII-related names or tags to identify sensitive data exposure."
      />

      {/* Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Scan Configuration</CardTitle>
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
          <Button onClick={runScan} disabled={loading} className="flex items-center gap-2">
            {loading
              ? <Loader2 className="h-4 w-4 animate-spin" />
              : <ShieldAlert className="h-4 w-4" />
            }
            {loading ? "Scanning…" : "Scan for PII"}
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

      {/* Loading animation */}
      {loading && (
        <div className="text-center py-16 text-muted-foreground">
          <div className="relative inline-flex">
            <ShieldAlert className="h-12 w-12 opacity-20" />
            <span className="absolute inset-0 flex items-center justify-center">
              <RefreshCw className="h-5 w-5 animate-spin text-amber-500" />
            </span>
          </div>
          <p className="mt-4 text-sm">Scanning all columns for PII patterns…</p>
          <p className="text-xs mt-1 opacity-60">This may take a few seconds depending on catalog size.</p>
        </div>
      )}

      {/* Summary cards */}
      {result && !loading && (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatCard label="PII Columns Found" value={rows.length} icon={ShieldAlert} color="text-red-600" />
            <StatCard label="Tagged PII" value={taggedCount} icon={Database} color="text-red-500" />
            <StatCard label="Tables Affected" value={uniqueTables} icon={Table2} color="text-amber-600" />
            <StatCard label="Schemas Affected" value={uniqueSchemas} icon={Layers} color="text-orange-600" />
          </div>

          {/* Results table */}
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between flex-wrap gap-2">
                <CardTitle className="text-sm font-medium">
                  PII Columns ({rows.length} found)
                </CardTitle>
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                  <input
                    type="text"
                    placeholder="Search…"
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
                  <ShieldAlert className="h-10 w-10 mx-auto mb-2 opacity-20" />
                  <p className="text-sm">No PII columns found matching your criteria.</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border bg-muted/30">
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Table</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Column</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Data Type</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Tag</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Match Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((row, i) => {
                        const catalog = row[catIdx] ?? "—";
                        const schema  = row[schIdx] ?? "—";
                        const table   = row[tblIdx] ?? "—";
                        const col     = row[colIdx] ?? "—";
                        const dtype   = row[typeIdx] ?? "—";
                        const tagName = row[tagNameIdx] ?? null;
                        const isTagged = tagName && ["pii","sensitive","classified","phi","pci"].includes(tagName.toLowerCase());

                        return (
                          <tr
                            key={i}
                            className={`border-b border-border last:border-0 hover:bg-muted/20 ${
                              isTagged ? "bg-red-50/30 dark:bg-red-950/10" : "bg-amber-50/20 dark:bg-amber-950/5"
                            }`}
                          >
                            <td className="px-4 py-2.5">
                              <span className="font-mono text-[11px] text-muted-foreground">
                                {catalog}.{schema}.
                              </span>
                              <span className="font-mono text-[11px] font-medium">{table}</span>
                            </td>
                            <td className="px-4 py-2.5 font-mono font-semibold text-[11px]">{col}</td>
                            <td className="px-4 py-2.5 font-mono text-[11px] text-muted-foreground">{dtype}</td>
                            <td className="px-4 py-2.5">
                              {tagName ? (
                                <Badge variant="outline" className="text-[10px] font-mono">{tagName}</Badge>
                              ) : (
                                <span className="text-muted-foreground/40">—</span>
                              )}
                            </td>
                            <td className="px-4 py-2.5">
                              <SensitivityBadge row={row} columns={columns} />
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

      {/* Empty state before scan */}
      {!result && !loading && !error && (
        <div className="text-center py-20 text-muted-foreground">
          <ShieldAlert className="h-14 w-14 mx-auto mb-3 opacity-15" />
          <p className="text-sm font-medium">No scan results yet</p>
          <p className="text-xs mt-1 opacity-70">
            Select a catalog (or leave blank for all) and click "Scan for PII" to identify sensitive columns.
          </p>
        </div>
      )}
    </div>
  );
}
