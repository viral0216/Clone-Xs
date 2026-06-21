// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { BrainCircuit, Database, Loader2, ChevronDown, ChevronRight } from "lucide-react";

const PAGE_SIZE = 50;

function StatusBadge({ status }) {
  const cls = status === "READY"
    ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300"
    : status?.includes("FAILED")
    ? "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300"
    : status?.includes("PENDING")
    ? "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300"
    : "bg-muted text-muted-foreground";
  return (
    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${cls}`}>
      {status ?? "—"}
    </span>
  );
}

function fmtDate(val) {
  if (!val) return "—";
  try { return new Date(typeof val === "number" ? val : val).toLocaleDateString(); } catch { return String(val).slice(0, 10); }
}

export default function RegisteredModelsPage() {
  const [inv, setInv] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [page, setPage] = useState(1);
  const [expandedRow, setExpandedRow] = useState(null);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(r => setInv(r))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const catalogs = useMemo(() => (inv?.catalogs ?? []).map(c => c.name), [inv]);

  const models = useMemo(() => {
    if (!inv) return [];
    const rows = [];
    for (const cat of inv.catalogs ?? []) {
      for (const sch of cat.schemas ?? []) {
        for (const m of sch.models ?? []) {
          const name = typeof m === "string" ? m : (m.name ?? "");
          if (!name) continue;
          const versions = (typeof m === "string" ? [] : m.versions) ?? [];
          const latest = versions.length > 0 ? versions[versions.length - 1].version : "—";
          rows.push({
            name,
            catalog: cat.name,
            schema: sch.name,
            full_name: (typeof m === "string" ? null : m.full_name) ?? `${cat.name}.${sch.name}.${name}`,
            owner: m.owner ?? "",
            comment: m.comment ?? "",
            versions,
            latest_version: latest,
          });
        }
      }
    }
    return rows;
  }, [inv]);

  const totalVersions = useMemo(() => models.reduce((s, m) => s + m.versions.length, 0), [models]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return models.filter(m =>
      (!catalogFilter || m.catalog === catalogFilter) &&
      (!q || m.name.toLowerCase().includes(q) || m.owner.toLowerCase().includes(q) || m.full_name.toLowerCase().includes(q))
    );
  }, [models, search, catalogFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function resetPage() { setPage(1); }

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Registered Models" icon={BrainCircuit} breadcrumbs={["Assessment", "UC Inventory", "Registered Models"]} description="All Unity Catalog registered ML models with version history across every schema." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading models…</span>
      </div>
    </div>
  );

  if (!inv || models.length === 0) return (
    <div className="space-y-4">
      <PageHeader title="Registered Models" icon={BrainCircuit} breadcrumbs={["Assessment", "UC Inventory", "Registered Models"]} description="All Unity Catalog registered ML models with version history across every schema." />
      <Card>
        <CardContent className="py-12 text-center">
          <Database className="h-10 w-10 mx-auto mb-3 opacity-20" />
          <p className="text-sm font-medium">No registered models found</p>
          <p className="text-xs text-muted-foreground mt-1">Run a scan with UC Inventory enabled, or no models are registered in this workspace.</p>
        </CardContent>
      </Card>
    </div>
  );

  return (
    <div className="space-y-4">
      <PageHeader
        title="Registered Models"
        icon={BrainCircuit}
        breadcrumbs={["Assessment", "UC Inventory", "Registered Models"]}
        description="All Unity Catalog registered ML models with version history across every schema."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-3">
        <Card>
          <CardContent className="pt-4 pb-3">
            <p className="text-xs text-muted-foreground">Total Models</p>
            <p className="text-2xl font-bold mt-0.5">{models.length}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4 pb-3">
            <p className="text-xs text-muted-foreground">Total Versions</p>
            <p className="text-2xl font-bold mt-0.5 text-blue-600">{totalVersions}</p>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2 items-center">
        <input
          value={search}
          onChange={e => { setSearch(e.target.value); resetPage(); }}
          placeholder="Search models…"
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring w-48"
        />
        <select
          value={catalogFilter}
          onChange={e => { setCatalogFilter(e.target.value); resetPage(); }}
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
        >
          <option value="">All catalogs</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        {(search || catalogFilter) && (
          <button onClick={() => { setSearch(""); setCatalogFilter(""); setPage(1); }} className="text-xs text-muted-foreground hover:text-foreground">Clear</button>
        )}
        <span className="text-xs text-muted-foreground ml-auto">{filtered.length} models</span>
      </div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Name", "Catalog", "Schema", "Owner", "Comment", "Latest Version", "Versions"].map(h => (
                    <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                  ))}
                  <th className="py-2 px-3 w-6" />
                </tr>
              </thead>
              <tbody>
                {visible.map((m) => {
                  const isOpen = expandedRow === m.full_name;
                  return (
                    <>
                      <tr
                        key={m.full_name}
                        className="border-t border-border hover:bg-muted/30 cursor-pointer transition-colors"
                        onClick={() => setExpandedRow(isOpen ? null : m.full_name)}
                      >
                        <td className="py-2 px-3 font-medium">{m.name}</td>
                        <td className="py-2 px-3 text-muted-foreground">{m.catalog}</td>
                        <td className="py-2 px-3 text-muted-foreground">{m.schema}</td>
                        <td className="py-2 px-3 text-muted-foreground">{m.owner || "—"}</td>
                        <td className="py-2 px-3 text-muted-foreground max-w-[160px] truncate">{m.comment || "—"}</td>
                        <td className="py-2 px-3 font-mono">{m.latest_version}</td>
                        <td className="py-2 px-3 text-muted-foreground">{m.versions.length}</td>
                        <td className="py-2 px-3 text-muted-foreground">
                          {m.versions.length > 0 && (isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />)}
                        </td>
                      </tr>
                      {isOpen && m.versions.length > 0 && (
                        <tr key={`${m.full_name}-exp`} className="border-t border-border bg-muted/20">
                          <td colSpan={8} className="px-4 py-3">
                            <p className="text-[11px] text-muted-foreground font-medium uppercase tracking-wide mb-2">Version History</p>
                            <table className="w-full text-xs">
                              <thead>
                                <tr className="border-b border-border">
                                  <th className="text-left py-1 px-2 text-muted-foreground">Version</th>
                                  <th className="text-left py-1 px-2 text-muted-foreground">Stage</th>
                                  <th className="text-left py-1 px-2 text-muted-foreground">Status</th>
                                  <th className="text-left py-1 px-2 text-muted-foreground">Created</th>
                                </tr>
                              </thead>
                              <tbody>
                                {[...m.versions].reverse().map((v, i) => (
                                  <tr key={i} className="border-t border-border/50">
                                    <td className="py-1 px-2 font-mono">{v.version ?? "—"}</td>
                                    <td className="py-1 px-2 text-muted-foreground">{v.stage ?? "—"}</td>
                                    <td className="py-1 px-2"><StatusBadge status={v.status} /></td>
                                    <td className="py-1 px-2 text-muted-foreground">{fmtDate(v.created_at ?? v.creation_timestamp)}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </td>
                        </tr>
                      )}
                    </>
                  );
                })}
              </tbody>
            </table>
          </div>
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-3 py-2 text-xs text-muted-foreground border-t border-border">
              <span>Page {page}/{totalPages} ({filtered.length} total)</span>
              <div className="flex gap-1">
                <button disabled={page === 1} onClick={() => setPage(p => p - 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">← Prev</button>
                <button disabled={page === totalPages} onClick={() => setPage(p => p + 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">Next →</button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
