// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { HardDrive, Database, Loader2, ChevronDown, ChevronRight } from "lucide-react";

const PAGE_SIZE = 50;

function VolTypeBadge({ type }) {
  const cls = type === "MANAGED"
    ? "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
    : type === "EXTERNAL"
    ? "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300"
    : "bg-muted text-muted-foreground";
  return (
    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${cls}`}>
      {type ?? "—"}
    </span>
  );
}

export default function VolumesPage() {
  const [inv, setInv] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [page, setPage] = useState(1);
  const [expandedRow, setExpandedRow] = useState(null);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(r => setInv(r))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const catalogs = useMemo(() => {
    if (!inv) return [];
    return (inv.catalogs ?? []).map(c => c.name);
  }, [inv]);

  const volumes = useMemo(() => {
    if (!inv) return [];
    const rows = [];
    for (const cat of inv.catalogs ?? []) {
      for (const sch of cat.schemas ?? []) {
        for (const vol of sch.volumes ?? []) {
          rows.push({
            name: vol.name ?? vol,
            catalog: cat.name,
            schema: sch.name,
            full_name: vol.full_name ?? `${cat.name}.${sch.name}.${vol.name ?? vol}`,
            volume_type: vol.volume_type ?? "—",
            storage_location: vol.storage_location ?? "",
            owner: vol.owner ?? "",
            comment: vol.comment ?? "",
            grants: vol.grants ?? [],
          });
        }
      }
    }
    return rows;
  }, [inv]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return volumes.filter(v =>
      (!catalogFilter || v.catalog === catalogFilter) &&
      (!typeFilter || v.volume_type === typeFilter) &&
      (!q || v.name.toLowerCase().includes(q) || v.owner.toLowerCase().includes(q) || v.comment.toLowerCase().includes(q) || v.full_name.toLowerCase().includes(q))
    );
  }, [volumes, search, catalogFilter, typeFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function setFilter(fn) {
    fn();
    setPage(1);
  }

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Volumes" icon={HardDrive} breadcrumbs={["Assessment", "UC Inventory", "Volumes"]} description="All Unity Catalog volumes — managed and external storage locations across every schema." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading volumes…</span>
      </div>
    </div>
  );

  if (!inv || volumes.length === 0) return (
    <div className="space-y-4">
      <PageHeader title="Volumes" icon={HardDrive} breadcrumbs={["Assessment", "UC Inventory", "Volumes"]} description="All Unity Catalog volumes — managed and external storage locations across every schema." />
      <Card>
        <CardContent className="py-12 text-center">
          <Database className="h-10 w-10 mx-auto mb-3 opacity-20" />
          <p className="text-sm font-medium">No volumes found</p>
          <p className="text-xs text-muted-foreground mt-1">Run a scan with UC Inventory enabled, or no volumes exist in this workspace.</p>
        </CardContent>
      </Card>
    </div>
  );

  const managed = volumes.filter(v => v.volume_type === "MANAGED").length;
  const external = volumes.filter(v => v.volume_type === "EXTERNAL").length;
  const withOwner = volumes.filter(v => v.owner).length;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Volumes"
        icon={HardDrive}
        breadcrumbs={["Assessment", "UC Inventory", "Volumes"]}
        description="All Unity Catalog volumes — managed and external storage locations across every schema."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { label: "Total Volumes", value: volumes.length, color: undefined },
          { label: "Managed", value: managed, color: "#3b82f6" },
          { label: "External", value: external, color: "#a855f7" },
          { label: "With Owner", value: withOwner, color: "#22c55e" },
        ].map(({ label, value, color }) => (
          <Card key={label}>
            <CardContent className="pt-4 pb-3">
              <p className="text-xs text-muted-foreground">{label}</p>
              <p className="text-2xl font-bold mt-0.5" style={color ? { color } : {}}>{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2 items-center">
        <input
          value={search}
          onChange={e => setFilter(() => setSearch(e.target.value))}
          placeholder="Search volumes…"
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring w-48"
        />
        <select
          value={catalogFilter}
          onChange={e => setFilter(() => setCatalogFilter(e.target.value))}
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
        >
          <option value="">All catalogs</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <div className="flex rounded-md border border-border overflow-hidden">
          {["", "MANAGED", "EXTERNAL"].map(t => (
            <button
              key={t}
              onClick={() => setFilter(() => setTypeFilter(t))}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${typeFilter === t ? "bg-primary text-primary-foreground" : "bg-background text-muted-foreground hover:bg-muted/50"}`}
            >
              {t || "All"}
            </button>
          ))}
        </div>
        {(search || catalogFilter || typeFilter) && (
          <button onClick={() => { setSearch(""); setCatalogFilter(""); setTypeFilter(""); setPage(1); }} className="text-xs text-muted-foreground hover:text-foreground">Clear</button>
        )}
        <span className="text-xs text-muted-foreground ml-auto">{filtered.length} volumes</span>
      </div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Name", "Catalog", "Schema", "Type", "Storage Location", "Owner", "Comment", "Grants"].map(h => (
                    <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                  ))}
                  <th className="py-2 px-3 w-6" />
                </tr>
              </thead>
              <tbody>
                {visible.map((vol) => {
                  const isOpen = expandedRow === vol.full_name;
                  return (
                    <>
                      <tr
                        key={vol.full_name}
                        className="border-t border-border hover:bg-muted/30 cursor-pointer transition-colors"
                        onClick={() => setExpandedRow(isOpen ? null : vol.full_name)}
                      >
                        <td className="py-2 px-3 font-medium">{vol.name}</td>
                        <td className="py-2 px-3 text-muted-foreground">{vol.catalog}</td>
                        <td className="py-2 px-3 text-muted-foreground">{vol.schema}</td>
                        <td className="py-2 px-3"><VolTypeBadge type={vol.volume_type} /></td>
                        <td className="py-2 px-3 text-muted-foreground max-w-[180px] truncate font-mono text-[10px]">{vol.storage_location || "—"}</td>
                        <td className="py-2 px-3 text-muted-foreground">{vol.owner || "—"}</td>
                        <td className="py-2 px-3 text-muted-foreground max-w-[140px] truncate">{vol.comment || "—"}</td>
                        <td className="py-2 px-3 text-muted-foreground">{vol.grants.length || "—"}</td>
                        <td className="py-2 px-3 text-muted-foreground">
                          {isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                        </td>
                      </tr>
                      {isOpen && (
                        <tr key={`${vol.full_name}-exp`} className="border-t border-border bg-muted/20">
                          <td colSpan={9} className="px-4 py-3">
                            <p className="text-[11px] text-muted-foreground font-medium uppercase tracking-wide mb-2">Full Name</p>
                            <p className="font-mono text-xs mb-3">{vol.full_name}</p>
                            {vol.grants.length > 0 ? (
                              <>
                                <p className="text-[11px] text-muted-foreground font-medium uppercase tracking-wide mb-1">Grants</p>
                                <table className="w-full text-xs">
                                  <thead>
                                    <tr className="border-b border-border">
                                      <th className="text-left py-1 px-2 text-muted-foreground">Principal</th>
                                      <th className="text-left py-1 px-2 text-muted-foreground">Privileges</th>
                                      <th className="text-left py-1 px-2 text-muted-foreground">Inherited From</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {vol.grants.map((g, i) => (
                                      <tr key={i} className="border-t border-border/50">
                                        <td className="py-1 px-2">{g.principal}</td>
                                        <td className="py-1 px-2">{(g.privileges ?? []).join(", ")}</td>
                                        <td className="py-1 px-2 text-muted-foreground">{g.inherited_from || "—"}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </>
                            ) : (
                              <p className="text-xs text-muted-foreground">No grants recorded.</p>
                            )}
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
