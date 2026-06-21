// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Search, Loader2, Table2, FileStack, FunctionSquare, BrainCircuit, Database, LayersIcon, Filter, X } from "lucide-react";

// ---------------------------------------------------------------------------
// Object type config
// ---------------------------------------------------------------------------
const TYPE_CONFIG = {
  catalog:  { label: "Catalog",  icon: Database,        color: "text-blue-500",   bg: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300" },
  schema:   { label: "Schema",   icon: LayersIcon,      color: "text-indigo-500", bg: "bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300" },
  table:    { label: "Table",    icon: Table2,           color: "text-purple-500", bg: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300" },
  view:     { label: "View",     icon: Table2,           color: "text-violet-500", bg: "bg-violet-100 text-violet-800 dark:bg-violet-900/30 dark:text-violet-300" },
  volume:   { label: "Volume",   icon: FileStack,        color: "text-pink-500",   bg: "bg-pink-100 text-pink-800 dark:bg-pink-900/30 dark:text-pink-300" },
  function: { label: "Function", icon: FunctionSquare,  color: "text-rose-500",   bg: "bg-rose-100 text-rose-800 dark:bg-rose-900/30 dark:text-rose-300" },
  model:    { label: "Model",    icon: BrainCircuit,    color: "text-orange-500", bg: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300" },
};

function typeCfg(t: string) {
  return TYPE_CONFIG[t] ?? { label: t, icon: Database, color: "text-muted-foreground", bg: "bg-muted text-muted-foreground" };
}

// ---------------------------------------------------------------------------
// Flatten inventory into a searchable flat list
// ---------------------------------------------------------------------------
function flattenInventory(inv: any) {
  const rows: any[] = [];

  for (const cat of inv.catalogs ?? []) {
    rows.push({
      type: "catalog",
      name: cat.name,
      full_name: cat.name,
      path: "",
      catalog: cat.name,
      schema: "",
      owner: cat.owner || "",
      comment: cat.comment || "",
      extra: cat.catalog_type || "",
    });

    for (const sch of cat.schemas ?? []) {
      rows.push({
        type: "schema",
        name: sch.name,
        full_name: `${cat.name}.${sch.name}`,
        path: cat.name,
        catalog: cat.name,
        schema: sch.name,
        owner: sch.owner || "",
        comment: sch.comment || "",
        extra: `${(sch.tables ?? []).length} tables`,
      });

      for (const tbl of sch.tables ?? []) {
        const isView = tbl.table_type === "VIEW";
        rows.push({
          type: isView ? "view" : "table",
          name: tbl.name,
          full_name: `${cat.name}.${sch.name}.${tbl.name}`,
          path: `${cat.name} › ${sch.name}`,
          catalog: cat.name,
          schema: sch.name,
          owner: tbl.owner || "",
          comment: tbl.comment || "",
          extra: tbl.table_type || "",
        });
      }

      for (const vol of sch.volumes ?? []) {
        rows.push({
          type: "volume",
          name: vol.name,
          full_name: `${cat.name}.${sch.name}.${vol.name}`,
          path: `${cat.name} › ${sch.name}`,
          catalog: cat.name,
          schema: sch.name,
          owner: vol.owner || "",
          comment: vol.comment || "",
          extra: vol.volume_type || "",
        });
      }

      for (const fn of sch.functions ?? []) {
        rows.push({
          type: "function",
          name: fn.name ?? fn,
          full_name: `${cat.name}.${sch.name}.${fn.name ?? fn}`,
          path: `${cat.name} › ${sch.name}`,
          catalog: cat.name,
          schema: sch.name,
          owner: fn.owner || "",
          comment: fn.comment || "",
          extra: "",
        });
      }

      for (const m of sch.models ?? []) {
        rows.push({
          type: "model",
          name: m.name ?? m,
          full_name: `${cat.name}.${sch.name}.${m.name ?? m}`,
          path: `${cat.name} › ${sch.name}`,
          catalog: cat.name,
          schema: sch.name,
          owner: m.owner || "",
          comment: m.comment || "",
          extra: "",
        });
      }
    }
  }

  return rows;
}

// ---------------------------------------------------------------------------
// Highlight matched text
// ---------------------------------------------------------------------------
function Highlight({ text, query }: { text: string; query: string }) {
  if (!query || !text) return <>{text}</>;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx < 0) return <>{text}</>;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="bg-yellow-200 dark:bg-yellow-900/60 text-inherit rounded-sm px-0.5">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
const PAGE_SIZE = 50;

export default function InventorySearchPage() {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<string[]>([]);
  const [catalogFilter, setCatalogFilter] = useState("");
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<any>(null);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => setInv(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const allRows = useMemo(() => inv ? flattenInventory(inv) : [], [inv]);

  const catalogs = useMemo(() => [...new Set(allRows.filter(r => r.type === "catalog").map(r => r.name))], [allRows]);

  const filtered = useMemo(() => {
    const q = query.toLowerCase().trim();
    return allRows.filter(row => {
      if (typeFilter.length && !typeFilter.includes(row.type)) return false;
      if (catalogFilter && row.catalog !== catalogFilter) return false;
      if (!q) return true;
      return (
        row.name.toLowerCase().includes(q) ||
        row.full_name.toLowerCase().includes(q) ||
        row.owner.toLowerCase().includes(q) ||
        row.comment.toLowerCase().includes(q)
      );
    });
  }, [allRows, query, typeFilter, catalogFilter]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function toggleType(t: string) {
    setTypeFilter(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]);
    setPage(1);
  }

  function handleQuery(q: string) {
    setQuery(q);
    setPage(1);
    setSelected(null);
  }

  const typeCounts = useMemo(() => {
    const c: Record<string, number> = {};
    allRows.forEach(r => { c[r.type] = (c[r.type] || 0) + 1; });
    return c;
  }, [allRows]);

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Object Search" icon={Search} breadcrumbs={["Assessment", "UC Inventory", "Search"]} description="Search across all Unity Catalog objects." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading inventory…</span>
      </div>
    </div>
  );

  if (!inv) return (
    <div className="space-y-4">
      <PageHeader title="Object Search" icon={Search} breadcrumbs={["Assessment", "UC Inventory", "Search"]} description="Search across all Unity Catalog objects." />
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          <Database className="h-10 w-10 mx-auto mb-3 opacity-30" />
          <p className="font-medium mb-1">No UC inventory available</p>
          <p>Re-run the scan with <strong>Include UC Inventory</strong> enabled.</p>
        </CardContent>
      </Card>
    </div>
  );

  return (
    <div className="space-y-4">
      <PageHeader
        title="Object Search"
        icon={Search}
        breadcrumbs={["Assessment", "UC Inventory", "Search"]}
        description={`Search across ${allRows.length.toLocaleString()} Unity Catalog objects — tables, schemas, volumes, functions, and models.`}
      />

      {/* Search + filters */}
      <div className="space-y-3">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search by name, owner, or comment…"
            value={query}
            onChange={e => handleQuery(e.target.value)}
            autoFocus
            className="w-full pl-10 pr-10 py-2.5 text-sm rounded-md border border-border bg-background focus:outline-none focus:ring-2 focus:ring-primary/50"
          />
          {query && (
            <button onClick={() => handleQuery("")} className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
              <X className="h-4 w-4" />
            </button>
          )}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <Filter className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          {/* Type pills */}
          {Object.entries(TYPE_CONFIG).map(([type, cfg]) => {
            const count = typeCounts[type] ?? 0;
            if (!count) return null;
            const active = typeFilter.includes(type);
            const Icon = cfg.icon;
            return (
              <button
                key={type}
                onClick={() => toggleType(type)}
                className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
                  active ? "bg-primary text-primary-foreground border-primary" : "border-border hover:bg-muted/60 text-muted-foreground"
                }`}
              >
                <Icon className="h-3 w-3" />
                {cfg.label} ({count})
              </button>
            );
          })}

          {/* Catalog select */}
          <select
            value={catalogFilter}
            onChange={e => { setCatalogFilter(e.target.value); setPage(1); }}
            className="text-xs border border-border rounded-md px-2 py-1 bg-background text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
          >
            <option value="">All catalogs</option>
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>

          {(typeFilter.length > 0 || catalogFilter) && (
            <button
              onClick={() => { setTypeFilter([]); setCatalogFilter(""); setPage(1); }}
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              <X className="h-3 w-3" /> Clear filters
            </button>
          )}

          <span className="ml-auto text-xs text-muted-foreground">
            {filtered.length.toLocaleString()} result{filtered.length !== 1 ? "s" : ""}
          </span>
        </div>
      </div>

      {/* Results + detail panel */}
      <div className={`grid gap-4 ${selected ? "grid-cols-1 lg:grid-cols-3" : ""}`}>
        <div className={selected ? "lg:col-span-2" : ""}>
          <div className="rounded-md border border-border overflow-hidden">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  <th className="text-left py-2 px-3 text-muted-foreground font-medium">Type</th>
                  <th className="text-left py-2 px-3 text-muted-foreground font-medium">Name</th>
                  <th className="text-left py-2 px-3 text-muted-foreground font-medium hidden sm:table-cell">Path</th>
                  <th className="text-left py-2 px-3 text-muted-foreground font-medium hidden md:table-cell">Owner</th>
                  <th className="text-left py-2 px-3 text-muted-foreground font-medium hidden lg:table-cell">Comment / Info</th>
                </tr>
              </thead>
              <tbody>
                {visible.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="py-12 text-center text-muted-foreground">
                      {query ? `No objects match "${query}"` : "No objects found"}
                    </td>
                  </tr>
                ) : visible.map((row, i) => {
                  const cfg = typeCfg(row.type);
                  const Icon = cfg.icon;
                  const isSelected = selected?.full_name === row.full_name && selected?.type === row.type;
                  return (
                    <tr
                      key={i}
                      className={`border-t border-border transition-colors cursor-pointer ${isSelected ? "bg-primary/5" : "hover:bg-muted/30"}`}
                      onClick={() => setSelected(isSelected ? null : row)}
                    >
                      <td className="py-2 px-3">
                        <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium ${cfg.bg}`}>
                          <Icon className="h-2.5 w-2.5" />
                          {cfg.label}
                        </span>
                      </td>
                      <td className="py-2 px-3 font-medium max-w-[200px] truncate">
                        <Highlight text={row.name} query={query} />
                      </td>
                      <td className="py-2 px-3 text-muted-foreground hidden sm:table-cell max-w-[180px] truncate">
                        <Highlight text={row.path} query={query} />
                      </td>
                      <td className="py-2 px-3 text-muted-foreground hidden md:table-cell max-w-[160px] truncate">
                        <Highlight text={row.owner} query={query} />
                      </td>
                      <td className="py-2 px-3 text-muted-foreground hidden lg:table-cell max-w-[200px] truncate">
                        <Highlight text={row.comment || row.extra} query={query} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-3 text-xs text-muted-foreground">
              <span>Page {page} of {totalPages} ({filtered.length.toLocaleString()} results)</span>
              <div className="flex gap-1">
                <button
                  disabled={page === 1}
                  onClick={() => setPage(p => p - 1)}
                  className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50 transition-colors"
                >
                  ← Prev
                </button>
                <button
                  disabled={page === totalPages}
                  onClick={() => setPage(p => p + 1)}
                  className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50 transition-colors"
                >
                  Next →
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Detail panel */}
        {selected && (
          <div className="lg:col-span-1">
            <div className="rounded-md border border-border bg-muted/20 p-4 space-y-3 sticky top-4">
              <div className="flex items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                  {(() => {
                    const cfg = typeCfg(selected.type);
                    const Icon = cfg.icon;
                    return (
                      <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium mb-1.5 ${cfg.bg}`}>
                        <Icon className="h-2.5 w-2.5" />
                        {cfg.label}
                      </span>
                    );
                  })()}
                  <p className="font-semibold text-sm break-all">{selected.name}</p>
                </div>
                <button onClick={() => setSelected(null)} className="text-muted-foreground hover:text-foreground shrink-0">
                  <X className="h-4 w-4" />
                </button>
              </div>

              <div className="space-y-2 text-xs">
                {[
                  ["Full name", selected.full_name],
                  ["Catalog", selected.catalog],
                  selected.schema && ["Schema", selected.schema],
                  selected.owner && ["Owner", selected.owner],
                  selected.extra && ["Info", selected.extra],
                  selected.comment && ["Comment", selected.comment],
                ].filter(Boolean).map(([label, value]) => (
                  <div key={label as string}>
                    <p className="text-muted-foreground text-[11px] uppercase tracking-wide font-medium">{label}</p>
                    <p className="font-mono break-all mt-0.5">{value as string}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
