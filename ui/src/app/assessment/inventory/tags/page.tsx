// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Tag, Database, Loader2 } from "lucide-react";

const PAGE_SIZE = 50;

const TYPE_COLORS = {
  TABLE:   "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300",
  COLUMN:  "bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300",
  SCHEMA:  "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
  CATALOG: "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
};

function TypeBadge({ type }) {
  return (
    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${TYPE_COLORS[type] ?? "bg-muted text-muted-foreground"}`}>
      {type}
    </span>
  );
}

function flattenTags(inv) {
  const rows = [];

  for (const cat of inv.catalogs ?? []) {
    for (const [k, v] of Object.entries(cat.tags ?? {})) {
      rows.push({ key: k, value: String(v), applied_to: cat.name, object_type: "CATALOG", catalog: cat.name });
    }
    for (const sch of cat.schemas ?? []) {
      for (const [k, v] of Object.entries(sch.tags ?? {})) {
        rows.push({ key: k, value: String(v), applied_to: `${cat.name}.${sch.name}`, object_type: "SCHEMA", catalog: cat.name });
      }
      for (const tbl of sch.tables ?? []) {
        for (const [k, v] of Object.entries(tbl.tags ?? {})) {
          rows.push({ key: k, value: String(v), applied_to: tbl.full_name ?? `${cat.name}.${sch.name}.${tbl.name}`, object_type: "TABLE", catalog: cat.name });
        }
        for (const col of tbl.columns ?? []) {
          for (const [k, v] of Object.entries(col.tags ?? {})) {
            rows.push({ key: k, value: String(v), applied_to: `${tbl.full_name ?? `${cat.name}.${sch.name}.${tbl.name}`}.${col.name}`, object_type: "COLUMN", catalog: cat.name });
          }
        }
      }
    }
  }
  return rows;
}

function countUntaggedTables(inv) {
  let n = 0;
  for (const cat of inv.catalogs ?? [])
    for (const sch of cat.schemas ?? [])
      for (const tbl of sch.tables ?? [])
        if (!tbl.tags || Object.keys(tbl.tags).length === 0) n++;
  return n;
}

export default function TagsBrowserPage() {
  const [inv, setInv] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [keyFilter, setKeyFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [page, setPage] = useState(1);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(r => setInv(r))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const catalogs = useMemo(() => (inv?.catalogs ?? []).map(c => c.name), [inv]);

  const tags = useMemo(() => (inv ? flattenTags(inv) : []), [inv]);
  const untaggedTables = useMemo(() => (inv ? countUntaggedTables(inv) : 0), [inv]);
  const uniqueKeys = useMemo(() => [...new Set(tags.map(t => t.key))].sort(), [tags]);
  const taggedTables = useMemo(() => new Set(tags.filter(t => t.object_type === "TABLE").map(t => t.applied_to)).size, [tags]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return tags.filter(t =>
      (!keyFilter || t.key === keyFilter) &&
      (!typeFilter || t.object_type === typeFilter) &&
      (!catalogFilter || t.catalog === catalogFilter) &&
      (!q || t.key.toLowerCase().includes(q) || t.value.toLowerCase().includes(q) || t.applied_to.toLowerCase().includes(q))
    );
  }, [tags, search, keyFilter, typeFilter, catalogFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function resetPage() { setPage(1); }

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Tags Browser" icon={Tag} breadcrumbs={["Assessment", "UC Inventory", "Tags"]} description="All Unity Catalog tags applied to tables, columns, schemas, and catalogs." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading tags…</span>
      </div>
    </div>
  );

  if (!inv) return (
    <div className="space-y-4">
      <PageHeader title="Tags Browser" icon={Tag} breadcrumbs={["Assessment", "UC Inventory", "Tags"]} description="All Unity Catalog tags applied to tables, columns, schemas, and catalogs." />
      <Card><CardContent className="py-12 text-center"><Database className="h-10 w-10 mx-auto mb-3 opacity-20" /><p className="text-sm font-medium">No inventory data</p><p className="text-xs text-muted-foreground mt-1">Run a scan with UC Inventory enabled.</p></CardContent></Card>
    </div>
  );

  if (tags.length === 0) return (
    <div className="space-y-4">
      <PageHeader title="Tags Browser" icon={Tag} breadcrumbs={["Assessment", "UC Inventory", "Tags"]} description="All Unity Catalog tags applied to tables, columns, schemas, and catalogs." />
      <Card>
        <CardContent className="py-12 text-center">
          <Tag className="h-10 w-10 mx-auto mb-3 opacity-20" />
          <p className="text-sm font-medium">No tags found</p>
          <p className="text-xs text-muted-foreground mt-1">No UC tags have been applied to any objects in this workspace. Add tags in Databricks and re-scan to see them here.</p>
        </CardContent>
      </Card>
    </div>
  );

  return (
    <div className="space-y-4">
      <PageHeader
        title="Tags Browser"
        icon={Tag}
        breadcrumbs={["Assessment", "UC Inventory", "Tags"]}
        description="All Unity Catalog tags applied to tables, columns, schemas, and catalogs."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { label: "Unique Tag Keys", value: uniqueKeys.length, color: "#E8453C" },
          { label: "Total Assignments", value: tags.length, color: undefined },
          { label: "Tagged Tables", value: taggedTables, color: "#a855f7" },
          { label: "Untagged Tables", value: untaggedTables, color: "#f97316" },
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
          onChange={e => { setSearch(e.target.value); resetPage(); }}
          placeholder="Search tags…"
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring w-44"
        />
        <select
          value={keyFilter}
          onChange={e => { setKeyFilter(e.target.value); resetPage(); }}
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring max-w-[160px]"
        >
          <option value="">All keys</option>
          {uniqueKeys.map(k => <option key={k} value={k}>{k}</option>)}
        </select>
        <select
          value={catalogFilter}
          onChange={e => { setCatalogFilter(e.target.value); resetPage(); }}
          className="text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
        >
          <option value="">All catalogs</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <div className="flex rounded-md border border-border overflow-hidden">
          {["", "TABLE", "COLUMN", "SCHEMA", "CATALOG"].map(t => (
            <button
              key={t}
              onClick={() => { setTypeFilter(t); resetPage(); }}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${typeFilter === t ? "bg-primary text-primary-foreground" : "bg-background text-muted-foreground hover:bg-muted/50"}`}
            >
              {t || "All"}
            </button>
          ))}
        </div>
        {(search || keyFilter || typeFilter || catalogFilter) && (
          <button onClick={() => { setSearch(""); setKeyFilter(""); setTypeFilter(""); setCatalogFilter(""); setPage(1); }} className="text-xs text-muted-foreground hover:text-foreground">Clear</button>
        )}
        <span className="text-xs text-muted-foreground ml-auto">{filtered.length} assignments</span>
      </div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Tag Key", "Tag Value", "Applied To", "Object Type", "Catalog"].map(h => (
                    <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visible.map((t, i) => (
                  <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                    <td className="py-2 px-3 font-medium">{t.key}</td>
                    <td className="py-2 px-3 text-muted-foreground">{t.value || <span className="text-muted-foreground/40 italic">empty</span>}</td>
                    <td className="py-2 px-3 font-mono text-[10px] text-muted-foreground max-w-[240px] truncate">{t.applied_to}</td>
                    <td className="py-2 px-3"><TypeBadge type={t.object_type} /></td>
                    <td className="py-2 px-3 text-muted-foreground">{t.catalog}</td>
                  </tr>
                ))}
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
