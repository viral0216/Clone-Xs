// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Loader2, ShieldCheck } from "lucide-react";

// ── privilege classification ──
const DANGEROUS = new Set(["ALL PRIVILEGES", "MANAGE", "CREATE CATALOG", "MANAGE GRANTS"]);
const ADMIN_PRIVS = new Set([
  ...DANGEROUS,
  "CREATE SCHEMA", "CREATE TABLE", "CREATE FUNCTION", "CREATE REGISTERED MODEL",
  "CREATE VOLUME", "CREATE CONNECTION", "CREATE STORAGE CREDENTIAL", "CREATE EXTERNAL LOCATION",
]);
const WRITE_PRIVS = new Set(["MODIFY", "WRITE_FILES", "EXECUTE", "RUN", "EDIT_METADATA"]);

function privCategory(p: string) {
  if (ADMIN_PRIVS.has(p)) return "admin";
  if (WRITE_PRIVS.has(p)) return "write";
  return "read";
}

// ── level badge styles ──
const LVL_CLS: Record<string, string> = {
  METASTORE: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300",
  CATALOG:   "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
  SCHEMA:    "bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300",
  TABLE:     "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
  VIEW:      "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
  MODEL:     "bg-teal-100 text-teal-700 dark:bg-teal-900/30 dark:text-teal-300",
};

function flattenGrants(inv: any) {
  const rows: any[] = [];

  const push = (level: string, catalog: string, g: any) =>
    rows.push({ level, catalog, full_name: g.full_name ?? "—", principal: g.principal ?? "—", privileges: g.privileges ?? [], inherited_from: g.inherited_from ?? "" });

  for (const g of inv.metastore_grants ?? []) push("METASTORE", "—", g);

  for (const cat of inv.catalogs ?? []) {
    for (const g of cat.grants ?? []) push("CATALOG", cat.name, g);
    for (const schema of cat.schemas ?? []) {
      for (const g of schema.grants ?? []) push("SCHEMA", cat.name, g);
      for (const table of schema.tables ?? []) {
        const lvl = table.table_type === "VIEW" ? "VIEW" : "TABLE";
        for (const g of table.grants ?? []) push(lvl, cat.name, g);
      }
      for (const model of schema.models ?? []) {
        for (const g of model.grants ?? []) push("MODEL", cat.name, g);
      }
    }
  }

  return rows;
}

const PAGE_SIZE = 50;
const LEVELS = ["METASTORE", "CATALOG", "SCHEMA", "TABLE", "VIEW", "MODEL"] as const;

export default function AccessControlPage() {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [levelFilter, setLevelFilter] = useState("");
  const [privFilter, setPrivFilter] = useState(""); // "" | "admin" | "write" | "read"
  const [page, setPage] = useState(1);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => { setInv(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const allGrants = useMemo(() => (inv ? flattenGrants(inv) : []), [inv]);
  const catalogs = useMemo(() => (inv ? [...new Set((inv.catalogs ?? []).map((c: any) => c.name))] as string[] : []), [inv]);

  const filtered = useMemo(() => {
    let rows = allGrants;
    if (search) {
      const q = search.toLowerCase();
      rows = rows.filter(r => r.principal?.toLowerCase().includes(q) || r.full_name?.toLowerCase().includes(q));
    }
    if (catalogFilter) rows = rows.filter(r => r.catalog === catalogFilter);
    if (levelFilter) rows = rows.filter(r => r.level === levelFilter);
    if (privFilter) rows = rows.filter(r => r.privileges.some((p: string) => privCategory(p) === privFilter));
    return rows;
  }, [allGrants, search, catalogFilter, levelFilter, privFilter]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const adminCount = useMemo(() => allGrants.filter(r => r.privileges.some((p: string) => DANGEROUS.has(p))).length, [allGrants]);
  const uniquePrincipals = useMemo(() => new Set(allGrants.map(r => r.principal)).size, [allGrants]);
  const inheritedCount = useMemo(() => allGrants.filter(r => r.inherited_from).length, [allGrants]);

  function resetPage() { setPage(1); }

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Access Control" icon={ShieldCheck} breadcrumbs={["Assessment", "UC Inventory", "Access Control"]} description="All Unity Catalog grants across every securable level." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading grants…</span>
      </div>
    </div>
  );

  return (
    <div className="space-y-4">
      <PageHeader
        title="Access Control"
        icon={ShieldCheck}
        breadcrumbs={["Assessment", "UC Inventory", "Access Control"]}
        description="All Unity Catalog grants across every securable level — metastore, catalog, schema, table, model."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <Card><CardContent className="pt-3 pb-3 text-center">
          <p className="text-2xl font-bold">{allGrants.length}</p>
          <p className="text-xs text-muted-foreground mt-0.5">Total Grant Records</p>
        </CardContent></Card>
        <Card><CardContent className="pt-3 pb-3 text-center">
          <p className="text-2xl font-bold">{uniquePrincipals}</p>
          <p className="text-xs text-muted-foreground mt-0.5">Unique Principals</p>
        </CardContent></Card>
        <Card><CardContent className="pt-3 pb-3 text-center">
          <p className={`text-2xl font-bold ${adminCount > 0 ? "text-red-600" : "text-green-600"}`}>{adminCount}</p>
          <p className="text-xs text-muted-foreground mt-0.5">Admin / Dangerous Grants</p>
        </CardContent></Card>
        <Card><CardContent className="pt-3 pb-3 text-center">
          <p className="text-2xl font-bold">{inheritedCount}</p>
          <p className="text-xs text-muted-foreground mt-0.5">Inherited Grants</p>
        </CardContent></Card>
      </div>

      {/* Filter bar */}
      <Card>
        <CardContent className="py-3 space-y-2">
          <div className="flex flex-wrap gap-2">
            <input
              type="search"
              placeholder="Search principal or object name…"
              value={search}
              onChange={e => { setSearch(e.target.value); resetPage(); }}
              className="flex-1 min-w-[200px] text-xs border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            />
            <select
              value={catalogFilter}
              onChange={e => { setCatalogFilter(e.target.value); resetPage(); }}
              className="text-xs border border-border rounded-md px-2 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            >
              <option value="">All Catalogs</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="flex flex-wrap items-center gap-1">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wide mr-1">Level:</span>
            {(["", ...LEVELS] as const).map(l => (
              <button key={l} onClick={() => { setLevelFilter(l); resetPage(); }}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${levelFilter === l ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80 text-muted-foreground"}`}>
                {l || "All"}
              </button>
            ))}
            <span className="w-px h-5 bg-border mx-2" />
            <span className="text-[10px] text-muted-foreground uppercase tracking-wide mr-1">Privilege:</span>
            {(["", "admin", "write", "read"] as const).map(p => (
              <button key={p} onClick={() => { setPrivFilter(p); resetPage(); }}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${privFilter === p ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80 text-muted-foreground"}`}>
                {p ? p.charAt(0).toUpperCase() + p.slice(1) : "All"}
              </button>
            ))}
          </div>
          <p className="text-[11px] text-muted-foreground">{filtered.length.toLocaleString()} records</p>
        </CardContent>
      </Card>

      {/* Grants table */}
      <Card>
        <CardContent className="pt-0 pb-3">
          <div className="overflow-x-auto rounded-md border border-border mt-3">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Level", "Object", "Catalog", "Principal", "Privileges", "Inherited From"].map(h => (
                    <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visible.map((row, i) => (
                  <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                    <td className="py-2 px-3 whitespace-nowrap">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${LVL_CLS[row.level] ?? LVL_CLS.TABLE}`}>{row.level}</span>
                    </td>
                    <td className="py-2 px-3 max-w-[220px] truncate font-mono text-[10px] text-muted-foreground" title={row.full_name}>{row.full_name}</td>
                    <td className="py-2 px-3 text-muted-foreground whitespace-nowrap">{row.catalog}</td>
                    <td className="py-2 px-3 max-w-[200px] truncate font-medium">{row.principal}</td>
                    <td className="py-2 px-3">
                      <div className="flex flex-wrap gap-1">
                        {row.privileges.map((p: string) => (
                          <span key={p} className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${DANGEROUS.has(p) ? "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300" : privCategory(p) === "write" ? "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300" : "bg-muted text-muted-foreground"}`}>
                            {p}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td className="py-2 px-3 text-muted-foreground whitespace-nowrap">
                      {row.inherited_from
                        ? <span className="font-mono text-[10px]">→ {row.inherited_from}</span>
                        : <span className="text-muted-foreground/30 text-[11px]">Direct</span>}
                    </td>
                  </tr>
                ))}
                {visible.length === 0 && (
                  <tr><td colSpan={6} className="py-8 text-center text-muted-foreground">No grants match the current filters.</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-2 text-xs text-muted-foreground">
              <span>Page {page}/{totalPages} ({filtered.length.toLocaleString()} total)</span>
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
