// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { FunctionSquare, Database, Loader2 } from "lucide-react";

const PAGE_SIZE = 50;

const LANG_COLORS = {
  PYTHON: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
  SQL:    "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
  SCALA:  "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300",
  R:      "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300",
};

function LangBadge({ lang }) {
  return (
    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${LANG_COLORS[lang] ?? "bg-muted text-muted-foreground"}`}>
      {lang ?? "SQL"}
    </span>
  );
}

function paramSummary(params) {
  if (!Array.isArray(params) || params.length === 0) return <span className="text-muted-foreground/50">none</span>;
  const names = params.slice(0, 3).map(p => p.name ?? p).join(", ");
  const extra = params.length > 3 ? ` +${params.length - 3}` : "";
  return <span className="text-muted-foreground font-mono text-[10px]">{names}{extra}</span>;
}

export default function FunctionsPage() {
  const [inv, setInv] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [catalogFilter, setCatalogFilter] = useState("");
  const [langFilter, setLangFilter] = useState("");
  const [page, setPage] = useState(1);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(r => setInv(r))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const catalogs = useMemo(() => (inv?.catalogs ?? []).map(c => c.name), [inv]);

  const functions = useMemo(() => {
    if (!inv) return [];
    const rows = [];
    for (const cat of inv.catalogs ?? []) {
      for (const sch of cat.schemas ?? []) {
        for (const fn of sch.functions ?? []) {
          const name = typeof fn === "string" ? fn : (fn.name ?? "");
          if (!name) continue;
          rows.push({
            name,
            catalog: cat.name,
            schema: sch.name,
            full_name: (typeof fn === "string" ? null : fn.full_name) ?? `${cat.name}.${sch.name}.${name}`,
            owner: fn.owner ?? "",
            comment: fn.comment ?? "",
            language: fn.language ?? "SQL",
            input_params: fn.input_params ?? [],
            return_type: fn.return_type ?? fn.return_type_text ?? "",
          });
        }
      }
    }
    return rows;
  }, [inv]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return functions.filter(f =>
      (!catalogFilter || f.catalog === catalogFilter) &&
      (!langFilter || f.language === langFilter) &&
      (!q || f.name.toLowerCase().includes(q) || f.owner.toLowerCase().includes(q) || f.full_name.toLowerCase().includes(q))
    );
  }, [functions, search, catalogFilter, langFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function resetPage() { setPage(1); }

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Functions (UDFs)" icon={FunctionSquare} breadcrumbs={["Assessment", "UC Inventory", "Functions"]} description="All registered Unity Catalog user-defined functions — Python, SQL, and Scala UDFs across every schema." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading functions…</span>
      </div>
    </div>
  );

  if (!inv || functions.length === 0) return (
    <div className="space-y-4">
      <PageHeader title="Functions (UDFs)" icon={FunctionSquare} breadcrumbs={["Assessment", "UC Inventory", "Functions"]} description="All registered Unity Catalog user-defined functions — Python, SQL, and Scala UDFs across every schema." />
      <Card>
        <CardContent className="py-12 text-center">
          <Database className="h-10 w-10 mx-auto mb-3 opacity-20" />
          <p className="text-sm font-medium">No functions found</p>
          <p className="text-xs text-muted-foreground mt-1">Run a scan with UC Inventory enabled, or no UDFs exist in this workspace.</p>
        </CardContent>
      </Card>
    </div>
  );

  const byLang = (l) => functions.filter(f => f.language === l).length;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Functions (UDFs)"
        icon={FunctionSquare}
        breadcrumbs={["Assessment", "UC Inventory", "Functions"]}
        description="All registered Unity Catalog user-defined functions — Python, SQL, and Scala UDFs across every schema."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { label: "Total Functions", value: functions.length, color: undefined },
          { label: "Python", value: byLang("PYTHON"), color: "#3b82f6" },
          { label: "SQL", value: byLang("SQL"), color: "#64748b" },
          { label: "Scala", value: byLang("SCALA"), color: "#22c55e" },
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
          placeholder="Search functions…"
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
        <div className="flex rounded-md border border-border overflow-hidden">
          {["", "PYTHON", "SQL", "SCALA", "R"].map(l => (
            <button
              key={l}
              onClick={() => { setLangFilter(l); resetPage(); }}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${langFilter === l ? "bg-primary text-primary-foreground" : "bg-background text-muted-foreground hover:bg-muted/50"}`}
            >
              {l || "All"}
            </button>
          ))}
        </div>
        {(search || catalogFilter || langFilter) && (
          <button onClick={() => { setSearch(""); setCatalogFilter(""); setLangFilter(""); setPage(1); }} className="text-xs text-muted-foreground hover:text-foreground">Clear</button>
        )}
        <span className="text-xs text-muted-foreground ml-auto">{filtered.length} functions</span>
      </div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Name", "Catalog", "Schema", "Language", "Parameters", "Return Type", "Owner", "Comment"].map(h => (
                    <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visible.map((fn, i) => (
                  <tr key={fn.full_name + i} className="border-t border-border hover:bg-muted/30 transition-colors">
                    <td className="py-2 px-3 font-medium font-mono text-[11px]">{fn.name}</td>
                    <td className="py-2 px-3 text-muted-foreground">{fn.catalog}</td>
                    <td className="py-2 px-3 text-muted-foreground">{fn.schema}</td>
                    <td className="py-2 px-3"><LangBadge lang={fn.language} /></td>
                    <td className="py-2 px-3">{paramSummary(fn.input_params)}</td>
                    <td className="py-2 px-3 text-muted-foreground font-mono text-[10px]">{fn.return_type || "—"}</td>
                    <td className="py-2 px-3 text-muted-foreground">{fn.owner || "—"}</td>
                    <td className="py-2 px-3 text-muted-foreground max-w-[160px] truncate">{fn.comment || "—"}</td>
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
