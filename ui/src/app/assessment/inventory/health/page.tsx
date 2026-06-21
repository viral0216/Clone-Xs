// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader2, Activity, ChevronDown, ChevronUp } from "lucide-react";

const PII_PATTERNS = [
  "email", "ssn", "social_security", "phone", "mobile", "address", "zip", "postal",
  "birth", "dob", "passport", "license", "credit_card", "card_number", "cvv",
  "tax_id", "salary", "income", "gender", "ethnicity", "race", "religion",
  "ip_address", "user_agent", "cookie", "session",
];

function pct(n: number, d: number) { return d === 0 ? 0 : Math.round((n / d) * 100); }

function PctBar({ value }: { value: number }) {
  const color = value >= 80 ? "bg-green-500" : value >= 40 ? "bg-yellow-500" : "bg-red-500";
  const text  = value >= 80 ? "text-green-600" : value >= 40 ? "text-yellow-600" : "text-red-600";
  return (
    <div className="flex items-center gap-2 min-w-[90px]">
      <div className="flex-1 h-1.5 bg-muted rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${value}%` }} />
      </div>
      <span className={`text-[11px] font-medium w-8 text-right shrink-0 ${text}`}>{value}%</span>
    </div>
  );
}

function ScoreCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color: string }) {
  return (
    <Card><CardContent className="pt-3 pb-3 text-center">
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
      {sub && <p className="text-[11px] text-muted-foreground">{sub}</p>}
      <p className="text-xs text-muted-foreground mt-0.5">{label}</p>
    </CardContent></Card>
  );
}

function pctColor(v: number) {
  return v >= 80 ? "text-green-600" : v >= 40 ? "text-yellow-600" : "text-red-600";
}

function computeHealth(inv: any) {
  let totalTables = 0, totalOwner = 0, totalDesc = 0, totalTags = 0;
  let totalCols = 0, totalColDesc = 0;
  const perCatalog: any[] = [];
  const abandoned: any[] = [];

  for (const cat of inv.catalogs ?? []) {
    let cTables = 0, cOwner = 0, cDesc = 0, cTags = 0, cCols = 0, cColDesc = 0;
    for (const schema of cat.schemas ?? []) {
      for (const table of schema.tables ?? []) {
        cTables++;
        const hasOwner   = !!table.owner;
        const hasComment = !!table.comment;
        const hasTags    = table.tags && Object.keys(table.tags).length > 0;
        if (hasOwner)   cOwner++;
        if (hasComment) cDesc++;
        if (hasTags)    cTags++;
        if (!hasOwner && !hasComment && !hasTags) {
          abandoned.push({ full_name: table.full_name, catalog: cat.name, updated_at: table.updated_at });
        }
        for (const col of table.columns ?? []) {
          cCols++;
          if (col.comment) cColDesc++;
        }
      }
    }
    totalTables += cTables; totalOwner += cOwner; totalDesc += cDesc;
    totalTags += cTags; totalCols += cCols; totalColDesc += cColDesc;
    perCatalog.push({
      name: cat.name, tables: cTables,
      ownerPct: pct(cOwner, cTables), descPct: pct(cDesc, cTables),
      tagPct: pct(cTags, cTables), cols: cCols, colDocPct: pct(cColDesc, cCols),
    });
  }

  return {
    perCatalog, abandoned,
    summary: {
      ownerPct:  pct(totalOwner, totalTables),
      descPct:   pct(totalDesc, totalTables),
      tagPct:    pct(totalTags, totalTables),
      colDocPct: pct(totalColDesc, totalCols),
      totalTables, totalCols,
    },
  };
}

function detectSensitive(inv: any) {
  const found: any[] = [];
  for (const cat of inv.catalogs ?? []) {
    for (const schema of cat.schemas ?? []) {
      for (const table of schema.tables ?? []) {
        for (const col of table.columns ?? []) {
          const lower = col.name.toLowerCase();
          const matched = PII_PATTERNS.find(p => lower.includes(p));
          if (matched) {
            found.push({
              col_name: col.name,
              table_full_name: table.full_name,
              catalog: cat.name,
              type_text: col.type_text ?? col.type_name ?? "—",
              matched_pattern: matched,
              masked: col.mask != null,
            });
          }
        }
      }
    }
  }
  return found;
}

type SortKey = "name" | "tables" | "ownerPct" | "descPct" | "colDocPct" | "tagPct";
const SENS_PAGE = 50;

export default function HealthPage() {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [showAbandoned, setShowAbandoned] = useState(false);
  const [sensFilter, setSensFilter] = useState<"" | "masked" | "unmasked">("");
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortAsc, setSortAsc] = useState(true);
  const [sensPage, setSensPage] = useState(1);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => { setInv(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const health    = useMemo(() => (inv ? computeHealth(inv)   : null), [inv]);
  const sensitive = useMemo(() => (inv ? detectSensitive(inv) : []),   [inv]);

  const sortedCatalogs = useMemo(() => {
    if (!health) return [];
    return [...health.perCatalog].sort((a, b) => {
      const av = a[sortKey], bv = b[sortKey];
      if (typeof av === "string") return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
      return sortAsc ? (av - bv) : (bv - av);
    });
  }, [health, sortKey, sortAsc]);

  function toggleSort(k: SortKey) {
    if (sortKey === k) setSortAsc(v => !v);
    else { setSortKey(k); setSortAsc(true); }
  }

  function SortTh({ k, label }: { k: SortKey; label: string }) {
    return (
      <th
        className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap cursor-pointer select-none hover:text-foreground"
        onClick={() => toggleSort(k)}
      >
        {label}{sortKey === k ? (sortAsc ? " ↑" : " ↓") : ""}
      </th>
    );
  }

  const filteredSens = useMemo(() => {
    if (sensFilter === "masked")   return sensitive.filter(r => r.masked);
    if (sensFilter === "unmasked") return sensitive.filter(r => !r.masked);
    return sensitive;
  }, [sensitive, sensFilter]);

  const sensTotalPages = Math.ceil(filteredSens.length / SENS_PAGE);
  const sensVisible    = filteredSens.slice((sensPage - 1) * SENS_PAGE, sensPage * SENS_PAGE);
  const unmaskedCount  = useMemo(() => sensitive.filter(r => !r.masked).length, [sensitive]);

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="Metadata Health" icon={Activity} breadcrumbs={["Assessment", "UC Inventory", "Metadata Health"]} description="Documentation coverage and sensitive column detection." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Analyzing inventory…</span>
      </div>
    </div>
  );

  const s = health?.summary ?? { ownerPct: 0, descPct: 0, tagPct: 0, colDocPct: 0, totalTables: 0, totalCols: 0 };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Metadata Health"
        icon={Activity}
        breadcrumbs={["Assessment", "UC Inventory", "Metadata Health"]}
        description="Documentation coverage and PII-pattern sensitive column detection across all Unity Catalog tables."
      />

      {/* Summary cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <ScoreCard label="Owner Coverage"      value={`${s.ownerPct}%`}  sub={`${s.totalTables} tables`}  color={pctColor(s.ownerPct)} />
        <ScoreCard label="Description Coverage" value={`${s.descPct}%`}  sub="tables with comment"         color={pctColor(s.descPct)} />
        <ScoreCard label="Column Docs"          value={`${s.colDocPct}%`} sub={`${s.totalCols} columns`}   color={pctColor(s.colDocPct)} />
        <ScoreCard label="Tag Coverage"         value={`${s.tagPct}%`}   sub="tables with tags"            color={pctColor(s.tagPct)} />
      </div>

      {/* Per-catalog breakdown */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Coverage by Catalog</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  <SortTh k="name"      label="Catalog" />
                  <SortTh k="tables"    label="Tables" />
                  <SortTh k="ownerPct"  label="Owner %" />
                  <SortTh k="descPct"   label="Description %" />
                  <SortTh k="colDocPct" label="Col Docs %" />
                  <SortTh k="tagPct"    label="Tags %" />
                </tr>
              </thead>
              <tbody>
                {sortedCatalogs.map(cat => (
                  <tr key={cat.name} className="border-t border-border hover:bg-muted/20 transition-colors">
                    <td className="py-2 px-3 font-medium">{cat.name}</td>
                    <td className="py-2 px-3 text-muted-foreground">{cat.tables}</td>
                    <td className="py-2 px-3"><PctBar value={cat.ownerPct} /></td>
                    <td className="py-2 px-3"><PctBar value={cat.descPct} /></td>
                    <td className="py-2 px-3"><PctBar value={cat.colDocPct} /></td>
                    <td className="py-2 px-3"><PctBar value={cat.tagPct} /></td>
                  </tr>
                ))}
                {sortedCatalogs.length === 0 && (
                  <tr><td colSpan={6} className="py-8 text-center text-muted-foreground">No catalog data available.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Abandoned tables (collapsible) */}
      {(health?.abandoned?.length ?? 0) > 0 && (
        <Card>
          <CardHeader className="pb-2 cursor-pointer" onClick={() => setShowAbandoned(v => !v)}>
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                Abandoned Tables
                <span className="text-xs font-normal text-yellow-600">{health!.abandoned.length} tables with no owner, description, or tags</span>
              </CardTitle>
              {showAbandoned ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
            </div>
          </CardHeader>
          {showAbandoned && (
            <CardContent>
              <div className="overflow-x-auto rounded-md border border-border">
                <table className="w-full text-xs">
                  <thead className="bg-muted/50">
                    <tr>
                      {["Table", "Catalog", "Last Updated"].map(h => (
                        <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {health!.abandoned.slice(0, 100).map((t: any, i: number) => (
                      <tr key={i} className="border-t border-border hover:bg-muted/30">
                        <td className="py-2 px-3 font-mono text-[10px]">{t.full_name}</td>
                        <td className="py-2 px-3 text-muted-foreground">{t.catalog}</td>
                        <td className="py-2 px-3 text-muted-foreground">{t.updated_at ? new Date(t.updated_at).toLocaleDateString() : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {health!.abandoned.length > 100 && (
                <p className="text-xs text-muted-foreground mt-1 text-right">Showing 100 of {health!.abandoned.length}</p>
              )}
            </CardContent>
          )}
        </Card>
      )}

      {/* Sensitive column detection */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            Sensitive Column Detection
            {unmaskedCount > 0 && (
              <span className="text-xs font-medium text-red-600">⚠ {unmaskedCount} unmasked</span>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2 mb-3 flex-wrap">
            <p className="text-xs text-muted-foreground flex-1 min-w-[200px]">
              {sensitive.length} column{sensitive.length !== 1 ? "s" : ""} matched PII patterns
              across {new Set(sensitive.map(r => r.table_full_name)).size} tables.
              {unmaskedCount > 0
                ? <span className="text-red-600"> {unmaskedCount} have no column masking policy.</span>
                : sensitive.length > 0
                ? <span className="text-green-600"> All are masked.</span>
                : null}
            </p>
            <div className="flex gap-1 shrink-0">
              {(["", "unmasked", "masked"] as const).map(f => (
                <button key={f} onClick={() => { setSensFilter(f); setSensPage(1); }}
                  className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${sensFilter === f ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80 text-muted-foreground"}`}>
                  {f === ""         ? `All (${sensitive.length})`
                  : f === "unmasked" ? `Unmasked ⚠ (${unmaskedCount})`
                  :                   `Masked (${sensitive.length - unmaskedCount})`}
                </button>
              ))}
            </div>
          </div>

          {sensitive.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-6">No PII-pattern columns detected in this workspace.</p>
          ) : (
            <>
              <div className="overflow-x-auto rounded-md border border-border">
                <table className="w-full text-xs">
                  <thead className="bg-muted/50">
                    <tr>
                      {["Column Name", "Table", "Catalog", "Data Type", "Pattern Matched", "Masking"].map(h => (
                        <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sensVisible.map((r: any, i: number) => (
                      <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                        <td className="py-2 px-3 font-medium font-mono text-[11px]">{r.col_name}</td>
                        <td className="py-2 px-3 font-mono text-[10px] text-muted-foreground max-w-[200px] truncate" title={r.table_full_name}>{r.table_full_name}</td>
                        <td className="py-2 px-3 text-muted-foreground whitespace-nowrap">{r.catalog}</td>
                        <td className="py-2 px-3 text-muted-foreground">{r.type_text}</td>
                        <td className="py-2 px-3">
                          <span className="font-mono text-[10px] bg-muted px-1.5 py-0.5 rounded">{r.matched_pattern}</span>
                        </td>
                        <td className="py-2 px-3">
                          {r.masked
                            ? <span className="px-1.5 py-0.5 rounded text-[10px] font-medium text-green-700 bg-green-100 dark:bg-green-900/30 dark:text-green-300">Masked</span>
                            : <span className="px-1.5 py-0.5 rounded text-[10px] font-medium text-red-700 bg-red-100 dark:bg-red-900/30 dark:text-red-300">⚠ No masking</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {sensTotalPages > 1 && (
                <div className="flex items-center justify-between mt-2 text-xs text-muted-foreground">
                  <span>Page {sensPage}/{sensTotalPages} ({filteredSens.length} total)</span>
                  <div className="flex gap-1">
                    <button disabled={sensPage === 1}            onClick={() => setSensPage(p => p - 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">← Prev</button>
                    <button disabled={sensPage === sensTotalPages} onClick={() => setSensPage(p => p + 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">Next →</button>
                  </div>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
