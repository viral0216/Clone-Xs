// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader2, AlertCircle, ChevronDown, ChevronUp, CheckCircle2, XCircle, AlertTriangle, Minus, RefreshCw, ExternalLink } from "lucide-react";

// ---------------------------------------------------------------------------
// Generic resource table with configurable columns
// ---------------------------------------------------------------------------
export interface ColDef {
  key: string;
  label: string;
  width?: string;
  render?: (row: any) => React.ReactNode;
}

function ResourceTable({ rows, columns, emptyMsg, getLink, wsUrl }: {
  rows: any[];
  columns: ColDef[];
  emptyMsg?: string;
  getLink?: (row: any, wsUrl: string) => string | null;
  wsUrl?: string;
}) {
  const [page, setPage] = useState(1);
  const PAGE = 25;
  const total = Math.ceil(rows.length / PAGE);
  const visible = rows.slice((page - 1) * PAGE, page * PAGE);
  const hasLinks = !!getLink && !!wsUrl;

  if (rows.length === 0) return (
    <p className="text-sm text-muted-foreground py-6 text-center">{emptyMsg ?? "No resources found."}</p>
  );
  return (
    <div>
      <div className="overflow-x-auto rounded-md border border-border">
        <table className="w-full text-xs">
          <thead className="bg-muted/50">
            <tr>
              {columns.map(c => (
                <th key={c.key} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap" style={c.width ? { width: c.width } : {}}>
                  {c.label}
                </th>
              ))}
              {hasLinks && <th className="py-2 px-3 w-8" />}
            </tr>
          </thead>
          <tbody>
            {visible.map((row, i) => {
              const href = hasLinks ? getLink!(row, wsUrl!) : null;
              return (
                <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors group">
                  {columns.map(c => (
                    <td key={c.key} className="py-2 px-3 max-w-[240px] truncate">
                      {c.render ? c.render(row) : (row[c.key] ?? "—")}
                    </td>
                  ))}
                  {hasLinks && (
                    <td className="py-2 px-3 text-right">
                      {href ? (
                        <a
                          href={href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1 text-[11px] text-primary/60 hover:text-primary opacity-0 group-hover:opacity-100 transition-opacity"
                          title="Open in Databricks"
                          onClick={e => e.stopPropagation()}
                        >
                          <ExternalLink className="h-3.5 w-3.5" />
                        </a>
                      ) : null}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {total > 1 && (
        <div className="flex items-center justify-between mt-2 text-xs text-muted-foreground">
          <span>Page {page}/{total} ({rows.length} total)</span>
          <div className="flex gap-1">
            <button disabled={page === 1} onClick={() => setPage(p => p - 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">← Prev</button>
            <button disabled={page === total} onClick={() => setPage(p => p + 1)} className="px-2 py-1 rounded border border-border disabled:opacity-40 hover:bg-muted/50">Next →</button>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Status badge for security findings
// ---------------------------------------------------------------------------
const STATUS_CFG: Record<string, { icon: any; cls: string }> = {
  FAIL:           { icon: XCircle,        cls: "text-red-600" },
  WARN:           { icon: AlertTriangle,  cls: "text-yellow-500" },
  PASS:           { icon: CheckCircle2,   cls: "text-green-600" },
  NOT_APPLICABLE: { icon: Minus,          cls: "text-muted-foreground" },
};

function FindingRow({ f }: { f: any }) {
  const [open, setOpen] = useState(false);
  const cfg = STATUS_CFG[f.status] ?? STATUS_CFG.NOT_APPLICABLE;
  const Icon = cfg.icon;
  const SEV_CLS: Record<string, string> = {
    critical: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    high:     "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300",
    medium:   "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    low:      "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  };
  return (
    <>
      <tr className="border-t border-border hover:bg-muted/30 cursor-pointer transition-colors" onClick={() => setOpen(v => !v)}>
        <td className="py-2 px-3 font-mono text-[11px] text-muted-foreground whitespace-nowrap">{f.check_id}</td>
        <td className="py-2 px-3 text-xs font-medium max-w-[260px] truncate">{f.title}</td>
        <td className="py-2 px-3">
          {f.severity && <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium capitalize ${SEV_CLS[f.severity?.toLowerCase()] ?? "bg-muted text-muted-foreground"}`}>{f.severity}</span>}
        </td>
        <td className="py-2 px-3">
          <span className={`flex items-center gap-1 text-xs font-medium ${cfg.cls}`}><Icon className="h-3.5 w-3.5" />{f.status}</span>
        </td>
        <td className="py-2 px-3 text-muted-foreground">{open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}</td>
      </tr>
      {open && (
        <tr className="border-t border-border bg-muted/20">
          <td colSpan={5} className="px-4 py-3 space-y-2 text-xs">
            {f.current_state && (
              <div>
                <p className="text-[11px] text-muted-foreground uppercase tracking-wide font-medium mb-0.5">Current State</p>
                <p className="leading-relaxed">{typeof f.current_state === "string" ? f.current_state : JSON.stringify(f.current_state, null, 2)}</p>
              </div>
            )}
            {f.recommendation && (
              <div className="pt-2 border-t border-border/50">
                <p className="text-[11px] text-muted-foreground uppercase tracking-wide font-medium mb-0.5">Recommendation</p>
                <p className="text-muted-foreground leading-relaxed">{f.recommendation}</p>
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Public interface — each sub-page passes its own resource sections
// ---------------------------------------------------------------------------
export interface ResourceSection {
  title: string;
  resourceType: string;   // key in workspace_resources.json
  columns: ColDef[];
  emptyMsg?: string;
  transform?: (rows: any[]) => any[];
  getLink?: (row: any, wsUrl: string) => string | null;
}

export interface StatCard {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
}

export interface WorkspaceCategoryPageProps {
  title: string;
  description: string;
  icon: React.ComponentType<any>;
  breadcrumb: string;
  findingCategories: string[];
  sections: ResourceSection[];
  statCards?: StatCard[];
}

export default function WorkspaceCategoryPage({
  title,
  description,
  icon: Icon,
  breadcrumb,
  findingCategories,
  sections,
  statCards,
}: WorkspaceCategoryPageProps) {
  const [resources, setResources] = useState<Record<string, any[]>>({});
  const [findings, setFindings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [noData, setNoData] = useState(false);
  const [statusFilter, setStatusFilter] = useState("");
  const [showFindings, setShowFindings] = useState(false);
  const [collecting, setCollecting] = useState(false);
  const [collectToken, setCollectToken] = useState("");
  const [wsUrl, setWsUrl] = useState("");

  async function loadData() {
    const catParam = findingCategories.map(c => encodeURIComponent(c)).join(",");
    const [ws, f, meta] = await Promise.all([
      api.get("/assessment/workspace-resources").catch(() => {
        setNoData(true);
        return null;
      }),
      api.get(`/assessment/findings?category=${catParam}`).catch(() => []),
      api.get("/assessment/latest").catch(() => null),
    ]);
    if (meta?.workspace_url) setWsUrl(meta.workspace_url.replace(/\/$/, ""));
    if (ws) {
      setNoData(false);
      const loaded: Record<string, any[]> = {};
      for (const sec of sections) {
        const raw = ws[sec.resourceType] ?? [];
        loaded[sec.resourceType] = sec.transform ? sec.transform(raw) : raw;
      }
      setResources(loaded);
    }
    setFindings(Array.isArray(f) ? f : []);
    setLoading(false);
  }

  useEffect(() => { loadData(); }, []);

  async function handleCollect() {
    if (!collectToken.trim()) return;
    setCollecting(true);
    try {
      // Read the workspace URL from the latest scan's meta
      const meta = await api.get("/assessment/latest").catch(() => null);
      const host = meta?.workspace_url ?? "";
      await fetch("/api/assessment/collect-resources", {
        method: "POST",
        headers: {
          "X-Databricks-Host": host,
          "X-Databricks-Token": collectToken.trim(),
        },
      });
      setCollectToken("");
      await loadData();
    } catch (e) {
      // Silently fail — noData stays true
    }
    setCollecting(false);
  }

  const counts = { PASS: 0, FAIL: 0, WARN: 0, NOT_APPLICABLE: 0 };
  findings.forEach(f => { counts[f.status] = (counts[f.status] || 0) + 1; });
  const visibleFindings = statusFilter ? findings.filter(f => f.status === statusFilter) : findings;
  const sortedFindings = [...visibleFindings].sort((a, b) => {
    const ord = { FAIL: 0, WARN: 1, PASS: 2, NOT_APPLICABLE: 3 };
    return (ord[a.status] ?? 4) - (ord[b.status] ?? 4);
  });

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title={title} icon={Icon} breadcrumbs={["Assessment", "UC Inventory", breadcrumb]} description={description} />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading {title.toLowerCase()}…</span>
      </div>
    </div>
  );

  return (
    <div className="space-y-4">
      <PageHeader title={title} icon={Icon} breadcrumbs={["Assessment", "UC Inventory", breadcrumb]} description={description} />

      {/* No resource data yet — offer quick collect with token */}
      {noData && (
        <Card className="border-yellow-500/30 bg-yellow-50/30 dark:bg-yellow-900/10">
          <CardContent className="py-4 space-y-3">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-4 w-4 text-yellow-600 mt-0.5 shrink-0" />
              <div className="text-sm">
                <p className="font-medium text-yellow-800 dark:text-yellow-300">Resource inventory not yet collected</p>
                <p className="text-yellow-700 dark:text-yellow-400 text-xs mt-0.5">
                  Provide your Databricks token to collect live resource lists without a full re-scan.
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2 pl-7">
              <input
                type="password"
                placeholder="Databricks PAT token (dapi…)"
                value={collectToken}
                onChange={e => setCollectToken(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleCollect()}
                className="flex-1 text-xs border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-primary max-w-xs"
              />
              <button
                onClick={handleCollect}
                disabled={collecting || !collectToken.trim()}
                className="text-xs px-3 py-1.5 bg-yellow-600 text-white rounded-md hover:bg-yellow-700 disabled:opacity-50 transition-colors flex items-center gap-1.5"
              >
                {collecting ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                {collecting ? "Collecting…" : "Collect Now"}
              </button>
              <Link to="/assessment/run" className="text-xs text-muted-foreground hover:underline">or re-run scan</Link>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Stat cards */}
      {statCards && statCards.length > 0 && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {statCards.map(({ label, value, sub, color }) => (
            <Card key={label}>
              <CardContent className="pt-3 pb-3 text-center">
                <p className="text-2xl font-bold" style={{ color: color || undefined }}>{value}</p>
                {sub && <p className="text-[11px] text-muted-foreground">{sub}</p>}
                <p className="text-xs text-muted-foreground mt-0.5">{label}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Resource sections */}
      {!noData && sections.map(sec => (
        <Card key={sec.resourceType}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              {sec.title}
              <span className="text-xs font-normal text-muted-foreground">
                {(resources[sec.resourceType] ?? []).length} found
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ResourceTable
              rows={resources[sec.resourceType] ?? []}
              columns={sec.columns}
              emptyMsg={sec.emptyMsg}
              getLink={sec.getLink}
              wsUrl={wsUrl}
            />
          </CardContent>
        </Card>
      ))}

      {/* Security findings (collapsible) */}
      {findings.length > 0 && (
        <Card>
          <CardHeader className="pb-2 cursor-pointer" onClick={() => setShowFindings(v => !v)}>
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                Security Posture
                <span className={`text-xs font-medium ${counts.FAIL > 0 ? "text-red-600" : "text-green-600"}`}>
                  {counts.FAIL > 0 ? `${counts.FAIL} issue${counts.FAIL !== 1 ? "s" : ""}` : "All clear"}
                </span>
              </CardTitle>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span>{findings.length} checks</span>
                {showFindings ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
              </div>
            </div>
          </CardHeader>
          {showFindings && (
            <CardContent>
              <div className="flex gap-1 flex-wrap mb-3">
                {(["", "FAIL", "WARN", "PASS", "NOT_APPLICABLE"] as const).map(s => (
                  <button key={s} onClick={() => setStatusFilter(s)} className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${statusFilter === s ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80 text-muted-foreground"}`}>
                    {s === "" ? `All (${findings.length})` : `${s} (${counts[s] || 0})`}
                  </button>
                ))}
              </div>
              <div className="overflow-x-auto rounded-md border border-border">
                <table className="w-full text-xs">
                  <thead className="bg-muted/50">
                    <tr>
                      {["Check ID", "Title", "Severity", "Status", ""].map(h => (
                        <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedFindings.map((f, i) => <FindingRow key={i} f={f} />)}
                  </tbody>
                </table>
              </div>
            </CardContent>
          )}
        </Card>
      )}
    </div>
  );
}
