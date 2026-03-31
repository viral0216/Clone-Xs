// @ts-nocheck
import { useState, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  AlertTriangle, Loader2, XCircle, Network, Eye, FunctionSquare, Database,
  Briefcase, Search, LayoutDashboard, Copy, ChevronDown, ChevronUp,
  Download, Sparkles, X,
} from "lucide-react";
import { toast } from "sonner";

function riskBadge(level: string) {
  const l = level?.toUpperCase();
  if (l === "HIGH") return <Badge variant="destructive" className="text-xs font-semibold">{level}</Badge>;
  if (l === "MEDIUM") return <Badge className="bg-yellow-600 text-white text-xs font-semibold">{level}</Badge>;
  if (l === "LOW") return <Badge className="bg-emerald-600 text-white text-xs font-semibold">{level}</Badge>;
  return <Badge variant="outline" className="text-xs">{level || "unknown"}</Badge>;
}

function fqn(item: any, nameKey: string) {
  const parts = [item.catalog, item.schema, item[nameKey]].filter(Boolean);
  return parts.join(".");
}

function copyText(text: string) {
  navigator.clipboard.writeText(text);
  toast.success("Copied to clipboard");
}

// ── Results Table ────────────────────────────────────────────────────────────

function ImpactTable({ items, columns, nameKey, sectionId }: {
  items: any[];
  columns: { key: string; label: string }[];
  nameKey: string;
  sectionId: string;
}) {
  const [expanded, setExpanded] = useState(true);
  const [search, setSearch] = useState("");
  const [groupBy, setGroupBy] = useState<string | null>(null);

  const filtered = items.filter(item => {
    if (!search) return true;
    const s = search.toLowerCase();
    return columns.some(c => String(item[c.key] || "").toLowerCase().includes(s));
  });

  // Group support for catalog/schema columns
  const groupableKeys = columns.filter(c => c.key === "catalog" || c.key === "schema").map(c => c.key);
  const groups = groupBy
    ? filtered.reduce<Record<string, any[]>>((acc, item) => {
        const key = item[groupBy] || "(none)";
        (acc[key] ??= []).push(item);
        return acc;
      }, {})
    : null;

  function exportCsv() {
    const header = columns.map(c => c.label).join(",");
    const rows = items.map(item => columns.map(c => `"${String(item[c.key] || "").replace(/"/g, '""')}"`).join(","));
    const blob = new Blob([[header, ...rows].join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `${sectionId}.csv`; a.click();
    URL.revokeObjectURL(url);
  }

  function renderRows(rows: any[], startIdx = 0) {
    return rows.map((item, i) => (
      <tr key={startIdx + i} className="border-b border-border/50 hover:bg-muted/10 transition-colors group">
        <td className="px-3 py-2 text-xs text-muted-foreground w-8">{startIdx + i + 1}</td>
        {columns.map(c => (
          <td key={c.key} className="px-3 py-2 font-mono text-xs">
            {c.key === nameKey ? (
              <span className="text-[#E8453C] font-medium">{item[c.key] || "-"}</span>
            ) : (
              <span className="text-foreground">{item[c.key] || "-"}</span>
            )}
          </td>
        ))}
        <td className="px-2 py-2 w-8">
          {item[nameKey] && (
            <button
              onClick={() => copyText(fqn(item, nameKey))}
              className="p-1 text-muted-foreground hover:text-foreground rounded opacity-0 group-hover:opacity-100 transition-opacity"
              title="Copy FQN"
            >
              <Copy className="h-3 w-3" />
            </button>
          )}
        </td>
      </tr>
    ));
  }

  return (
    <div id={sectionId}>
      {/* Toolbar */}
      <div className="flex items-center justify-between mb-3 gap-2">
        <div className="flex items-center gap-2">
          <button onClick={() => setExpanded(!expanded)} className="text-muted-foreground hover:text-foreground">
            {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </button>
          <Badge variant="outline" className="text-xs tabular-nums">{filtered.length}{search && items.length !== filtered.length ? ` / ${items.length}` : ""}</Badge>
        </div>
        {expanded && (
          <div className="flex items-center gap-2">
            {groupableKeys.length > 0 && (
              <select
                value={groupBy || ""}
                onChange={e => setGroupBy(e.target.value || null)}
                className="text-xs bg-background border border-border rounded-md px-2 py-1 text-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]/50"
              >
                <option value="">No grouping</option>
                {groupableKeys.map(k => <option key={k} value={k}>Group by {k}</option>)}
              </select>
            )}
            {items.length > 5 && (
              <div className="relative">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Filter..."
                  value={search}
                  onChange={e => setSearch(e.target.value)}
                  className="pl-7 pr-3 py-1 text-xs bg-background border border-border rounded-md w-44 focus:outline-none focus:ring-1 focus:ring-[#E8453C]/50"
                />
              </div>
            )}
            <button onClick={exportCsv} className="p-1 text-muted-foreground hover:text-foreground rounded" title="Export CSV">
              <Download className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
      </div>

      {expanded && (
        <div className="border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-muted/30 border-b border-border">
                <th className="text-left px-3 py-2 text-xs font-semibold text-muted-foreground w-8">#</th>
                {columns.map(c => (
                  <th key={c.key} className="text-left px-3 py-2 text-xs font-semibold text-muted-foreground">{c.label}</th>
                ))}
                <th className="w-8" />
              </tr>
            </thead>
            <tbody>
              {groups ? (
                Object.entries(groups).sort(([a], [b]) => a.localeCompare(b)).map(([group, rows]) => {
                  const startIdx = filtered.indexOf(rows[0]);
                  return (
                    <Fragment key={group}>
                      <tr className="bg-muted/15">
                        <td colSpan={columns.length + 2} className="px-3 py-1.5 text-xs font-semibold text-muted-foreground">
                          {group} <span className="font-normal text-muted-foreground/60">({rows.length})</span>
                        </td>
                      </tr>
                      {renderRows(rows, startIdx)}
                    </Fragment>
                  );
                })
              ) : (
                renderRows(filtered)
              )}
              {filtered.length === 0 && (
                <tr><td colSpan={columns.length + 2} className="px-4 py-6 text-center text-muted-foreground text-xs">No matches</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── AI Insight Renderer ──────────────────────────────────────────────────────

function AiInsightPanel({ insight, onDismiss }: { insight: string; onDismiss: () => void }) {
  // Simple markdown-like rendering
  const lines = insight.split("\n").map((line, i) => {
    const t = line.trim();
    if (!t) return null;
    if (t.startsWith("## ")) return <h3 key={i} className="font-semibold text-[#E8453C] text-xs mt-2 mb-1">{t.slice(3)}</h3>;
    if (t.startsWith("# ")) return <h3 key={i} className="font-semibold text-[#E8453C] text-xs mt-2 mb-1">{t.slice(2)}</h3>;
    if (t.startsWith("- ") || t.startsWith("* ")) return <li key={i} className="text-xs ml-4 list-disc text-foreground leading-relaxed">{t.slice(2)}</li>;
    const html = t.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    return <p key={i} className="text-xs text-foreground leading-relaxed" dangerouslySetInnerHTML={{ __html: html }} />;
  });

  return (
    <div className="p-3 rounded-lg bg-[#E8453C]/5 border border-[#E8453C]/20">
      <div className="flex items-center justify-between mb-2">
        <span className="flex items-center gap-1.5 font-semibold text-[#E8453C] text-[11px]">
          <Sparkles className="h-3.5 w-3.5" /> AI Impact Analysis
        </span>
        <button onClick={onDismiss} className="text-muted-foreground hover:text-foreground text-[10px]">Dismiss</button>
      </div>
      <div className="space-y-0.5">{lines}</div>
    </div>
  );
}

// ── Main Page ────────────────────────────────────────────────────────────────

import { Fragment } from "react";

export default function ImpactPage() {
  const { job, run, isRunning } = usePageJob("impact");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [schema, setSchema] = useState(job?.params?.schema || "");
  const [table, setTable] = useState(job?.params?.table || "");
  const [aiInsight, setAiInsight] = useState<string | null>(null);
  const [aiLoading, setAiLoading] = useState(false);

  const results = job?.data as any;
  const views = results?.affected_views || [];
  const functions = results?.affected_functions || [];
  const downstream = results?.downstream_tables || [];
  const jobs = results?.referencing_jobs || [];
  const queries = results?.active_queries || [];
  const dashboards = results?.dashboard_references || [];

  const sections = [
    {
      id: "affected-views",
      title: "Affected Views",
      icon: Eye,
      items: views,
      nameKey: "view",
      columns: [
        { key: "catalog", label: "Catalog" },
        { key: "schema", label: "Schema" },
        { key: "view", label: "View" },
      ],
    },
    {
      id: "affected-functions",
      title: "Affected Functions",
      icon: FunctionSquare,
      items: functions,
      nameKey: "function",
      columns: [
        { key: "catalog", label: "Catalog" },
        { key: "schema", label: "Schema" },
        { key: "function", label: "Function" },
      ],
    },
    {
      id: "downstream-tables",
      title: "Downstream Tables",
      icon: Database,
      items: downstream,
      nameKey: "table",
      columns: [
        { key: "catalog", label: "Catalog" },
        { key: "schema", label: "Schema" },
        { key: "table", label: "Table" },
      ],
    },
    {
      id: "referencing-jobs",
      title: "Referencing Jobs",
      icon: Briefcase,
      items: jobs,
      nameKey: "job_name",
      columns: [
        { key: "job_id", label: "Job ID" },
        { key: "job_name", label: "Job Name" },
        { key: "task", label: "Task" },
      ],
    },
    {
      id: "active-queries",
      title: "Active Queries",
      icon: Search,
      items: queries,
      nameKey: "query_id",
      columns: [
        { key: "query_id", label: "Query ID" },
        { key: "executed_by", label: "User" },
        { key: "start_time", label: "Started" },
      ],
    },
    {
      id: "dashboard-references",
      title: "Dashboard References",
      icon: LayoutDashboard,
      items: dashboards,
      nameKey: "name",
      columns: [
        { key: "dashboard_id", label: "Dashboard ID" },
        { key: "name", label: "Name" },
      ],
    },
  ];

  // Only show sections that have data, except for stat cards which always show top 3
  const nonEmptySections = sections.filter(s => s.items.length > 0);
  const statCards = sections.slice(0, 3);

  function scrollToSection(id: string) {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  async function summarizeWithAI() {
    if (aiInsight) { setAiInsight(null); return; }
    setAiLoading(true);
    try {
      const res = await api.post("/ai/summarize", {
        context_type: "report",
        data: {
          type: "impact_analysis",
          catalog,
          schema: schema || "all",
          table: table || "all tables",
          risk_level: results.risk_level,
          total_dependent_objects: results.total_dependent_objects,
          affected_views_count: views.length,
          affected_views_sample: views.slice(0, 10).map((v: any) => `${v.catalog}.${v.schema}.${v.view}`),
          affected_functions_count: functions.length,
          referencing_jobs_count: jobs.length,
          referencing_jobs_sample: jobs.slice(0, 5).map((j: any) => j.job_name),
          active_queries_count: queries.length,
          dashboard_references_count: dashboards.length,
          instruction: "Analyze this impact report for a Databricks Unity Catalog. Provide:\n## Risk Assessment\nBrief assessment of the blast radius and severity.\n## Key Concerns\n- Top 2-3 specific concerns based on the data.\n## Recommended Actions\n- 2-3 concrete steps before making schema changes.\nBe concise and specific to the data provided.",
        },
      });
      setAiInsight(res.summary || "No insights available");
    } catch {
      toast.error("AI analysis unavailable");
    }
    setAiLoading(false);
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Impact Analysis"
        icon={AlertTriangle}
        description="Assess the blast radius of schema changes — shows which views, functions, and downstream consumers would be affected before you modify a table."
        breadcrumbs={["Discovery", "Impact Analysis"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage"
        docsLabel="Unity Catalog lineage"
      />

      {/* Catalog Picker */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table={table}
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
              onTableChange={setTable}
            />
            <Button onClick={() => { setAiInsight(null); run({ catalog, schema, table }, () => api.post("/impact", { catalog, schema: schema || undefined, table: table || undefined })); }} disabled={!catalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Network className="h-4 w-4 mr-2" />}
              {isRunning ? "Analyzing..." : "Analyze Impact"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Empty state */}
      {!results && !isRunning && job?.status !== "error" && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center text-muted-foreground py-12">
            <Network className="h-10 w-10 mx-auto mb-3 opacity-40" />
            <p>Select a catalog and click Analyze Impact to assess the blast radius</p>
          </CardContent>
        </Card>
      )}

      {/* Risk level + AI insight */}
      {results?.risk_level && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <AlertTriangle className="h-6 w-6 text-muted-foreground" />
                <span className="font-semibold text-foreground">Overall Risk Level</span>
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={summarizeWithAI}
                  disabled={aiLoading}
                  className="flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded-md bg-[#E8453C]/10 text-[#E8453C] hover:bg-[#E8453C]/20 transition-colors disabled:opacity-50"
                >
                  {aiLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Sparkles className="h-3 w-3" />}
                  {aiInsight ? "Hide" : "Explain with AI"}
                </button>
                {results.total_dependent_objects != null && (
                  <span className="text-sm text-muted-foreground">{results.total_dependent_objects} dependent objects</span>
                )}
                {riskBadge(results.risk_level)}
              </div>
            </div>
            {aiInsight && <AiInsightPanel insight={aiInsight} onDismiss={() => setAiInsight(null)} />}
          </CardContent>
        </Card>
      )}

      {/* KPI stat cards — clickable to scroll */}
      {results && (
        <div className="grid grid-cols-3 gap-4">
          {statCards.map(s => {
            const hasItems = s.items.length > 0;
            return (
              <Card
                key={s.id}
                className={`bg-card border-border transition-colors ${hasItems ? "cursor-pointer hover:bg-accent/50" : ""}`}
                onClick={hasItems ? () => scrollToSection(s.id) : undefined}
              >
                <CardContent className="pt-6 text-center">
                  <s.icon className={`h-5 w-5 mx-auto mb-1 ${hasItems ? "text-[#E8453C]" : "text-muted-foreground"}`} />
                  <p className={`text-2xl font-bold ${hasItems ? "text-foreground" : "text-muted-foreground"}`}>{s.items.length}</p>
                  <p className="text-xs text-muted-foreground mt-1">{s.title}</p>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}

      {/* Additional stat row for jobs/queries/dashboards if any have data */}
      {results && (jobs.length > 0 || queries.length > 0 || dashboards.length > 0) && (
        <div className="grid grid-cols-3 gap-4">
          {sections.slice(3).map(s => (
            <Card
              key={s.id}
              className={`bg-card border-border transition-colors ${s.items.length > 0 ? "cursor-pointer hover:bg-accent/50" : ""}`}
              onClick={s.items.length > 0 ? () => scrollToSection(s.id) : undefined}
            >
              <CardContent className="pt-6 text-center">
                <s.icon className={`h-5 w-5 mx-auto mb-1 ${s.items.length > 0 ? "text-[#E8453C]" : "text-muted-foreground"}`} />
                <p className={`text-2xl font-bold ${s.items.length > 0 ? "text-foreground" : "text-muted-foreground"}`}>{s.items.length}</p>
                <p className="text-xs text-muted-foreground mt-1">{s.title}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Detail sections */}
      {nonEmptySections.map(section => (
        <Card key={section.id} className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              <section.icon className="h-4 w-4 text-muted-foreground" />
              {section.title}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ImpactTable items={section.items} columns={section.columns} nameKey={section.nameKey} sectionId={section.id} />
          </CardContent>
        </Card>
      ))}

      {/* Error state */}
      {job?.status === "error" && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
