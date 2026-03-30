// @ts-nocheck
/**
 * SqlCell — SQL editor + results table + chart for notebook cells.
 * Features: syntax highlighting, auto-viz, AI (fix/explain/generate), output collapse
 */
import { useState, useRef, useEffect } from "react";
import { api } from "@/lib/api-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Play, Loader2, Rows3, BarChart3, Copy, ChevronDown, ChevronRight, FileDown, FileJson, Database,
} from "lucide-react";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  AreaChart, Area, ScatterChart, Scatter,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { recommendVisualization, buildColumnStats } from "../auto-viz";
import { arrayToCSV, downloadFile } from "@/lib/pdf-export";
import DataProfilePanel from "../DataProfilePanel";
import { toast } from "sonner";

const _kwSet = new Set(["SELECT","FROM","WHERE","AND","OR","NOT","IN","IS","NULL","AS","JOIN","LEFT","RIGHT","INNER","OUTER","FULL","CROSS","ON","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","DISTINCT","ALL","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","ALTER","TABLE","VIEW","INDEX","DATABASE","SCHEMA","CATALOG","UNION","INTERSECT","EXCEPT","EXISTS","BETWEEN","LIKE","CASE","WHEN","THEN","ELSE","END","CAST","COALESCE","NULLIF","COUNT","SUM","AVG","MIN","MAX","ROW_NUMBER","RANK","DENSE_RANK","OVER","PARTITION","WITH","RECURSIVE","DESCRIBE","SHOW","EXPLAIN","TRUE","FALSE","ASC","DESC","TABLESAMPLE","LATERAL"]);

function highlightSQL(code: string): string {
  let html = code.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  html = html.replace(/'[^']*'/g, m => `<span style="color:#C73A32">${m}</span>`);
  html = html.replace(/\b(\d+\.?\d*)\b/g, `<span style="color:#B83028">$1</span>`);
  html = html.replace(/--.*/g, m => `<span style="color:#9CA3AF;font-style:italic">${m}</span>`);
  html = html.replace(/\b([A-Z_]+)\b/gi, (m) => {
    if (_kwSet.has(m.toUpperCase())) return `<span style="color:#E8453C;font-weight:600">${m}</span>`;
    return m;
  });
  // Highlight {{params}} in blue
  html = html.replace(/\{\{(\w+)\}\}/g, `<span style="color:#3B82F6;font-weight:600;background:#3B82F620;padding:0 2px;border-radius:2px">{{$1}}</span>`);
  return html;
}

type ColType = "integer" | "long" | "double" | "float" | "decimal" | "string" | "boolean" | "date" | "timestamp" | "binary" | "array" | "map" | "struct" | "null";
function inferColumnType(values: any[]): ColType {
  for (const v of values) {
    if (v == null) continue;
    const s = String(v);
    if (typeof v === "boolean" || s === "true" || s === "false") return "boolean";
    if (typeof v === "number") return Number.isInteger(v) ? "integer" : "double";
    if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(s)) return "timestamp";
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return "date";
    if (/^-?\d+$/.test(s) && s.length < 10) return "integer";
    if (/^-?\d+\.\d+$/.test(s)) return "double";
  }
  return "string";
}

// Simple inline markdown renderer for AI responses
function AiMarkdown({ text }: { text: string }) {
  const lines = text.split("\n");
  return (
    <div className="space-y-1">
      {lines.map((line, i) => {
        const trimmed = line.trim();
        if (!trimmed) return null;
        if (trimmed.startsWith("## ")) return <p key={i} className="font-semibold text-foreground mt-1.5 mb-0.5">{trimmed.slice(3)}</p>;
        if (trimmed.startsWith("# ")) return <p key={i} className="font-bold text-foreground mt-1.5 mb-0.5">{trimmed.slice(2)}</p>;
        if (trimmed.startsWith("- ") || trimmed.startsWith("• ")) {
          return <div key={i} className="flex gap-2 pl-1"><span className="text-[#E8453C] shrink-0 mt-0.5">•</span><span>{trimmed.slice(2)}</span></div>;
        }
        return <p key={i}>{trimmed}</p>;
      })}
    </div>
  );
}

interface Props {
  content: string;
  onChange: (content: string) => void;
  results?: any[] | null;
  error?: string | null;
  elapsed?: number | null;
  running?: boolean;
  collapsed?: boolean;
  lastRunAt?: number | null;
  cellIndex?: number;
  viewMode?: "table" | "chart" | "profile";
  chartType?: string;
  chartXCol?: string;
  chartYCol?: string;
  searchQuery?: string;
  onRun: () => void;
  onUpdateCell: (patch: Record<string, any>) => void;
  onToggleCollapse?: () => void;
}

const COLORS = ["#E8453C", "#C73A32", "#F06D55", "#B83028", "#F4897A", "#9E2620", "#D94F3C", "#E86B5C"];

export default function SqlCell({
  content, onChange, results, error, elapsed, running, collapsed, lastRunAt, cellIndex,
  viewMode = "table", chartType = "bar", chartXCol = "", chartYCol = "", searchQuery,
  onRun, onUpdateCell, onToggleCollapse,
}: Props) {
  const editorRef = useRef<HTMLTextAreaElement>(null);
  const [page, setPage] = useState(0);
  const pageSize = 25;
  const [runTimer, setRunTimer] = useState(0);
  const [relativeTime, setRelativeTime] = useState("");
  const [aiFixing, setAiFixing] = useState(false);
  const [aiFix, setAiFix] = useState<{ fixedSql: string } | null>(null);
  const [aiExplain, setAiExplain] = useState<string | null>(null);
  const [aiExplainLoading, setAiExplainLoading] = useState(false);
  const [showGenerate, setShowGenerate] = useState(false);
  const [generatePrompt, setGeneratePrompt] = useState("");
  const [generating, setGenerating] = useState(false);
  const [creatingView, setCreatingView] = useState(false);

  // Live running timer
  useEffect(() => {
    if (!running) { setRunTimer(0); return; }
    const start = Date.now();
    const iv = setInterval(() => setRunTimer(Math.floor((Date.now() - start) / 1000)), 1000);
    return () => clearInterval(iv);
  }, [running]);

  // Relative time ("ran 2m ago")
  useEffect(() => {
    if (!lastRunAt) { setRelativeTime(""); return; }
    function update() {
      const secs = Math.floor((Date.now() - (lastRunAt || 0)) / 1000);
      if (secs < 60) setRelativeTime(`${secs}s ago`);
      else if (secs < 3600) setRelativeTime(`${Math.floor(secs / 60)}m ago`);
      else setRelativeTime(`${Math.floor(secs / 3600)}h ago`);
    }
    update();
    const iv = setInterval(update, 30000);
    return () => clearInterval(iv);
  }, [lastRunAt]);

  // Search highlighting in SQL
  function highlightWithSearch(code: string): string {
    let html = highlightSQL(code);
    if (searchQuery?.trim()) {
      const escaped = searchQuery.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      html = html.replace(new RegExp(`(${escaped})`, "gi"), `<mark style="background:#FBBF24;color:#000;border-radius:2px;padding:0 1px">$1</mark>`);
    }
    return html;
  }

  // Create temp view from cell SQL
  async function createTempView() {
    if (!content.trim() || cellIndex == null) return;
    setCreatingView(true);
    try {
      await api.post("/reconciliation/execute-sql", { sql: `CREATE OR REPLACE TEMP VIEW cell_${cellIndex} AS (${content.trim().replace(/;$/, "")})` });
      toast.success(`Created temp view cell_${cellIndex} — reference it in other cells`);
    } catch (e: any) { toast.error(`View failed: ${e.message}`); }
    setCreatingView(false);
  }

  const columns = results?.length ? Object.keys(results[0]) : [];
  const columnTypes: Record<string, ColType> = {};
  if (results?.length) { for (const col of columns) { columnTypes[col] = inferColumnType(results.slice(0, 5).map(r => r[col])); } }

  const hasOutput = (results && results.length > 0) || error;

  function handleKeyDown(e: React.KeyboardEvent) {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); onRun(); }
    if (e.key === "Tab") {
      e.preventDefault();
      const ta = editorRef.current;
      if (ta) {
        const start = ta.selectionStart;
        const end = ta.selectionEnd;
        const val = ta.value;
        onChange(val.substring(0, start) + "  " + val.substring(end));
        setTimeout(() => { ta.selectionStart = ta.selectionEnd = start + 2; }, 0);
      }
    }
  }

  async function fixWithAI() {
    if (!error || !content.trim()) return;
    setAiFixing(true);
    setAiFix(null);
    try {
      const res = await api.post("/ai/summarize", {
        context_type: "report",
        data: {
          type: "fix_sql", sql: content.trim(), error,
          instruction: "The SQL query below failed with the given error on Databricks. Fix the SQL query and return ONLY the corrected SQL. No explanation, no markdown."
        },
      });
      const fixed = (res.summary || "").trim();
      const clean = fixed.startsWith("```") ? fixed.split("\n").slice(1).join("\n").split("```")[0]?.trim() || fixed : fixed;
      setAiFix({ fixedSql: clean });
    } catch (e: any) {
      setAiFix(null);
    }
    setAiFixing(false);
  }

  async function explainWithAI() {
    if (!results?.length) return;
    setAiExplainLoading(true);
    setAiExplain(null);
    try {
      const stats = buildColumnStats(columns, columnTypes, results);
      const res = await api.post("/ai/summarize", {
        context_type: "query_explain",
        data: { sql: content.trim(), columns, column_types: columnTypes, row_count: results.length, column_stats: stats, sample_rows: results.slice(0, 5) },
      });
      setAiExplain(res.summary || "No explanation available");
    } catch (e: any) {
      setAiExplain(`Failed: ${e.message}`);
    }
    setAiExplainLoading(false);
  }

  async function generateSql() {
    if (!generatePrompt.trim()) return;
    setGenerating(true);
    try {
      const res = await api.post("/ai/summarize", {
        context_type: "report",
        data: {
          type: "generate_sql", question: generatePrompt.trim(),
          instruction: "Convert this natural language question into a valid Databricks SQL query. Use Unity Catalog three-level namespace. Return ONLY the SQL query. No explanation, no markdown, no code blocks. Add LIMIT 100 unless specified otherwise."
        },
      });
      const genSql = (res.summary || "").trim();
      const clean = genSql.startsWith("```") ? genSql.split("\n").slice(1).join("\n").split("```")[0]?.trim() || genSql : genSql;
      if (clean) onChange(clean);
      setShowGenerate(false);
      setGeneratePrompt("");
    } catch {}
    setGenerating(false);
  }

  return (
    <div>
      {/* SQL Editor */}
      <div className="relative border-b border-border/30">
        <div className="absolute inset-0 pointer-events-none p-3 pr-24 font-mono text-xs whitespace-pre-wrap break-words overflow-hidden leading-5"
          dangerouslySetInnerHTML={{ __html: highlightWithSearch(content) + "\n" }} />
        <textarea
          ref={editorRef}
          value={content}
          onChange={e => onChange(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="-- Write SQL here (Ctrl+Enter to run)  |  Use {{param}} for variables"
          className="w-full min-h-[60px] p-3 pr-24 text-xs font-mono bg-transparent resize-y focus:outline-none caret-foreground leading-5"
          style={{ color: "transparent", caretColor: "var(--foreground)" }}
          spellCheck={false}
        />
        <div className="absolute top-1 right-1 flex items-center gap-0.5">
          {/* Generate with AI button */}
          <Button size="sm" variant="ghost" className="h-6 w-6 p-0" onClick={() => setShowGenerate(!showGenerate)} title="Generate SQL with AI">
            <svg className="h-3 w-3 text-[#E8453C]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" /></svg>
          </Button>
          {/* Run button */}
          <Button size="sm" variant="ghost" className="h-7 w-7 p-0" onClick={onRun} disabled={running || !content.trim()} title="Run (Ctrl+Enter)">
            {running ? <Loader2 className="h-3.5 w-3.5 animate-spin text-[#E8453C]" /> : <Play className="h-3.5 w-3.5 text-[#E8453C]" />}
          </Button>
        </div>
      </div>

      {/* Generate SQL prompt */}
      {showGenerate && (
        <div className="flex items-center gap-2 px-3 py-2 bg-[#E8453C]/5 border-b border-[#E8453C]/20">
          <input value={generatePrompt} onChange={e => setGeneratePrompt(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter") generateSql(); if (e.key === "Escape") setShowGenerate(false); }}
            placeholder="Describe what you want to query in plain English..."
            className="flex-1 text-xs bg-background border border-border rounded px-2 py-1 focus:outline-none focus:border-[#E8453C]/40"
            autoFocus />
          <Button size="sm" variant="ghost" onClick={generateSql} disabled={generating} className="text-[10px] text-[#E8453C] gap-1">
            {generating ? <Loader2 className="h-3 w-3 animate-spin" /> : null} Generate
          </Button>
          <button onClick={() => setShowGenerate(false)} className="text-muted-foreground hover:text-foreground text-[10px]">Cancel</button>
        </div>
      )}

      {/* Error with Fix AI */}
      {error && (
        <div className="px-3 py-2 bg-red-500/5 border-b border-red-500/20">
          <div className="flex items-start justify-between gap-2">
            <p className="text-xs text-red-400 font-mono flex-1">{error}</p>
            <button onClick={fixWithAI} disabled={aiFixing}
              className="shrink-0 flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded bg-[#E8453C]/10 text-[#E8453C] hover:bg-[#E8453C]/20 disabled:opacity-50">
              {aiFixing ? <><Loader2 className="h-3 w-3 animate-spin" /> Fixing...</> : <><svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" /></svg> Fix with AI</>}
            </button>
          </div>
          {aiFix?.fixedSql && (
            <div className="mt-2 rounded border border-[#E8453C]/20 bg-[#E8453C]/5 p-2">
              <div className="flex items-center justify-between mb-1">
                <span className="text-[9px] font-semibold text-[#E8453C] uppercase">AI Fix</span>
                <button onClick={() => { onChange(aiFix.fixedSql); setAiFix(null); onUpdateCell({ error: null }); }}
                  className="px-2 py-0.5 text-[9px] font-medium bg-[#E8453C] text-white rounded hover:bg-[#E8453C]/90">Apply</button>
              </div>
              <pre className="text-[10px] font-mono text-foreground whitespace-pre-wrap">{aiFix.fixedSql}</pre>
            </div>
          )}
        </div>
      )}

      {/* Results with collapsible output */}
      {hasOutput && !error && results && results.length > 0 && (
        <div>
          {/* View toggle + meta + AI explain + collapse */}
          <div className="flex items-center gap-2 px-3 py-1 bg-muted/10 border-b border-border/30 text-[10px]">
            {onToggleCollapse && (
              <button onClick={onToggleCollapse} className="text-muted-foreground hover:text-foreground" title={collapsed ? "Expand output" : "Collapse output"}>
                {collapsed ? <ChevronRight className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
              </button>
            )}
            <div className="flex items-center bg-muted rounded-md p-0.5 border border-border">
              {(["table", "chart", "profile"] as const).map(mode => (
                <button key={mode} onClick={() => {
                    if (mode === "chart") {
                      const rec = recommendVisualization(columns, columnTypes, results.slice(0, 50), results.length);
                      onUpdateCell({ viewMode: "chart", chartType: rec.chartType, chartXCol: rec.xCol, chartYCol: rec.yCol });
                    } else { onUpdateCell({ viewMode: mode }); }
                  }}
                  className={`flex items-center gap-1 px-2 py-0.5 rounded font-semibold transition-all ${viewMode === mode ? "bg-background shadow-sm text-foreground" : "text-muted-foreground hover:text-foreground"}`}>
                  {mode === "table" ? <Rows3 className="h-3 w-3" /> : mode === "chart" ? <BarChart3 className="h-3 w-3" /> : <BarChart3 className="h-3 w-3" />}
                  {mode === "table" ? "Table" : mode === "chart" ? "Chart" : "Profile"}
                </button>
              ))}
            </div>
            <span className="text-muted-foreground">
              {results.length} rows{elapsed != null ? ` · ${(elapsed / 1000).toFixed(2)}s` : ""}
              {running && runTimer > 0 ? ` · ${runTimer}s` : ""}
              {relativeTime && !running ? ` · ${relativeTime}` : ""}
            </span>

            {/* AI Explain */}
            <button onClick={explainWithAI} disabled={aiExplainLoading}
              className={`flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] hover:bg-accent/30 ${aiExplain ? "text-[#E8453C]" : "text-muted-foreground hover:text-foreground"}`}
              title="Explain results with AI">
              {aiExplainLoading ? <Loader2 className="h-3 w-3 animate-spin" /> :
                <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>}
              <span>Explain</span>
            </button>

            {/* Create temp view */}
            {cellIndex != null && (
              <button onClick={createTempView} disabled={creatingView}
                className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] text-muted-foreground hover:text-foreground hover:bg-accent/30"
                title={`Create temp view cell_${cellIndex}`}>
                {creatingView ? <Loader2 className="h-3 w-3 animate-spin" /> : <Database className="h-3 w-3" />}
                <span>View</span>
              </button>
            )}

            {/* Export + Copy */}
            <div className="ml-auto flex items-center gap-0.5">
              <button onClick={() => { downloadFile(arrayToCSV(results), "results.csv", "text/csv"); toast.success("CSV downloaded"); }}
                className="text-muted-foreground hover:text-foreground p-0.5" title="Download CSV">
                <FileDown className="h-3 w-3" />
              </button>
              <button onClick={() => { downloadFile(JSON.stringify(results, null, 2), "results.json", "application/json"); toast.success("JSON downloaded"); }}
                className="text-muted-foreground hover:text-foreground p-0.5" title="Download JSON">
                <FileJson className="h-3 w-3" />
              </button>
              <button onClick={() => { navigator.clipboard.writeText(JSON.stringify(results, null, 2)); toast.success("Copied"); }}
                className="text-muted-foreground hover:text-foreground p-0.5" title="Copy to clipboard">
                <Copy className="h-3 w-3" />
              </button>
            </div>
          </div>

          {/* AI Explain card */}
          {aiExplain && !collapsed && (
            <div className="mx-2 mt-1.5 mb-1 p-2.5 rounded-lg bg-gradient-to-r from-[#E8453C]/5 to-[#F06D55]/5 border border-[#E8453C]/20 text-[10px] text-foreground leading-relaxed">
              <div className="flex items-center justify-between mb-1">
                <span className="flex items-center gap-1 font-semibold text-[#E8453C] text-[10px]">
                  <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>
                  AI Explanation
                </span>
                <button onClick={() => setAiExplain(null)} className="text-muted-foreground hover:text-foreground text-[9px]">Dismiss</button>
              </div>
              <AiMarkdown text={aiExplain} />
            </div>
          )}

          {/* Collapsed summary */}
          {collapsed && (
            <div className="px-3 py-1.5 text-[10px] text-muted-foreground italic">
              Output collapsed — {results.length} rows, {columns.length} columns
            </div>
          )}

          {/* Table view */}
          {!collapsed && viewMode === "table" && (() => {
            const totalPages = Math.ceil(results.length / pageSize);
            const pageRows = results.slice(page * pageSize, (page + 1) * pageSize);
            return (
              <div className="max-h-[300px] overflow-auto">
                <table className="w-full text-[11px] font-mono">
                  <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                    <tr className="border-b border-border">
                      <th className="px-2 py-1.5 text-left text-[10px] text-muted-foreground font-semibold w-8">#</th>
                      {columns.map(col => (
                        <th key={col} className="px-2 py-1.5 text-left text-[10px] text-muted-foreground font-semibold">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {pageRows.map((row, i) => (
                      <tr key={page * pageSize + i} className={`border-b border-border/30 hover:bg-accent/30 ${i % 2 === 1 ? "bg-muted/20" : ""}`}>
                        <td className="px-2 py-1 text-muted-foreground">{page * pageSize + i + 1}</td>
                        {columns.map(col => (
                          <td key={col} className={`px-2 py-1 truncate max-w-[200px] ${row[col] == null ? "text-muted-foreground/40 italic" : ""}`}
                            onClick={() => { navigator.clipboard.writeText(String(row[col] ?? "")); toast.success("Copied"); }}>
                            {row[col] == null ? "NULL" : String(row[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div className="flex items-center justify-end gap-2 px-2 py-1 bg-muted/50 border-t border-border text-[10px] text-muted-foreground">
                    <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0} className="px-1 disabled:opacity-30">‹</button>
                    <span>{page + 1}/{totalPages}</span>
                    <button onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1} className="px-1 disabled:opacity-30">›</button>
                  </div>
                )}
              </div>
            );
          })()}

          {/* Chart view */}
          {!collapsed && viewMode === "chart" && (() => {
            const xCol = chartXCol || columns[0] || "";
            const numCols = columns.filter(c => results.slice(0, 10).some(r => !isNaN(Number(r[c])) && r[c] != null));
            const yCol = chartYCol || numCols.find(c => c !== xCol) || columns[1] || "";
            const data = results.slice(0, 30).map(r => ({ ...r, [yCol]: Number(r[yCol]) || 0 }));
            const tooltipStyle = { fontSize: 10, borderRadius: 6, border: "1px solid var(--border)", background: "var(--popover)" };

            return (
              <div className="p-3 h-[250px]">
                <ResponsiveContainer width="100%" height="100%">
                  {chartType === "line" ? (
                    <LineChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xCol} tick={{ fontSize: 9 }} /><YAxis tick={{ fontSize: 9 }} /><Tooltip contentStyle={tooltipStyle} /><Legend wrapperStyle={{ fontSize: 10 }} /><Line type="monotone" dataKey={yCol} stroke="#E8453C" strokeWidth={2} dot={false} /></LineChart>
                  ) : chartType === "pie" ? (
                    <PieChart><Pie data={data.map(d => ({ name: d[xCol], value: Number(d[yCol]) || 0 }))} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={{ fontSize: 9 }}>{data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}</Pie><Tooltip contentStyle={tooltipStyle} /><Legend wrapperStyle={{ fontSize: 10 }} /></PieChart>
                  ) : chartType === "area" ? (
                    <AreaChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xCol} tick={{ fontSize: 9 }} /><YAxis tick={{ fontSize: 9 }} /><Tooltip contentStyle={tooltipStyle} /><Area type="monotone" dataKey={yCol} stroke="#E8453C" fill="#E8453C" fillOpacity={0.2} /></AreaChart>
                  ) : chartType === "scatter" ? (
                    <ScatterChart><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xCol} tick={{ fontSize: 9 }} /><YAxis dataKey={yCol} tick={{ fontSize: 9 }} /><Tooltip contentStyle={tooltipStyle} /><Scatter data={data} fill="#E8453C" /></ScatterChart>
                  ) : (
                    <BarChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xCol} tick={{ fontSize: 9 }} /><YAxis tick={{ fontSize: 9 }} /><Tooltip contentStyle={tooltipStyle} /><Legend wrapperStyle={{ fontSize: 10 }} /><Bar dataKey={yCol} fill="#E8453C" radius={[3, 3, 0, 0]} /></BarChart>
                  )}
                </ResponsiveContainer>
              </div>
            );
          })()}

          {/* Profile view */}
          {!collapsed && viewMode === "profile" && (
            <div className="max-h-[400px] overflow-auto">
              <DataProfilePanel querySql={content} onClose={() => onUpdateCell({ viewMode: "table" })} />
            </div>
          )}
        </div>
      )}

      {results && results.length === 0 && !error && (
        <div className="px-3 py-2 text-[10px] text-muted-foreground text-center">Query returned no rows.</div>
      )}
    </div>
  );
}
