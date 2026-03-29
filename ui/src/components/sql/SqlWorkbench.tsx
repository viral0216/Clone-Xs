// @ts-nocheck
/**
 * SqlWorkbench — Reusable catalog browser + SQL query editor.
 *
 * Left panel: tree view of Catalogs → Schemas → Tables (click to insert FQN into query)
 * Right panel: SQL textarea, Run button, results table
 *
 * Features: autocomplete, query tabs, query history, saved queries, column sorting,
 *   JSON export, SQL formatting, keyboard shortcuts, table preview, pagination
 */
import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import ResizeHandle from "@/components/ResizeHandle";
import {
  ChevronRight, ChevronDown, Database, FolderOpen, Table2,
  Play, Loader2, Copy, Download, Terminal, PanelBottomOpen,
  PanelBottomClose, Search, X, Zap, Plus, Trash2, Clock,
  Save, BookOpen, ArrowUpDown, ArrowUp, ArrowDown, FileJson, AlignLeft,
  Keyboard, Info, Pin, PinOff, Rows3, BarChart3, Code, PieChart as PieChartIcon,
  GitCompare, CalendarClock, Share2, Network,
} from "lucide-react";
import { toast } from "sonner";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  AreaChart, Area, ScatterChart, Scatter, RadarChart, Radar,
  PolarGrid, PolarAngleAxis, PolarRadiusAxis, Treemap,
  FunnelChart, Funnel, LabelList, ComposedChart,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

// ── SQL Syntax Highlighter ───────────────────────────────────────────────────

const _sqlKwSet = new Set(["SELECT","FROM","WHERE","AND","OR","NOT","IN","IS","NULL","AS","JOIN","LEFT","RIGHT","INNER","OUTER","FULL","CROSS","ON","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","DISTINCT","ALL","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","ALTER","TABLE","VIEW","INDEX","DATABASE","SCHEMA","CATALOG","UNION","INTERSECT","EXCEPT","EXISTS","BETWEEN","LIKE","CASE","WHEN","THEN","ELSE","END","CAST","COALESCE","NULLIF","COUNT","SUM","AVG","MIN","MAX","ROW_NUMBER","RANK","DENSE_RANK","OVER","PARTITION","WITH","RECURSIVE","DESCRIBE","SHOW","EXPLAIN","TRUE","FALSE","ASC","DESC","TABLESAMPLE","LATERAL"]);
const _sqlFnSet = new Set(["COUNT","SUM","AVG","MIN","MAX","ROW_NUMBER","RANK","DENSE_RANK","COALESCE","NULLIF","CAST","IFNULL","NVL","CONCAT","LENGTH","SUBSTR","SUBSTRING","UPPER","LOWER","TRIM","REPLACE","DATE","YEAR","MONTH","DAY","NOW","CURRENT_DATE","CURRENT_TIMESTAMP"]);

function highlightSQL(code: string): string {
  // Escape HTML
  let html = code.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  // Strings (single quotes)
  html = html.replace(/'[^']*'/g, m => `<span style="color:#C73A32">${m}</span>`);
  // Numbers
  html = html.replace(/\b(\d+\.?\d*)\b/g, `<span style="color:#B83028">$1</span>`);
  // Comments
  html = html.replace(/--.*/g, m => `<span style="color:#9CA3AF;font-style:italic">${m}</span>`);
  // Keywords
  html = html.replace(/\b([A-Z_]+)\b/gi, (m) => {
    if (_sqlKwSet.has(m.toUpperCase())) return `<span style="color:#E8453C;font-weight:600">${m}</span>`;
    if (_sqlFnSet.has(m.toUpperCase())) return `<span style="color:#D94F3C">${m}</span>`;
    return m;
  });
  // Dot-separated identifiers (catalog.schema.table)
  html = html.replace(/\b(\w+\.\w+(?:\.\w+)?)\b/g, `<span style="color:#F06D55">$&</span>`);
  return html;
}

// ── Execution Plan Tree ─────────────────────────────────────────────────────

function ExplainTree({ data }: { data: any[] }) {
  if (!data?.length) return null;
  // EXPLAIN output is usually a single column with plan text
  const planText = data.map(r => Object.values(r)[0]).join("\n");
  const lines = planText.split("\n");
  return (
    <div className="p-4 font-mono text-[11px] overflow-auto h-full">
      <p className="text-xs font-semibold text-muted-foreground mb-3 uppercase">Execution Plan</p>
      {lines.map((line, i) => {
        const indent = line.search(/\S/);
        const isOperator = /^[\s]*[+\-*|\\]/.test(line) || /^\s*(Scan|Filter|Project|Aggregate|Sort|Join|Exchange|HashAggregate|BroadcastHashJoin|SortMergeJoin|WholeStageCodegen|FileScan|InMemoryTableScan)/i.test(line.trim());
        const isHeader = i === 0 || line.startsWith("==");
        return (
          <div key={i} style={{ paddingLeft: Math.max(0, indent) * 4 }}
            className={`py-0.5 ${isHeader ? "text-[#E8453C] font-semibold" : isOperator ? "text-foreground" : "text-muted-foreground"}`}>
            {line || "\u00A0"}
          </div>
        );
      })}
    </div>
  );
}

// ── Schema Diagram ──────────────────────────────────────────────────────────

function SchemaDiagram({ tables }: { tables: { name: string; columns: string[] }[] }) {
  const [positions, setPositions] = useState<Record<string, { x: number; y: number }>>({});
  const dragging = useRef<{ name: string; startX: number; startY: number; origX: number; origY: number } | null>(null);

  // Initialize grid positions for new tables
  useEffect(() => {
    setPositions(prev => {
      const next = { ...prev };
      const cols = Math.max(1, Math.ceil(Math.sqrt(tables.length)));
      tables.forEach((t, i) => {
        if (!next[t.name]) {
          next[t.name] = { x: (i % cols) * 220 + 16, y: Math.floor(i / cols) * 260 + 40 };
        }
      });
      return next;
    });
  }, [tables]);

  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      const dx = e.clientX - dragging.current.startX;
      const dy = e.clientY - dragging.current.startY;
      setPositions(prev => ({ ...prev, [dragging.current!.name]: { x: dragging.current!.origX + dx, y: dragging.current!.origY + dy } }));
    };
    const onUp = () => { dragging.current = null; document.body.style.cursor = ""; document.body.style.userSelect = ""; };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
    return () => { document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseup", onUp); };
  }, []);

  if (!tables.length) return (
    <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
      <Network className="h-8 w-8 mb-2 opacity-20" />
      <p className="text-sm">No schema to display</p>
      <p className="text-xs mt-1">Write a query with a FROM clause (e.g. SELECT * FROM catalog.schema.table), then switch to Schema tab</p>
    </div>
  );

  return (
    <div className="relative overflow-auto h-full" style={{ minHeight: 500, minWidth: 800 }}>
      <p className="absolute top-3 left-4 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider z-10">Schema Diagram — drag tables to rearrange</p>
      {tables.map((t) => {
        const pos = positions[t.name] || { x: 16, y: 40 };
        return (
          <div key={t.name} className="absolute select-none" style={{ left: pos.x, top: pos.y, width: 200 }}>
            <div className="rounded-lg border border-border shadow-md bg-popover overflow-hidden">
              {/* Header — drag handle */}
              <div
                className="px-3 py-1.5 bg-[#E8453C] text-white text-[11px] font-bold cursor-grab active:cursor-grabbing flex items-center justify-between"
                onMouseDown={(e) => {
                  e.preventDefault();
                  dragging.current = { name: t.name, startX: e.clientX, startY: e.clientY, origX: pos.x, origY: pos.y };
                  document.body.style.cursor = "grabbing";
                  document.body.style.userSelect = "none";
                }}
              >
                <span className="truncate">{t.name}</span>
                <span className="text-white/50 text-[9px]">{t.columns.length} cols</span>
              </div>
              {/* Columns */}
              <div className="max-h-[200px] overflow-y-auto">
                {t.columns.map((col, ci) => {
                  const parts = col.match(/^(.+?)\s*\((.+)\)$/);
                  return (
                    <div key={ci} className={`flex items-center justify-between px-3 py-1 text-[10px] border-b border-border/30 last:border-0 ${ci % 2 === 1 ? "bg-muted/20" : ""}`}>
                      <span className="font-mono text-foreground truncate">{parts ? parts[1] : col}</span>
                      {parts && <span className="text-muted-foreground text-[9px] ml-2 shrink-0">{parts[2]}</span>}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Catalog Tree ─────────────────────────────────────────────────────────────

interface TreeNode {
  name: string;
  type: "catalog" | "schema" | "table";
  fqn: string;
  children?: TreeNode[];
  loaded?: boolean;
}

// Column info cache for table preview on hover
const _tableColumns: Record<string, string[]> = {};

function CatalogTree({ onSelect, onShowDDL }: { onSelect: (fqn: string) => void; onShowDDL?: (fqn: string) => void }) {
  const [catalogs, setCatalogs] = useState<TreeNode[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [filter, setFilter] = useState("");
  const [hoveredTable, setHoveredTable] = useState<string | null>(null);
  const [hoverPos, setHoverPos] = useState({ top: 0, left: 0 });
  const [hoverColumns, setHoverColumns] = useState<string[]>([]);
  const [ctxMenu, setCtxMenu] = useState<{ fqn: string; x: number; y: number } | null>(null);

  useEffect(() => {
    const close = () => setCtxMenu(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, []);

  useEffect(() => {
    api.get("/catalogs").then((data: string[]) => {
      const list = (Array.isArray(data) ? data : []);
      setCatalogs(list.map((c) => ({
        name: c, type: "catalog" as const, fqn: c, children: [], loaded: false,
      })));
      addCompletionItems(list.map(c => ({ label: c, type: "catalog" as const })));
    }).catch(() => {});
  }, []);

  async function toggleExpand(node: TreeNode) {
    const key = node.fqn;
    if (expanded.has(key)) {
      setExpanded((prev) => { const n = new Set(prev); n.delete(key); return n; });
      return;
    }
    if (!node.loaded) {
      setLoading((prev) => new Set(prev).add(key));
      try {
        if (node.type === "catalog") {
          const schemas = await api.get(`/catalogs/${node.name}/schemas`);
          const list = Array.isArray(schemas) ? schemas : [];
          node.children = list.map((s: string) => ({
            name: s, type: "schema" as const, fqn: `${node.name}.${s}`, children: [], loaded: false,
          }));
          addCompletionItems(list.map(s => ({ label: `${node.name}.${s}`, type: "schema" as const })));
        } else if (node.type === "schema") {
          const parts = node.fqn.split(".");
          const tables = await api.get(`/catalogs/${parts[0]}/${parts[1]}/tables`);
          const list = Array.isArray(tables) ? tables : [];
          node.children = list.map((t: string) => ({
            name: t, type: "table" as const, fqn: `${parts[0]}.${parts[1]}.${t}`,
          }));
          addCompletionItems(list.map(t => ({ label: `${parts[0]}.${parts[1]}.${t}`, type: "table" as const })));
        }
        node.loaded = true;
      } catch {}
      setLoading((prev) => { const n = new Set(prev); n.delete(key); return n; });
    }
    setExpanded((prev) => new Set(prev).add(key));
    setCatalogs([...catalogs]);
  }

  async function handleTableHover(fqn: string, e: React.MouseEvent) {
    const rect = (e.target as HTMLElement).getBoundingClientRect();
    setHoverPos({ top: rect.top, left: rect.right + 8 });
    setHoveredTable(fqn);
    if (_tableColumns[fqn]) {
      setHoverColumns(_tableColumns[fqn]);
    } else {
      const parts = fqn.split(".");
      try {
        const info = await api.get(`/catalogs/${parts[0]}/${parts[1]}/${parts[2]}/info`);
        const cols = info?.columns?.map((c: any) => `${c.column_name || c.name || "?"} (${c.data_type || c.type_text || c.type_name || "?"})`) || [];
        _tableColumns[fqn] = cols;
        setHoverColumns(cols);
      } catch {
        setHoverColumns(["Unable to load columns"]);
      }
    }
  }

  function renderNode(node: TreeNode, depth: number = 0) {
    if (filter && !node.fqn.toLowerCase().includes(filter.toLowerCase())) {
      if (!node.children?.some((c) => c.fqn.toLowerCase().includes(filter.toLowerCase()))) {
        return null;
      }
    }
    const isExpanded = expanded.has(node.fqn);
    const isLoading = loading.has(node.fqn);
    const Icon = node.type === "catalog" ? Database : node.type === "schema" ? FolderOpen : Table2;
    const isTable = node.type === "table";

    return (
      <div key={node.fqn}>
        {isTable ? (
          <div
            className="flex items-center gap-1.5 w-full text-left px-2 py-1 text-xs hover:bg-accent/50 rounded transition-colors cursor-grab active:cursor-grabbing select-none"
            style={{ paddingLeft: `${depth * 16 + 8}px` }}
            onClick={() => onSelect(node.fqn)}
            onMouseEnter={(e) => handleTableHover(node.fqn, e)}
            onMouseLeave={() => setHoveredTable(null)}
            onContextMenu={(e) => { e.preventDefault(); setCtxMenu({ fqn: node.fqn, x: e.clientX, y: e.clientY }); }}
            title={`Click to insert: ${node.fqn}`}
            draggable
            onDragStart={(e) => {
              e.dataTransfer.setData("text/plain", `SELECT * FROM ${node.fqn} LIMIT 100`);
              e.dataTransfer.setData("application/x-table-fqn", node.fqn);
              e.dataTransfer.effectAllowed = "copy";
              const ghost = document.createElement("div");
              ghost.textContent = node.fqn;
              ghost.style.cssText = "position:fixed;top:-100px;padding:4px 8px;background:#E8453C;color:white;border-radius:4px;font-size:11px;font-family:monospace;white-space:nowrap;";
              document.body.appendChild(ghost);
              e.dataTransfer.setDragImage(ghost, 0, 0);
              setTimeout(() => document.body.removeChild(ghost), 0);
            }}
          >
            <div className="w-3" />
            <Table2 className="h-3 w-3 shrink-0 text-[#E8453C]" />
            <span className="truncate font-mono">{node.name}</span>
            <span className="ml-auto text-[9px] text-muted-foreground/50">drag</span>
          </div>
        ) : (
          <button
            className="flex items-center gap-1.5 w-full text-left px-2 py-1 text-xs hover:bg-accent/50 rounded transition-colors"
            style={{ paddingLeft: `${depth * 16 + 8}px` }}
            onClick={() => toggleExpand(node)}
          >
            {isLoading ? <Loader2 className="h-3 w-3 animate-spin text-muted-foreground shrink-0" /> :
             isExpanded ? <ChevronDown className="h-3 w-3 text-muted-foreground shrink-0" /> :
             <ChevronRight className="h-3 w-3 text-muted-foreground shrink-0" />}
            <Icon className="h-3 w-3 shrink-0 text-muted-foreground" />
            <span className="truncate">{node.name}</span>
          </button>
        )}
        {isExpanded && node.children?.map((child) => renderNode(child, depth + 1))}
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-2 pt-2 pb-1">
        <div className="relative">
          <Search className="absolute left-2 top-1.5 h-3 w-3 text-muted-foreground" />
          <input
            className="w-full pl-7 pr-2 py-1 text-xs bg-background border border-border rounded focus:outline-none focus:ring-1 focus:ring-[#E8453C]/30"
            placeholder="Filter..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          {filter && <button className="absolute right-1.5 top-1.5" onClick={() => setFilter("")}><X className="h-3 w-3 text-muted-foreground" /></button>}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto py-1">
        {catalogs.length === 0 ? (
          <p className="text-xs text-muted-foreground px-3 py-2">Loading catalogs...</p>
        ) : (
          catalogs.map((c) => renderNode(c))
        )}
      </div>
      {/* Table preview tooltip */}
      {hoveredTable && hoverColumns.length > 0 && (
        <div className="fixed z-[110] bg-popover border border-border rounded-lg shadow-lg p-2 max-w-[280px] max-h-[200px] overflow-y-auto" style={{ top: hoverPos.top, left: hoverPos.left }}>
          <p className="text-[10px] font-semibold text-muted-foreground mb-1 uppercase">Columns</p>
          {hoverColumns.map((c, i) => (
            <p key={i} className="text-[11px] font-mono text-foreground/80 py-0.5 border-b border-border/30 last:border-0">{c}</p>
          ))}
        </div>
      )}
      {/* Context menu */}
      {ctxMenu && (
        <div className="fixed z-[120] bg-popover border border-border rounded-lg shadow-lg py-1 w-48" style={{ top: ctxMenu.y, left: ctxMenu.x }}>
          <button className="w-full text-left px-3 py-1.5 text-xs hover:bg-accent/50 flex items-center gap-2" onClick={() => { onSelect(ctxMenu.fqn); setCtxMenu(null); }}>
            <Play className="h-3 w-3" /> SELECT * FROM
          </button>
          <button className="w-full text-left px-3 py-1.5 text-xs hover:bg-accent/50 flex items-center gap-2" onClick={() => { onShowDDL?.(ctxMenu.fqn); setCtxMenu(null); }}>
            <Code className="h-3 w-3" /> SHOW CREATE TABLE
          </button>
          <button className="w-full text-left px-3 py-1.5 text-xs hover:bg-accent/50 flex items-center gap-2" onClick={() => { navigator.clipboard.writeText(ctxMenu.fqn); toast.success("Copied"); setCtxMenu(null); }}>
            <Copy className="h-3 w-3" /> Copy FQN
          </button>
        </div>
      )}
    </div>
  );
}

// ── Column Type Icons (Databricks-style) ─────────────────────────────────────

type ColType = "integer" | "long" | "double" | "float" | "decimal" | "string" | "boolean" | "date" | "timestamp" | "binary" | "array" | "map" | "struct" | "null";

function inferColumnType(values: any[]): ColType {
  for (const v of values) {
    if (v == null) continue;
    const s = String(v);
    if (typeof v === "boolean" || s === "true" || s === "false") return "boolean";
    if (typeof v === "number") return Number.isInteger(v) ? (Math.abs(v) > 2147483647 ? "long" : "integer") : "double";
    if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(s)) return "timestamp";
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return "date";
    if (/^-?\d+$/.test(s) && s.length < 10) return "integer";
    if (/^-?\d{10,18}$/.test(s)) return "long";
    if (/^-?\d+\.\d+$/.test(s)) return "double";
    if (s.startsWith("[")) return "array";
    if (s.startsWith("{")) return "map";
  }
  return "string";
}

function TypeIcon({ type }: { type: ColType }) {
  const cfg: Record<string, { svg: JSX.Element; bg: string; fg: string; label: string }> = {
    integer:   { label: "INT",       bg: "#E3F2FD", fg: "#1565C0", svg: <path d="M7 4h2v16H7zm8 4h2v12h-2z" /> },
    long:      { label: "BIGINT",    bg: "#E3F2FD", fg: "#1565C0", svg: <path d="M7 4h2v16H7zm8 4h2v12h-2z" /> },
    double:    { label: "DOUBLE",    bg: "#E3F2FD", fg: "#1565C0", svg: <><path d="M7 4h2v16H7z" /><circle cx="16" cy="16" r="2" /></> },
    float:     { label: "FLOAT",     bg: "#E3F2FD", fg: "#1565C0", svg: <><path d="M7 4h2v16H7z" /><circle cx="16" cy="16" r="2" /></> },
    decimal:   { label: "DECIMAL",   bg: "#E3F2FD", fg: "#1565C0", svg: <><path d="M7 4h2v16H7z" /><circle cx="16" cy="16" r="2" /></> },
    string:    { label: "STRING",    bg: "#E8F5E9", fg: "#2E7D32", svg: <text x="5" y="17" fontSize="14" fontWeight="700" fontFamily="serif">A</text> },
    boolean:   { label: "BOOLEAN",   bg: "#F3E5F5", fg: "#7B1FA2", svg: <><path d="M9 3L5 21M19 3l-4 18" /><path d="M4 9h16M4 15h16" /></> },
    date:      { label: "DATE",      bg: "#FFF8E1", fg: "#F57F17", svg: <><rect x="3" y="4" width="18" height="18" rx="2" fill="none" stroke="currentColor" strokeWidth="2" /><path d="M3 10h18M8 2v4M16 2v4" stroke="currentColor" strokeWidth="2" /></> },
    timestamp: { label: "TIMESTAMP", bg: "#FFF8E1", fg: "#F57F17", svg: <><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="2" /><path d="M12 6v6l4 2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /></> },
    binary:    { label: "BINARY",    bg: "#ECEFF1", fg: "#546E7A", svg: <text x="3" y="17" fontSize="12" fontWeight="700" fontFamily="monospace">01</text> },
    array:     { label: "ARRAY",     bg: "#E0F2F1", fg: "#00695C", svg: <text x="4" y="17" fontSize="14" fontWeight="700" fontFamily="monospace">[]</text> },
    map:       { label: "MAP",       bg: "#E0F2F1", fg: "#00695C", svg: <text x="2" y="17" fontSize="12" fontWeight="700" fontFamily="monospace">{"{}"}</text> },
    struct:    { label: "STRUCT",    bg: "#FFF3E0", fg: "#E65100", svg: <text x="2" y="17" fontSize="12" fontWeight="700" fontFamily="monospace">{"{}"}</text> },
    null:      { label: "NULL",      bg: "#F5F5F5", fg: "#9E9E9E", svg: <text x="4" y="17" fontSize="13" fontWeight="700" fontFamily="monospace">∅</text> },
  };
  const c = cfg[type] || cfg.string;
  return (
    <span className="inline-flex items-center justify-center w-[20px] h-[18px] rounded shrink-0" style={{ backgroundColor: c.bg, color: c.fg }} title={c.label}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" stroke="none">{c.svg}</svg>
    </span>
  );
}

// ── SQL Formatter ────────────────────────────────────────────────────────────

function formatSQL(raw: string): string {
  const kw = ["SELECT", "FROM", "WHERE", "AND", "OR", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
    "FULL JOIN", "CROSS JOIN", "ON", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
    "UNION", "UNION ALL", "INTERSECT", "EXCEPT", "INSERT INTO", "VALUES", "UPDATE", "SET",
    "DELETE FROM", "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "WITH", "AS"];
  let s = raw.trim().replace(/\s+/g, " ");
  // Uppercase keywords
  for (const k of kw) {
    s = s.replace(new RegExp(`\\b${k}\\b`, "gi"), k);
  }
  // Newlines before major keywords
  for (const k of ["SELECT", "FROM", "WHERE", "AND", "OR", "JOIN", "LEFT JOIN", "RIGHT JOIN",
    "INNER JOIN", "FULL JOIN", "CROSS JOIN", "ON", "GROUP BY", "ORDER BY", "HAVING",
    "LIMIT", "OFFSET", "UNION", "UNION ALL", "WITH"]) {
    s = s.replace(new RegExp(`\\s+${k}\\b`, "g"), `\n${k}`);
  }
  // Indent after SELECT/FROM
  const lines = s.split("\n");
  return lines.map((l, i) => i === 0 ? l.trim() : "  " + l.trim()).join("\n").replace(/^  (SELECT|FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|WITH)/gm, "$1");
}

// ── SQL Keywords for autocomplete ───────────────────────────────────────────

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS",
  "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON",
  "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "DISTINCT", "ALL",
  "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "DROP",
  "ALTER", "TABLE", "VIEW", "INDEX", "DATABASE", "SCHEMA", "CATALOG",
  "UNION", "INTERSECT", "EXCEPT", "EXISTS", "BETWEEN", "LIKE", "CASE",
  "WHEN", "THEN", "ELSE", "END", "CAST", "COALESCE", "NULLIF",
  "COUNT", "SUM", "AVG", "MIN", "MAX", "ROW_NUMBER", "RANK", "DENSE_RANK",
  "OVER", "PARTITION BY", "WITH", "RECURSIVE", "DESCRIBE", "SHOW", "EXPLAIN",
  "TRUE", "FALSE", "ASC", "DESC", "TABLESAMPLE", "LATERAL",
];

const _completionItems: { label: string; type: "keyword" | "catalog" | "schema" | "table" }[] =
  SQL_KEYWORDS.map(k => ({ label: k, type: "keyword" }));

function addCompletionItems(items: { label: string; type: "catalog" | "schema" | "table" }[]) {
  for (const item of items) {
    if (!_completionItems.some(c => c.label === item.label && c.type === item.type)) {
      _completionItems.push(item);
    }
  }
}

// ── Saved queries (localStorage) ────────────────────────────────────────────

interface SavedQuery { name: string; sql: string; ts: number; }

function loadSavedQueries(): SavedQuery[] {
  try { return JSON.parse(localStorage.getItem("clxs-saved-queries") || "[]"); } catch { return []; }
}
function persistSavedQueries(q: SavedQuery[]) {
  localStorage.setItem("clxs-saved-queries", JSON.stringify(q));
}

// ── Query history (localStorage, last 30) ───────────────────────────────────

interface HistoryEntry { sql: string; ts: number; rows?: number; elapsed?: number; }

function loadHistory(): HistoryEntry[] {
  try { return JSON.parse(localStorage.getItem("clxs-query-history") || "[]"); } catch { return []; }
}
function pushHistory(entry: HistoryEntry) {
  const h = loadHistory();
  h.unshift(entry);
  if (h.length > 30) h.length = 30;
  localStorage.setItem("clxs-query-history", JSON.stringify(h));
}

// ── Main Component ──────────────────────────────────────────────────────────

interface TabState {
  id: number;
  name: string;
  sql: string;
  results: any[] | null;
  error: string | null;
  elapsed: number | null;
}

let _nextTabId = 1;

export default function SqlWorkbench({ embedded = false }: { embedded?: boolean }) {
  const [open, setOpen] = useState(embedded);
  const [running, setRunning] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Tabs
  const [tabs, setTabs] = useState<TabState[]>([{ id: _nextTabId++, name: "Query 1", sql: "", results: null, error: null, elapsed: null }]);
  const [activeTabId, setActiveTabId] = useState(tabs[0].id);
  const activeTab = tabs.find(t => t.id === activeTabId) || tabs[0];

  function updateTab(id: number, patch: Partial<TabState>) {
    setTabs(prev => prev.map(t => t.id === id ? { ...t, ...patch } : t));
  }
  function addTab() {
    const t: TabState = { id: _nextTabId++, name: `Query ${_nextTabId - 1}`, sql: "", results: null, error: null, elapsed: null };
    setTabs(prev => [...prev, t]);
    setActiveTabId(t.id);
  }
  function closeTab(id: number) {
    if (tabs.length <= 1 || pinnedTabs.has(id)) return;
    const idx = tabs.findIndex(t => t.id === id);
    const next = tabs.filter(t => t.id !== id);
    setTabs(next);
    if (activeTabId === id) setActiveTabId(next[Math.min(idx, next.length - 1)].id);
  }

  // Derived from active tab
  const sql = activeTab.sql;
  const results = activeTab.results;
  const error = activeTab.error;
  const elapsed = activeTab.elapsed;
  function setSql(v: string | ((p: string) => string)) {
    const val = typeof v === "function" ? v(activeTab.sql) : v;
    updateTab(activeTabId, { sql: val });
  }
  function setResults(r: any[] | null) { updateTab(activeTabId, { results: r }); }
  function setError(e: string | null) { updateTab(activeTabId, { error: e }); }
  function setElapsed(e: number | null) { updateTab(activeTabId, { elapsed: e }); }

  // Pagination
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  // Column sorting
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  // Results view mode
  const [viewMode, setViewMode] = useState<"table" | "chart" | "explain" | "schema" | "profile" | "describe" | "lineage" | "sample">("table");
  const [chartType, setChartType] = useState<"bar" | "hbar" | "stacked" | "line" | "area" | "composed" | "scatter" | "pie" | "radar" | "funnel" | "treemap" | "map">("bar");
  const [chartXCol, setChartXCol] = useState<string>("");
  const [chartYCol, setChartYCol] = useState<string>("");

  // Result filters — per-column text filter
  const [columnFilters, setColumnFilters] = useState<Record<string, string>>({});
  const [showFilters, setShowFilters] = useState(false);
  function setColFilter(col: string, val: string) {
    setColumnFilters(prev => {
      const next = { ...prev };
      if (val) next[col] = val; else delete next[col];
      return next;
    });
    setPage(0);
  }
  function clearAllFilters() { setColumnFilters({}); setPage(0); }

  // Pin tabs
  const [pinnedTabs, setPinnedTabs] = useState<Set<number>>(new Set());
  function togglePin(id: number) { setPinnedTabs(prev => { const n = new Set(prev); if (n.has(id)) n.delete(id); else n.add(id); return n; }); }

  // Row detail drawer
  const [detailRow, setDetailRow] = useState<any | null>(null);

  // Column stats popup
  const [statsCol, setStatsCol] = useState<string | null>(null);

  // Multi-statement
  const [stmtResults, setStmtResults] = useState<{ sql: string; results: any[]; error?: string }[]>([]);
  const [activeStmt, setActiveStmt] = useState(0);

  // Find & replace
  const [showFind, setShowFind] = useState(false);
  const [findText, setFindText] = useState("");
  const [replaceText, setReplaceText] = useState("");

  // More menu dropdown
  const [showMore, setShowMore] = useState(false);
  const moreRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const close = (e: MouseEvent) => { if (moreRef.current && !moreRef.current.contains(e.target as Node)) setShowMore(false); };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);

  // Query diff
  const [showDiff, setShowDiff] = useState(false);
  const [diffSql, setDiffSql] = useState("");
  const [diffResults, setDiffResults] = useState<any[] | null>(null);
  const [diffRunning, setDiffRunning] = useState(false);

  // Scheduled queries
  const [showSchedule, setShowSchedule] = useState(false);
  const [scheduleInterval, setScheduleInterval] = useState(60); // seconds
  const [scheduleActive, setScheduleActive] = useState(false);
  const scheduleRef = useRef<any>(null);

  // Schema diagram
  const [showSchema, setShowSchema] = useState(false);

  // Explain results (separate from main results)
  const [explainResults, setExplainResults] = useState<any[] | null>(null);
  const [explainLoading, setExplainLoading] = useState(false);

  // Describe results
  const [describeResults, setDescribeResults] = useState<any[] | null>(null);
  const [describeLoading, setDescribeLoading] = useState(false);

  // Sample results
  const [sampleResults, setSampleResults] = useState<any[] | null>(null);
  const [sampleLoading, setSampleLoading] = useState(false);

  // Panels: history, saved queries
  const [showHistory, setShowHistory] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [history, setHistoryState] = useState<HistoryEntry[]>(loadHistory);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(loadSavedQueries);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [saveName, setSaveName] = useState("");

  // Autocomplete state
  const [suggestions, setSuggestions] = useState<{ label: string; type: string }[]>([]);
  const [acIndex, setAcIndex] = useState(0);
  const [acVisible, setAcVisible] = useState(false);
  const [acPos, setAcPos] = useState({ top: 0, left: 0 });
  const acRef = useRef<HTMLDivElement>(null);

  // Execution mode
  const [execMode, setExecMode] = useState<"spark" | "warehouse">("spark");
  const [warehouses, setWarehouses] = useState<any[]>([]);
  const [selectedWarehouse, setSelectedWarehouse] = useState("");
  const [sparkAvailable, setSparkAvailable] = useState(false);
  const [startingWarehouse, setStartingWarehouse] = useState(false);

  function loadWarehouses() {
    api.get("/auth/warehouses").then((wh: any[]) => {
      const list = Array.isArray(wh) ? wh : [];
      setWarehouses(list);
      if (list.length > 0 && !selectedWarehouse) {
        const running = list.find((w) => w.state === "RUNNING");
        setSelectedWarehouse(running?.id || list[0].id);
      }
    }).catch(() => {});
  }

  useEffect(() => {
    loadWarehouses();
    api.get("/reconciliation/spark-status").then((s: any) => {
      setSparkAvailable(s?.available ?? false);
      if (!s?.available) setExecMode("warehouse");
    }).catch(() => setExecMode("warehouse"));
  }, []);

  const selectedWh = warehouses.find((w) => w.id === selectedWarehouse);
  const whState = selectedWh?.state || "";
  const whRunning = whState === "RUNNING";

  async function startWarehouse() {
    if (!selectedWarehouse) return;
    setStartingWarehouse(true);
    try {
      await api.post("/warehouse/start", { warehouse_id: selectedWarehouse });
      const poll = setInterval(async () => {
        try {
          const wh = await api.get("/auth/warehouses");
          const list = Array.isArray(wh) ? wh : [];
          setWarehouses(list);
          if (list.find((w: any) => w.id === selectedWarehouse)?.state === "RUNNING") {
            clearInterval(poll);
            setStartingWarehouse(false);
          }
        } catch {}
      }, 3000);
      setTimeout(() => { clearInterval(poll); setStartingWarehouse(false); }, 120000);
    } catch (e: any) {
      setError("Failed to start warehouse: " + (e.message || ""));
      setStartingWarehouse(false);
    }
  }

  function insertAtCursor(text: string) {
    const ta = textareaRef.current;
    if (!ta) { setSql((prev) => prev + (prev ? "\n" : "") + `SELECT * FROM ${text} LIMIT 100`); return; }
    const start = ta.selectionStart;
    const end = ta.selectionEnd;
    const before = sql.slice(0, start);
    const after = sql.slice(end);
    const insert = before ? text : `SELECT * FROM ${text} LIMIT 100`;
    const needsSpace = before && !before.endsWith(" ") && !before.endsWith("\n");
    setSql(before + (needsSpace ? " " : "") + insert + after);
    setTimeout(() => { ta.selectionStart = ta.selectionEnd = start + insert.length; ta.focus(); }, 0);
  }

  async function runQuery() {
    if (!sql.trim()) return;
    if (execMode === "warehouse" && !whRunning) {
      setError(`Warehouse is ${whState || "not selected"}. Start it first or switch to Spark Connect mode.`);
      return;
    }
    // Multi-statement: split by semicolons
    const statements = sql.trim().split(/;\s*/).filter(s => s.trim());
    if (statements.length > 1) {
      setRunning(true); setResults(null); setError(null); setSortCol(null); setColumnFilters({}); setStmtResults([]);
      const start = Date.now();
      const allResults: { sql: string; results: any[]; error?: string }[] = [];
      for (const stmt of statements) {
        try {
          const payload: any = { sql: stmt.trim() };
          if (execMode === "spark") payload.use_spark = true;
          else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
          const data = await api.post("/reconciliation/execute-sql", payload);
          allResults.push({ sql: stmt.trim(), results: Array.isArray(data) ? data : [] });
        } catch (e: any) {
          allResults.push({ sql: stmt.trim(), results: [], error: e.message || "Failed" });
        }
      }
      setStmtResults(allResults);
      setActiveStmt(allResults.length - 1);
      // Show last successful result in main view
      const last = allResults[allResults.length - 1];
      setResults(last.results);
      if (last.error) setError(last.error);
      setElapsed(Date.now() - start); setPage(0);
      pushHistory({ sql: sql.trim(), ts: Date.now(), rows: last.results.length, elapsed: Date.now() - start });
      setHistoryState(loadHistory());
      setRunning(false);
      return;
    }

    setRunning(true); setResults(null); setError(null); setSortCol(null); setColumnFilters({}); setStmtResults([]);
    const start = Date.now();
    try {
      const payload: any = { sql: sql.trim() };
      if (execMode === "spark") { payload.use_spark = true; }
      else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
      const data = await api.post("/reconciliation/execute-sql", payload);
      const rows = Array.isArray(data) ? data : [];
      setResults(rows);
      setPage(0);
      setElapsed(Date.now() - start);
      pushHistory({ sql: sql.trim(), ts: Date.now(), rows: rows.length, elapsed: Date.now() - start });
      setHistoryState(loadHistory());
    } catch (e: any) {
      setError(e.message || "Query failed");
      setElapsed(Date.now() - start);
    } finally {
      setRunning(false);
    }
  }

  async function runExplain() {
    if (!sql.trim()) return;
    setSql(`EXPLAIN ${sql.trim()}`);
    setTimeout(runQuery, 50);
  }

  // ── Autocomplete ──────────────────────────────────────────────────────────
  function getWordAtCursor() {
    const ta = textareaRef.current;
    if (!ta) return { word: "", start: 0, end: 0 };
    const pos = ta.selectionStart;
    const text = sql.slice(0, pos);
    const match = text.match(/[\w.]+$/);
    if (!match) return { word: "", start: pos, end: pos };
    return { word: match[0], start: pos - match[0].length, end: pos };
  }
  function updateSuggestions() {
    const { word } = getWordAtCursor();
    if (word.length < 1) { setAcVisible(false); return; }
    const lower = word.toLowerCase();
    const matches = _completionItems.filter(c => c.label.toLowerCase().startsWith(lower) && c.label.toLowerCase() !== lower).slice(0, 10);
    if (matches.length === 0) { setAcVisible(false); return; }
    setSuggestions(matches);
    setAcIndex(0);
    setAcVisible(true);
    const ta = textareaRef.current;
    if (ta) {
      const rect = ta.getBoundingClientRect();
      const lines = sql.slice(0, ta.selectionStart).split("\n");
      setAcPos({ top: rect.top + lines.length * 20 + 4, left: rect.left + Math.min(lines[lines.length - 1].length * 7.8, rect.width - 220) });
    }
  }
  function applySuggestion(item: { label: string }) {
    const { start, end } = getWordAtCursor();
    setSql(sql.slice(0, start) + item.label + " " + sql.slice(end));
    setAcVisible(false);
    const ta = textareaRef.current;
    if (ta) { const p = start + item.label.length + 1; setTimeout(() => { ta.selectionStart = ta.selectionEnd = p; ta.focus(); }, 0); }
  }

  // ── Save query ────────────────────────────────────────────────────────────
  function saveQuery() {
    if (!saveName.trim() || !sql.trim()) return;
    const updated = [...savedQueries.filter(q => q.name !== saveName.trim()), { name: saveName.trim(), sql: sql.trim(), ts: Date.now() }];
    persistSavedQueries(updated);
    setSavedQueries(updated);
    setSaveDialogOpen(false);
    setSaveName("");
  }
  function deleteSaved(name: string) {
    const updated = savedQueries.filter(q => q.name !== name);
    persistSavedQueries(updated);
    setSavedQueries(updated);
  }

  // ── Auto-run EXPLAIN when switching to Plan tab ────────────────────────────
  useEffect(() => {
    if (viewMode !== "explain" || !sql.trim() || explainLoading) return;
    // Don't re-run if we already have explain results for this query
    if (explainResults) return;
    setExplainLoading(true);
    (async () => {
      try {
        const explainSql = sql.trim().toUpperCase().startsWith("EXPLAIN") ? sql.trim() : `EXPLAIN ${sql.trim()}`;
        const payload: any = { sql: explainSql };
        if (execMode === "spark") payload.use_spark = true;
        else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
        const data = await api.post("/reconciliation/execute-sql", payload);
        setExplainResults(Array.isArray(data) ? data : []);
      } catch (e: any) {
        setExplainResults([{ plan: `EXPLAIN failed: ${e.message || "Unknown error"}` }]);
      }
      setExplainLoading(false);
    })();
  }, [viewMode]);

  // Reset tab-specific results when SQL changes
  useEffect(() => { setExplainResults(null); setDescribeResults(null); setSampleResults(null); }, [sql]);

  // Helper: extract table FQN from SQL
  function getTableFromSql(): string | null {
    const m = sql.match(/FROM\s+([\w.]+)/i);
    return m ? m[1] : null;
  }

  // ── Auto-run DESCRIBE when switching to Describe tab ──────────────────────
  useEffect(() => {
    if (viewMode !== "describe" || !sql.trim() || describeLoading || describeResults) return;
    const table = getTableFromSql();
    if (!table) return;
    setDescribeLoading(true);
    (async () => {
      try {
        const payload: any = { sql: `DESCRIBE TABLE EXTENDED ${table}` };
        if (execMode === "spark") payload.use_spark = true;
        else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
        const data = await api.post("/reconciliation/execute-sql", payload);
        setDescribeResults(Array.isArray(data) ? data : []);
      } catch (e: any) {
        setDescribeResults([{ col_name: "Error", data_type: e.message || "Failed" }]);
      }
      setDescribeLoading(false);
    })();
  }, [viewMode]);

  // ── Auto-run Sample when switching to Sample tab ──────────────────────────
  useEffect(() => {
    if (viewMode !== "sample" || !sql.trim() || sampleLoading || sampleResults) return;
    const table = getTableFromSql();
    if (!table) return;
    setSampleLoading(true);
    (async () => {
      try {
        const payload: any = { sql: `SELECT * FROM ${table} LIMIT 20` };
        if (execMode === "spark") payload.use_spark = true;
        else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
        const data = await api.post("/reconciliation/execute-sql", payload);
        setSampleResults(Array.isArray(data) ? data : []);
      } catch (e: any) {
        setSampleResults([]);
      }
      setSampleLoading(false);
    })();
  }, [viewMode]);

  // ── Query Diff ────────────────────────────────────────────────────────────
  async function runDiffQuery() {
    if (!diffSql.trim()) return;
    setDiffRunning(true);
    try {
      const payload: any = { sql: diffSql.trim() };
      if (execMode === "spark") payload.use_spark = true;
      else { payload.use_spark = false; payload.warehouse_id = selectedWarehouse; }
      const data = await api.post("/reconciliation/execute-sql", payload);
      setDiffResults(Array.isArray(data) ? data : []);
    } catch (e: any) { toast.error(e.message || "Diff query failed"); }
    finally { setDiffRunning(false); }
  }

  // ── Scheduled query ──────────────────────────────────────────────────────
  function startSchedule() {
    if (scheduleRef.current) clearInterval(scheduleRef.current);
    setScheduleActive(true);
    runQuery(); // run immediately
    scheduleRef.current = setInterval(runQuery, scheduleInterval * 1000);
  }
  function stopSchedule() {
    setScheduleActive(false);
    if (scheduleRef.current) { clearInterval(scheduleRef.current); scheduleRef.current = null; }
  }
  useEffect(() => { return () => { if (scheduleRef.current) clearInterval(scheduleRef.current); }; }, []);

  // ── Share query ──────────────────────────────────────────────────────────
  function shareQuery() {
    const encoded = btoa(encodeURIComponent(sql));
    const url = `${window.location.origin}/sql-workbench#q=${encoded}`;
    navigator.clipboard.writeText(url);
    toast.success("Share link copied to clipboard");
  }

  // Load shared query from URL hash on mount
  useEffect(() => {
    const hash = window.location.hash;
    if (hash.startsWith("#q=")) {
      try { const decoded = decodeURIComponent(atob(hash.slice(3))); if (decoded) setSql(decoded); } catch {}
    }
  }, []);

  // ── Schema diagram data ────────────────────────────────────────────────────
  const [schemaTables, setSchemaTables] = useState<{ name: string; columns: string[] }[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(false);

  useEffect(() => {
    if (viewMode !== "schema") return;
    // First use any already-loaded table columns from hover cache
    const cached = Object.entries(_tableColumns).map(([fqn, cols]) => ({
      name: fqn.split(".").pop() || fqn, columns: cols,
    }));
    if (cached.length > 0) { setSchemaTables(cached); return; }

    // Otherwise, try to extract table name from current SQL and fetch its schema
    const tableMatch = sql.match(/FROM\s+([\w.]+)/i);
    if (!tableMatch) return;
    const fqn = tableMatch[1];
    const parts = fqn.split(".");
    if (parts.length < 2) return;

    setSchemaLoading(true);
    (async () => {
      try {
        // Fetch all tables in the schema
        const catalog = parts[0];
        const schema = parts[1];
        const tables = await api.get(`/catalogs/${catalog}/${schema}/tables`);
        const tableList = Array.isArray(tables) ? tables : [];

        // Fetch column info for each table (up to 20)
        const results: { name: string; columns: string[] }[] = [];
        for (const t of tableList.slice(0, 20)) {
          try {
            const info = await api.get(`/catalogs/${catalog}/${schema}/${t}/info`);
            const cols = info?.columns?.map((c: any) => `${c.column_name || c.name || "?"} (${c.data_type || c.type_text || c.type_name || "?"})`) || [];
            _tableColumns[`${catalog}.${schema}.${t}`] = cols;
            results.push({ name: t, columns: cols });
          } catch { results.push({ name: t, columns: [] }); }
        }
        setSchemaTables(results);
      } catch {}
      setSchemaLoading(false);
    })();
  }, [viewMode, sql]);

  // ── Show DDL for a table ──────────────────────────────────────────────────
  function showDDL(fqn: string) {
    setSql(`SHOW CREATE TABLE ${fqn}`);
    setTimeout(runQuery, 50);
  }

  // ── Find & Replace ───────────────────────────────────────────────────────
  function findNext() {
    if (!findText) return;
    const idx = sql.toLowerCase().indexOf(findText.toLowerCase());
    if (idx >= 0) {
      const ta = textareaRef.current;
      if (ta) { ta.focus(); ta.selectionStart = idx; ta.selectionEnd = idx + findText.length; }
    }
  }
  function replaceOne() {
    if (!findText) return;
    const idx = sql.toLowerCase().indexOf(findText.toLowerCase());
    if (idx >= 0) { setSql(sql.slice(0, idx) + replaceText + sql.slice(idx + findText.length)); }
  }
  function replaceAll() {
    if (!findText) return;
    setSql(sql.replace(new RegExp(findText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi"), replaceText));
  }

  // ── Column stats ─────────────────────────────────────────────────────────
  const columnStats = useMemo(() => {
    if (!statsCol || !results?.length) return null;
    const vals = results.map(r => r[statsCol]);
    const nonNull = vals.filter(v => v != null);
    const distinct = new Set(nonNull.map(String)).size;
    const nullCount = vals.length - nonNull.length;
    const nums = nonNull.map(Number).filter(n => !isNaN(n));
    return {
      total: vals.length, nullCount, distinct,
      min: nums.length ? Math.min(...nums) : nonNull.length ? nonNull.sort()[0] : null,
      max: nums.length ? Math.max(...nums) : nonNull.length ? nonNull.sort().reverse()[0] : null,
      avg: nums.length ? (nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2) : null,
    };
  }, [statsCol, results]);

  // ── Keyboard handler ──────────────────────────────────────────────────────
  function handleKeyDown(e: React.KeyboardEvent) {
    if (acVisible) {
      if (e.key === "ArrowDown") { e.preventDefault(); setAcIndex(i => (i + 1) % suggestions.length); return; }
      if (e.key === "ArrowUp") { e.preventDefault(); setAcIndex(i => (i - 1 + suggestions.length) % suggestions.length); return; }
      if (e.key === "Tab" || e.key === "Enter") { if (suggestions[acIndex]) { e.preventDefault(); applySuggestion(suggestions[acIndex]); return; } }
      if (e.key === "Escape") { e.preventDefault(); setAcVisible(false); return; }
    }
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); setAcVisible(false); runQuery(); }
    if ((e.metaKey || e.ctrlKey) && e.key === "l") { e.preventDefault(); setSql(""); setResults(null); setError(null); }
    if ((e.metaKey || e.ctrlKey) && e.key === "s") { e.preventDefault(); setSaveDialogOpen(true); }
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === "F") { e.preventDefault(); setSql(formatSQL(sql)); }
    if ((e.metaKey || e.ctrlKey) && e.key === "h") { e.preventDefault(); setShowFind(!showFind); }
  }

  // ── Exports ───────────────────────────────────────────────────────────────
  function copyResults() {
    if (!results?.length) return;
    const headers = Object.keys(results[0]).join("\t");
    const rows = results.map((r) => Object.values(r).map((v) => String(v ?? "")).join("\t"));
    navigator.clipboard.writeText([headers, ...rows].join("\n"));
  }
  function downloadCsv() {
    if (!results?.length) return;
    const headers = Object.keys(results[0]).join(",");
    const rows = results.map((r) => Object.values(r).map((v) => `"${String(v ?? "").replace(/"/g, '""')}"`).join(","));
    const blob = new Blob([[headers, ...rows].join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "query_results.csv"; a.click();
    URL.revokeObjectURL(url);
  }
  function downloadJson() {
    if (!results?.length) return;
    const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "query_results.json"; a.click();
    URL.revokeObjectURL(url);
  }

  // ── Layout state ──────────────────────────────────────────────────────────
  const [browserWidth, setBrowserWidth] = useState(256);
  const [editorHeight, setEditorHeight] = useState(160);
  const editorDragging = useRef(false);
  const [panelHeight, setPanelHeight] = useState(50);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const isDragging = useRef(false);

  function handleDragStart(e: React.MouseEvent) {
    e.preventDefault(); isDragging.current = true;
    const startY = e.clientY; const startHeight = panelHeight;
    function onMove(ev: MouseEvent) { if (!isDragging.current) return; setPanelHeight(Math.min(95, Math.max(20, startHeight + (startY - ev.clientY) / window.innerHeight * 100))); }
    function onUp() { isDragging.current = false; document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseup", onUp); document.body.style.cursor = ""; document.body.style.userSelect = ""; }
    document.body.style.cursor = "row-resize"; document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove); document.addEventListener("mouseup", onUp);
  }
  function toggleFullscreen() { setIsFullscreen(!isFullscreen); setPanelHeight(isFullscreen ? 50 : 95); }

  // ── Filtered + Sorted results ──────────────────────────────────────────
  const filteredResults = useMemo(() => {
    if (!results) return results;
    const filterKeys = Object.keys(columnFilters);
    if (filterKeys.length === 0) return results;
    return results.filter(row =>
      filterKeys.every(col => {
        const v = String(row[col] ?? "NULL").toLowerCase();
        return v.includes(columnFilters[col].toLowerCase());
      })
    );
  }, [results, columnFilters]);

  const sortedResults = useMemo(() => {
    if (!filteredResults || !sortCol) return filteredResults;
    return [...filteredResults].sort((a, b) => {
      const va = a[sortCol]; const vb = b[sortCol];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const na = Number(va); const nb = Number(vb);
      const cmp = (!isNaN(na) && !isNaN(nb)) ? na - nb : String(va).localeCompare(String(vb));
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [filteredResults, sortCol, sortDir]);

  function toggleSort(col: string) {
    if (sortCol === col) { setSortDir(d => d === "asc" ? "desc" : "asc"); }
    else { setSortCol(col); setSortDir("asc"); }
    setPage(0);
  }

  const activeFilterCount = Object.keys(columnFilters).length;

  // ── Render ────────────────────────────────────────────────────────────────

  if (!open) {
    if (embedded) return null;
    return (
      <button onClick={() => setOpen(true)} className="fixed bottom-4 right-4 z-50 flex items-center gap-2 px-4 py-2.5 bg-[#E8453C]/5 text-[#E8453C] text-sm font-medium rounded-lg shadow-lg hover:bg-[#E8453C]/10 transition-colors border border-[#E8453C]/20">
        <Terminal className="h-4 w-4 text-[#E8453C]" /> SQL Workbench <PanelBottomOpen className="h-3.5 w-3.5 text-[#E8453C]/50" />
      </button>
    );
  }

  const columns = results?.length ? Object.keys(results[0]) : [];
  const columnTypes: Record<string, ColType> = {};
  if (results?.length) { for (const col of columns) { columnTypes[col] = inferColumnType(results.slice(0, 5).map((r) => r[col])); } }
  const displayResults = sortedResults || filteredResults || results;

  return (
    <div
      className={embedded ? "flex flex-col h-full bg-background" : "fixed bottom-0 left-0 right-0 z-50 bg-background border-t-2 border-border shadow-2xl flex flex-col"}
      style={embedded ? undefined : { height: `${panelHeight}vh`, transition: isDragging.current ? "none" : "height 0.2s ease" }}
    >
      {!embedded && <div className="h-1.5 cursor-row-resize group flex items-center justify-center hover:bg-[#E8453C]/10 active:bg-[#E8453C]/20 shrink-0" onMouseDown={handleDragStart} onDoubleClick={toggleFullscreen} title="Drag to resize">
        <div className="w-10 h-1 rounded-full bg-border group-hover:bg-[#E8453C]/50 transition-colors" />
      </div>}

      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-muted/30 shrink-0">
        <div className="flex items-center gap-3 overflow-x-auto">
          <Terminal className="h-4 w-4 text-[#E8453C] shrink-0" />
          <span className="text-[13px] font-semibold shrink-0">SQL Workbench</span>
          <div className="w-px h-5 bg-border mx-2 shrink-0" />

          {/* Execution mode toggle */}
          <div className="flex items-center bg-muted rounded-lg p-0.5 border border-border shrink-0">
            <button onClick={() => setExecMode("spark")} disabled={!sparkAvailable}
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold transition-all ${execMode === "spark" ? "bg-[#E8453C] text-white shadow-sm" : "text-muted-foreground hover:text-foreground"} ${!sparkAvailable ? "opacity-40 cursor-not-allowed" : ""}`}>
              <Zap className="h-3 w-3" /> Spark Connect
            </button>
            <button onClick={() => setExecMode("warehouse")}
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold transition-all ${execMode === "warehouse" ? "bg-[#E8453C] text-white shadow-sm" : "text-muted-foreground hover:text-foreground"}`}>
              <Database className="h-3 w-3" /> SQL Warehouse
            </button>
          </div>

          {execMode === "warehouse" && (
            <div className="flex items-center gap-1.5 shrink-0">
              <select value={selectedWarehouse} onChange={(e) => setSelectedWarehouse(e.target.value)} className="h-7 text-xs bg-background border border-border rounded-md px-2 min-w-[160px]">
                {warehouses.length === 0 && <option value="">No warehouses</option>}
                {warehouses.map((wh) => <option key={wh.id} value={wh.id}>{wh.name} ({wh.state})</option>)}
              </select>
              <span className={`inline-flex items-center gap-1 text-[10px] font-semibold px-1.5 py-0.5 rounded ${whRunning ? "text-green-500 bg-green-500/10" : whState === "STARTING" || startingWarehouse ? "text-amber-500 bg-amber-500/10" : "text-muted-foreground bg-muted/50"}`}>
                <span className={`h-1.5 w-1.5 rounded-full ${whRunning ? "bg-green-500" : whState === "STARTING" || startingWarehouse ? "bg-amber-500 animate-pulse" : "bg-muted-foreground"}`} />
                {startingWarehouse ? "STARTING" : whState || "—"}
              </span>
              {!whRunning && !startingWarehouse && selectedWarehouse && <Button size="sm" variant="outline" className="h-6 text-[10px] gap-1" onClick={startWarehouse}><Play className="h-3 w-3" /> Start</Button>}
              <button onClick={loadWarehouses} className="p-1 rounded hover:bg-accent/50 text-muted-foreground" title="Refresh"><svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 21h5v-5"/></svg></button>
            </div>
          )}

        </div>
        <div className="flex items-center gap-1 shrink-0">
          {/* Group: Editor */}
          <Button size="sm" variant="ghost" onClick={() => setShowHistory(!showHistory)} title="History" className={`gap-1 ${showHistory ? "bg-accent" : ""}`}><Clock className="h-3.5 w-3.5" /><span className="text-[10px] hidden xl:inline">History</span></Button>
          <Button size="sm" variant="ghost" onClick={() => setShowSaved(!showSaved)} title="Saved" className={`gap-1 ${showSaved ? "bg-accent" : ""}`}><BookOpen className="h-3.5 w-3.5" /><span className="text-[10px] hidden xl:inline">Saved</span></Button>
          <Button size="sm" variant="ghost" onClick={() => setSql(formatSQL(sql))} title="Format (Ctrl+Shift+F)" className="gap-1"><AlignLeft className="h-3.5 w-3.5" /><span className="text-[10px] hidden xl:inline">Format</span></Button>

          <div className="w-px h-4 bg-border mx-0.5" />

          {/* Group: Export (only with results) */}
          {results?.length > 0 && (<>
            <Button size="sm" variant="ghost" onClick={() => setShowFilters(!showFilters)} title="Filter" className={showFilters ? "bg-accent" : ""}>
              <Search className="h-3.5 w-3.5" />
              {activeFilterCount > 0 && <span className="ml-0.5 text-[8px] bg-[#E8453C] text-white rounded-full w-3.5 h-3.5 flex items-center justify-center">{activeFilterCount}</span>}
            </Button>
            <Button size="sm" variant="ghost" onClick={copyResults} title="Copy"><Copy className="h-3.5 w-3.5" /></Button>
            <Button size="sm" variant="ghost" onClick={downloadCsv} title="CSV"><Download className="h-3.5 w-3.5" /></Button>
            <Button size="sm" variant="ghost" onClick={downloadJson} title="JSON"><FileJson className="h-3.5 w-3.5" /></Button>
            <div className="w-px h-4 bg-border mx-0.5" />
          </>)}

          {/* More dropdown */}
          <div ref={moreRef} className="relative">
            <Button size="sm" variant="ghost" onClick={() => setShowMore(!showMore)} title="More tools" className={`gap-1 ${showMore ? "bg-accent" : ""}`}>
              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="currentColor"><circle cx="5" cy="12" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="19" cy="12" r="2"/></svg>
              <span className="text-[10px] hidden xl:inline">More</span>
            </Button>
            {showMore && (
              <div className="absolute top-full right-0 mt-1 w-52 bg-popover border border-border rounded-lg shadow-lg py-1 z-[100]">
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { shareQuery(); setShowMore(false); }}>
                  <Share2 className="h-3.5 w-3.5 text-muted-foreground" /> Share Query Link
                </button>
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { setShowDiff(!showDiff); setShowMore(false); }}>
                  <GitCompare className="h-3.5 w-3.5 text-muted-foreground" /> Query Diff {showDiff && <span className="ml-auto text-[9px] text-[#E8453C]">ON</span>}
                </button>
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { setShowSchedule(!showSchedule); setShowMore(false); }}>
                  <CalendarClock className={`h-3.5 w-3.5 ${scheduleActive ? "text-green-500" : "text-muted-foreground"}`} /> Schedule Query {scheduleActive && <span className="ml-auto text-[9px] text-green-500">ACTIVE</span>}
                </button>
                <div className="h-px bg-border my-1" />
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { setShowFind(!showFind); setShowMore(false); }}>
                  <Search className="h-3.5 w-3.5 text-muted-foreground" /> Find & Replace <kbd className="ml-auto text-[9px] text-muted-foreground bg-muted px-1 rounded">Ctrl+H</kbd>
                </button>
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { setSaveDialogOpen(true); setShowMore(false); }}>
                  <Save className="h-3.5 w-3.5 text-muted-foreground" /> Save Query <kbd className="ml-auto text-[9px] text-muted-foreground bg-muted px-1 rounded">Ctrl+S</kbd>
                </button>
                <div className="h-px bg-border my-1" />
                <button className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-accent/50 transition-colors" onClick={() => { setShowShortcuts(!showShortcuts); setShowMore(false); }}>
                  <Keyboard className="h-3.5 w-3.5 text-muted-foreground" /> Keyboard Shortcuts
                </button>
              </div>
            )}
          </div>

          {/* Fullscreen / Close (floating mode) */}
          {!embedded && (<>
            <Button size="sm" variant="ghost" onClick={toggleFullscreen} title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>
              {isFullscreen ? <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
                : <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/></svg>}
            </Button>
            <Button size="sm" variant="ghost" onClick={() => { setOpen(false); setIsFullscreen(false); setPanelHeight(50); }}><PanelBottomClose className="h-3.5 w-3.5" /></Button>
          </>)}
        </div>
      </div>

      {/* Keyboard shortcuts overlay */}
      {showShortcuts && (
        <div className="absolute top-14 right-4 z-[100] bg-popover border border-border rounded-lg shadow-lg p-3 w-64">
          <p className="text-xs font-semibold mb-2">Keyboard Shortcuts</p>
          {[["Ctrl+Enter", "Run query"], ["Ctrl+L", "Clear editor"], ["Ctrl+S", "Save query"], ["Ctrl+Shift+F", "Format SQL"], ["Ctrl+H", "Find & Replace"], ["Tab", "Accept suggestion"], ["Esc", "Dismiss autocomplete"], ["Click cell", "Copy value"], ["Click #", "Row details"], ["Right-click col", "Column stats"], ["Right-click table", "Context menu"]].map(([k, v]) => (
            <div key={k} className="flex justify-between text-[11px] py-1 border-b border-border/30 last:border-0">
              <kbd className="px-1.5 py-0.5 rounded bg-muted text-muted-foreground font-mono text-[10px]">{k}</kbd>
              <span className="text-muted-foreground">{v}</span>
            </div>
          ))}
          <button onClick={() => setShowShortcuts(false)} className="mt-2 text-[10px] text-muted-foreground hover:text-foreground">Close</button>
        </div>
      )}

      {/* Save dialog */}
      {saveDialogOpen && (
        <div className="absolute top-14 right-4 z-[100] bg-popover border border-border rounded-lg shadow-lg p-3 w-64">
          <p className="text-xs font-semibold mb-2">Save Query</p>
          <input value={saveName} onChange={(e) => setSaveName(e.target.value)} placeholder="Query name..." className="w-full text-xs border border-border rounded px-2 py-1 bg-background mb-2" onKeyDown={(e) => { if (e.key === "Enter") saveQuery(); }} autoFocus />
          <div className="flex gap-2">
            <Button size="sm" onClick={saveQuery} disabled={!saveName.trim()}>Save</Button>
            <Button size="sm" variant="outline" onClick={() => setSaveDialogOpen(false)}>Cancel</Button>
          </div>
        </div>
      )}

      {/* Schedule popup */}
      {showSchedule && (
        <div className="absolute top-14 right-4 z-[100] bg-popover border border-border rounded-lg shadow-lg p-3 w-64">
          <p className="text-xs font-semibold mb-2">Schedule Query</p>
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[11px] text-muted-foreground">Run every</span>
            <input type="number" value={scheduleInterval} onChange={e => setScheduleInterval(Math.max(5, Number(e.target.value)))} className="w-16 text-xs border border-border rounded px-2 py-1 bg-background" min={5} />
            <span className="text-[11px] text-muted-foreground">seconds</span>
          </div>
          <div className="flex gap-2">
            {!scheduleActive ? (
              <Button size="sm" onClick={() => { startSchedule(); setShowSchedule(false); }} disabled={!sql.trim()}>Start</Button>
            ) : (
              <Button size="sm" variant="outline" onClick={() => { stopSchedule(); setShowSchedule(false); }} className="text-red-500 border-red-500/30">Stop</Button>
            )}
            <Button size="sm" variant="outline" onClick={() => setShowSchedule(false)}>Close</Button>
          </div>
          {scheduleActive && <p className="text-[10px] text-green-500 mt-2">Running every {scheduleInterval}s</p>}
        </div>
      )}

      {/* Query Tabs */}
      <div className="flex items-center border-b border-border bg-muted/20 shrink-0 overflow-x-auto">
        {tabs.map(t => {
          const isPinned = pinnedTabs.has(t.id);
          return (
            <div key={t.id} onClick={() => setActiveTabId(t.id)}
              className={`flex items-center gap-1 px-3 py-1.5 text-xs cursor-pointer border-r border-border shrink-0 ${t.id === activeTabId ? "bg-background font-medium" : "text-muted-foreground hover:bg-accent/30"}`}>
              {isPinned && <Pin className="h-2.5 w-2.5 text-[#E8453C]" />}
              <span className="truncate max-w-[120px]">{t.name}</span>
              <button onClick={(e) => { e.stopPropagation(); togglePin(t.id); }} className="p-0.5 rounded hover:bg-accent/50" title={isPinned ? "Unpin" : "Pin"}>
                {isPinned ? <PinOff className="h-2.5 w-2.5" /> : <Pin className="h-2.5 w-2.5 opacity-30" />}
              </button>
              {tabs.length > 1 && !isPinned && <button onClick={(e) => { e.stopPropagation(); closeTab(t.id); }} className="p-0.5 rounded hover:bg-accent/50"><X className="h-2.5 w-2.5" /></button>}
            </div>
          );
        })}
        <button onClick={addTab} className="px-2 py-1.5 text-muted-foreground hover:text-foreground hover:bg-accent/30" title="New tab"><Plus className="h-3.5 w-3.5" /></button>
      </div>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left: Catalog Browser */}
        <div className="border-r border-border bg-muted/5 overflow-hidden flex flex-col shrink-0" style={{ width: browserWidth }}>
          <div className="px-3 py-2.5 border-b border-border bg-muted/30">
            <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5"><Database className="h-3 w-3" />Catalog Browser</p>
          </div>
          <CatalogTree onSelect={insertAtCursor} onShowDDL={showDDL} />
        </div>
        <ResizeHandle width={browserWidth} onResize={setBrowserWidth} min={180} max={450} side="right" />

        {/* Middle: Query editor + results */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Find & Replace bar */}
          {showFind && (
            <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border bg-muted/20 shrink-0">
              <Search className="h-3 w-3 text-muted-foreground shrink-0" />
              <input value={findText} onChange={e => setFindText(e.target.value)} placeholder="Find..." className="text-xs bg-background border border-border rounded px-2 py-0.5 w-32" onKeyDown={e => { if (e.key === "Enter") findNext(); }} />
              <input value={replaceText} onChange={e => setReplaceText(e.target.value)} placeholder="Replace..." className="text-xs bg-background border border-border rounded px-2 py-0.5 w-32" />
              <Button size="sm" variant="outline" className="h-6 text-[10px]" onClick={findNext}>Find</Button>
              <Button size="sm" variant="outline" className="h-6 text-[10px]" onClick={replaceOne}>Replace</Button>
              <Button size="sm" variant="outline" className="h-6 text-[10px]" onClick={replaceAll}>All</Button>
              <button onClick={() => setShowFind(false)} className="p-0.5"><X className="h-3 w-3 text-muted-foreground" /></button>
            </div>
          )}

          {/* SQL Input */}
          <div className="border-b border-border p-2 flex gap-2 relative shrink-0" style={{ height: editorHeight }}
            onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); e.dataTransfer.dropEffect = "copy"; setDragOver(true); }}
            onDragLeave={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragOver(false); }}
            onDrop={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(false); const fqn = e.dataTransfer.getData("application/x-table-fqn"); const pt = e.dataTransfer.getData("text/plain"); if (fqn) insertAtCursor(fqn); else if (pt?.includes(".")) insertAtCursor(pt); }}>
            {dragOver && <div className="absolute inset-2 z-10 rounded-lg border-2 border-dashed border-[#E8453C] bg-[#E8453C]/10 flex items-center justify-center pointer-events-none"><span className="text-sm font-medium text-[#E8453C]">Drop table here</span></div>}
            {/* Syntax-highlighted SQL editor */}
            <div className="flex-1 relative rounded-lg border border-border/60 overflow-hidden bg-muted/30">
              <pre className="absolute inset-0 p-3 font-mono text-[13px] leading-relaxed whitespace-pre-wrap break-words overflow-hidden pointer-events-none" aria-hidden="true"
                dangerouslySetInnerHTML={{ __html: highlightSQL(sql) + (sql.endsWith("\n") ? "\u00A0" : "") }} />
              <textarea ref={textareaRef} value={sql}
                onChange={(e) => { setSql(e.target.value); setTimeout(updateSuggestions, 0); }}
                onKeyDown={handleKeyDown}
                onBlur={() => setTimeout(() => setAcVisible(false), 150)}
                placeholder="Enter SQL query... (Ctrl+Enter to run, or drag a table here)"
                className="relative w-full h-full bg-transparent text-transparent caret-foreground font-mono text-[13px] leading-relaxed p-3 resize-none focus:outline-none"
                style={{ caretColor: "var(--foreground)" }} />
            </div>
            {/* Autocomplete dropdown */}
            {acVisible && suggestions.length > 0 && (
              <div ref={acRef} className="fixed z-[100] w-[260px] max-h-[200px] overflow-y-auto bg-popover border border-border rounded-lg shadow-lg py-1" style={{ top: acPos.top, left: acPos.left }}>
                {suggestions.map((s, idx) => (
                  <button key={s.label + s.type}
                    className={`w-full flex items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors ${idx === acIndex ? "bg-accent text-accent-foreground" : "hover:bg-accent/50"}`}
                    onMouseDown={(e) => { e.preventDefault(); applySuggestion(s); }} onMouseEnter={() => setAcIndex(idx)}>
                    <span className={`inline-flex items-center justify-center w-4 h-4 rounded text-[9px] font-bold shrink-0 ${s.type === "keyword" ? "bg-blue-500/15 text-blue-500" : s.type === "catalog" ? "bg-amber-500/15 text-amber-500" : s.type === "schema" ? "bg-green-500/15 text-green-500" : "bg-[#E8453C]/15 text-[#E8453C]"}`}>
                      {s.type === "keyword" ? "K" : s.type === "catalog" ? "C" : s.type === "schema" ? "S" : "T"}
                    </span>
                    <span className="font-mono truncate">{s.label}</span>
                    <span className="ml-auto text-[9px] text-muted-foreground">{s.type}</span>
                  </button>
                ))}
              </div>
            )}
            <div className="flex flex-col gap-1 shrink-0">
              <Button size="sm" onClick={runQuery} disabled={running || !sql.trim()} className="gap-1.5">
                {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />} Run
              </Button>
              <Button size="sm" variant="outline" onClick={() => { setSql(""); setResults(null); setError(null); setSortCol(null); }}>Clear</Button>
              <Button size="sm" variant="outline" onClick={runExplain} disabled={running || !sql.trim()} title="EXPLAIN query"><Info className="h-3.5 w-3.5" /></Button>
            </div>
          </div>

          {/* Vertical resize handle */}
          <div className="h-1.5 shrink-0 cursor-row-resize group flex items-center justify-center hover:bg-[#E8453C]/10 active:bg-[#E8453C]/20 transition-colors"
            onMouseDown={(e) => { e.preventDefault(); editorDragging.current = true; const sY = e.clientY; const sH = editorHeight;
              document.body.style.cursor = "row-resize"; document.body.style.userSelect = "none";
              const m = (ev: MouseEvent) => { if (!editorDragging.current) return; setEditorHeight(Math.min(500, Math.max(80, sH + (ev.clientY - sY)))); };
              const u = () => { editorDragging.current = false; document.body.style.cursor = ""; document.body.style.userSelect = ""; document.removeEventListener("mousemove", m); document.removeEventListener("mouseup", u); };
              document.addEventListener("mousemove", m); document.addEventListener("mouseup", u); }}>
            <div className="w-10 h-px bg-border group-hover:bg-[#E8453C] transition-colors" />
          </div>

          {/* Multi-statement tabs */}
          {stmtResults.length > 1 && (
            <div className="flex items-center gap-0.5 px-2 py-1 border-b border-border bg-muted/10 shrink-0 overflow-x-auto">
              {stmtResults.map((s, i) => (
                <button key={i} onClick={() => { setActiveStmt(i); setResults(s.results); if (s.error) setError(s.error); else setError(null); setPage(0); setSortCol(null); }}
                  className={`px-2 py-0.5 text-[10px] rounded ${i === activeStmt ? "bg-[#E8453C] text-white" : "text-muted-foreground hover:bg-accent/30"} ${s.error ? "border border-red-500/30" : ""}`}>
                  Stmt {i + 1}{s.error ? " ✗" : ` (${s.results.length})`}
                </button>
              ))}
            </div>
          )}

          {/* Results view toggle */}
          {displayResults && displayResults.length > 0 && (
            <div className="flex items-center gap-2 px-3 py-1 border-b border-border bg-muted/10 shrink-0">
              <div className="flex items-center bg-muted rounded-md p-0.5 border border-border">
                {([["table", Rows3, "Table"], ["chart", BarChart3, "Chart"], ["profile", BarChart3, "Profile"], ["describe", Code, "Describe"], ["explain", Info, "Plan"], ["sample", Table2, "Sample"], ["schema", Network, "Schema"]] as const).map(([mode, Icon, label]) => (
                  <button key={mode} onClick={() => setViewMode(mode)}
                    className={`flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold transition-all ${viewMode === mode ? "bg-background shadow-sm text-foreground" : "text-muted-foreground hover:text-foreground"}`}>
                    <Icon className="h-3 w-3" /> {label}
                  </button>
                ))}
              </div>
              {viewMode === "chart" && (<>
                <div className="flex items-center gap-1.5">
                  <span className="text-[10px] text-muted-foreground">Type:</span>
                  <select value={chartType} onChange={e => setChartType(e.target.value as any)} className="text-[10px] bg-background border border-border rounded px-1 py-0.5">
                    <option value="bar">Bar</option>
                    <option value="hbar">Horizontal Bar</option>
                    <option value="stacked">Stacked Bar</option>
                    <option value="line">Line</option>
                    <option value="area">Area</option>
                    <option value="composed">Composed (Bar+Line)</option>
                    <option value="scatter">Scatter</option>
                    <option value="pie">Pie</option>
                    <option value="funnel">Funnel</option>
                    <option value="radar">Radar</option>
                    <option value="treemap">Treemap</option>
                    <option value="map">Geo Map</option>
                  </select>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="text-[10px] text-muted-foreground">X:</span>
                  <select value={chartXCol} onChange={e => setChartXCol(e.target.value)} className="text-[10px] bg-background border border-border rounded px-1 py-0.5 max-w-[120px]">
                    <option value="">Auto</option>
                    {columns.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                {!["pie", "treemap", "funnel", "radar", "map"].includes(chartType) && (
                  <div className="flex items-center gap-1.5">
                    <span className="text-[10px] text-muted-foreground">Y:</span>
                    <select value={chartYCol} onChange={e => setChartYCol(e.target.value)} className="text-[10px] bg-background border border-border rounded px-1 py-0.5 max-w-[120px]">
                      <option value="">Auto (first numeric)</option>
                      {columns.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </div>
                )}
              </>)}
            </div>
          )}

          {/* Results */}
          <div className="flex-1 overflow-auto relative">
            {/* Running progress overlay */}
            {running && (
              <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/60 backdrop-blur-sm">
                <div className="flex flex-col items-center gap-2">
                  <Loader2 className="h-6 w-6 animate-spin text-[#E8453C]" />
                  <span className="text-xs text-muted-foreground">Executing query...</span>
                </div>
              </div>
            )}
            {error && <div className="p-3 text-sm text-red-400 font-mono bg-red-500/5">{error}</div>}
            {results && results.length === 0 && !error && <div className="p-4 text-sm text-muted-foreground text-center">Query returned no rows.</div>}
            {displayResults && displayResults.length > 0 && viewMode === "chart" && (() => {
              // Brand-harmonized palette — primary red with warm/cool complements
              const COLORS = ["#E8453C", "#C73A32", "#F06D55", "#B83028", "#F4897A", "#9E2620", "#D94F3C", "#E86B5C", "#F09080", "#D43E35", "#C05048", "#E87A6E"];
              const MAX_CHART_ITEMS = 30;
              const tooltipStyle = { fontSize: 11, borderRadius: 8, border: "1px solid var(--border)", background: "var(--popover)" };

              // Auto-detect columns
              const xCol = chartXCol || columns[0] || "";
              const numCols = columns.filter(c => displayResults.slice(0, 10).some(r => !isNaN(Number(r[c])) && r[c] != null && r[c] !== ""));
              const yCol = chartYCol || numCols.find(c => c !== xCol) || columns[1] || "";

              let chartData: any[];
              let chartNote = "";

              if (["pie", "treemap", "funnel", "radar"].includes(chartType)) {
                // Aggregate by X column, sum Y values — show top N + "Other"
                const agg: Record<string, number> = {};
                for (const r of displayResults) {
                  const key = String(r[xCol] ?? "NULL");
                  agg[key] = (agg[key] || 0) + (Number(r[yCol]) || 1);
                }
                const sorted = Object.entries(agg).sort((a, b) => b[1] - a[1]);
                if (sorted.length > MAX_CHART_ITEMS) {
                  const top = sorted.slice(0, MAX_CHART_ITEMS - 1);
                  const otherVal = sorted.slice(MAX_CHART_ITEMS - 1).reduce((s, [, v]) => s + v, 0);
                  chartData = [...top.map(([name, value]) => ({ name, value })), { name: `Other (${sorted.length - MAX_CHART_ITEMS + 1})`, value: otherVal }];
                  chartNote = `Aggregated ${sorted.length} unique values into top ${MAX_CHART_ITEMS - 1} + Other`;
                } else {
                  chartData = sorted.map(([name, value]) => ({ name, value }));
                  chartNote = `${sorted.length} unique values`;
                }
              } else {
                // Bar/Line: limit to MAX_CHART_ITEMS rows, or aggregate if too many
                const uniqueX = new Set(displayResults.map(r => String(r[xCol] ?? "")));
                if (uniqueX.size > MAX_CHART_ITEMS && !chartXCol) {
                  // Aggregate: group by X, avg Y
                  const agg: Record<string, { sum: number; count: number }> = {};
                  for (const r of displayResults) {
                    const key = String(r[xCol] ?? "NULL");
                    if (!agg[key]) agg[key] = { sum: 0, count: 0 };
                    agg[key].sum += Number(r[yCol]) || 0;
                    agg[key].count++;
                  }
                  const sorted = Object.entries(agg).sort((a, b) => b[1].sum - a[1].sum).slice(0, MAX_CHART_ITEMS);
                  chartData = sorted.map(([key, { sum, count }]) => ({ [xCol]: key, [yCol]: Math.round(sum / count * 100) / 100 }));
                  chartNote = `Top ${MAX_CHART_ITEMS} of ${uniqueX.size} unique values (averaged)`;
                } else {
                  chartData = displayResults.slice(0, MAX_CHART_ITEMS).map(r => ({
                    [xCol]: String(r[xCol] ?? ""),
                    [yCol]: Number(r[yCol]) || 0,
                  }));
                  if (displayResults.length > MAX_CHART_ITEMS) {
                    chartNote = `Showing first ${MAX_CHART_ITEMS} of ${displayResults.length} rows`;
                  }
                }
              }

              const aggData = chartData.map(r => ({ name: r[xCol] || r.name, value: r[yCol] || r.value || 0 }));
              const radarData = aggData.map(r => ({ subject: r.name, value: r.value }));
              const funnelData = aggData.map((r, i) => ({ ...r, fill: COLORS[i % COLORS.length] }));
              const xAngle = chartData.length > 10 ? -35 : 0;
              const xAnchor = chartData.length > 10 ? "end" : "middle";
              const xHeight = chartData.length > 10 ? 70 : 40;
              const grid = <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />;
              const xAx = <XAxis dataKey={xCol} tick={{ fontSize: 10 }} angle={xAngle} textAnchor={xAnchor} height={xHeight} interval={0} />;
              const yAx = <YAxis tick={{ fontSize: 10 }} width={60} />;
              const tip = <Tooltip contentStyle={tooltipStyle} />;

              // Detect second numeric column for stacked/composed
              const numCols2 = numCols.filter(c => c !== yCol);
              const y2Col = numCols2[0] || "";

              // Geo map: detect lat/lng columns
              const latCol = columns.find(c => /^(lat|latitude)$/i.test(c));
              const lngCol = columns.find(c => /^(lng|lon|longitude)$/i.test(c));
              const mapData = (chartType === "map" && latCol && lngCol) ? displayResults.slice(0, 500).map(r => ({
                lat: Number(r[latCol]) || 0, lng: Number(r[lngCol]) || 0,
                label: columns.find(c => c !== latCol && c !== lngCol) ? String(r[columns.find(c => c !== latCol && c !== lngCol)!] ?? "") : "",
              })).filter(p => p.lat !== 0 || p.lng !== 0) : [];

              function renderChart() {
                switch (chartType) {
                  case "bar": return (
                    <BarChart data={chartData}>{grid}{xAx}{yAx}{tip}<Bar dataKey={yCol} fill="#E8453C" radius={[4, 4, 0, 0]} maxBarSize={60} /></BarChart>
                  );
                  case "hbar": return (
                    <BarChart data={chartData} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
                      <XAxis type="number" tick={{ fontSize: 10 }} />
                      <YAxis dataKey={xCol} type="category" tick={{ fontSize: 9 }} width={100} />
                      {tip}<Bar dataKey={yCol} fill="#E8453C" radius={[0, 4, 4, 0]} maxBarSize={30} />
                    </BarChart>
                  );
                  case "stacked": return (
                    <BarChart data={chartData.map((r, i) => ({ ...r, ...(y2Col ? { [y2Col]: Number(displayResults[i]?.[y2Col]) || 0 } : {}) }))}>
                      {grid}{xAx}{yAx}{tip}<Legend wrapperStyle={{ fontSize: 10 }} />
                      <Bar dataKey={yCol} stackId="a" fill="#E8453C" radius={y2Col ? undefined : [4, 4, 0, 0]} />
                      {y2Col && <Bar dataKey={y2Col} stackId="a" fill="#F06D55" radius={[4, 4, 0, 0]} />}
                    </BarChart>
                  );
                  case "line": return (
                    <LineChart data={chartData}>{grid}{xAx}{yAx}{tip}<Line type="monotone" dataKey={yCol} stroke="#E8453C" strokeWidth={2} dot={{ r: 3, fill: "#E8453C" }} activeDot={{ r: 5 }} /></LineChart>
                  );
                  case "area": return (
                    <AreaChart data={chartData}>{grid}{xAx}{yAx}{tip}
                      <defs><linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#E8453C" stopOpacity={0.3} /><stop offset="95%" stopColor="#E8453C" stopOpacity={0} /></linearGradient></defs>
                      <Area type="monotone" dataKey={yCol} stroke="#E8453C" strokeWidth={2} fill="url(#areaGrad)" dot={{ r: 2, fill: "#E8453C" }} />
                    </AreaChart>
                  );
                  case "composed": return (
                    <ComposedChart data={chartData.map((r, i) => ({ ...r, ...(y2Col ? { [y2Col]: Number(displayResults[i]?.[y2Col]) || 0 } : {}) }))}>
                      {grid}{xAx}{yAx}{tip}<Legend wrapperStyle={{ fontSize: 10 }} />
                      <Bar dataKey={yCol} fill="#E8453C" radius={[4, 4, 0, 0]} maxBarSize={40} barSize={30} />
                      {y2Col && <Line type="monotone" dataKey={y2Col} stroke="#F06D55" strokeWidth={2} dot={{ r: 3 }} />}
                    </ComposedChart>
                  );
                  case "scatter": return (
                    <ScatterChart>{grid}
                      <XAxis dataKey={xCol} tick={{ fontSize: 10 }} name={xCol} type="number" domain={["auto", "auto"]} />
                      <YAxis dataKey={yCol} tick={{ fontSize: 10 }} name={yCol} width={60} />
                      {tip}<Scatter data={chartData} fill="#E8453C">{chartData.map((_, idx) => <Cell key={idx} fill={COLORS[idx % COLORS.length]} />)}</Scatter>
                    </ScatterChart>
                  );
                  case "pie": return (
                    <PieChart>
                      <Pie data={aggData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius="75%" innerRadius="30%"
                        label={({ name, percent }) => percent > 0.03 ? `${String(name).length > 12 ? String(name).slice(0, 12) + "…" : name} ${(percent * 100).toFixed(0)}%` : ""} labelLine={false}>
                        {aggData.map((_, idx) => <Cell key={idx} fill={COLORS[idx % COLORS.length]} />)}
                      </Pie>{tip}<Legend wrapperStyle={{ fontSize: 10, maxHeight: 60, overflow: "auto" }} />
                    </PieChart>
                  );
                  case "funnel": return (
                    <FunnelChart>
                      <Tooltip contentStyle={tooltipStyle} />
                      <Funnel dataKey="value" nameKey="name" data={funnelData} isAnimationActive>
                        <LabelList position="center" fill="#fff" fontSize={10} dataKey="name" />
                      </Funnel>
                    </FunnelChart>
                  );
                  case "radar": return (
                    <RadarChart cx="50%" cy="50%" outerRadius="70%" data={radarData}>
                      <PolarGrid stroke="var(--border)" /><PolarAngleAxis dataKey="subject" tick={{ fontSize: 9 }} /><PolarRadiusAxis tick={{ fontSize: 9 }} />
                      {tip}<Radar dataKey="value" stroke="#E8453C" fill="#E8453C" fillOpacity={0.25} strokeWidth={2} />
                    </RadarChart>
                  );
                  case "treemap": return (
                    <Treemap data={aggData.map((d, i) => ({ name: d.name, size: d.value, fill: COLORS[i % COLORS.length] }))} dataKey="size" nameKey="name" aspectRatio={4 / 3}
                      content={({ x, y, width, height, name, fill }) => (
                        width > 30 && height > 20 ? (
                          <g><rect x={x} y={y} width={width} height={height} fill={fill} stroke="var(--background)" strokeWidth={2} rx={4} />
                            <text x={x + width / 2} y={y + height / 2} textAnchor="middle" dominantBaseline="central" fill="white" fontSize={Math.min(11, width / 6)} fontWeight={600}>
                              {String(name).length > width / 7 ? String(name).slice(0, Math.floor(width / 7)) + "…" : name}</text></g>
                        ) : <rect x={x} y={y} width={width} height={height} fill={fill} stroke="var(--background)" strokeWidth={1} rx={2} />
                      )}>{tip}</Treemap>
                  );
                  case "map": return null; // handled separately below
                  default: return null;
                }
              }

              // Geo Map — simple SVG world projection
              if (chartType === "map") {
                if (!latCol || !lngCol) return (
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    <div className="text-center"><p className="text-sm font-medium">No geo columns detected</p><p className="text-xs mt-1">Map requires columns named latitude/longitude (or lat/lng/lon)</p></div>
                  </div>
                );
                // Simple equirectangular projection
                const w = 800, h = 400;
                const project = (lat: number, lng: number) => [((lng + 180) / 360) * w, ((90 - lat) / 180) * h];
                return (
                  <div className="p-4 h-full flex flex-col items-center justify-center">
                    <svg viewBox={`0 0 ${w} ${h}`} className="w-full max-h-full border border-border rounded-lg bg-muted/20" style={{ maxWidth: 900 }}>
                      {/* Simple world outline */}
                      <rect x={0} y={0} width={w} height={h} fill="none" />
                      {/* Grid lines */}
                      {[-60,-30,0,30,60].map(lat => { const y = ((90-lat)/180)*h; return <line key={`lat${lat}`} x1={0} y1={y} x2={w} y2={y} stroke="var(--border)" strokeWidth={0.5} opacity={0.3} />; })}
                      {[-120,-60,0,60,120].map(lng => { const x = ((lng+180)/360)*w; return <line key={`lng${lng}`} x1={x} y1={0} x2={x} y2={h} stroke="var(--border)" strokeWidth={0.5} opacity={0.3} />; })}
                      {/* Equator */}
                      <line x1={0} y1={h/2} x2={w} y2={h/2} stroke="var(--border)" strokeWidth={1} opacity={0.5} />
                      {/* Data points */}
                      {mapData.map((p, i) => {
                        const [px, py] = project(p.lat, p.lng);
                        return <g key={i}>
                          <circle cx={px} cy={py} r={4} fill="#E8453C" opacity={0.7} stroke="white" strokeWidth={1}>
                            <title>{p.label || `${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}`}</title>
                          </circle>
                        </g>;
                      })}
                    </svg>
                    <p className="text-[10px] text-muted-foreground mt-2">{mapData.length} points plotted from {latCol}/{lngCol}</p>
                  </div>
                );
              }

              return (
                <div className="p-4 flex-1 flex flex-col min-h-0">
                  <div className="flex-1 min-h-[250px]">
                    <ResponsiveContainer width="100%" height="100%" minHeight={250}>
                      {renderChart()}
                    </ResponsiveContainer>
                  </div>
                  {chartNote && <p className="text-[10px] text-muted-foreground text-center mt-1 shrink-0">{chartNote}</p>}
                </div>
              );
            })()}
            {displayResults && displayResults.length > 0 && viewMode === "table" && (() => {
              const totalPages = Math.ceil(displayResults.length / pageSize);
              const startIdx = page * pageSize;
              const pageRows = displayResults.slice(startIdx, startIdx + pageSize);
              return (<>
                <table className="w-full text-[12px] font-mono">
                  <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                    <tr className="border-b border-border">
                      <th className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold w-10">#</th>
                      {columns.map((col) => (
                        <th key={col} className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold cursor-pointer hover:text-foreground select-none"
                          onClick={() => toggleSort(col)}
                          onContextMenu={(e) => { e.preventDefault(); setStatsCol(statsCol === col ? null : col); }}>
                          <span className="flex items-center gap-1.5">
                            <TypeIcon type={columnTypes[col] || "string"} />
                            {col}
                            {sortCol === col ? (sortDir === "asc" ? <ArrowUp className="h-3 w-3 text-[#E8453C]" /> : <ArrowDown className="h-3 w-3 text-[#E8453C]" />) : <ArrowUpDown className="h-3 w-3 opacity-20" />}
                          </span>
                        </th>
                      ))}
                    </tr>
                    {showFilters && (
                      <tr className="border-b border-border bg-muted/40">
                        <th className="px-3 py-1">
                          {activeFilterCount > 0 && <button onClick={clearAllFilters} className="text-[9px] text-[#E8453C] hover:underline" title="Clear all filters">Clear</button>}
                        </th>
                        {columns.map(col => (
                          <th key={col} className="px-1 py-1">
                            <input
                              value={columnFilters[col] || ""}
                              onChange={e => setColFilter(col, e.target.value)}
                              placeholder="Filter..."
                              className="w-full text-[10px] font-normal bg-background border border-border/60 rounded px-1.5 py-0.5 focus:outline-none focus:border-[#E8453C]/40"
                            />
                          </th>
                        ))}
                      </tr>
                    )}
                  </thead>
                  <tbody>
                    {pageRows.map((row, i) => (
                      <tr key={startIdx + i} className={`border-b border-border/30 hover:bg-accent/30 ${i % 2 === 1 ? "bg-muted/20" : ""}`}>
                        <td className="px-3 py-1.5 text-muted-foreground cursor-pointer hover:text-foreground" onClick={() => setDetailRow(detailRow === row ? null : row)} title="Click for row details">{startIdx + i + 1}</td>
                        {columns.map((col) => {
                          const v = row[col];
                          const isNull = v == null;
                          return (
                            <td key={col}
                              className={`px-3 py-1.5 truncate max-w-[250px] cursor-pointer hover:bg-accent/50 ${isNull ? "text-muted-foreground/40 italic" : ""}`}
                              onClick={() => { navigator.clipboard.writeText(String(v ?? "")); toast.success("Copied"); }}
                              title="Click to copy">
                              {isNull ? "NULL" : String(v)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="sticky bottom-0 flex items-center justify-between px-3 py-1.5 bg-muted/50 border-t border-border text-[11px] text-muted-foreground">
                  <span>{displayResults.length}{activeFilterCount > 0 ? ` of ${results.length}` : ""} row{displayResults.length !== 1 ? "s" : ""}{elapsed != null ? ` · ${(elapsed / 1000).toFixed(2)}s` : ""}{sortCol ? ` · sorted by ${sortCol} ${sortDir}` : ""}{activeFilterCount > 0 ? ` · ${activeFilterCount} filter${activeFilterCount > 1 ? "s" : ""}` : ""}</span>
                  <div className="flex items-center gap-2">
                    <span>Rows/page:</span>
                    <select value={pageSize} onChange={(e) => { setPageSize(Number(e.target.value)); setPage(0); }} className="bg-background border border-border rounded px-1 py-0.5 text-[11px]">
                      {[25, 50, 100, 250, 500].map(n => <option key={n} value={n}>{n}</option>)}
                    </select>
                    <span>{startIdx + 1}–{Math.min(startIdx + pageSize, displayResults.length)} of {displayResults.length}</span>
                    <button onClick={() => setPage(0)} disabled={page === 0} className="px-1 py-0.5 rounded hover:bg-accent/50 disabled:opacity-30">⟨⟨</button>
                    <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0} className="px-1 py-0.5 rounded hover:bg-accent/50 disabled:opacity-30">⟨</button>
                    <span>{page + 1}/{totalPages}</span>
                    <button onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1} className="px-1 py-0.5 rounded hover:bg-accent/50 disabled:opacity-30">⟩</button>
                    <button onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1} className="px-1 py-0.5 rounded hover:bg-accent/50 disabled:opacity-30">⟩⟩</button>
                  </div>
                </div>
              </>);
            })()}
            {/* Column stats popup */}
            {statsCol && columnStats && (
              <div className="fixed z-[100] bg-popover border border-border rounded-lg shadow-lg p-3 w-56" style={{ top: 120, right: 80 }}>
                <div className="flex items-center justify-between mb-2">
                  <p className="text-xs font-semibold">{statsCol}</p>
                  <button onClick={() => setStatsCol(null)}><X className="h-3 w-3 text-muted-foreground" /></button>
                </div>
                {[["Total", columnStats.total], ["Nulls", columnStats.nullCount], ["Distinct", columnStats.distinct],
                  ["Min", columnStats.min], ["Max", columnStats.max], ["Avg", columnStats.avg]].map(([k, v]) => (
                  v != null && <div key={k} className="flex justify-between text-[11px] py-0.5 border-b border-border/30">
                    <span className="text-muted-foreground">{k}</span>
                    <span className="font-mono">{v}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Row detail drawer */}
            {detailRow && columns.length > 0 && (
              <div className="fixed z-[100] top-0 right-0 h-full w-80 bg-popover border-l border-border shadow-2xl overflow-y-auto">
                <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-muted/30 sticky top-0">
                  <p className="text-xs font-semibold">Row Details</p>
                  <button onClick={() => setDetailRow(null)}><X className="h-4 w-4 text-muted-foreground" /></button>
                </div>
                <div className="p-3">
                  {columns.map(col => (
                    <div key={col} className="py-2 border-b border-border/30">
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{col}</p>
                      <p className={`text-[12px] font-mono mt-0.5 break-all ${detailRow[col] == null ? "text-muted-foreground/40 italic" : ""}`}
                        onClick={() => { navigator.clipboard.writeText(String(detailRow[col] ?? "")); toast.success("Copied"); }}
                        className="cursor-pointer hover:bg-accent/30 rounded px-1 -mx-1">
                        {detailRow[col] == null ? "NULL" : String(detailRow[col])}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Explain Plan view */}
            {viewMode === "explain" && explainLoading && (
              <div className="flex items-center justify-center h-full text-muted-foreground gap-2">
                <Loader2 className="h-5 w-5 animate-spin text-[#E8453C]" />
                <span className="text-sm">Running EXPLAIN...</span>
              </div>
            )}
            {viewMode === "explain" && !explainLoading && explainResults && explainResults.length > 0 && (
              <ExplainTree data={explainResults} />
            )}
            {viewMode === "explain" && !explainLoading && (!explainResults || explainResults.length === 0) && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <Info className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Write a SQL query and switch to Plan tab to see the execution plan</p>
              </div>
            )}

            {/* Profiler view — computed from current results */}
            {viewMode === "profile" && results && results.length > 0 && (() => {
              const cols = Object.keys(results[0]);
              const profiles = cols.map(col => {
                const vals = results.map(r => r[col]);
                const nonNull = vals.filter(v => v != null);
                const nullCount = vals.length - nonNull.length;
                const distinct = new Set(nonNull.map(String)).size;
                const nums = nonNull.map(Number).filter(n => !isNaN(n));
                const strs = nonNull.map(String);
                return {
                  column: col,
                  type: inferColumnType(vals.slice(0, 5)),
                  total: vals.length,
                  nulls: nullCount,
                  nullPct: ((nullCount / vals.length) * 100).toFixed(1),
                  distinct,
                  distinctPct: ((distinct / vals.length) * 100).toFixed(1),
                  min: nums.length ? Math.min(...nums) : strs.sort()[0] ?? "—",
                  max: nums.length ? Math.max(...nums) : strs.sort().reverse()[0] ?? "—",
                  avg: nums.length ? (nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2) : "—",
                  sample: String(nonNull[0] ?? "NULL"),
                };
              });
              return (
                <div className="overflow-auto h-full">
                  <table className="w-full text-[11px] font-mono">
                    <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                      <tr className="border-b border-border">
                        {["Column", "Type", "Total", "Nulls", "Null%", "Distinct", "Dist%", "Min", "Max", "Avg", "Sample"].map(h => (
                          <th key={h} className="px-3 py-2 text-left text-[10px] text-muted-foreground font-semibold">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {profiles.map((p, i) => (
                        <tr key={p.column} className={`border-b border-border/30 hover:bg-accent/30 ${i % 2 === 1 ? "bg-muted/20" : ""}`}>
                          <td className="px-3 py-1.5 font-semibold text-foreground">{p.column}</td>
                          <td className="px-3 py-1.5"><Badge variant="outline" className="text-[9px]">{p.type}</Badge></td>
                          <td className="px-3 py-1.5">{p.total}</td>
                          <td className="px-3 py-1.5">{p.nulls > 0 ? <span className="text-amber-500">{p.nulls}</span> : <span className="text-green-500">0</span>}</td>
                          <td className="px-3 py-1.5">
                            <div className="flex items-center gap-1">
                              <div className="w-12 h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-amber-500 rounded-full" style={{ width: `${p.nullPct}%` }} /></div>
                              <span className="text-[9px]">{p.nullPct}%</span>
                            </div>
                          </td>
                          <td className="px-3 py-1.5">{p.distinct}</td>
                          <td className="px-3 py-1.5">
                            <div className="flex items-center gap-1">
                              <div className="w-12 h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-[#E8453C] rounded-full" style={{ width: `${p.distinctPct}%` }} /></div>
                              <span className="text-[9px]">{p.distinctPct}%</span>
                            </div>
                          </td>
                          <td className="px-3 py-1.5 truncate max-w-[100px]">{String(p.min)}</td>
                          <td className="px-3 py-1.5 truncate max-w-[100px]">{String(p.max)}</td>
                          <td className="px-3 py-1.5">{String(p.avg)}</td>
                          <td className="px-3 py-1.5 truncate max-w-[120px] text-muted-foreground">{p.sample}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
            {viewMode === "profile" && (!results || results.length === 0) && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <BarChart3 className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Run a query first to see column profiles</p>
              </div>
            )}

            {/* Describe view */}
            {viewMode === "describe" && describeLoading && (
              <div className="flex items-center justify-center h-full text-muted-foreground gap-2">
                <Loader2 className="h-5 w-5 animate-spin text-[#E8453C]" />
                <span className="text-sm">Running DESCRIBE TABLE EXTENDED...</span>
              </div>
            )}
            {viewMode === "describe" && !describeLoading && describeResults && describeResults.length > 0 && (
              <div className="overflow-auto h-full">
                <table className="w-full text-[11px] font-mono">
                  <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                    <tr className="border-b border-border">
                      {Object.keys(describeResults[0]).map(h => (
                        <th key={h} className="px-3 py-2 text-left text-[10px] text-muted-foreground font-semibold">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {describeResults.map((row, i) => {
                      const vals = Object.values(row);
                      const isSection = String(vals[0] || "").startsWith("#");
                      return (
                        <tr key={i} className={`border-b border-border/30 ${isSection ? "bg-[#E8453C]/5 font-semibold" : i % 2 === 1 ? "bg-muted/20" : ""} hover:bg-accent/30`}>
                          {vals.map((v, ci) => (
                            <td key={ci} className={`px-3 py-1.5 ${isSection ? "text-[#E8453C]" : ci === 0 ? "text-foreground font-medium" : "text-muted-foreground"} truncate max-w-[300px]`}>
                              {String(v ?? "")}
                            </td>
                          ))}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
            {viewMode === "describe" && !describeLoading && !describeResults && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <Code className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Write a query with a FROM clause to see table metadata</p>
              </div>
            )}

            {/* Sample view */}
            {viewMode === "sample" && sampleLoading && (
              <div className="flex items-center justify-center h-full text-muted-foreground gap-2">
                <Loader2 className="h-5 w-5 animate-spin text-[#E8453C]" />
                <span className="text-sm">Loading sample data...</span>
              </div>
            )}
            {viewMode === "sample" && !sampleLoading && sampleResults && sampleResults.length > 0 && (
              <div className="overflow-auto h-full">
                <table className="w-full text-[12px] font-mono">
                  <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                    <tr className="border-b border-border">
                      <th className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold w-10">#</th>
                      {Object.keys(sampleResults[0]).map(col => (
                        <th key={col} className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sampleResults.map((row, i) => (
                      <tr key={i} className={`border-b border-border/30 hover:bg-accent/30 ${i % 2 === 1 ? "bg-muted/20" : ""}`}>
                        <td className="px-3 py-1.5 text-muted-foreground">{i + 1}</td>
                        {Object.values(row).map((v, ci) => (
                          <td key={ci} className={`px-3 py-1.5 truncate max-w-[200px] ${v == null ? "text-muted-foreground/40 italic" : ""}`}>
                            {v == null ? "NULL" : String(v)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="px-3 py-1.5 bg-muted/50 border-t border-border text-[10px] text-muted-foreground">
                  Sample: {sampleResults.length} rows from {getTableFromSql() || "table"}
                </div>
              </div>
            )}
            {viewMode === "sample" && !sampleLoading && (!sampleResults || sampleResults.length === 0) && !sampleLoading && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <Table2 className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Write a query with a FROM clause to preview table data</p>
              </div>
            )}

            {/* Schema Diagram view */}
            {viewMode === "schema" && schemaLoading && (
              <div className="flex items-center justify-center h-full text-muted-foreground gap-2">
                <Loader2 className="h-5 w-5 animate-spin text-[#E8453C]" />
                <span className="text-sm">Loading schema...</span>
              </div>
            )}
            {viewMode === "schema" && !schemaLoading && (
              <SchemaDiagram tables={schemaTables} />
            )}

            {!results && !error && !running && viewMode === "table" && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <Terminal className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Click a table in the catalog browser to start</p>
                <p className="text-xs mt-1">Or type a SQL query and press Ctrl+Enter</p>
              </div>
            )}
          </div>
        </div>

        {/* Right panel: Diff */}
        {showDiff && (
          <div className="w-[340px] border-l border-border bg-muted/5 overflow-hidden flex flex-col shrink-0">
            <div className="px-3 py-2.5 border-b border-border bg-muted/30 flex items-center justify-between">
              <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5"><GitCompare className="h-3 w-3" />Query Diff</p>
              <button onClick={() => setShowDiff(false)}><X className="h-3 w-3 text-muted-foreground" /></button>
            </div>
            <div className="p-2 border-b border-border">
              <textarea value={diffSql} onChange={e => setDiffSql(e.target.value)} placeholder="Enter comparison query..."
                className="w-full h-20 bg-muted/30 text-foreground font-mono text-[11px] p-2 rounded border border-border/60 resize-none focus:outline-none" />
              <Button size="sm" onClick={runDiffQuery} disabled={diffRunning || !diffSql.trim()} className="mt-1 w-full gap-1">
                {diffRunning ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />} Run & Compare
              </Button>
            </div>
            {diffResults && results && (
              <div className="flex-1 overflow-auto p-2 text-[10px]">
                <div className="flex justify-between mb-2 text-muted-foreground">
                  <span>Main: {results.length} rows</span>
                  <span>Diff: {diffResults.length} rows</span>
                </div>
                {(() => {
                  const mainCols = results.length ? Object.keys(results[0]) : [];
                  const diffCols = diffResults.length ? Object.keys(diffResults[0]) : [];
                  const addedCols = diffCols.filter(c => !mainCols.includes(c));
                  const removedCols = mainCols.filter(c => !diffCols.includes(c));
                  const rowDiff = diffResults.length - results.length;
                  return (
                    <div className="space-y-2">
                      <div className="p-2 rounded bg-muted/30 border border-border/40">
                        <p className="font-semibold text-foreground mb-1">Row Count</p>
                        <p className={rowDiff === 0 ? "text-green-500" : "text-amber-500"}>
                          {rowDiff === 0 ? "Same" : rowDiff > 0 ? `+${rowDiff} more rows` : `${rowDiff} fewer rows`}
                        </p>
                      </div>
                      <div className="p-2 rounded bg-muted/30 border border-border/40">
                        <p className="font-semibold text-foreground mb-1">Columns</p>
                        {addedCols.length > 0 && <p className="text-green-500">+ {addedCols.join(", ")}</p>}
                        {removedCols.length > 0 && <p className="text-red-500">- {removedCols.join(", ")}</p>}
                        {addedCols.length === 0 && removedCols.length === 0 && <p className="text-green-500">Same columns</p>}
                      </div>
                      {diffResults.length > 0 && results.length > 0 && (
                        <div className="p-2 rounded bg-muted/30 border border-border/40">
                          <p className="font-semibold text-foreground mb-1">Sample Comparison (row 1)</p>
                          {mainCols.filter(c => diffCols.includes(c)).slice(0, 10).map(col => {
                            const v1 = String(results[0][col] ?? "NULL");
                            const v2 = String(diffResults[0][col] ?? "NULL");
                            const same = v1 === v2;
                            return (
                              <div key={col} className="flex justify-between py-0.5 border-b border-border/20">
                                <span className="text-muted-foreground font-mono">{col}</span>
                                <span className={same ? "text-green-500" : "text-amber-500"}>{same ? "=" : `${v1} → ${v2}`}</span>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  );
                })()}
              </div>
            )}
            {!diffResults && <p className="p-4 text-xs text-muted-foreground text-center">Enter a query and click "Run & Compare" to diff against main results</p>}
          </div>
        )}

        {/* Right panel: History or Saved Queries */}
        {(showHistory || showSaved) && (
          <div className="w-64 border-l border-border bg-muted/5 overflow-hidden flex flex-col shrink-0">
            <div className="px-3 py-2.5 border-b border-border bg-muted/30 flex items-center justify-between">
              <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5">
                {showHistory ? <><Clock className="h-3 w-3" />History</> : <><BookOpen className="h-3 w-3" />Saved</>}
              </p>
              <button onClick={() => { setShowHistory(false); setShowSaved(false); }}><X className="h-3 w-3 text-muted-foreground" /></button>
            </div>
            <div className="flex-1 overflow-y-auto">
              {showHistory && history.map((h, i) => (
                <button key={i} onClick={() => { setSql(h.sql); setShowHistory(false); }}
                  className="w-full text-left px-3 py-2 border-b border-border/30 hover:bg-accent/30 transition-colors">
                  <p className="text-[11px] font-mono text-foreground truncate">{h.sql}</p>
                  <p className="text-[9px] text-muted-foreground mt-0.5">{new Date(h.ts).toLocaleString()}{h.rows != null ? ` · ${h.rows} rows` : ""}{h.elapsed != null ? ` · ${(h.elapsed / 1000).toFixed(1)}s` : ""}</p>
                </button>
              ))}
              {showHistory && history.length === 0 && <p className="text-xs text-muted-foreground px-3 py-4 text-center">No history yet</p>}
              {showSaved && savedQueries.map((q) => (
                <div key={q.name} className="flex items-center gap-1 px-3 py-2 border-b border-border/30 hover:bg-accent/30">
                  <button onClick={() => { setSql(q.sql); setShowSaved(false); }} className="flex-1 text-left min-w-0">
                    <p className="text-[11px] font-medium text-foreground truncate">{q.name}</p>
                    <p className="text-[9px] font-mono text-muted-foreground truncate">{q.sql}</p>
                  </button>
                  <button onClick={() => deleteSaved(q.name)} className="p-1 rounded hover:bg-red-500/10" title="Delete"><Trash2 className="h-3 w-3 text-muted-foreground hover:text-red-500" /></button>
                </div>
              ))}
              {showSaved && savedQueries.length === 0 && <p className="text-xs text-muted-foreground px-3 py-4 text-center">No saved queries. Press Ctrl+S to save.</p>}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
