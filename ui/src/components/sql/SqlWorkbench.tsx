// @ts-nocheck
/**
 * SqlWorkbench — Reusable catalog browser + SQL query editor.
 *
 * Left panel: tree view of Catalogs → Schemas → Tables (click to insert FQN into query)
 * Right panel: SQL textarea, Run button, results table
 */
import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import {
  ChevronRight, ChevronDown, Database, FolderOpen, Table2,
  Play, Loader2, Copy, Download, Terminal, PanelBottomOpen,
  PanelBottomClose, Search, X, Zap,
} from "lucide-react";

// ── Catalog Tree ─────────────────────────────────────────────────────────────

interface TreeNode {
  name: string;
  type: "catalog" | "schema" | "table";
  fqn: string;
  children?: TreeNode[];
  loaded?: boolean;
}

function CatalogTree({ onSelect }: { onSelect: (fqn: string) => void }) {
  const [catalogs, setCatalogs] = useState<TreeNode[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [filter, setFilter] = useState("");

  useEffect(() => {
    api.get("/catalogs").then((data: string[]) => {
      setCatalogs((Array.isArray(data) ? data : []).map((c) => ({
        name: c, type: "catalog" as const, fqn: c, children: [], loaded: false,
      })));
    }).catch(() => {});
  }, []);

  async function toggleExpand(node: TreeNode) {
    const key = node.fqn;
    if (expanded.has(key)) {
      setExpanded((prev) => { const n = new Set(prev); n.delete(key); return n; });
      return;
    }

    // Load children if needed
    if (!node.loaded) {
      setLoading((prev) => new Set(prev).add(key));
      try {
        if (node.type === "catalog") {
          const schemas = await api.get(`/catalogs/${node.name}/schemas`);
          node.children = (Array.isArray(schemas) ? schemas : []).map((s: string) => ({
            name: s, type: "schema" as const, fqn: `${node.name}.${s}`, children: [], loaded: false,
          }));
        } else if (node.type === "schema") {
          const parts = node.fqn.split(".");
          const tables = await api.get(`/catalogs/${parts[0]}/${parts[1]}/tables`);
          node.children = (Array.isArray(tables) ? tables : []).map((t: string) => ({
            name: t, type: "table" as const, fqn: `${parts[0]}.${parts[1]}.${t}`,
          }));
        }
        node.loaded = true;
      } catch {}
      setLoading((prev) => { const n = new Set(prev); n.delete(key); return n; });
    }

    setExpanded((prev) => new Set(prev).add(key));
    setCatalogs([...catalogs]); // force re-render
  }

  function renderNode(node: TreeNode, depth: number = 0) {
    if (filter && !node.fqn.toLowerCase().includes(filter.toLowerCase())) {
      // Show if any child matches
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
            title={`Drag to query editor, or click to insert: ${node.fqn}`}
            draggable
            onDragStart={(e) => {
              e.dataTransfer.setData("text/plain", `SELECT * FROM ${node.fqn} LIMIT 100`);
              e.dataTransfer.setData("application/x-table-fqn", node.fqn);
              e.dataTransfer.effectAllowed = "copy";
              // Create a drag image
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
            title={`Expand ${node.name}`}
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

/** SVG icons matching Databricks column type indicators */
function TypeIcon({ type }: { type: ColType }) {
  // All numeric types → same blue icon
  // String → green, Boolean → purple, Date/Timestamp → amber, Complex → teal
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
    <span
      className="inline-flex items-center justify-center w-[20px] h-[18px] rounded shrink-0"
      style={{ backgroundColor: c.bg, color: c.fg }}
      title={c.label}
    >
      <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" stroke="none">
        {c.svg}
      </svg>
    </span>
  );
}

// ── SQL Editor + Results ─────────────────────────────────────────────────────

export default function SqlWorkbench() {
  const [open, setOpen] = useState(false);
  const [sql, setSql] = useState("");
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Execution mode: "spark" (serverless) or "warehouse" (SQL warehouse)
  const [execMode, setExecMode] = useState<"spark" | "warehouse">("spark");
  const [warehouses, setWarehouses] = useState<any[]>([]);
  const [selectedWarehouse, setSelectedWarehouse] = useState("");
  const [sparkAvailable, setSparkAvailable] = useState(false);
  const [startingWarehouse, setStartingWarehouse] = useState(false);

  function loadWarehouses() {
    api.get("/auth/warehouses").then((wh: any[]) => {
      const list = Array.isArray(wh) ? wh : [];
      setWarehouses(list);
      // Auto-select first running warehouse, or first available
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
      // Poll for status
      const poll = setInterval(async () => {
        try {
          const wh = await api.get("/auth/warehouses");
          const list = Array.isArray(wh) ? wh : [];
          setWarehouses(list);
          const target = list.find((w: any) => w.id === selectedWarehouse);
          if (target?.state === "RUNNING") {
            clearInterval(poll);
            setStartingWarehouse(false);
          }
        } catch {}
      }, 3000);
      // Timeout after 2 minutes
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
    setSql(before + insert + after);
    setTimeout(() => { ta.selectionStart = ta.selectionEnd = start + insert.length; ta.focus(); }, 0);
  }

  async function runQuery() {
    if (!sql.trim()) return;

    // Check warehouse is running when in warehouse mode
    if (execMode === "warehouse" && !whRunning) {
      setError(`Warehouse is ${whState || "not selected"}. Start it first or switch to Serverless mode.`);
      return;
    }

    setRunning(true);
    setResults(null);
    setError(null);
    const start = Date.now();
    try {
      const payload: any = { sql: sql.trim() };
      let endpoint: string;

      if (execMode === "spark") {
        endpoint = "/reconciliation/execute-sql";
        payload.use_spark = true;
      } else {
        endpoint = "/reconciliation/execute-sql";
        payload.use_spark = false;
        payload.warehouse_id = selectedWarehouse;
      }

      const data = await api.post(endpoint, payload);
      setResults(Array.isArray(data) ? data : []);
      setElapsed(Date.now() - start);
    } catch (e: any) {
      setError(e.message || "Query failed");
      setElapsed(Date.now() - start);
    } finally {
      setRunning(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
  }

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
    const csv = [headers, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "query_results.csv"; a.click();
    URL.revokeObjectURL(url);
  }

  // Panel height + drag resize + fullscreen
  const [panelHeight, setPanelHeight] = useState(50); // percentage of viewport
  const [isFullscreen, setIsFullscreen] = useState(false);
  const isDragging = useRef(false);

  function handleDragStart(e: React.MouseEvent) {
    e.preventDefault();
    isDragging.current = true;
    const startY = e.clientY;
    const startHeight = panelHeight;

    function onMove(ev: MouseEvent) {
      if (!isDragging.current) return;
      const deltaY = startY - ev.clientY;
      const deltaPct = (deltaY / window.innerHeight) * 100;
      const newHeight = Math.min(95, Math.max(20, startHeight + deltaPct));
      setPanelHeight(newHeight);
    }
    function onUp() {
      isDragging.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    }
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }

  function toggleFullscreen() {
    if (isFullscreen) {
      setIsFullscreen(false);
      setPanelHeight(50);
    } else {
      setIsFullscreen(true);
      setPanelHeight(95);
    }
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="fixed bottom-4 right-4 z-50 flex items-center gap-2 px-4 py-2.5 bg-[#E8453C]/5 text-[#E8453C] text-sm font-medium rounded-lg shadow-lg hover:bg-[#E8453C]/10 transition-colors border border-[#E8453C]/20"
      >
        <Terminal className="h-4 w-4 text-[#E8453C]" />
        SQL Workbench
        <PanelBottomOpen className="h-3.5 w-3.5 text-[#E8453C]/50" />
      </button>
    );
  }

  const columns = results?.length ? Object.keys(results[0]) : [];
  const columnTypes: Record<string, ColType> = {};
  if (results?.length) {
    for (const col of columns) {
      const sampleValues = results.slice(0, 5).map((r) => r[col]);
      columnTypes[col] = inferColumnType(sampleValues);
    }
  }

  return (
    <div
      className="fixed bottom-0 left-0 right-0 z-50 bg-background border-t-2 border-border shadow-2xl flex flex-col"
      style={{ height: `${panelHeight}vh`, transition: isDragging.current ? "none" : "height 0.2s ease" }}
    >
      {/* Drag handle to resize */}
      <div
        className="h-1.5 cursor-row-resize group flex items-center justify-center hover:bg-[#E8453C]/10 active:bg-[#E8453C]/20 shrink-0"
        onMouseDown={handleDragStart}
        onDoubleClick={toggleFullscreen}
        title="Drag to resize, double-click to toggle fullscreen"
      >
        <div className="w-10 h-1 rounded-full bg-border group-hover:bg-[#E8453C]/50 transition-colors" />
      </div>

      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-border bg-muted/30 shrink-0">
        <div className="flex items-center gap-3">
          <Terminal className="h-4 w-4 text-[#E8453C]" />
          <span className="text-[13px] font-semibold">SQL Workbench</span>

          <div className="w-px h-5 bg-border" style={{ marginInline: "44px" }} />

          {/* Execution mode toggle */}
          <div className="flex items-center bg-muted rounded-lg p-1 border border-border">
            <button
              onClick={() => setExecMode("spark")}
              disabled={!sparkAvailable}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                execMode === "spark"
                  ? "bg-[#E8453C] text-white shadow-sm"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/80"
              } ${!sparkAvailable ? "opacity-40 cursor-not-allowed" : ""}`}
              title={sparkAvailable ? "Run via Spark Connect (serverless)" : "Spark not connected"}
            >
              <Zap className="h-3.5 w-3.5" /> Serverless
            </button>
            <button
              onClick={() => setExecMode("warehouse")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                execMode === "warehouse"
                  ? "bg-[#E8453C] text-white shadow-sm"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/80"
              }`}
              title="Run via SQL Warehouse"
            >
              <Database className="h-3.5 w-3.5" /> SQL Warehouse
            </button>
          </div>

          {/* Warehouse selector (only when warehouse mode) */}
          {execMode === "warehouse" && (
            <div className="flex items-center gap-1.5">
              <select
                value={selectedWarehouse}
                onChange={(e) => setSelectedWarehouse(e.target.value)}
                className="h-8 text-xs bg-background border border-border rounded-md px-2 focus:outline-none focus:ring-2 focus:ring-[#E8453C]/30 min-w-[180px]"
              >
                {warehouses.length === 0 && <option value="">No warehouses</option>}
                {warehouses.map((wh) => (
                  <option key={wh.id} value={wh.id}>{wh.name} ({wh.state})</option>
                ))}
              </select>
              {/* State indicator */}
              <span className={`inline-flex items-center gap-1 text-[10px] font-semibold px-1.5 py-0.5 rounded ${
                whRunning ? "text-green-500 bg-green-500/10" :
                whState === "STARTING" || startingWarehouse ? "text-amber-500 bg-amber-500/10" :
                "text-muted-foreground bg-muted/50"
              }`}>
                <span className={`h-1.5 w-1.5 rounded-full ${
                  whRunning ? "bg-green-500" :
                  whState === "STARTING" || startingWarehouse ? "bg-amber-500 animate-pulse" :
                  "bg-muted-foreground"
                }`} />
                {startingWarehouse ? "STARTING" : whState || "—"}
              </span>
              {/* Start button */}
              {!whRunning && !startingWarehouse && selectedWarehouse && (
                <Button size="sm" variant="outline" className="h-7 text-[11px] gap-1" onClick={startWarehouse}>
                  <Play className="h-3 w-3" /> Start
                </Button>
              )}
              {startingWarehouse && (
                <span className="flex items-center gap-1 text-[11px] text-amber-500">
                  <Loader2 className="h-3 w-3 animate-spin" /> Starting...
                </span>
              )}
              {/* Refresh */}
              <button onClick={loadWarehouses} className="p-1 rounded hover:bg-accent/50 text-muted-foreground" title="Refresh warehouse list">
                <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 21h5v-5"/></svg>
              </button>
            </div>
          )}

          {elapsed != null && (
            <Badge variant="outline" className="text-[10px]">{(elapsed / 1000).toFixed(2)}s</Badge>
          )}
          {results && (
            <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">{results.length} rows</Badge>
          )}
        </div>
        <div className="flex items-center gap-1">
          {results?.length > 0 && (
            <>
              <Button size="sm" variant="ghost" onClick={copyResults} title="Copy to clipboard"><Copy className="h-3.5 w-3.5" /></Button>
              <Button size="sm" variant="ghost" onClick={downloadCsv} title="Download CSV"><Download className="h-3.5 w-3.5" /></Button>
            </>
          )}
          <Button size="sm" variant="ghost" onClick={toggleFullscreen} title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>
            {isFullscreen ? (
              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
            ) : (
              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
            )}
          </Button>
          <Button size="sm" variant="ghost" onClick={() => { setOpen(false); setIsFullscreen(false); setPanelHeight(50); }}><PanelBottomClose className="h-3.5 w-3.5" /></Button>
        </div>
      </div>

      {/* Body: Catalog tree + Editor + Results */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left: Catalog Browser */}
        <div className="w-64 border-r border-border bg-muted/10 overflow-hidden flex flex-col shrink-0">
          <div className="px-3 py-2 border-b border-border">
            <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Catalog Browser</p>
          </div>
          <CatalogTree onSelect={insertAtCursor} />
        </div>

        {/* Right: Query editor + results */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* SQL Input */}
          <div
            className="border-b border-border p-2 flex gap-2 relative"
            onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); e.dataTransfer.dropEffect = "copy"; setDragOver(true); }}
            onDragLeave={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragOver(false); }}
            onDrop={(e) => {
              e.preventDefault();
              e.stopPropagation();
              setDragOver(false);
              const fqn = e.dataTransfer.getData("application/x-table-fqn");
              const plainText = e.dataTransfer.getData("text/plain");
              if (fqn) {
                insertAtCursor(fqn);
              } else if (plainText && plainText.includes(".")) {
                insertAtCursor(plainText);
              }
            }}
          >
            {dragOver && (
              <div className="absolute inset-2 z-10 rounded-lg border-2 border-dashed border-[#E8453C] bg-[#E8453C]/10 flex items-center justify-center pointer-events-none">
                <span className="text-sm font-medium text-[#E8453C]">Drop table here to generate SELECT query</span>
              </div>
            )}
            <textarea
              ref={textareaRef}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Enter SQL query... (Ctrl+Enter to run, or drag a table here)"
              className="flex-1 bg-[#1C1C1C] text-green-400 font-mono text-[13px] leading-relaxed p-3 rounded-lg border border-border resize-none focus:outline-none focus:ring-1 focus:ring-[#E8453C]/30 min-h-[80px] max-h-[140px]"
              rows={4}
            />
            <div className="flex flex-col gap-1 shrink-0">
              <Button size="sm" onClick={runQuery} disabled={running || !sql.trim()} className="gap-1.5">
                {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                Run
              </Button>
              <Button size="sm" variant="outline" onClick={() => { setSql(""); setResults(null); setError(null); }}>Clear</Button>
            </div>
          </div>

          {/* Results */}
          <div className="flex-1 overflow-auto">
            {error && (
              <div className="p-3 text-sm text-red-400 font-mono bg-red-500/5">{error}</div>
            )}
            {results && results.length === 0 && !error && (
              <div className="p-4 text-sm text-muted-foreground text-center">Query returned no rows.</div>
            )}
            {results && results.length > 0 && (
              <table className="w-full text-[12px] font-mono">
                <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                  <tr className="border-b border-border">
                    <th className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold w-10">#</th>
                    {columns.map((col) => (
                      <th key={col} className="px-3 py-2 text-left text-[11px] text-muted-foreground font-semibold">
                        <span className="flex items-center gap-1.5">
                          <TypeIcon type={columnTypes[col] || "string"} />
                          {col}
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, i) => (
                    <tr key={i} className="border-b border-border/30 hover:bg-accent/30">
                      <td className="px-3 py-1.5 text-muted-foreground">{i + 1}</td>
                      {columns.map((col) => (
                        <td key={col} className="px-3 py-1.5 truncate max-w-[250px]">{String(row[col] ?? "NULL")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {!results && !error && !running && (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <Terminal className="h-8 w-8 mb-2 opacity-20" />
                <p className="text-sm">Click a table in the catalog browser to start</p>
                <p className="text-xs mt-1">Or type a SQL query and press Ctrl+Enter</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
