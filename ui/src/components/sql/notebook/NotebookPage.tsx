// @ts-nocheck
/**
 * NotebookPage — Multi-cell SQL + Markdown notebook for Data Lab.
 *
 * Features: Catalog browser sidebar, execution counter, parameterized cells,
 * cell duplication, auto-save, table of contents, keyboard shortcuts, output collapse
 */
import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { api } from "@/lib/api-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Play, Plus, Save, FolderOpen, FileDown, FileUp, Trash2,
  ChevronUp, ChevronDown, ChevronRight, GripVertical, Code, Type,
  Loader2, X, BookOpen, Copy, PanelLeftClose, PanelLeftOpen,
  ListTree, Settings2, ChevronsUpDown, Search, Presentation, FileCode, LayoutTemplate,
} from "lucide-react";
import { toast } from "sonner";
import { downloadFile } from "@/lib/pdf-export";
import { exportNotebookAsHTML } from "@/lib/notebook-html-export";
import NotebookSearchBar from "./NotebookSearchBar";
import PresentationMode from "./PresentationMode";
import { notebookTemplates } from "./notebookTemplates";
import {
  useNotebook, loadNotebooks, deleteNotebook, extractParams, interpolateParams, extractToc,
  type SavedNotebook,
} from "@/hooks/useNotebook";
import MarkdownCell from "./MarkdownCell";
import SqlCell from "./SqlCell";

// Reuse CatalogTree from SqlWorkbench — inline a simplified version for notebooks
function CatalogBrowser({ onInsert }: { onInsert: (fqn: string) => void }) {
  const [catalogs, setCatalogs] = useState<{ name: string; schemas?: { name: string; tables?: string[] }[] }[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [filter, setFilter] = useState("");

  useEffect(() => {
    api.get("/catalogs").then((data: string[]) => {
      setCatalogs((Array.isArray(data) ? data : []).map(c => ({ name: c })));
    }).catch(() => {});
  }, []);

  async function toggleCatalog(cat: string) {
    if (expanded.has(cat)) {
      setExpanded(prev => { const n = new Set(prev); n.delete(cat); return n; });
      return;
    }
    setLoading(prev => new Set(prev).add(cat));
    try {
      const schemas: string[] = await api.get(`/catalogs/${cat}/schemas`);
      setCatalogs(prev => prev.map(c => c.name === cat ? { ...c, schemas: (schemas || []).map(s => ({ name: s })) } : c));
    } catch {}
    setLoading(prev => { const n = new Set(prev); n.delete(cat); return n; });
    setExpanded(prev => new Set(prev).add(cat));
  }

  async function toggleSchema(cat: string, schema: string) {
    const key = `${cat}.${schema}`;
    if (expanded.has(key)) {
      setExpanded(prev => { const n = new Set(prev); n.delete(key); return n; });
      return;
    }
    setLoading(prev => new Set(prev).add(key));
    try {
      const tables: string[] = await api.get(`/catalogs/${cat}/${schema}/tables`);
      setCatalogs(prev => prev.map(c => c.name === cat ? {
        ...c, schemas: (c.schemas || []).map(s => s.name === schema ? { ...s, tables: tables || [] } : s)
      } : c));
    } catch {}
    setLoading(prev => { const n = new Set(prev); n.delete(key); return n; });
    setExpanded(prev => new Set(prev).add(key));
  }

  const filteredCatalogs = filter
    ? catalogs.filter(c => c.name.toLowerCase().includes(filter.toLowerCase()) ||
        c.schemas?.some(s => s.name.toLowerCase().includes(filter.toLowerCase()) ||
          s.tables?.some(t => t.toLowerCase().includes(filter.toLowerCase()))))
    : catalogs;

  return (
    <div className="flex flex-col h-full text-[11px]">
      <div className="px-2 py-1.5 border-b border-border">
        <input value={filter} onChange={e => setFilter(e.target.value)}
          placeholder="Filter catalogs..."
          className="w-full text-[10px] bg-muted/30 border border-border/60 rounded px-2 py-1 focus:outline-none focus:border-[#E8453C]/40" />
      </div>
      <div className="flex-1 overflow-y-auto px-1 py-1">
        {filteredCatalogs.map(cat => (
          <div key={cat.name}>
            <button className="flex items-center gap-1 w-full px-1.5 py-1 rounded hover:bg-accent/30 text-left"
              onClick={() => toggleCatalog(cat.name)}>
              {loading.has(cat.name) ? <Loader2 className="h-3 w-3 animate-spin shrink-0" /> :
                expanded.has(cat.name) ? <ChevronDown className="h-3 w-3 shrink-0" /> : <ChevronRight className="h-3 w-3 shrink-0" />}
              <span className="font-medium text-foreground truncate">{cat.name}</span>
            </button>
            {expanded.has(cat.name) && cat.schemas?.map(schema => (
              <div key={schema.name} className="ml-3">
                <button className="flex items-center gap-1 w-full px-1.5 py-0.5 rounded hover:bg-accent/30 text-left"
                  onClick={() => toggleSchema(cat.name, schema.name)}>
                  {loading.has(`${cat.name}.${schema.name}`) ? <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0" /> :
                    expanded.has(`${cat.name}.${schema.name}`) ? <ChevronDown className="h-2.5 w-2.5 shrink-0" /> : <ChevronRight className="h-2.5 w-2.5 shrink-0" />}
                  <span className="text-muted-foreground truncate">{schema.name}</span>
                </button>
                {expanded.has(`${cat.name}.${schema.name}`) && schema.tables?.map(table => (
                  <button key={table}
                    className="flex items-center gap-1 w-full ml-3 px-1.5 py-0.5 rounded hover:bg-accent/30 text-left text-[10px]"
                    onClick={() => onInsert(`${cat.name}.${schema.name}.${table}`)}
                    title={`Click to insert SELECT * FROM ${cat.name}.${schema.name}.${table}`}>
                    <span className="text-[#E8453C]/60 shrink-0">T</span>
                    <span className="truncate">{table}</span>
                  </button>
                ))}
              </div>
            ))}
          </div>
        ))}
        {filteredCatalogs.length === 0 && (
          <p className="text-[10px] text-muted-foreground text-center py-4">No catalogs found</p>
        )}
      </div>
    </div>
  );
}

export default function NotebookPage() {
  const {
    state, addCell, deleteCell, duplicateCell, moveCell, updateCell,
    setTitle, loadNotebook, newNotebook, save, setRunningAll,
    incrementExecution, setParams, setFocusedCell, toggleCollapse,
    reorderCells, importCells, undo, redo,
  } = useNotebook();
  const [showLoad, setShowLoad] = useState(false);
  const [showSidebar, setShowSidebar] = useState(true);
  const [showToc, setShowToc] = useState(false);
  const [showParams, setShowParams] = useState(false);
  const [showSearch, setShowSearch] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showTemplates, setShowTemplates] = useState(false);
  const [presentationMode, setPresentationMode] = useState(false);
  const [dragIdx, setDragIdx] = useState<number | null>(null);
  const [dropIdx, setDropIdx] = useState<number | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const cellRefs = useRef<Record<string, HTMLDivElement | null>>({});

  // Extract parameters and ToC
  const paramNames = useMemo(() => extractParams(state.cells), [state.cells]);
  const toc = useMemo(() => extractToc(state.cells), [state.cells]);

  // Show params bar automatically when params are detected
  useEffect(() => {
    if (paramNames.length > 0 && !showParams) setShowParams(true);
  }, [paramNames.length]);

  // Run a single SQL cell with param interpolation and execution counter
  const runCell = useCallback(async (cellId: string) => {
    const cell = state.cells.find(c => c.id === cellId);
    if (!cell || cell.type !== "sql" || !cell.content.trim()) return;
    updateCell(cellId, { running: true, error: null, results: null, elapsed: null });
    incrementExecution(cellId);
    const sqlWithParams = interpolateParams(cell.content.trim(), state.params);
    const start = Date.now();
    try {
      const res = await api.post("/reconciliation/execute-sql", { sql: sqlWithParams });
      const elapsed = Date.now() - start;
      if (res.error) {
        updateCell(cellId, { running: false, error: res.error, elapsed });
      } else {
        updateCell(cellId, { running: false, results: res.results || res, elapsed });
      }
    } catch (e: any) {
      updateCell(cellId, { running: false, error: e.message || "Execution failed", elapsed: Date.now() - start });
    }
  }, [state.cells, state.params, updateCell, incrementExecution]);

  // Run all SQL cells sequentially
  const runAll = useCallback(async () => {
    setRunningAll(true);
    for (const cell of state.cells) {
      if (cell.type === "sql" && cell.content.trim()) {
        await runCell(cell.id);
      }
    }
    setRunningAll(false);
    toast.success("All cells executed");
  }, [state.cells, runCell, setRunningAll]);

  // Save handler
  const handleSave = useCallback(() => {
    save();
    toast.success("Notebook saved");
  }, [save]);

  // Export as .sql
  const exportSql = useCallback(() => {
    const lines: string[] = [];
    for (const cell of state.cells) {
      if (cell.type === "markdown") {
        lines.push(cell.content.split("\n").map(l => `-- ${l}`).join("\n"));
      } else {
        lines.push(cell.content);
      }
      lines.push("");
    }
    const blob = new Blob([lines.join("\n")], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${state.title.replace(/\s+/g, "_")}.sql`;
    a.click();
    URL.revokeObjectURL(url);
  }, [state]);

  // Import SQL file
  const handleImportSql = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const text = reader.result as string;
      const statements = text.split(";").map(s => s.trim()).filter(s => s.length > 0);
      const cells = statements.map(stmt => {
        const isComment = stmt.split("\n").every(l => l.trim().startsWith("--") || !l.trim());
        const id = Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
        return {
          id,
          type: (isComment ? "markdown" : "sql") as "sql" | "markdown",
          content: isComment ? stmt.split("\n").map(l => l.replace(/^--\s?/, "")).join("\n") : stmt,
        };
      });
      importCells(cells);
      toast.success(`Imported ${cells.length} cells from ${file.name}`);
    };
    reader.readAsText(file);
    e.target.value = "";
  }, [importCells]);

  // Export as HTML report
  const exportHtml = useCallback(() => {
    const html = exportNotebookAsHTML(state.title, state.cells, state.params);
    downloadFile(html, `${state.title.replace(/\s+/g, "_")}.html`, "text/html");
    toast.success("HTML report exported");
  }, [state]);

  // Load template
  const loadTemplate = useCallback((template: typeof notebookTemplates[0]) => {
    const cells = template.cells.map(c => ({
      ...c,
      id: Math.random().toString(36).slice(2, 10) + Date.now().toString(36),
    }));
    loadNotebook({ id: "", title: template.name, cells, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
    setShowTemplates(false);
    toast.success(`Loaded template: ${template.name}`);
  }, [loadNotebook]);

  // Insert FQN from catalog browser into focused cell
  const insertFromCatalog = useCallback((fqn: string) => {
    const focused = state.focusedCellId || state.cells.find(c => c.type === "sql")?.id;
    if (focused) {
      const cell = state.cells.find(c => c.id === focused);
      if (cell) {
        updateCell(focused, { content: cell.content ? `${cell.content}\nSELECT * FROM ${fqn} LIMIT 100` : `SELECT * FROM ${fqn} LIMIT 100` });
      }
    } else {
      addCell("sql");
      // Will be set after render
      setTimeout(() => {
        const lastSql = state.cells.filter(c => c.type === "sql").pop();
        if (lastSql) updateCell(lastSql.id, { content: `SELECT * FROM ${fqn} LIMIT 100` });
      }, 50);
    }
    toast.success(`Inserted: ${fqn}`);
  }, [state.cells, state.focusedCellId, updateCell, addCell]);

  // Scroll to cell (for ToC)
  const scrollToCell = useCallback((cellId: string) => {
    cellRefs.current[cellId]?.scrollIntoView({ behavior: "smooth", block: "start" });
    setFocusedCell(cellId);
  }, [setFocusedCell]);

  // Global keyboard shortcuts
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      // Ctrl/Cmd + S: Save
      if ((e.ctrlKey || e.metaKey) && e.key === "s") {
        e.preventDefault();
        handleSave();
      }
      // Ctrl/Cmd + F: Find
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        setShowSearch(true);
      }
      // Ctrl/Cmd + Z: Undo
      if ((e.ctrlKey || e.metaKey) && e.key === "z" && !e.shiftKey) {
        e.preventDefault();
        undo();
      }
      // Ctrl/Cmd + Shift + Z: Redo
      if ((e.ctrlKey || e.metaKey) && e.key === "z" && e.shiftKey) {
        e.preventDefault();
        redo();
      }
      // Shift + Enter: Run focused cell and advance to next
      if (e.shiftKey && e.key === "Enter" && !e.ctrlKey && !e.metaKey) {
        const focused = state.focusedCellId;
        if (focused) {
          const cell = state.cells.find(c => c.id === focused);
          if (cell?.type === "sql") {
            e.preventDefault();
            runCell(focused);
            // Advance focus to next cell
            const idx = state.cells.findIndex(c => c.id === focused);
            if (idx < state.cells.length - 1) {
              setFocusedCell(state.cells[idx + 1].id);
              cellRefs.current[state.cells[idx + 1].id]?.scrollIntoView({ behavior: "smooth", block: "nearest" });
            }
          }
        }
      }
      // Escape: Blur editor
      if (e.key === "Escape") {
        (document.activeElement as HTMLElement)?.blur();
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [state.focusedCellId, state.cells, handleSave, runCell, setFocusedCell]);

  // Presentation mode
  if (presentationMode) {
    return <PresentationMode cells={state.cells} onExit={() => setPresentationMode(false)} />;
  }

  return (
    <div className="flex flex-col h-full bg-background">
      {/* Hidden file input for SQL import */}
      <input ref={fileInputRef} type="file" accept=".sql,.txt" className="hidden" onChange={handleImportSql} />

      {/* Toolbar */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-border bg-muted/20 shrink-0">
        <Button size="sm" variant="ghost" onClick={() => setShowSidebar(!showSidebar)} title={showSidebar ? "Hide catalog" : "Show catalog"} className="h-7 w-7 p-0">
          {showSidebar ? <PanelLeftClose className="h-3.5 w-3.5" /> : <PanelLeftOpen className="h-3.5 w-3.5" />}
        </Button>
        <BookOpen className="h-4 w-4 text-[#E8453C]" />
        <input
          value={state.title}
          onChange={e => setTitle(e.target.value)}
          className="text-sm font-semibold bg-transparent border-0 focus:outline-none focus:bg-muted/30 rounded px-1 -mx-1 min-w-[200px]"
          placeholder="Notebook title..."
        />
        {state.dirty && <Badge variant="outline" className="text-[8px] text-amber-500 border-amber-500/30">Unsaved</Badge>}
        {!state.dirty && state.id && <Badge variant="outline" className="text-[8px] text-green-500 border-green-500/30">Saved</Badge>}

        <div className="ml-auto flex items-center gap-1">
          {toc.length > 0 && (
            <Button size="sm" variant="ghost" onClick={() => setShowToc(!showToc)} title="Table of Contents" className={`gap-1 text-[10px] ${showToc ? "bg-accent" : ""}`}>
              <ListTree className="h-3 w-3" /> ToC
            </Button>
          )}
          {paramNames.length > 0 && (
            <Button size="sm" variant="ghost" onClick={() => setShowParams(!showParams)} title="Parameters" className={`gap-1 text-[10px] ${showParams ? "bg-accent" : ""}`}>
              <Settings2 className="h-3 w-3" /> Params
              <Badge variant="outline" className="text-[7px] ml-0.5">{paramNames.length}</Badge>
            </Button>
          )}
          <Button size="sm" variant="ghost" onClick={() => setShowSearch(!showSearch)} title="Find (Ctrl+F)" className={`h-7 w-7 p-0 ${showSearch ? "bg-accent" : ""}`}>
            <Search className="h-3 w-3" />
          </Button>

          <div className="w-px h-4 bg-border mx-0.5" />

          <Button size="sm" variant="ghost" onClick={() => addCell("sql")} title="Add SQL cell" className="gap-1 text-[10px]">
            <Plus className="h-3 w-3" /> SQL
          </Button>
          <Button size="sm" variant="ghost" onClick={() => addCell("markdown")} title="Add Markdown cell" className="gap-1 text-[10px]">
            <Plus className="h-3 w-3" /> MD
          </Button>

          <div className="w-px h-4 bg-border mx-0.5" />

          <Button size="sm" variant="ghost" onClick={runAll} disabled={state.runningAll} title="Run All" className="gap-1 text-[10px] text-[#E8453C]">
            {state.runningAll ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />} Run All
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setPresentationMode(true)} title="Presentation Mode" className="h-7 w-7 p-0">
            <Presentation className="h-3 w-3" />
          </Button>

          <div className="w-px h-4 bg-border mx-0.5" />

          <Button size="sm" variant="ghost" onClick={handleSave} title="Save (Ctrl+S)" className="gap-1 text-[10px]">
            <Save className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setShowLoad(true)} title="Open notebook" className="gap-1 text-[10px]">
            <FolderOpen className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setShowTemplates(true)} title="Templates" className="gap-1 text-[10px]">
            <LayoutTemplate className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={newNotebook} title="New notebook" className="gap-1 text-[10px]">
            <FileUp className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={() => fileInputRef.current?.click()} title="Import .sql file" className="gap-1 text-[10px]">
            <FileCode className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={exportSql} title="Export as .sql" className="gap-1 text-[10px]">
            <FileDown className="h-3 w-3" />
          </Button>
          <Button size="sm" variant="ghost" onClick={exportHtml} title="Export as HTML report" className="gap-1 text-[10px]">
            <FileDown className="h-3 w-3 text-[#E8453C]" />
          </Button>
        </div>
      </div>

      {/* Parameters bar */}
      {showParams && paramNames.length > 0 && (
        <div className="flex items-center gap-3 px-4 py-1.5 border-b border-border bg-blue-500/5 shrink-0">
          <span className="text-[10px] font-semibold text-blue-500">Parameters</span>
          {paramNames.map(p => (
            <div key={p} className="flex items-center gap-1">
              <label className="text-[10px] text-muted-foreground font-mono">{`{{${p}}}`}:</label>
              <input
                value={state.params[p] || ""}
                onChange={e => setParams({ ...state.params, [p]: e.target.value })}
                placeholder={p}
                className="text-[10px] font-mono bg-background border border-border rounded px-1.5 py-0.5 w-[120px] focus:outline-none focus:border-blue-500/40"
              />
            </div>
          ))}
        </div>
      )}

      {/* Search bar */}
      {showSearch && (
        <NotebookSearchBar
          cells={state.cells}
          searchQuery={searchQuery}
          onSearchChange={setSearchQuery}
          onClose={() => { setShowSearch(false); setSearchQuery(""); }}
          onNavigate={scrollToCell}
        />
      )}

      {/* Main content */}
      <div className="flex flex-1 min-h-0">
        {/* Catalog browser sidebar */}
        {showSidebar && (
          <div className="w-56 border-r border-border shrink-0 overflow-hidden">
            <div className="px-2 py-1.5 border-b border-border bg-muted/20 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
              Catalog Browser
            </div>
            <CatalogBrowser onInsert={insertFromCatalog} />
          </div>
        )}

        {/* ToC sidebar */}
        {showToc && toc.length > 0 && (
          <div className="w-48 border-r border-border shrink-0 overflow-y-auto bg-muted/5">
            <div className="px-2 py-1.5 border-b border-border bg-muted/20 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
              Contents
            </div>
            <div className="p-1.5">
              {toc.map((item, i) => (
                <button key={i} onClick={() => scrollToCell(item.cellId)}
                  className="block w-full text-left px-2 py-1 rounded hover:bg-accent/30 text-[10px] truncate"
                  style={{ paddingLeft: `${(item.level - 1) * 12 + 8}px` }}>
                  <span className={item.level === 1 ? "font-semibold text-foreground" : item.level === 2 ? "text-foreground" : "text-muted-foreground"}>
                    {item.text}
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Cells */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          {state.cells.map((cell, idx) => (
            <div key={cell.id} className={`group mb-3 ${dropIdx === idx ? "border-t-2 border-[#E8453C]" : ""}`}
              ref={el => { cellRefs.current[cell.id] = el; }}
              onClick={() => setFocusedCell(cell.id)}
              onDragOver={e => { e.preventDefault(); setDropIdx(idx); }}
              onDragLeave={() => setDropIdx(null)}
              onDrop={e => { e.preventDefault(); if (dragIdx != null && dragIdx !== idx) reorderCells(dragIdx, idx); setDragIdx(null); setDropIdx(null); }}>
              {/* Cell card */}
              <div className={`rounded-lg border ${
                cell.running ? "border-[#E8453C]/40 shadow-[0_0_8px_rgba(232,69,60,0.15)]" :
                state.focusedCellId === cell.id ? "border-[#E8453C]/30" : "border-border"
              } bg-background overflow-hidden transition-shadow`}>
                {/* Cell toolbar */}
                <div className="flex items-center gap-1 px-2 py-1 bg-muted/20 border-b border-border/50 text-[10px]">
                  <span draggable onDragStart={() => setDragIdx(idx)} onDragEnd={() => { setDragIdx(null); setDropIdx(null); }} className="cursor-grab active:cursor-grabbing">
                  <GripVertical className="h-3 w-3 text-muted-foreground/40" />
                </span>

                  {/* Execution counter */}
                  {cell.type === "sql" && (
                    <span className={`font-mono text-[9px] min-w-[24px] text-center ${cell.running ? "text-[#E8453C]" : "text-muted-foreground"}`}>
                      [{cell.running ? "*" : cell.executionCount || " "}]
                    </span>
                  )}

                  <Badge variant="outline" className={`text-[8px] ${cell.type === "sql" ? "text-[#E8453C] border-[#E8453C]/30" : "text-blue-500 border-blue-500/30"}`}>
                    {cell.type === "sql" ? "SQL" : "MD"}
                  </Badge>

                  <div className="ml-auto flex items-center gap-0.5">
                    {/* Toggle type */}
                    {cell.type === "sql" ? (
                      <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => updateCell(cell.id, { type: "markdown" })} title="Convert to Markdown">
                        <Type className="h-3 w-3" />
                      </Button>
                    ) : (
                      <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => updateCell(cell.id, { type: "sql" })} title="Convert to SQL">
                        <Code className="h-3 w-3" />
                      </Button>
                    )}
                    {/* Collapse output */}
                    {cell.type === "sql" && (cell.results?.length > 0 || cell.error) && (
                      <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => toggleCollapse(cell.id)} title={cell.collapsed ? "Expand output" : "Collapse output"}>
                        <ChevronsUpDown className="h-3 w-3" />
                      </Button>
                    )}
                    {/* Duplicate */}
                    <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => duplicateCell(cell.id)} title="Duplicate cell">
                      <Copy className="h-3 w-3" />
                    </Button>
                    {/* Move up/down */}
                    <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => moveCell(cell.id, "up")} disabled={idx === 0} title="Move up">
                      <ChevronUp className="h-3 w-3" />
                    </Button>
                    <Button size="sm" variant="ghost" className="h-5 w-5 p-0" onClick={() => moveCell(cell.id, "down")} disabled={idx === state.cells.length - 1} title="Move down">
                      <ChevronDown className="h-3 w-3" />
                    </Button>
                    {/* Delete */}
                    <Button size="sm" variant="ghost" className="h-5 w-5 p-0 text-muted-foreground hover:text-red-500" onClick={() => deleteCell(cell.id)} disabled={state.cells.length <= 1} title="Delete cell">
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </div>
                </div>

                {/* Cell content */}
                {cell.type === "sql" ? (
                  <SqlCell
                    content={cell.content}
                    onChange={content => updateCell(cell.id, { content })}
                    results={cell.results}
                    error={cell.error}
                    elapsed={cell.elapsed}
                    running={cell.running}
                    collapsed={cell.collapsed}
                    lastRunAt={cell.lastRunAt}
                    cellIndex={idx}
                    viewMode={cell.viewMode || "table"}
                    chartType={cell.chartType || "bar"}
                    chartXCol={cell.chartXCol || ""}
                    chartYCol={cell.chartYCol || ""}
                    searchQuery={searchQuery}
                    onRun={() => runCell(cell.id)}
                    onUpdateCell={patch => updateCell(cell.id, patch)}
                    onToggleCollapse={() => toggleCollapse(cell.id)}
                  />
                ) : (
                  <MarkdownCell
                    content={cell.content}
                    onChange={content => updateCell(cell.id, { content })}
                  />
                )}
              </div>

              {/* Add cell between cards */}
              <div className="flex items-center justify-center py-1 opacity-0 group-hover:opacity-100 transition-opacity">
                <button onClick={() => addCell("sql", cell.id)} className="text-[9px] text-muted-foreground hover:text-[#E8453C] px-2 py-0.5 rounded hover:bg-accent/30">+ SQL</button>
                <span className="text-muted-foreground/30 mx-1">|</span>
                <button onClick={() => addCell("markdown", cell.id)} className="text-[9px] text-muted-foreground hover:text-blue-500 px-2 py-0.5 rounded hover:bg-accent/30">+ Markdown</button>
              </div>
            </div>
          ))}

          {/* Keyboard shortcuts hint at bottom */}
          <div className="flex items-center justify-center gap-4 py-4 text-[9px] text-muted-foreground/50">
            <span><kbd className="px-1 bg-muted rounded">Ctrl+Enter</kbd> Run cell</span>
            <span><kbd className="px-1 bg-muted rounded">Shift+Enter</kbd> Run & advance</span>
            <span><kbd className="px-1 bg-muted rounded">Ctrl+S</kbd> Save</span>
            <span><kbd className="px-1 bg-muted rounded">Ctrl+F</kbd> Find</span>
            <span><kbd className="px-1 bg-muted rounded">Ctrl+Z</kbd> Undo</span>
            <span><kbd className="px-1 bg-muted rounded">Esc</kbd> Blur</span>
          </div>
        </div>
      </div>

      {/* Load dialog */}
      {showLoad && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={() => setShowLoad(false)}>
          <div className="bg-popover border border-border rounded-xl shadow-2xl w-[420px] max-h-[400px] flex flex-col" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-4 py-3 border-b border-border">
              <h3 className="text-sm font-semibold">Open Notebook</h3>
              <button onClick={() => setShowLoad(false)}><X className="h-4 w-4 text-muted-foreground" /></button>
            </div>
            <div className="flex-1 overflow-y-auto p-2">
              {loadNotebooks().length === 0 ? (
                <p className="text-xs text-muted-foreground text-center py-8">No saved notebooks yet</p>
              ) : (
                loadNotebooks().map(nb => (
                  <div key={nb.id} className="flex items-center justify-between px-3 py-2 rounded-lg hover:bg-accent/30 cursor-pointer group"
                    onClick={() => { loadNotebook(nb); setShowLoad(false); toast.success(`Loaded: ${nb.title}`); }}>
                    <div>
                      <p className="text-xs font-semibold">{nb.title}</p>
                      <p className="text-[10px] text-muted-foreground">{nb.cells.length} cells · {new Date(nb.updatedAt).toLocaleDateString()}</p>
                    </div>
                    <button className="opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-red-500 p-1"
                      onClick={(e) => { e.stopPropagation(); deleteNotebook(nb.id); toast.success("Deleted"); setShowLoad(s => s); }}>
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      {/* Templates dialog */}
      {showTemplates && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={() => setShowTemplates(false)}>
          <div className="bg-popover border border-border rounded-xl shadow-2xl w-[500px] max-h-[450px] flex flex-col" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-4 py-3 border-b border-border">
              <h3 className="text-sm font-semibold">Notebook Templates</h3>
              <button onClick={() => setShowTemplates(false)}><X className="h-4 w-4 text-muted-foreground" /></button>
            </div>
            <div className="flex-1 overflow-y-auto p-2">
              {notebookTemplates.map(t => (
                <div key={t.name} className="flex items-center justify-between px-3 py-3 rounded-lg hover:bg-accent/30 cursor-pointer"
                  onClick={() => loadTemplate(t)}>
                  <div>
                    <p className="text-xs font-semibold">{t.name}</p>
                    <p className="text-[10px] text-muted-foreground">{t.description}</p>
                    <p className="text-[9px] text-muted-foreground/60 mt-0.5">{t.cells.length} cells</p>
                  </div>
                  <LayoutTemplate className="h-4 w-4 text-muted-foreground/40 shrink-0" />
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
