// @ts-nocheck
"use client";

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Share2, Loader2, Search, Info, ChevronRight, ChevronDown,
  Database, Layers, Table2, Eye, User, Hash, Type, Clock,
  ToggleLeft, Braces, ChevronLeft, BookOpen, Briefcase,
  LayoutDashboard, GitBranch, Code2, FileText, X, Columns,
  ArrowUpCircle, ArrowDownCircle, AlertCircle, Download,
  PanelRightOpen, PanelRightClose, Plus, AlertTriangle,
  Maximize2, Minimize2, ZoomIn, ZoomOut, RotateCcw, GripVertical,
} from "lucide-react";
import { cn } from "@/lib/utils";

// ─── Constants ────────────────────────────────────────────────────────────────

const TIME_RANGES = [
  { label: "7d",  ms: 7   * 86_400_000 },
  { label: "30d", ms: 30  * 86_400_000 },
  { label: "90d", ms: 90  * 86_400_000 },
  { label: "1Y",  ms: 365 * 86_400_000 },
  { label: "All", ms: 0 },
];

const ENTITY_FILTERS = [
  { key: "all",       label: "All",        icon: Share2         },
  { key: "TABLE",     label: "Tables",     icon: Table2         },
  { key: "VIEW",      label: "Views",      icon: Eye            },
  { key: "NOTEBOOK",  label: "Notebooks",  icon: BookOpen       },
  { key: "JOB",       label: "Jobs",       icon: Briefcase      },
  { key: "DASHBOARD", label: "Dashboards", icon: LayoutDashboard},
  { key: "PIPELINE",  label: "Pipelines",  icon: GitBranch      },
  { key: "QUERY",     label: "Queries",    icon: Code2          },
];

// ─── Column type icon ─────────────────────────────────────────────────────────

function ColTypeIcon({ type = "" }) {
  const t = (type || "").toLowerCase();
  if (t.includes("timestamp") || t.includes("date"))
    return <Clock className="h-3 w-3 shrink-0 text-amber-500" />;
  if (t.includes("int") || t.includes("long") || t.includes("short") || t.includes("byte"))
    return <Hash className="h-3 w-3 shrink-0 text-blue-500" />;
  if (t.includes("double") || t.includes("float") || t.includes("decimal") || t.includes("numeric"))
    return <span className="text-[9px] font-bold text-indigo-500 shrink-0 leading-none">.0</span>;
  if (t.includes("bool"))
    return <ToggleLeft className="h-3 w-3 shrink-0 text-green-500" />;
  if (t.includes("array") || t.includes("map") || t.includes("struct"))
    return <Braces className="h-3 w-3 shrink-0 text-violet-500" />;
  return <Type className="h-3 w-3 shrink-0 text-muted-foreground" />;
}

// ─── Entity meta (icon + colour per type) ────────────────────────────────────

function entityMeta(type) {
  switch ((type || "").toUpperCase()) {
    case "VIEW":      return { Icon: Eye,              color: "text-violet-500", bg: "bg-violet-500/10" };
    case "NOTEBOOK":  return { Icon: BookOpen,         color: "text-amber-500",  bg: "bg-amber-500/10"  };
    case "JOB":       return { Icon: Briefcase,        color: "text-sky-500",    bg: "bg-sky-500/10"    };
    case "DASHBOARD": return { Icon: LayoutDashboard,  color: "text-pink-500",   bg: "bg-pink-500/10"   };
    case "PIPELINE":  return { Icon: GitBranch,        color: "text-teal-500",   bg: "bg-teal-500/10"   };
    case "QUERY":     return { Icon: Code2,            color: "text-orange-500", bg: "bg-orange-500/10" };
    case "FILE":      return { Icon: FileText,         color: "text-slate-500",  bg: "bg-slate-500/10"  };
    default:          return { Icon: Table2,           color: "text-muted-foreground", bg: "bg-muted"   };
  }
}

// ─── Non-table entity card (notebook / job / dashboard / pipeline / query) ────

function EntityCard({ entity, nodeRef }) {
  const { Icon, color, bg } = entityMeta(entity.entity_type);
  const label = entity.name || entity.entity_type || "Unknown";

  return (
    <div ref={nodeRef} className="w-[220px] shrink-0">
      <div className="rounded-xl border border-border shadow-sm overflow-hidden">
        <div className="px-3 py-3 bg-card">
          <div className="flex items-center gap-2 mb-1.5">
            <div className={cn("h-5 w-5 rounded flex items-center justify-center shrink-0", bg)}>
              <Icon className={cn("h-3 w-3", color)} />
            </div>
            <span className={cn("text-[10px] font-semibold uppercase tracking-wide", color)}>
              {entity.entity_type}
            </span>
          </div>
          <p className="text-[12px] font-medium text-foreground/90 truncate leading-snug" title={label}>
            {label}
          </p>
          {entity.job_id      && <p className="text-[10px] text-muted-foreground/60 mt-0.5">Job: {entity.job_id}</p>}
          {entity.run_id      && <p className="text-[10px] text-muted-foreground/60">Run: {entity.run_id}</p>}
          {entity.pipeline_id && <p className="text-[10px] text-muted-foreground/60">Pipeline: {entity.pipeline_id}</p>}
          {entity.dashboard_id && <p className="text-[10px] text-muted-foreground/60">ID: {entity.dashboard_id}</p>}
          {entity.workspace_id && <p className="text-[10px] text-muted-foreground/60 truncate">WS: {entity.workspace_id}</p>}
        </div>
      </div>
    </div>
  );
}

// ─── Table / View node card ───────────────────────────────────────────────────

const COL_PAGE = 8;

function TableNodeCard({
  entity,
  isTarget = false,
  onClick,
  nodeRef,
  tableInfoMap,
  selectedCol,
  onColumnClick,
  onColumnRef,
  colLineage,
  isSecondLevel = false,
  onExpand,
  isExpanded = false,
}) {
  const fqn    = entity.table_name || "";
  const parts  = fqn.split(".");
  const table  = parts.length >= 3 ? parts[2] : fqn;
  const path   = parts.length >= 2 ? parts.slice(0, -1).join(".") : "";
  const isView = (entity.entity_type || entity.table_type || "").toUpperCase().includes("VIEW");
  const navigable = !isTarget && parts.length >= 3;
  const info   = tableInfoMap[fqn] || { columns: [], owner: "", loading: false };

  const [search, setSearch] = useState("");
  const [page,   setPage]   = useState(0);
  useEffect(() => setPage(0), [search]);

  const filtered   = info.columns.filter(c =>
    (c.column_name || c.name || "").toLowerCase().includes(search.toLowerCase()),
  );
  const totalPages = Math.ceil(filtered.length / COL_PAGE);
  const pageCols   = filtered.slice(page * COL_PAGE, (page + 1) * COL_PAGE);

  // Which columns are highlighted by column lineage?
  const colHighlight = useMemo(() => {
    if (!colLineage) return { up: new Set(), down: new Set() };
    const up   = new Set(
      (colLineage.upstream_cols || []).filter(c => c.table_name === fqn).map(c => c.name),
    );
    const down = new Set(
      (colLineage.downstream_cols || []).filter(c => c.table_name === fqn).map(c => c.name),
    );
    return { up, down };
  }, [colLineage, fqn]);

  const cardWidth = isSecondLevel ? "w-[200px]" : "w-[240px]";

  return (
    <div ref={nodeRef} className={cn(cardWidth, "shrink-0", isSecondLevel && "scale-90 origin-top")}>
      <div className={cn(
        "rounded-xl border overflow-hidden",
        isTarget ? "border-primary/50 shadow-md ring-1 ring-primary/20" : "border-border shadow-sm",
      )}>
        {/* Header */}
        <button
          onClick={navigable ? onClick : undefined}
          disabled={isTarget || !navigable}
          className={cn(
            "w-full text-left px-3 pt-3 pb-2.5 transition-colors",
            isTarget    ? "bg-primary/5 cursor-default"
            : navigable ? "bg-card hover:bg-muted/40 cursor-pointer group"
            : "bg-card cursor-not-allowed opacity-70",
          )}
        >
          <div className="flex items-center gap-1.5 mb-2">
            <div className={cn("h-5 w-5 rounded flex items-center justify-center shrink-0", isTarget ? "bg-primary/15" : "bg-muted")}>
              {isView
                ? <Eye className={cn("h-3 w-3", isTarget ? "text-primary" : "text-violet-500")} />
                : <Table2 className={cn("h-3 w-3", isTarget ? "text-primary" : "text-muted-foreground")} />}
            </div>
            <span className={cn(
              "text-[10px] font-medium px-1.5 py-0.5 rounded-full border leading-none",
              isTarget ? "border-primary/30 text-primary bg-primary/10"
              : "border-border/70 text-muted-foreground bg-muted/40",
            )}>
              {isTarget ? "Target" : isView ? "View" : "Table"}
            </span>
          </div>
          <p className={cn(
            "text-sm font-semibold font-mono truncate leading-snug",
            isTarget ? "text-primary" : "text-foreground group-hover:text-primary transition-colors",
          )}>
            {table}
          </p>
          {path && <p className="text-[10px] text-muted-foreground/60 font-mono truncate mt-0.5">{path}</p>}
          {info.owner && (
            <div className="flex items-center gap-1 mt-1.5">
              <User className="h-2.5 w-2.5 text-muted-foreground/50 shrink-0" />
              <p className="text-[10px] text-muted-foreground/60 truncate">{info.owner}</p>
            </div>
          )}
        </button>

        {/* Columns section — hidden on 2nd level to save space */}
        {!isSecondLevel && (
          <div className="border-t border-border/60 bg-background/60">
            {/* Search bar */}
            <div className="px-2 py-1.5 border-b border-border/40">
              {info.loading ? (
                <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground py-0.5">
                  <Loader2 className="h-3 w-3 animate-spin" />Loading columns…
                </div>
              ) : (
                <>
                  <div className="flex items-center gap-1.5 mb-1">
                    <Search className="h-2.5 w-2.5 text-muted-foreground/50 shrink-0" />
                    <input type="text" value={search}
                      onChange={e => setSearch(e.target.value)}
                      onClick={e => e.stopPropagation()}
                      placeholder="Search columns…"
                      className="flex-1 text-[10px] bg-transparent outline-none placeholder:text-muted-foreground/40 text-foreground min-w-0"
                    />
                    {selectedCol?.tableFqn === fqn && (
                      <span className="text-[9px] text-primary font-medium shrink-0">● col</span>
                    )}
                  </div>
                  <p className="text-[10px] text-muted-foreground/50">
                    {filtered.length} column{filtered.length !== 1 ? "s" : ""}
                    {search ? ` matching "${search}"` : ""}
                  </p>
                </>
              )}
            </div>

            {/* Column rows */}
            {!info.loading && pageCols.length > 0 && (
              <div className="divide-y divide-border/30">
                {pageCols.map((col) => {
                  const colName = col.column_name || col.name || "";
                  const dtype   = col.data_type   || col.type || "";
                  const isSelected = selectedCol?.tableFqn === fqn && selectedCol?.colName === colName;
                  const isUp       = colHighlight.up.has(colName);
                  const isDown     = colHighlight.down.has(colName);
                  return (
                    <button
                      key={colName}
                      ref={el => onColumnRef?.(fqn, colName, el)}
                      onClick={e => { e.stopPropagation(); onColumnClick?.(fqn, colName); }}
                      title={`Click to view column lineage for ${colName}`}
                      className={cn(
                        "flex items-center gap-1.5 px-2.5 py-1 w-full text-left transition-colors",
                        isSelected ? "bg-primary/15 ring-1 ring-inset ring-primary/30"
                        : isUp     ? "bg-blue-500/10"
                        : isDown   ? "bg-emerald-500/10"
                        : "hover:bg-muted/50 cursor-pointer",
                      )}
                    >
                      <ColTypeIcon type={dtype} />
                      <span className={cn(
                        "text-[11px] font-mono truncate flex-1 min-w-0",
                        isSelected ? "text-primary font-semibold"
                        : isUp || isDown ? "text-foreground"
                        : "text-foreground/80",
                      )}>
                        {colName}
                      </span>
                      <span className="text-[10px] text-muted-foreground/60 shrink-0 ml-auto pl-1">{dtype}</span>
                      {isUp   && <ArrowUpCircle   className="h-2.5 w-2.5 shrink-0 text-blue-500" />}
                      {isDown && <ArrowDownCircle className="h-2.5 w-2.5 shrink-0 text-emerald-500" />}
                    </button>
                  );
                })}
              </div>
            )}

            {!info.loading && info.columns.length === 0 && (
              <p className="text-[10px] text-muted-foreground/40 italic px-2.5 py-2">No columns available</p>
            )}

            {/* Pagination */}
            {!info.loading && totalPages > 1 && (
              <div className="flex items-center justify-between px-2 py-1 border-t border-border/40">
                <button onClick={e => { e.stopPropagation(); setPage(p => Math.max(0, p - 1)); }}
                  disabled={page === 0}
                  className="flex items-center gap-0.5 text-[10px] text-muted-foreground hover:text-foreground disabled:opacity-30 transition-colors">
                  <ChevronLeft className="h-2.5 w-2.5" />Prev
                </button>
                <span className="text-[10px] text-muted-foreground/50">
                  {page * COL_PAGE + 1}–{Math.min((page + 1) * COL_PAGE, filtered.length)} / {filtered.length}
                </span>
                <button onClick={e => { e.stopPropagation(); setPage(p => Math.min(totalPages - 1, p + 1)); }}
                  disabled={page >= totalPages - 1}
                  className="flex items-center gap-0.5 text-[10px] text-muted-foreground hover:text-foreground disabled:opacity-30 transition-colors">
                  Next<ChevronRight className="h-2.5 w-2.5" />
                </button>
              </div>
            )}
          </div>
        )}

        {/* Expand button — only on non-target, non-second-level nodes */}
        {!isTarget && !isSecondLevel && onExpand && (
          <div className="border-t border-border/60 bg-background/40">
            <button
              onClick={e => { e.stopPropagation(); onExpand(fqn); }}
              title={isExpanded ? "Collapse expanded neighbors" : "Expand neighbors (2nd hop)"}
              className={cn(
                "flex items-center justify-center gap-1 w-full py-1 text-[10px] transition-colors",
                isExpanded
                  ? "text-primary bg-primary/5 hover:bg-primary/10"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/50",
              )}
            >
              <Plus className={cn("h-2.5 w-2.5 transition-transform", isExpanded && "rotate-45")} />
              {isExpanded ? "Collapse" : "Expand"}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── SVG overlay (table + column lineage bezier lines) ────────────────────────

function LineageOverlay({ tableLines, colLines }) {
  return (
    <svg className="absolute inset-0 pointer-events-none overflow-visible" style={{ width: "100%", height: "100%" }}>
      <defs>
        <marker id="arr-up"     markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" className="fill-blue-300 dark:fill-blue-700" />
        </marker>
        <marker id="arr-dn"     markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" className="fill-emerald-300 dark:fill-emerald-700" />
        </marker>
        <marker id="arr-col-up" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" fill="#3b82f6" />
        </marker>
        <marker id="arr-col-dn" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" fill="#10b981" />
        </marker>
      </defs>
      {tableLines.map((l, i) => {
        const dx = (l.x2 - l.x1) * 0.5;
        const d  = `M${l.x1},${l.y1} C${l.x1+dx},${l.y1} ${l.x2-dx},${l.y2} ${l.x2},${l.y2}`;
        return (
          <path key={`t${i}`} d={d} fill="none" strokeWidth="1.5"
            markerEnd={l.type === "upstream" ? "url(#arr-up)" : "url(#arr-dn)"}
            className={l.type === "upstream"
              ? "stroke-blue-300 dark:stroke-blue-700"
              : "stroke-emerald-300 dark:stroke-emerald-700"} />
        );
      })}
      {colLines.map((l, i) => {
        const dx = (l.x2 - l.x1) * 0.4;
        const d  = `M${l.x1},${l.y1} C${l.x1+dx},${l.y1} ${l.x2-dx},${l.y2} ${l.x2},${l.y2}`;
        return (
          <path key={`c${i}`} d={d} fill="none" strokeWidth="1.5" strokeDasharray="4 3" opacity="0.9"
            markerEnd={l.type === "upstream" ? "url(#arr-col-up)" : "url(#arr-col-dn)"}
            stroke={l.type === "upstream" ? "#3b82f6" : "#10b981"} />
        );
      })}
    </svg>
  );
}

// ─── System events panel ──────────────────────────────────────────────────────

function SystemEventsPanel({ tableName }) {
  const [rows,    setRows]    = useState(null);
  const [cols,    setCols]    = useState([]);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState("");

  function load() {
    setLoading(true); setError("");
    api.get("/assessment/lineage/system-events", { table_name: tableName, limit: 50 })
      .then(d => { setCols(d.columns || []); setRows(d.rows || []); })
      .catch(e => setError(e?.message || "Query failed"))
      .finally(() => setLoading(false));
  }

  if (!rows && !loading) return (
    <div className="px-4 py-3 border-t border-border/60">
      <button onClick={load} className="flex items-center gap-1.5 text-xs text-primary hover:underline">
        <Database className="h-3.5 w-3.5" />Query system.access.table_lineage
      </button>
      <p className="text-[10px] text-muted-foreground/60 mt-0.5 ml-5">Raw lineage events from system tables</p>
    </div>
  );

  return (
    <div className="border-t border-border/60 p-4">
      <div className="flex items-center gap-2 mb-2">
        <Database className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-xs font-semibold text-muted-foreground">system.access.table_lineage</span>
        {loading && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
        {error && <span className="text-xs text-destructive">{error}</span>}
      </div>
      {rows && rows.length === 0 && <p className="text-xs text-muted-foreground/60 italic">No events found</p>}
      {rows && rows.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-border/60">
          <table className="text-[10px] w-full">
            <thead className="bg-muted/40 border-b border-border/60">
              <tr>{cols.map(c => (
                <th key={c} className="px-2 py-1.5 text-left font-medium text-muted-foreground whitespace-nowrap">{c}</th>
              ))}</tr>
            </thead>
            <tbody className="divide-y divide-border/30">
              {rows.map((row, i) => (
                <tr key={i} className="hover:bg-muted/20">
                  {row.map((cell, j) => (
                    <td key={j} className="px-2 py-1 font-mono text-foreground/80 whitespace-nowrap max-w-[180px] truncate" title={cell}>
                      {cell ?? "—"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─── Impact Analysis panel ────────────────────────────────────────────────────

function ImpactAnalysisPanel({ lineage, onClose }) {
  const downstream = lineage?.downstream_tables || [];

  // Group downstream by entity type
  const groups = useMemo(() => {
    const g = {};
    downstream.forEach(e => {
      const t = e.entity_type || "TABLE";
      if (!g[t]) g[t] = [];
      g[t].push(e);
    });
    return g;
  }, [downstream]);

  const groupOrder = ["TABLE", "VIEW", "NOTEBOOK", "JOB", "DASHBOARD", "PIPELINE", "QUERY"];
  const presentGroups = groupOrder.filter(k => groups[k] && groups[k].length > 0);
  const otherKeys = Object.keys(groups).filter(k => !groupOrder.includes(k));

  function renderGroup(typeKey) {
    const items = groups[typeKey] || [];
    const { Icon, color, bg } = entityMeta(typeKey);
    return (
      <div key={typeKey} className="mb-4">
        <div className="flex items-center gap-2 mb-2">
          <div className={cn("h-5 w-5 rounded flex items-center justify-center shrink-0", bg)}>
            <Icon className={cn("h-3 w-3", color)} />
          </div>
          <span className="text-[11px] font-semibold text-foreground/80 uppercase tracking-wide">
            {typeKey === "TABLE" ? "Tables" : typeKey === "VIEW" ? "Views" : typeKey.charAt(0) + typeKey.slice(1).toLowerCase() + "s"}
          </span>
          <span className="ml-auto text-[10px] text-muted-foreground bg-muted/60 px-1.5 py-0.5 rounded-full">
            {items.length}
          </span>
        </div>
        <div className="space-y-1">
          {items.map((e, i) => {
            const name = e.table_name || e.name || e.entity_type || "Unknown";
            const shortName = name.includes(".") ? name.split(".").pop() : name;
            return (
              <div key={i} className="flex items-center gap-2 px-2 py-1.5 rounded-lg bg-destructive/5 border border-destructive/10">
                <AlertTriangle className="h-3 w-3 text-destructive/60 shrink-0" />
                <span className="text-[11px] font-mono text-foreground/80 truncate flex-1" title={name}>
                  {shortName}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div className="w-[280px] shrink-0 border-l border-border flex flex-col bg-background overflow-hidden">
      {/* Panel header */}
      <div className="px-4 py-3 border-b border-border shrink-0 flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 text-destructive/70 shrink-0" />
        <div className="flex-1 min-w-0">
          <p className="text-[11px] font-semibold text-foreground">Impact Analysis</p>
          <p className="text-[10px] text-muted-foreground/60 truncate">
            Direct impact (1 hop)
          </p>
        </div>
        <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition-colors shrink-0">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* Warning banner */}
      <div className="mx-3 mt-3 px-3 py-2 rounded-lg bg-destructive/5 border border-destructive/20">
        <p className="text-[10px] text-destructive/80 leading-relaxed">
          If <span className="font-mono font-semibold">{lineage?.table_name?.split(".").pop()}</span> is removed,
          the following downstream assets will break:
        </p>
      </div>

      <ScrollArea className="flex-1 mt-3">
        <div className="px-3 pb-4">
          {downstream.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <Share2 className="h-8 w-8 mx-auto mb-2 opacity-20" />
              <p className="text-xs font-medium">No downstream impact</p>
              <p className="text-[10px] mt-1 opacity-60">Nothing depends on this table</p>
            </div>
          ) : (
            <>
              <div className="mb-3 px-1">
                <p className="text-[10px] text-muted-foreground">
                  <span className="font-semibold text-destructive">{downstream.length}</span> asset{downstream.length !== 1 ? "s" : ""} affected
                </p>
              </div>
              {presentGroups.map(renderGroup)}
              {otherKeys.map(renderGroup)}
            </>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}

// ─── Lineage graph ────────────────────────────────────────────────────────────

function LineageGraph({ lineage, onNavigate, entityFilter, tableInfoMap, onFetchTableInfo, graphContainerRef }) {
  const graphRef      = useRef(null);
  const centerRef     = useRef(null);
  const upRefs        = useRef([]);
  const downRefs      = useRef([]);
  const panContainerRef = useRef(null);
  const colRefs       = useRef(new Map());   // "${fqn}:${colName}" → HTMLElement

  // Pan + zoom
  const [offset,     setOffset]    = useState({ x: 0, y: 0 });
  const [scale,      setScale]     = useState(1);
  const [isDragging, setIsDragging] = useState(false);
  const isDraggingRef = useRef(false);   // ref copy — no stale-closure in document handlers
  const dragOrigin    = useRef({ x: 0, y: 0 });
  const offsetRef     = useRef({ x: 0, y: 0 });
  // Keep offsetRef in sync so mousedown always uses the latest offset
  useEffect(() => { offsetRef.current = offset; }, [offset]);

  // Per-card drag offsets: { [cardKey]: { x, y } }
  const [cardOffsets, setCardOffsets] = useState({});
  const cardOffsetsRef = useRef({});
  useEffect(() => { cardOffsetsRef.current = cardOffsets; }, [cardOffsets]);
  const cardDragRef = useRef(null);   // { key, startX, startY, origX, origY }
  const [isDraggingCard, setIsDraggingCard] = useState(false);
  // ref copy of scale so document-level handlers don't go stale
  const scaleRef = useRef(1);
  useEffect(() => { scaleRef.current = scale; }, [scale]);

  const handleMouseDown = useCallback((e) => {
    if (e.button !== 0) return;

    // Card drag: grip handle takes priority
    const dragHandle = e.target.closest("[data-drag-handle]");
    if (dragHandle) {
      e.preventDefault();
      const cardKey = dragHandle.dataset.dragHandle;
      if (cardKey) {
        const orig = cardOffsetsRef.current[cardKey] || { x: 0, y: 0 };
        cardDragRef.current = { key: cardKey, startX: e.clientX, startY: e.clientY, origX: orig.x, origY: orig.y };
        setIsDraggingCard(true);
      }
      return;
    }

    // Skip interactive elements for canvas pan
    if (e.target.closest("button, a, input, select, textarea")) return;

    // Canvas pan
    e.preventDefault();
    isDraggingRef.current = true;
    setIsDragging(true);
    dragOrigin.current = {
      x: e.clientX - offsetRef.current.x,
      y: e.clientY - offsetRef.current.y,
    };
  }, []);

  // Attach mousemove + mouseup to document so dragging works anywhere on the page
  useEffect(() => {
    const onMove = (e) => {
      // Per-card drag
      if (cardDragRef.current) {
        const { key, startX, startY, origX, origY } = cardDragRef.current;
        const s = scaleRef.current;
        const dx = (e.clientX - startX) / s;
        const dy = (e.clientY - startY) / s;
        setCardOffsets(prev => ({ ...prev, [key]: { x: origX + dx, y: origY + dy } }));
        return;
      }
      // Canvas pan
      if (!isDraggingRef.current) return;
      setOffset({ x: e.clientX - dragOrigin.current.x, y: e.clientY - dragOrigin.current.y });
    };
    const onUp = () => {
      if (cardDragRef.current) {
        cardDragRef.current = null;
        setIsDraggingCard(false);
        return;
      }
      if (!isDraggingRef.current) return;
      isDraggingRef.current = false;
      setIsDragging(false);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup",   onUp);
    return () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup",   onUp);
    };
  }, []);

  // Non-passive wheel so preventDefault() actually works
  useEffect(() => {
    const el = panContainerRef.current;
    if (!el) return;
    const onWheel = (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.08 : 0.08;
      setScale(s => Math.min(2, Math.max(0.25, +(s + delta).toFixed(2))));
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);

  const [tableLines, setTableLines] = useState([]);
  const [selectedCol, setSelectedCol] = useState(null);    // {tableFqn, colName}
  const [colLineage,  setColLineage]  = useState(null);
  const [colLines,    setColLines]    = useState([]);
  const [colLoading,  setColLoading]  = useState(false);
  const [colError,    setColError]    = useState("");

  // Multi-hop expand state
  const [expandedNodes, setExpandedNodes] = useState(new Set());
  // expandedData: fqn → { upstream_tables, downstream_tables }
  const [expandedData, setExpandedData] = useState({});
  const [expandLoading, setExpandLoading] = useState({});

  // Filter by entity type
  const upstream = useMemo(() =>
    (lineage?.upstream_tables   || []).filter(e =>
      entityFilter === "all" || (e.entity_type || "TABLE") === entityFilter),
    [lineage, entityFilter]);
  const downstream = useMemo(() =>
    (lineage?.downstream_tables || []).filter(e =>
      entityFilter === "all" || (e.entity_type || "TABLE") === entityFilter),
    [lineage, entityFilter]);

  const hasData = upstream.length > 0 || downstream.length > 0;

  // Derive 2nd-level nodes for upstream and downstream
  const upstream2 = useMemo(() => {
    const all = [];
    upstream.forEach(e => {
      const fqn = e.table_name || "";
      if (!expandedNodes.has(fqn) || !expandedData[fqn]) return;
      const d = expandedData[fqn];
      (d.upstream_tables || []).forEach(u => {
        if (!all.find(x => x.table_name === u.table_name)) all.push(u);
      });
    });
    return all;
  }, [upstream, expandedNodes, expandedData]);

  const downstream2 = useMemo(() => {
    const all = [];
    downstream.forEach(e => {
      const fqn = e.table_name || "";
      if (!expandedNodes.has(fqn) || !expandedData[fqn]) return;
      const d = expandedData[fqn];
      (d.downstream_tables || []).forEach(u => {
        if (!all.find(x => x.table_name === u.table_name)) all.push(u);
      });
    });
    return all;
  }, [downstream, expandedNodes, expandedData]);

  // Refs for 2nd-level nodes
  const up2Refs   = useRef([]);
  const down2Refs = useRef([]);

  // Trigger column-info fetches for TABLE/VIEW nodes
  useEffect(() => {
    const isTable = e => !e.entity_type || ["TABLE","VIEW"].includes(e.entity_type);
    const fqns = [
      lineage?.table_name,
      ...upstream.filter(isTable).map(e => e.table_name),
      ...downstream.filter(isTable).map(e => e.table_name),
    ].filter(fqn => fqn && fqn.split(".").length === 3);
    [...new Set(fqns)].forEach(onFetchTableInfo);
  }, [lineage, upstream, downstream]);

  // Column click → fetch column lineage
  const handleColumnClick = useCallback(async (tableFqn, colName) => {
    if (selectedCol?.tableFqn === tableFqn && selectedCol?.colName === colName) {
      setSelectedCol(null); setColLineage(null); setColLines([]); setColError(""); return;
    }
    setSelectedCol({ tableFqn, colName });
    setColLineage(null); setColLines([]); setColError("");
    setColLoading(true);
    try {
      const result = await api.get("/assessment/lineage/column", {
        table_name: tableFqn, column_name: colName,
      });
      setColLineage(result);
    } catch (e) {
      setColError(e?.message || "Column lineage unavailable");
    } finally {
      setColLoading(false);
    }
  }, [selectedCol]);

  const handleColumnRef = useCallback((fqn, colName, el) => {
    const key = `${fqn}:${colName}`;
    if (el) colRefs.current.set(key, el);
    else colRefs.current.delete(key);
  }, []);

  // Multi-hop expand handler
  const handleExpand = useCallback(async (fqn) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(fqn)) {
        next.delete(fqn);
        return next;
      }
      next.add(fqn);
      return next;
    });

    if (expandedData[fqn]) return; // already fetched

    setExpandLoading(l => ({ ...l, [fqn]: true }));
    try {
      const result = await api.get("/assessment/lineage/table", { table_name: fqn });
      setExpandedData(d => ({ ...d, [fqn]: result }));
      // Also fetch table info for 2nd level nodes
      const isTable = e => !e.entity_type || ["TABLE","VIEW"].includes(e.entity_type);
      [
        ...(result.upstream_tables || []).filter(isTable).map(e => e.table_name),
        ...(result.downstream_tables || []).filter(isTable).map(e => e.table_name),
      ].filter(f => f && f.split(".").length === 3).forEach(onFetchTableInfo);
    } catch (e) {
      // Silently fail expand
    } finally {
      setExpandLoading(l => ({ ...l, [fqn]: false }));
    }
  }, [expandedData, onFetchTableInfo]);

  // Table-level bezier lines
  useLayoutEffect(() => {
    function calc() {
      if (!graphRef.current || !centerRef.current) return;
      const base  = graphRef.current.getBoundingClientRect();
      const S     = scale; // scale factor for coordinate conversion
      const px    = (v) => v / S; // screen-px → canvas CSS-px
      const cRect = centerRef.current.getBoundingClientRect();
      const cx1   = px(cRect.left  - base.left);
      const cx2   = px(cRect.right - base.left);
      const cy    = px(cRect.top + cRect.height / 2 - base.top);
      const next  = [];
      upRefs.current.forEach(ref => {
        if (!ref) return;
        const r = ref.getBoundingClientRect();
        next.push({ x1: px(r.right - base.left), y1: px(r.top + r.height/2 - base.top), x2: cx1, y2: cy, type: "upstream" });
      });
      downRefs.current.forEach(ref => {
        if (!ref) return;
        const r = ref.getBoundingClientRect();
        next.push({ x1: cx2, y1: cy, x2: px(r.left - base.left), y2: px(r.top + r.height/2 - base.top), type: "downstream" });
      });
      up2Refs.current.forEach((ref, idx) => {
        if (!ref) return;
        const node2 = upstream2[idx];
        if (!node2) return;
        upRefs.current.forEach((ref1, idx1) => {
          if (!ref1) return;
          const fqn1 = upstream[idx1]?.table_name;
          if (!fqn1 || !expandedNodes.has(fqn1)) return;
          const data1 = expandedData[fqn1];
          if (!data1) return;
          if (!(data1.upstream_tables || []).some(x => x.table_name === node2.table_name)) return;
          const r1 = ref1.getBoundingClientRect();
          const r2 = ref.getBoundingClientRect();
          next.push({
            x1: px(r2.right - base.left), y1: px(r2.top + r2.height/2 - base.top),
            x2: px(r1.left  - base.left), y2: px(r1.top + r1.height/2 - base.top),
            type: "upstream",
          });
        });
      });
      down2Refs.current.forEach((ref, idx) => {
        if (!ref) return;
        const node2 = downstream2[idx];
        if (!node2) return;
        downRefs.current.forEach((ref1, idx1) => {
          if (!ref1) return;
          const fqn1 = downstream[idx1]?.table_name;
          if (!fqn1 || !expandedNodes.has(fqn1)) return;
          const data1 = expandedData[fqn1];
          if (!data1) return;
          if (!(data1.downstream_tables || []).some(x => x.table_name === node2.table_name)) return;
          const r1 = ref1.getBoundingClientRect();
          const r2 = ref.getBoundingClientRect();
          next.push({
            x1: px(r1.right - base.left), y1: px(r1.top + r1.height/2 - base.top),
            x2: px(r2.left  - base.left), y2: px(r2.top + r2.height/2 - base.top),
            type: "downstream",
          });
        });
      });
      setTableLines(next);
    }
    calc();
    window.addEventListener("resize", calc);
    return () => window.removeEventListener("resize", calc);
  }, [upstream, downstream, upstream2, downstream2, tableInfoMap, expandedNodes, expandedData, scale, cardOffsets]);

  // Column-level bezier lines
  useLayoutEffect(() => {
    if (!colLineage || !selectedCol) { setColLines([]); return; }
    const base = graphRef.current?.getBoundingClientRect();
    if (!base) return;

    const S  = scale;
    const px = (v) => v / S;

    const srcEl = colRefs.current.get(`${selectedCol.tableFqn}:${selectedCol.colName}`);
    if (!srcEl) return;

    const srcRect = srcEl.getBoundingClientRect();
    const srcL    = px(srcRect.left  - base.left);
    const srcR    = px(srcRect.right - base.left);
    const srcMy   = px(srcRect.top + srcRect.height / 2 - base.top);

    const lines = [];

    (colLineage.upstream_cols || []).forEach(col => {
      const el = colRefs.current.get(`${col.table_name}:${col.name}`);
      if (!el) return;
      const r = el.getBoundingClientRect();
      lines.push({
        x1: px(r.right - base.left), y1: px(r.top + r.height/2 - base.top),
        x2: srcL, y2: srcMy,
        type: "upstream",
      });
    });

    (colLineage.downstream_cols || []).forEach(col => {
      const el = colRefs.current.get(`${col.table_name}:${col.name}`);
      if (!el) return;
      const r = el.getBoundingClientRect();
      lines.push({
        x1: srcR, y1: srcMy,
        x2: px(r.left - base.left), y2: px(r.top + r.height/2 - base.top),
        type: "downstream",
      });
    });

    setColLines(lines);
  }, [colLineage, selectedCol, scale, cardOffsets]);

  if (!hasData) return (
    <div className="py-12 text-center text-muted-foreground">
      <Share2 className="h-10 w-10 mx-auto mb-3 opacity-20" />
      <p className="text-sm font-medium">No lineage tracked for this table</p>
      <p className="text-xs mt-1 opacity-60 max-w-xs mx-auto">
        Lineage is captured automatically when this table is read or written by a Databricks workload.
      </p>
    </div>
  );

  function renderNode(entity, i, direction, isSecondLevel = false) {
    const isTable = !entity.entity_type || ["TABLE", "VIEW"].includes(entity.entity_type);
    let refs;
    if (isSecondLevel) {
      refs = direction === "upstream" ? up2Refs : down2Refs;
    } else {
      refs = direction === "upstream" ? upRefs : downRefs;
    }
    const fqn = entity.table_name || "";
    const isExpanded = expandedNodes.has(fqn);
    const isExpLoading = expandLoading[fqn];
    const cardKey = `${direction}-${isSecondLevel ? "2" : "1"}-${i}`;
    const co = cardOffsets[cardKey] || { x: 0, y: 0 };

    const dragHandle = (
      <div
        data-drag-handle={cardKey}
        className="flex items-center justify-center gap-1 w-full py-1 cursor-grab active:cursor-grabbing select-none opacity-40 hover:opacity-80 transition-opacity"
        title="Drag to reposition"
      >
        <GripVertical className="h-3 w-3 text-muted-foreground" />
      </div>
    );

    if (isTable) return (
      <div key={cardKey} className="relative" data-draggable-card={cardKey} style={{ transform: `translate(${co.x}px, ${co.y}px)` }}>
        {dragHandle}
        {isExpLoading && (
          <div className="absolute -bottom-3 left-1/2 -translate-x-1/2 z-20">
            <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
          </div>
        )}
        <TableNodeCard
          entity={entity}
          direction={direction}
          onClick={() => onNavigate(entity.table_name)}
          nodeRef={el => (refs.current[i] = el)}
          tableInfoMap={tableInfoMap}
          selectedCol={selectedCol}
          onColumnClick={handleColumnClick}
          onColumnRef={handleColumnRef}
          colLineage={colLineage}
          isSecondLevel={isSecondLevel}
          onExpand={isSecondLevel ? undefined : handleExpand}
          isExpanded={isExpanded}
        />
      </div>
    );
    return (
      <div key={cardKey} className="relative" data-draggable-card={cardKey} style={{ transform: `translate(${co.x}px, ${co.y}px)` }}>
        {dragHandle}
        <EntityCard
          entity={entity}
          direction={direction}
          nodeRef={el => (refs.current[i] = el)}
        />
      </div>
    );
  }

  return (
    <div>
      {/* Column lineage banner */}
      {selectedCol && (
        <div className={cn(
          "mx-4 mt-3 mb-0 px-3 py-2 rounded-lg border flex items-center gap-2 text-xs",
          colLoading ? "border-border bg-muted/30"
          : colError  ? "border-destructive/30 bg-destructive/5"
          : "border-primary/30 bg-primary/5",
        )}>
          <Columns className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          <span className="font-medium shrink-0">Column lineage:</span>
          <code className="font-mono text-primary truncate">{selectedCol.tableFqn}.{selectedCol.colName}</code>
          {colLoading && <Loader2 className="h-3 w-3 animate-spin ml-1 shrink-0" />}
          {colError && <span className="text-destructive ml-1 shrink-0">{colError}</span>}
          {colLineage && !colLoading && (
            <span className="text-muted-foreground ml-1 shrink-0">
              {(colLineage.upstream_cols||[]).length} upstream · {(colLineage.downstream_cols||[]).length} downstream
            </span>
          )}
          <button onClick={() => { setSelectedCol(null); setColLineage(null); setColLines([]); }}
            className="ml-auto text-muted-foreground hover:text-foreground transition-colors shrink-0">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {/* Pan / zoom container */}
      <div
        ref={panContainerRef}
        className="relative overflow-hidden min-h-[200px] select-none"
        style={{ cursor: isDragging ? "grabbing" : "grab" }}
        onMouseDown={handleMouseDown}
      >
        {/* Canvas — translated + scaled */}
        <div ref={el => { graphRef.current = el; if (graphContainerRef) graphContainerRef.current = el; }}
          className="relative flex items-start justify-center gap-10 py-8 px-6 min-h-[200px]"
          style={{ transform: `translate(${offset.x}px, ${offset.y}px) scale(${scale})`, transformOrigin: "0 0", willChange: "transform" }}
        >
        <LineageOverlay tableLines={tableLines} colLines={colLines} />

        {/* Upstream level 2 */}
        {upstream2.length > 0 && (
          <div className="flex flex-col gap-3 z-10">
            <p className="text-[10px] font-semibold text-blue-400/70 uppercase tracking-widest text-center">
              Level 2 ({upstream2.length})
            </p>
            {upstream2.map((e, i) => renderNode(e, i, "upstream", true))}
          </div>
        )}

        {/* Upstream level 1 */}
        {upstream.length > 0 ? (
          <div className="flex flex-col gap-4 z-10">
            <p className="text-[10px] font-semibold text-blue-500 uppercase tracking-widest text-center">
              Producers ({upstream.length})
            </p>
            {upstream.map((e, i) => renderNode(e, i, "upstream"))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center pt-12 z-10 w-[80px]">
            <p className="text-[10px] text-muted-foreground/30 uppercase tracking-widest text-center">No producers</p>
          </div>
        )}

        {/* Center */}
        <div
          className="flex flex-col items-center gap-2 z-10"
          data-draggable-card="center"
          style={{ transform: `translate(${(cardOffsets["center"]||{x:0}).x}px, ${(cardOffsets["center"]||{y:0}).y}px)` }}
        >
          <p className="text-[10px] font-semibold text-foreground/60 uppercase tracking-widest text-center">Current Table</p>
          <div
            data-drag-handle="center"
            className="flex items-center justify-center gap-1 w-full py-1 cursor-grab active:cursor-grabbing select-none opacity-40 hover:opacity-80 transition-opacity"
            title="Drag to reposition"
          >
            <GripVertical className="h-3 w-3 text-muted-foreground" />
          </div>
          <TableNodeCard
            entity={{ table_name: lineage.table_name, entity_type: "TABLE" }}
            isTarget
            nodeRef={centerRef}
            tableInfoMap={tableInfoMap}
            selectedCol={selectedCol}
            onColumnClick={handleColumnClick}
            onColumnRef={handleColumnRef}
            colLineage={colLineage}
          />
          <p className="text-[10px] text-muted-foreground text-center mt-1">
            {(lineage.upstream_tables||[]).length} upstream · {(lineage.downstream_tables||[]).length} downstream
          </p>
        </div>

        {/* Downstream level 1 */}
        {downstream.length > 0 ? (
          <div className="flex flex-col gap-4 z-10">
            <p className="text-[10px] font-semibold text-emerald-500 uppercase tracking-widest text-center">
              Consumers ({downstream.length})
            </p>
            {downstream.map((e, i) => renderNode(e, i, "downstream"))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center pt-12 z-10 w-[80px]">
            <p className="text-[10px] text-muted-foreground/30 uppercase tracking-widest text-center">No consumers</p>
          </div>
        )}

        {/* Downstream level 2 */}
        {downstream2.length > 0 && (
          <div className="flex flex-col gap-3 z-10">
            <p className="text-[10px] font-semibold text-emerald-400/70 uppercase tracking-widest text-center">
              Level 2 ({downstream2.length})
            </p>
            {downstream2.map((e, i) => renderNode(e, i, "downstream", true))}
          </div>
        )}
        </div>{/* end canvas */}

        {/* Pan/zoom controls overlay */}
        <div className="absolute bottom-3 right-3 z-20 flex items-center gap-1 rounded-lg border border-border bg-background/90 backdrop-blur px-1.5 py-1 shadow-sm">
          <button
            onClick={() => setScale(s => Math.min(2, +(s + 0.15).toFixed(2)))}
            title="Zoom in"
            className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
          >
            <ZoomIn className="h-3.5 w-3.5" />
          </button>
          <span className="text-[10px] font-mono text-muted-foreground w-9 text-center">
            {Math.round(scale * 100)}%
          </span>
          <button
            onClick={() => setScale(s => Math.max(0.25, +(s - 0.15).toFixed(2)))}
            title="Zoom out"
            className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
          >
            <ZoomOut className="h-3.5 w-3.5" />
          </button>
          <div className="w-px h-4 bg-border mx-0.5" />
          <button
            onClick={() => { setOffset({ x: 0, y: 0 }); setScale(1); setCardOffsets({}); }}
            title="Reset view and card positions"
            className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
          >
            <RotateCcw className="h-3.5 w-3.5" />
          </button>
        </div>

        {/* Hint */}
        {(offset.x !== 0 || offset.y !== 0 || scale !== 1) && (
          <div className="absolute top-2 left-1/2 -translate-x-1/2 z-20 pointer-events-none">
            <span className="text-[10px] text-muted-foreground/60 bg-background/80 backdrop-blur px-2 py-0.5 rounded-full border border-border/40">
              Drag to pan · Scroll to zoom · Reset ↺
            </span>
          </div>
        )}
      </div>{/* end pan container */}
    </div>
  );
}

// ─── UC tree browser ──────────────────────────────────────────────────────────

function TableTree({ onSelect, selected }) {
  const [catalogs,    setCatalogs]    = useState([]);
  const [schemas,     setSchemas]     = useState({});
  const [tables,      setTables]      = useState({});
  const [openCats,    setOpenCats]    = useState({});
  const [openSchemas, setOpenSchemas] = useState({});
  const [loadingCat,  setLoadingCat]  = useState({});
  const [loadingScm,  setLoadingScm]  = useState({});
  const [catLoading,  setCatLoading]  = useState(true);

  useEffect(() => {
    api.get("/catalogs")
      .then(data => {
        const list = Array.isArray(data) ? data : [];
        setCatalogs(list);
        if (list.length > 0) expandCatalog(list[0], true);
      })
      .catch(() => {})
      .finally(() => setCatLoading(false));
  }, []);

  function expandCatalog(cat, auto = false) {
    if (schemas[cat] !== undefined) {
      setOpenCats(o => ({ ...o, [cat]: auto ? true : !o[cat] }));
      return;
    }
    setLoadingCat(l => ({ ...l, [cat]: true }));
    api.get(`/catalogs/${cat}/schemas`)
      .then(d => { setSchemas(s => ({ ...s, [cat]: Array.isArray(d) ? d : [] })); setOpenCats(o => ({ ...o, [cat]: true })); })
      .catch(() => setOpenCats(o => ({ ...o, [cat]: true })))
      .finally(() => setLoadingCat(l => ({ ...l, [cat]: false })));
  }

  function expandSchema(cat, schema) {
    const key = `${cat}.${schema}`;
    if (tables[key] !== undefined) { setOpenSchemas(o => ({ ...o, [key]: !o[key] })); return; }
    setLoadingScm(l => ({ ...l, [key]: true }));
    api.get(`/catalogs/${cat}/${schema}/tables`)
      .then(d => { setTables(t => ({ ...t, [key]: Array.isArray(d) ? d : [] })); setOpenSchemas(o => ({ ...o, [key]: true })); })
      .catch(() => setOpenSchemas(o => ({ ...o, [key]: true })))
      .finally(() => setLoadingScm(l => ({ ...l, [key]: false })));
  }

  if (catLoading) return (
    <div className="flex items-center justify-center h-20 text-xs text-muted-foreground gap-1.5">
      <Loader2 className="h-3.5 w-3.5 animate-spin" />Loading…
    </div>
  );

  return (
    <div className="text-xs select-none space-y-0.5">
      {catalogs.map(cat => (
        <div key={cat}>
          <button onClick={() => expandCatalog(cat)}
            className="flex items-center gap-1.5 w-full text-left px-2 py-1.5 rounded-md hover:bg-muted/60 transition-colors">
            {loadingCat[cat]
              ? <Loader2 className="h-3 w-3 animate-spin shrink-0 text-muted-foreground" />
              : openCats[cat] ? <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
              : <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />}
            <Database className="h-3 w-3 shrink-0 text-primary/70" />
            <span className="font-medium truncate">{cat}</span>
          </button>
          {openCats[cat] && (schemas[cat] || []).map(schema => {
            const key = `${cat}.${schema}`;
            return (
              <div key={schema} className="ml-4 space-y-0.5">
                <button onClick={() => expandSchema(cat, schema)}
                  className="flex items-center gap-1.5 w-full text-left px-2 py-1 rounded-md hover:bg-muted/60 transition-colors">
                  {loadingScm[key]
                    ? <Loader2 className="h-3 w-3 animate-spin shrink-0 text-muted-foreground" />
                    : openSchemas[key] ? <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
                    : <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />}
                  <Layers className="h-3 w-3 shrink-0 text-amber-500/80" />
                  <span className="truncate text-muted-foreground">{schema}</span>
                </button>
                {openSchemas[key] && (tables[key] || []).map(tbl => {
                  const fqn = `${cat}.${schema}.${tbl}`;
                  return (
                    <button key={tbl} onClick={() => onSelect(fqn)}
                      className={cn(
                        "flex items-center gap-1.5 w-full text-left px-2 py-1 rounded-md transition-colors ml-4",
                        selected === fqn
                          ? "bg-primary/10 text-primary font-medium"
                          : "hover:bg-muted/60 text-muted-foreground hover:text-foreground",
                      )}>
                      <Table2 className="h-3 w-3 shrink-0" />
                      <span className="truncate">{tbl}</span>
                    </button>
                  );
                })}
                {openSchemas[key] && !(tables[key]||[]).length && !loadingScm[key] && (
                  <p className="ml-8 py-1 text-[11px] text-muted-foreground/50 italic">no tables</p>
                )}
              </div>
            );
          })}
          {openCats[cat] && !(schemas[cat]||[]).length && !loadingCat[cat] && (
            <p className="ml-6 py-1 text-[11px] text-muted-foreground/50 italic">no schemas</p>
          )}
        </div>
      ))}
    </div>
  );
}

// ─── Main page ────────────────────────────────────────────────────────────────

export default function DataLineagePage() {
  const [tableName,    setTableName]    = useState("");
  const [lineage,      setLineage]      = useState(null);
  const [loading,      setLoading]      = useState(false);
  const [error,        setError]        = useState("");
  const [history,      setHistory]      = useState([]);
  const [timeRange,    setTimeRange]    = useState("1Y");
  const [entityFilter, setEntityFilter] = useState("all");
  const [tableInfoMap, setTableInfoMap] = useState({});

  // Impact Analysis panel
  const [impactOpen, setImpactOpen] = useState(false);

  // Full-screen mode
  const [fullscreen, setFullscreen] = useState(false);

  // Graph container ref for SVG export
  const graphContainerRef = useRef(null);

  // Escape key exits full-screen
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape" && fullscreen) setFullscreen(false); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [fullscreen]);

  // ── Feature 1: Deep-link – read URL params on mount ──────────────────────
  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    const tParam = params.get("table");
    const trParam = params.get("timeRange");
    if (trParam && TIME_RANGES.some(r => r.label === trParam)) {
      setTimeRange(trParam);
    }
    if (tParam) {
      setTableName(tParam);
      // Defer so timeRange state settles first
      setTimeout(() => lookupLineage(tParam), 0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Entity type counts for filter pills
  const entityCounts = useMemo(() => {
    if (!lineage) return {};
    const all = [...(lineage.upstream_tables||[]), ...(lineage.downstream_tables||[])];
    const acc = { all: all.length };
    all.forEach(e => { const t = e.entity_type || "TABLE"; acc[t] = (acc[t] || 0) + 1; });
    return acc;
  }, [lineage]);

  const fetchTableInfo = useCallback((fqn) => {
    if (!fqn || tableInfoMap[fqn] !== undefined) return;
    setTableInfoMap(m => ({ ...m, [fqn]: { columns: [], owner: "", loading: true } }));
    const [cat, schema, tbl] = fqn.split(".");
    api.get(`/catalogs/${cat}/${schema}/${tbl}/info`)
      .then(info => setTableInfoMap(m => ({
        ...m, [fqn]: { columns: info.columns || [], owner: info.owner || "", loading: false },
      })))
      .catch(() => setTableInfoMap(m => ({
        ...m, [fqn]: { columns: [], owner: "", loading: false },
      })));
  }, [tableInfoMap]);

  async function lookupLineage(name) {
    const target = (name || tableName).trim();
    if (!target) return;
    setLoading(true); setError(""); setLineage(null);
    setTableName(target); setEntityFilter("all");
    setImpactOpen(false);

    const rangeMs = TIME_RANGES.find(r => r.label === timeRange)?.ms || 0;
    const params = { table_name: target };
    if (rangeMs > 0) {
      params.start_time_ms = Date.now() - rangeMs;
      params.end_time_ms   = Date.now();
    }

    try {
      const result = await api.get("/assessment/lineage/table", params);
      setLineage(result);
      setHistory(h => [target, ...h.filter(x => x !== target)].slice(0, 10));

      // ── Feature 1: Update URL with ?table= and ?timeRange= ──────────────
      if (typeof window !== "undefined") {
        const url = new URL(window.location.href);
        url.searchParams.set("table", target);
        url.searchParams.set("timeRange", timeRange);
        window.history.replaceState({}, "", url.toString());
      }
    } catch (e) {
      setError(e?.message ?? "Failed to fetch lineage.");
    } finally {
      setLoading(false);
    }
  }

  // ── Feature 2: Export lineage as SVG ─────────────────────────────────────
  function handleExportSVG() {
    const container = graphContainerRef.current;
    if (!container) return;
    const svgEl = container.querySelector("svg");
    if (!svgEl) return;

    // Clone the SVG so we can embed basic styles
    const clone = svgEl.cloneNode(true);
    // Embed a white/dark-agnostic background rect
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("width", "100%");
    rect.setAttribute("height", "100%");
    rect.setAttribute("fill", "#ffffff");
    clone.insertBefore(rect, clone.firstChild);

    // Set explicit dimensions from bounding box
    const bbox = svgEl.getBoundingClientRect();
    clone.setAttribute("width", String(Math.ceil(bbox.width)));
    clone.setAttribute("height", String(Math.ceil(bbox.height)));
    clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");

    const serializer = new XMLSerializer();
    const svgStr = serializer.serializeToString(clone);
    const blob = new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const safeName = (lineage?.table_name || "lineage").replace(/\./g, "_");
    a.href = url;
    a.download = `lineage-${safeName}.svg`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  return (
    <div className={cn(
      "flex flex-col overflow-hidden",
      fullscreen
        ? "fixed inset-0 z-50 bg-background"
        : "h-full",
    )}>
      <PageHeader
        title="Data Lineage Explorer"
        icon={Share2}
        breadcrumbs={["Assessment", "UC Inventory", "Data Lineage"]}
        description="Trace upstream sources, downstream consumers, notebooks, jobs, dashboards, and column-level lineage."
      />

      <div className="flex flex-1 overflow-hidden border-t border-border">
        {/* UC tree */}
        <div className="w-60 shrink-0 border-r border-border flex flex-col overflow-hidden bg-muted/10">
          <div className="px-3 py-2.5 border-b border-border shrink-0">
            <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Unity Catalog</p>
          </div>
          <ScrollArea className="flex-1">
            <div className="p-2"><TableTree onSelect={lookupLineage} selected={tableName} /></div>
          </ScrollArea>
        </div>

        {/* Main */}
        <div className="flex-1 flex flex-col overflow-hidden min-w-0">
          {/* Search + time range */}
          <div className="px-4 pt-3.5 pb-3 shrink-0 border-b border-border bg-background">
            <div className="flex gap-2 mb-2">
              <input
                type="text" value={tableName}
                onChange={e => setTableName(e.target.value)}
                onKeyDown={e => e.key === "Enter" && lookupLineage()}
                placeholder="catalog.schema.table"
                className="flex-1 px-3 py-2 text-sm border border-input rounded-lg bg-muted/30 focus:bg-background focus:outline-none focus:ring-1 focus:ring-ring font-mono transition-colors"
              />
              <div className="flex items-center gap-0.5 rounded-lg border border-border bg-muted/30 px-1 shrink-0">
                {TIME_RANGES.map(r => (
                  <button key={r.label} onClick={() => setTimeRange(r.label)}
                    className={cn(
                      "px-2 py-1 text-[11px] font-medium rounded-md transition-colors",
                      timeRange === r.label
                        ? "bg-background text-foreground shadow-sm"
                        : "text-muted-foreground hover:text-foreground",
                    )}>
                    {r.label}
                  </button>
                ))}
              </div>
              <Button onClick={() => lookupLineage()} disabled={loading || !tableName.trim()} className="shrink-0">
                {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              </Button>
            </div>
            {history.length > 1 && (
              <div className="flex gap-1.5 flex-wrap mb-1.5">
                <span className="text-[10px] text-muted-foreground mt-0.5">Recent:</span>
                {history.slice(1).map(h => (
                  <button key={h} onClick={() => lookupLineage(h)}
                    className="text-[10px] font-mono text-muted-foreground hover:text-foreground underline underline-offset-2 transition-colors">
                    {h}
                  </button>
                ))}
              </div>
            )}
            <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground/70">
              <Info className="h-3 w-3 shrink-0" />
              Click any column name in a node to trace its column-level lineage.
            </div>
          </div>

          {/* Entity filter bar */}
          {lineage && !loading && (
            <div className="flex items-center gap-1.5 px-4 py-2 border-b border-border/60 bg-background shrink-0 overflow-x-auto scrollbar-none">
              {ENTITY_FILTERS.map(f => {
                const count = entityCounts[f.key] ?? 0;
                if (f.key !== "all" && count === 0) return null;
                const Icon = f.icon;
                return (
                  <button key={f.key} onClick={() => setEntityFilter(f.key)}
                    className={cn(
                      "flex items-center gap-1 px-2.5 py-1 rounded-full text-[11px] font-medium transition-colors shrink-0",
                      entityFilter === f.key
                        ? "bg-primary text-primary-foreground"
                        : "bg-muted/50 text-muted-foreground hover:text-foreground hover:bg-muted",
                    )}>
                    <Icon className="h-3 w-3 shrink-0" />
                    {f.label}
                    <span className={cn(
                      "ml-0.5 text-[10px] px-1 rounded-full",
                      entityFilter === f.key
                        ? "bg-primary-foreground/20 text-primary-foreground"
                        : "bg-muted text-muted-foreground",
                    )}>
                      {f.key === "all" ? entityCounts.all : count}
                    </span>
                  </button>
                );
              })}
            </div>
          )}

          {/* Content area + optional Impact Analysis panel */}
          <div className="flex flex-1 overflow-hidden">
            <div className="flex-1 overflow-auto">
              {error && (
                <div className="m-4">
                  <div className="flex items-start gap-2 p-3 rounded-lg border border-destructive/30 bg-destructive/5">
                    <AlertCircle className="h-4 w-4 text-destructive shrink-0 mt-0.5" />
                    <p className="text-sm text-destructive">{error}</p>
                  </div>
                </div>
              )}

              {loading && (
                <div className="flex items-center justify-center py-20 gap-2 text-muted-foreground">
                  <Loader2 className="h-5 w-5 animate-spin" />
                  <span className="text-sm">Fetching lineage…</span>
                </div>
              )}

              {lineage && !loading && (
                <div className="m-4 rounded-xl border border-border overflow-hidden shadow-sm">
                  {/* Header */}
                  <div className="flex items-center gap-3 px-4 py-3 border-b border-border bg-muted/10">
                    <Share2 className="h-4 w-4 text-muted-foreground shrink-0" />
                    <div className="min-w-0">
                      <p className="text-[10px] text-muted-foreground leading-none mb-0.5">Data lineage for</p>
                      <p className="text-sm font-semibold font-mono text-primary truncate">{lineage.table_name}</p>
                    </div>
                    <div className="ml-auto flex items-center gap-3 shrink-0 text-xs">
                      <span className="flex items-center gap-1 text-blue-500">
                        <span className="h-2 w-2 rounded-full bg-blue-400 inline-block" />
                        {(lineage.upstream_tables||[]).length} upstream
                      </span>
                      <span className="flex items-center gap-1 text-emerald-500">
                        <span className="h-2 w-2 rounded-full bg-emerald-400 inline-block" />
                        {(lineage.downstream_tables||[]).length} downstream
                      </span>
                      <span className="text-muted-foreground border-l border-border pl-3">
                        {timeRange === "All" ? "All time" : `Last ${timeRange}`}
                      </span>

                      {/* Feature 2: Export SVG button */}
                      <button
                        onClick={handleExportSVG}
                        title="Export graph as SVG"
                        className="flex items-center gap-1.5 px-2.5 py-1 rounded-md border border-border/70 text-[11px] text-muted-foreground hover:text-foreground hover:bg-muted/60 transition-colors"
                      >
                        <Download className="h-3 w-3" />
                        Export SVG
                      </button>

                      {/* Full-screen toggle */}
                      <button
                        onClick={() => setFullscreen(f => !f)}
                        title={fullscreen ? "Exit full screen (Esc)" : "Full screen"}
                        className="flex items-center gap-1.5 px-2.5 py-1 rounded-md border border-border/70 text-[11px] text-muted-foreground hover:text-foreground hover:bg-muted/60 transition-colors"
                      >
                        {fullscreen
                          ? <><Minimize2 className="h-3 w-3" /> Exit full screen</>
                          : <><Maximize2 className="h-3 w-3" /> Full screen</>
                        }
                      </button>

                      {/* Feature 3: Impact Analysis button */}
                      <button
                        onClick={() => setImpactOpen(o => !o)}
                        title="Show impact analysis"
                        className={cn(
                          "flex items-center gap-1.5 px-2.5 py-1 rounded-md border text-[11px] transition-colors",
                          impactOpen
                            ? "border-destructive/40 text-destructive bg-destructive/5 hover:bg-destructive/10"
                            : "border-border/70 text-muted-foreground hover:text-foreground hover:bg-muted/60",
                        )}
                      >
                        {impactOpen
                          ? <PanelRightClose className="h-3 w-3" />
                          : <PanelRightOpen className="h-3 w-3" />}
                        Impact Analysis
                      </button>
                    </div>
                  </div>

                  {/* Graph */}
                  <div className="overflow-x-auto">
                    <LineageGraph
                      lineage={lineage}
                      onNavigate={lookupLineage}
                      entityFilter={entityFilter}
                      tableInfoMap={tableInfoMap}
                      onFetchTableInfo={fetchTableInfo}
                      graphContainerRef={graphContainerRef}
                    />
                  </div>

                  {/* System events */}
                  <SystemEventsPanel tableName={lineage.table_name} />
                </div>
              )}

              {!lineage && !loading && !error && (
                <div className="flex flex-col items-center justify-center py-20 text-muted-foreground">
                  <div className="h-16 w-16 rounded-2xl bg-muted/40 flex items-center justify-center mb-4">
                    <Share2 className="h-8 w-8 opacity-30" />
                  </div>
                  <p className="text-sm font-medium">Select a table to explore its lineage</p>
                  <p className="text-xs mt-1 opacity-60">Choose from the catalog tree or type a full name above</p>
                  <div className="mt-6 grid grid-cols-2 gap-3 max-w-md">
                    {[
                      { icon: Columns,        label: "Column-level lineage",      desc: "Click any column to trace its sources and consumers" },
                      { icon: Share2,         label: "Table & view lineage",      desc: "Upstream producers and downstream consumers" },
                      { icon: Briefcase,      label: "Jobs, notebooks & dashboards", desc: "See which compute assets read and write this table" },
                      { icon: Database,       label: "System table events",       desc: "Raw events from system.access.table_lineage" },
                    ].map(({ icon: Icon, label, desc }) => (
                      <div key={label} className="flex items-start gap-2.5 p-3 rounded-xl border border-border/60 bg-muted/20">
                        <div className="h-7 w-7 rounded-lg bg-muted flex items-center justify-center shrink-0 mt-0.5">
                          <Icon className="h-3.5 w-3.5 text-muted-foreground" />
                        </div>
                        <div>
                          <p className="text-xs font-medium text-foreground">{label}</p>
                          <p className="text-[10px] text-muted-foreground/70 mt-0.5">{desc}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Feature 3: Impact Analysis slide-in panel */}
            {impactOpen && lineage && (
              <ImpactAnalysisPanel
                lineage={lineage}
                onClose={() => setImpactOpen(false)}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
