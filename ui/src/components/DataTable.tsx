// @ts-nocheck
import { useState, useMemo, useEffect, useCallback } from "react";
import {
  ChevronUp, ChevronDown, ChevronsUpDown,
  ChevronLeft, ChevronRight, Search,
  GripVertical, RotateCcw,
} from "lucide-react";
import {
  DndContext, closestCenter, PointerSensor, KeyboardSensor, useSensor, useSensors,
} from "@dnd-kit/core";
import {
  SortableContext, horizontalListSortingStrategy, useSortable, arrayMove,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";

/* ── types ────────────────────────────────────────────────── */

export interface Column {
  key: string;
  label: string;
  sortable?: boolean;
  sortKey?: string;       // Use a different field for sorting (e.g., sort by bytes, display formatted)
  align?: "left" | "right" | "center";
  render?: (value: any, row: any) => React.ReactNode;
  className?: string;
  headerClassName?: string;
  width?: string;
}

interface DataTableProps {
  data: any[];
  columns: Column[];
  searchable?: boolean;
  searchPlaceholder?: string;
  searchKeys?: string[];   // Which fields to search (defaults to all column keys)
  pageSize?: number;
  onRowClick?: (row: any, index: number) => void;
  rowClassName?: (row: any) => string;
  emptyMessage?: string;
  actions?: React.ReactNode;   // Slot for buttons above table (export, etc.)
  stickyHeader?: boolean;
  compact?: boolean;
  draggableColumns?: boolean;  // Opt-in column reordering via drag
  tableId?: string;            // localStorage key — required when draggableColumns is true
}

type SortDir = "asc" | "desc" | null;

/* ── sortable header cell ─────────────────────────────────── */

function SortableColumnHeader({
  col, draggable, isSorted, sortDir, onSort, py, px,
}: {
  col: Column;
  draggable: boolean;
  isSorted: boolean;
  sortDir: SortDir;
  onSort: (key: string) => void;
  py: string;
  px: string;
}) {
  const {
    attributes, listeners, setNodeRef, transform, transition, isDragging,
  } = useSortable({ id: col.key, disabled: !draggable });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    width: col.width || undefined,
  };

  return (
    <th
      ref={setNodeRef}
      style={style}
      className={`${py} ${px} font-medium text-muted-foreground text-${col.align || "left"} ${col.headerClassName || ""} ${col.sortable ? "select-none hover:text-foreground transition-colors" : ""}`}
    >
      <span className="inline-flex items-center gap-1">
        {draggable && (
          <span
            className="inline-flex cursor-grab active:cursor-grabbing touch-none"
            {...attributes}
            {...listeners}
          >
            <GripVertical className="h-3 w-3 text-muted-foreground/40 hover:text-muted-foreground transition-colors" />
          </span>
        )}
        <span
          className={`inline-flex items-center gap-1 ${col.sortable ? "cursor-pointer" : ""}`}
          onClick={col.sortable ? () => onSort(col.key) : undefined}
        >
          {col.label}
          {col.sortable && (
            isSorted ? (
              sortDir === "asc"
                ? <ChevronUp className="h-3 w-3 text-blue-600" />
                : <ChevronDown className="h-3 w-3 text-blue-600" />
            ) : (
              <ChevronsUpDown className="h-3 w-3 opacity-30" />
            )
          )}
        </span>
      </span>
    </th>
  );
}

/* ── column order persistence ─────────────────────────────── */

function loadColumnOrder(storageKey: string | null, columns: Column[]): string[] {
  const defaultOrder = columns.map(c => c.key);
  if (!storageKey) return defaultOrder;
  try {
    const raw = localStorage.getItem(storageKey);
    if (!raw) return defaultOrder;
    const saved: string[] = JSON.parse(raw);
    const colKeys = new Set(defaultOrder);
    const valid = saved.filter(k => colKeys.has(k));
    const missing = defaultOrder.filter(k => !valid.includes(k));
    return [...valid, ...missing];
  } catch {
    return defaultOrder;
  }
}

/* ── component ────────────────────────────────────────────── */

export default function DataTable({
  data,
  columns,
  searchable = false,
  searchPlaceholder = "Search...",
  searchKeys,
  pageSize = 25,
  onRowClick,
  rowClassName,
  emptyMessage = "No data",
  actions,
  stickyHeader = true,
  compact = false,
  draggableColumns = false,
  tableId,
}: DataTableProps) {
  const storageKey = draggableColumns && tableId ? `clxs-col-order-${tableId}` : null;

  const [search, setSearch] = useState("");
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const [page, setPage] = useState(0);
  const [columnOrder, setColumnOrder] = useState<string[]>(() =>
    loadColumnOrder(storageKey, columns),
  );

  // Sync column order when columns prop changes (e.g., dynamic columns)
  useEffect(() => {
    setColumnOrder(prev => {
      const colKeys = new Set(columns.map(c => c.key));
      const valid = prev.filter(k => colKeys.has(k));
      const missing = columns.map(c => c.key).filter(k => !valid.includes(k));
      const next = [...valid, ...missing];
      // Only update if actually changed
      if (next.length === prev.length && next.every((k, i) => k === prev[i])) return prev;
      return next;
    });
  }, [columns]);

  // Persist column order
  useEffect(() => {
    if (storageKey) {
      localStorage.setItem(storageKey, JSON.stringify(columnOrder));
    }
  }, [columnOrder, storageKey]);

  // Ordered columns for rendering
  const orderedColumns = useMemo(() => {
    if (!draggableColumns) return columns;
    const colMap = new Map(columns.map(c => [c.key, c]));
    return columnOrder.map(key => colMap.get(key)).filter(Boolean) as Column[];
  }, [columns, columnOrder, draggableColumns]);

  const defaultOrder = useMemo(() => columns.map(c => c.key), [columns]);
  const isReordered = draggableColumns &&
    JSON.stringify(columnOrder) !== JSON.stringify(defaultOrder);

  // DnD sensors
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor),
  );

  const handleDragEnd = useCallback((event: any) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setColumnOrder(prev => {
        const oldIndex = prev.indexOf(active.id as string);
        const newIndex = prev.indexOf(over.id as string);
        return arrayMove(prev, oldIndex, newIndex);
      });
    }
  }, []);

  const resetColumnOrder = useCallback(() => {
    setColumnOrder(defaultOrder);
  }, [defaultOrder]);

  // Filter
  const keys = searchKeys || orderedColumns.map(c => c.key);
  const filtered = useMemo(() => {
    if (!search.trim()) return data;
    const q = search.toLowerCase();
    return data.filter(row =>
      keys.some(k => String(row[k] ?? "").toLowerCase().includes(q))
    );
  }, [data, search, keys]);

  // Sort
  const sorted = useMemo(() => {
    if (!sortCol || !sortDir) return filtered;
    const col = orderedColumns.find(c => c.key === sortCol);
    const sk = col?.sortKey || sortCol;
    return [...filtered].sort((a, b) => {
      const av = a[sk], bv = b[sk];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      const sa = String(av).toLowerCase(), sb = String(bv).toLowerCase();
      return sortDir === "asc" ? sa.localeCompare(sb) : sb.localeCompare(sa);
    });
  }, [filtered, sortCol, sortDir, orderedColumns]);

  // Paginate
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const safePage = Math.min(page, totalPages - 1);
  const paginated = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize);

  function handleSort(key: string) {
    if (sortCol === key) {
      if (sortDir === "asc") setSortDir("desc");
      else if (sortDir === "desc") { setSortCol(null); setSortDir(null); }
    } else {
      setSortCol(key);
      setSortDir("asc");
    }
    setPage(0);
  }

  const py = compact ? "py-1.5" : "py-2";
  const px = compact ? "px-2" : "px-3";
  const textSize = compact ? "text-xs" : "text-sm";

  /* ── header row ─────────────────────────────────────────── */

  const headerRow = (
    <tr className="border-b border-border">
      {orderedColumns.map(col =>
        draggableColumns ? (
          <SortableColumnHeader
            key={col.key}
            col={col}
            draggable
            isSorted={sortCol === col.key}
            sortDir={sortDir}
            onSort={handleSort}
            py={py}
            px={px}
          />
        ) : (
          <th
            key={col.key}
            className={`${py} ${px} font-medium text-muted-foreground text-${col.align || "left"} ${col.headerClassName || ""} ${col.sortable ? "cursor-pointer select-none hover:text-foreground transition-colors" : ""}`}
            style={col.width ? { width: col.width } : undefined}
            onClick={col.sortable ? () => handleSort(col.key) : undefined}
          >
            <span className="inline-flex items-center gap-1">
              {col.label}
              {col.sortable && (
                sortCol === col.key ? (
                  sortDir === "asc"
                    ? <ChevronUp className="h-3 w-3 text-blue-600" />
                    : <ChevronDown className="h-3 w-3 text-blue-600" />
                ) : (
                  <ChevronsUpDown className="h-3 w-3 opacity-30" />
                )
              )}
            </span>
          </th>
        )
      )}
    </tr>
  );

  return (
    <div>
      {/* Toolbar */}
      {(searchable || actions || isReordered) && (
        <div className="flex items-center justify-between gap-3 mb-3 flex-wrap">
          {searchable && (
            <div className="relative flex-1 min-w-[200px] max-w-sm">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <input
                type="text"
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(0); }}
                placeholder={searchPlaceholder}
                className="w-full pl-9 pr-3 py-2 text-sm bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-blue-600 focus:border-blue-600"
              />
            </div>
          )}
          <div className="flex items-center gap-2">
            {isReordered && (
              <button
                onClick={resetColumnOrder}
                title="Reset column order"
                className="inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-muted-foreground hover:text-foreground border border-border rounded-lg hover:bg-muted transition-colors"
              >
                <RotateCcw className="h-3 w-3" />
                Reset columns
              </button>
            )}
            {actions}
          </div>
        </div>
      )}

      {/* Table */}
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
          <table className={`w-full ${textSize}`}>
            <thead className={`bg-muted/50 ${stickyHeader ? "sticky top-0 z-10" : ""}`}>
              {draggableColumns ? (
                <DndContext
                  sensors={sensors}
                  collisionDetection={closestCenter}
                  onDragEnd={handleDragEnd}
                >
                  <SortableContext items={columnOrder} strategy={horizontalListSortingStrategy}>
                    {headerRow}
                  </SortableContext>
                </DndContext>
              ) : (
                headerRow
              )}
            </thead>
            <tbody>
              {paginated.length === 0 ? (
                <tr>
                  <td colSpan={orderedColumns.length} className="py-12 text-center text-muted-foreground">
                    {emptyMessage}
                  </td>
                </tr>
              ) : (
                paginated.map((row, i) => (
                  <tr
                    key={i}
                    className={`border-b border-border/50 transition-colors ${onRowClick ? "cursor-pointer" : ""} hover:bg-muted/30 ${rowClassName ? rowClassName(row) : ""}`}
                    onClick={onRowClick ? () => onRowClick(row, safePage * pageSize + i) : undefined}
                  >
                    {orderedColumns.map(col => (
                      <td
                        key={col.key}
                        className={`${py} ${px} text-${col.align || "left"} ${col.className || ""}`}
                      >
                        {col.render ? col.render(row[col.key], row) : (row[col.key] ?? "—")}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {sorted.length > pageSize && (
        <div className="flex items-center justify-between mt-3 text-xs text-muted-foreground">
          <span>
            Showing {safePage * pageSize + 1}–{Math.min((safePage + 1) * pageSize, sorted.length)} of {sorted.length}
            {search && ` (filtered from ${data.length})`}
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={safePage === 0}
              className="p-1.5 rounded-md hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            {Array.from({ length: Math.min(totalPages, 5) }, (_, i) => {
              let pageNum;
              if (totalPages <= 5) pageNum = i;
              else if (safePage < 3) pageNum = i;
              else if (safePage > totalPages - 4) pageNum = totalPages - 5 + i;
              else pageNum = safePage - 2 + i;
              return (
                <button
                  key={pageNum}
                  onClick={() => setPage(pageNum)}
                  className={`w-7 h-7 rounded-md text-xs font-medium transition-colors ${safePage === pageNum ? "bg-blue-600 text-white" : "hover:bg-muted"}`}
                >
                  {pageNum + 1}
                </button>
              );
            })}
            <button
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={safePage >= totalPages - 1}
              className="p-1.5 rounded-md hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
