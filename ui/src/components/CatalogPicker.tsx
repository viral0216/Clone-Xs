// @ts-nocheck
import { useState, useRef, useEffect, useLayoutEffect } from "react";
import { createPortal } from "react-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api-client";
import { Loader2, RefreshCw, ChevronDown, Check } from "lucide-react";

interface CatalogPickerProps {
  catalog?: string;
  schema?: string;
  table?: string;
  onCatalogChange?: (catalog: string) => void;
  onSchemaChange?: (schema: string) => void;
  onTableChange?: (table: string) => void;
  showSchema?: boolean;
  showTable?: boolean;
  schemaLabel?: string;
  tableLabel?: string;
  placeholder?: string;
  /** Alias for `catalog` — used by some pages. */
  value?: string;
  /** Alias for `onCatalogChange` — used by some pages. */
  onChange?: (catalog: string) => void;
  /** Unique ID prefix when multiple pickers are on the same page. */
  idPrefix?: string;
  /** Opt-in multi-select mode. When true, the catalog dropdown becomes
   * a checkbox popover and emits `string[]` via `onCatalogsChange`.
   * Schema / Table sub-pickers are hidden in multi mode (they're
   * single-catalog concepts). */
  multi?: boolean;
  /** Multi-select catalogs. Only consulted when `multi=true`. */
  selectedCatalogs?: string[];
  /** Multi-select callback. Only consulted when `multi=true`. */
  onCatalogsChange?: (catalogs: string[]) => void;
}

export default function CatalogPicker({
  catalog: catalogProp,
  schema = "",
  table = "",
  onCatalogChange: onCatalogChangeProp,
  onSchemaChange,
  onTableChange,
  showSchema = true,
  showTable = true,
  schemaLabel = "Schema",
  tableLabel = "Table",
  value,
  onChange,
  idPrefix,
  multi = false,
  selectedCatalogs = [],
  onCatalogsChange,
}: CatalogPickerProps) {
  // Support both prop conventions: catalog/onCatalogChange and value/onChange
  const catalog = catalogProp ?? value ?? "";
  const onCatalogChange = onCatalogChangeProp ?? onChange ?? (() => {});
  const prefix = idPrefix ? `${idPrefix}-` : "catalog-picker-";
  const qc = useQueryClient();
  // Multi-select popover state. The popover is rendered via a portal
  // anchored to document.body and positioned with fixed coords from the
  // trigger button's bounding rect — necessary because our `Card`
  // component sets `overflow: hidden`, which would otherwise clip the
  // dropdown when it's rendered inline. We close on outside-click +
  // re-measure on scroll / resize so the popover tracks its anchor.
  const [multiOpen, setMultiOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const popoverRef = useRef<HTMLDivElement | null>(null);
  const [popoverRect, setPopoverRect] = useState<{ top: number; left: number; width: number } | null>(null);

  useLayoutEffect(() => {
    if (!multiOpen) return;
    const measure = () => {
      const el = triggerRef.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      setPopoverRect({ top: r.bottom + 4, left: r.left, width: r.width });
    };
    measure();
    window.addEventListener("scroll", measure, true);
    window.addEventListener("resize", measure);
    return () => {
      window.removeEventListener("scroll", measure, true);
      window.removeEventListener("resize", measure);
    };
  }, [multiOpen]);

  useEffect(() => {
    if (!multiOpen) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as Node;
      if (popoverRef.current?.contains(target)) return;
      if (triggerRef.current?.contains(target)) return;
      setMultiOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [multiOpen]);

  // Cached queries — persist to localStorage via React Query persister
  const catalogsQuery = useQuery<string[]>({
    queryKey: ["catalogs"],
    queryFn: () => api.get("/catalogs"),
    staleTime: 1000 * 60 * 10, // 10 min — catalogs rarely change
    gcTime: 1000 * 60 * 60 * 24, // 24h in cache
  });

  const schemasQuery = useQuery<string[]>({
    queryKey: ["schemas", catalog],
    queryFn: () => api.get(`/catalogs/${catalog}/schemas`),
    enabled: !!catalog && showSchema,
    staleTime: 1000 * 60 * 10,
    gcTime: 1000 * 60 * 60 * 24,
  });

  const tablesQuery = useQuery<string[]>({
    queryKey: ["tables", catalog, schema],
    queryFn: () => api.get(`/catalogs/${catalog}/${schema}/tables`),
    enabled: !!catalog && !!schema && showTable,
    staleTime: 1000 * 60 * 5, // 5 min — tables change more often
    gcTime: 1000 * 60 * 60 * 24,
  });

  const catalogs = catalogsQuery.data || [];
  const schemas = schemasQuery.data || [];
  const tables = tablesQuery.data || [];

  const isRefreshing = catalogsQuery.isFetching || schemasQuery.isFetching || tablesQuery.isFetching;

  function handleRefresh() {
    qc.invalidateQueries({ queryKey: ["catalogs"] });
    if (catalog) qc.invalidateQueries({ queryKey: ["schemas", catalog] });
    if (catalog && schema) qc.invalidateQueries({ queryKey: ["tables", catalog, schema] });
  }

  const selectClass =
    "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#E8453C]/30 focus:border-[#E8453C]";

  return (
    <div className="space-y-3">
      <div className="flex flex-col sm:flex-row gap-3 sm:gap-4 sm:items-end">
        {/* Catalog */}
        <div className="flex-1">
          <label htmlFor={`${prefix}catalog`} className="text-sm font-medium mb-1 block">
            {multi ? "Catalogs" : "Catalog"}
          </label>
          {catalogsQuery.isLoading && !catalogsQuery.data ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading catalogs...
            </div>
          ) : multi ? (
            // Multi-select popover. The button stays inline; the
            // dropdown panel is portalled to <body> with fixed
            // positioning so the parent Card's `overflow: hidden`
            // can't clip it.
            <>
              <button
                type="button"
                id={`${prefix}catalog`}
                ref={triggerRef}
                className={`${selectClass} text-left flex items-center justify-between`}
                onClick={() => setMultiOpen((o) => !o)}
              >
                <span className="truncate">
                  {selectedCatalogs.length === 0
                    ? "Select catalogs..."
                    : selectedCatalogs.length === 1
                    ? selectedCatalogs[0]
                    : `${selectedCatalogs.length} catalogs selected`}
                </span>
                <ChevronDown className="h-4 w-4 ml-2 shrink-0 text-muted-foreground" />
              </button>
              {multiOpen && catalogs.length > 0 && popoverRect && createPortal(
                <div
                  ref={popoverRef}
                  className="fixed z-[1000] max-h-64 overflow-y-auto bg-background border border-input rounded-md shadow-lg"
                  style={{ top: popoverRect.top, left: popoverRect.left, width: popoverRect.width }}
                >
                  {/* Quick "select all / clear" controls — useful for
                      catalogs lists with 5+ entries. */}
                  <div className="flex items-center justify-between px-3 py-1.5 border-b border-border text-xs sticky top-0 bg-background">
                    <button
                      type="button"
                      onClick={() => onCatalogsChange?.([...catalogs])}
                      className="text-[#E8453C] hover:underline"
                    >
                      Select all
                    </button>
                    <button
                      type="button"
                      onClick={() => onCatalogsChange?.([])}
                      className="text-muted-foreground hover:text-foreground"
                    >
                      Clear
                    </button>
                  </div>
                  {catalogs.map((c) => {
                    const checked = selectedCatalogs.includes(c);
                    return (
                      <label
                        key={c}
                        className="flex items-center gap-2 px-3 py-1.5 hover:bg-muted/50 cursor-pointer text-sm"
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={(e) => {
                            const next = e.target.checked
                              ? [...selectedCatalogs, c]
                              : selectedCatalogs.filter((x) => x !== c);
                            onCatalogsChange?.(next);
                          }}
                          className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
                        />
                        <span className="flex-1 truncate">{c}</span>
                        {checked && <Check className="h-3.5 w-3.5 text-[#E8453C]" />}
                      </label>
                    );
                  })}
                </div>,
                document.body,
              )}
            </>
          ) : catalogs.length > 0 ? (
            <select id={`${prefix}catalog`} className={selectClass} value={catalog} onChange={(e) => {
              onCatalogChange(e.target.value);
              onSchemaChange?.("");
              onTableChange?.("");
            }}>
              <option value="">Select catalog...</option>
              {catalogs.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          ) : (
            <input
              id={`${prefix}catalog`}
              className={selectClass}
              value={catalog}
              onChange={(e) => onCatalogChange(e.target.value)}
              placeholder="Enter catalog name"
            />
          )}
        </div>

        {/* Schema */}
        {showSchema && (
          <div className="flex-1">
            <label htmlFor={`${prefix}schema`} className="text-sm font-medium mb-1 block">{schemaLabel}</label>
            {schemasQuery.isLoading && !schemasQuery.data ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading...
              </div>
            ) : schemas.length > 0 ? (
              <select id={`${prefix}schema`} className={selectClass} value={schema} onChange={(e) => {
                onSchemaChange?.(e.target.value);
                onTableChange?.("");
              }}>
                <option value="">All schemas</option>
                {schemas.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
            ) : (
              <input
                id={`${prefix}schema`}
                className={selectClass}
                value={schema}
                onChange={(e) => onSchemaChange?.(e.target.value)}
                placeholder={catalog ? "No schemas found" : "Select catalog first"}
                disabled={!catalog}
                aria-describedby={!catalog ? "schema-hint" : undefined}
              />
            )}
            {!catalog && <span id="schema-hint" className="sr-only">Select a catalog first to browse schemas</span>}
          </div>
        )}

        {/* Table */}
        {showTable && (
          <div className="flex-1">
            <label htmlFor={`${prefix}table`} className="text-sm font-medium mb-1 block">{tableLabel}</label>
            {tablesQuery.isLoading && !tablesQuery.data ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading...
              </div>
            ) : tables.length > 0 ? (
              <select id={`${prefix}table`} className={selectClass} value={table} onChange={(e) => onTableChange?.(e.target.value)}>
                <option value="">All tables</option>
                {tables.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
            ) : (
              <input
                id={`${prefix}table`}
                className={selectClass}
                value={table}
                onChange={(e) => onTableChange?.(e.target.value)}
                placeholder={schema ? "No tables found" : "Select schema first"}
                disabled={!schema}
                aria-describedby={!schema ? "table-hint" : undefined}
              />
            )}
            {!schema && <span id="table-hint" className="sr-only">Select a schema first to browse tables</span>}
          </div>
        )}

        {/* Refresh button */}
        <div className="shrink-0 sm:pb-0.5">
          <button
            type="button"
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="p-2 rounded-lg border border-border text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors disabled:opacity-50"
            title="Refresh catalog data"
            aria-label="Refresh catalog data"
          >
            <RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
          </button>
        </div>
      </div>
    </div>
  );
}
