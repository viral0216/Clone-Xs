// @ts-nocheck
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api-client";
import { Loader2, RefreshCw } from "lucide-react";

interface CatalogPickerProps {
  catalog: string;
  schema?: string;
  table?: string;
  onCatalogChange: (catalog: string) => void;
  onSchemaChange?: (schema: string) => void;
  onTableChange?: (table: string) => void;
  showSchema?: boolean;
  showTable?: boolean;
  schemaLabel?: string;
  tableLabel?: string;
}

export default function CatalogPicker({
  catalog,
  schema = "",
  table = "",
  onCatalogChange,
  onSchemaChange,
  onTableChange,
  showSchema = true,
  showTable = true,
  schemaLabel = "Schema",
  tableLabel = "Table",
}: CatalogPickerProps) {
  const qc = useQueryClient();

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
          <label htmlFor="catalog-picker-catalog" className="text-sm font-medium mb-1 block">Catalog</label>
          {catalogsQuery.isLoading && !catalogsQuery.data ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading catalogs...
            </div>
          ) : catalogs.length > 0 ? (
            <select id="catalog-picker-catalog" className={selectClass} value={catalog} onChange={(e) => {
              onCatalogChange(e.target.value);
              onSchemaChange?.("");
              onTableChange?.("");
            }}>
              <option value="">Select catalog...</option>
              {catalogs.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          ) : (
            <input
              id="catalog-picker-catalog"
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
            <label htmlFor="catalog-picker-schema" className="text-sm font-medium mb-1 block">{schemaLabel}</label>
            {schemasQuery.isLoading && !schemasQuery.data ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading...
              </div>
            ) : schemas.length > 0 ? (
              <select id="catalog-picker-schema" className={selectClass} value={schema} onChange={(e) => {
                onSchemaChange?.(e.target.value);
                onTableChange?.("");
              }}>
                <option value="">All schemas</option>
                {schemas.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
            ) : (
              <input
                id="catalog-picker-schema"
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
            <label htmlFor="catalog-picker-table" className="text-sm font-medium mb-1 block">{tableLabel}</label>
            {tablesQuery.isLoading && !tablesQuery.data ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> Loading...
              </div>
            ) : tables.length > 0 ? (
              <select id="catalog-picker-table" className={selectClass} value={table} onChange={(e) => onTableChange?.(e.target.value)}>
                <option value="">All tables</option>
                {tables.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
            ) : (
              <input
                id="catalog-picker-table"
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
