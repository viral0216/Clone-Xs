// @ts-nocheck
import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import { Loader2 } from "lucide-react";

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
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [loadingCatalogs, setLoadingCatalogs] = useState(false);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);

  // Load catalogs on mount
  useEffect(() => {
    setLoadingCatalogs(true);
    api.get<string[]>("/catalogs")
      .then((data) => setCatalogs(data || []))
      .catch(() => setCatalogs([]))
      .finally(() => setLoadingCatalogs(false));
  }, []);

  // Load schemas when catalog changes
  useEffect(() => {
    if (!catalog || !showSchema) {
      setSchemas([]);
      setTables([]);
      return;
    }
    setLoadingSchemas(true);
    setSchemas([]);
    setTables([]);
    api.get<string[]>(`/catalogs/${catalog}/schemas`)
      .then((data) => setSchemas(data || []))
      .catch(() => setSchemas([]))
      .finally(() => setLoadingSchemas(false));
  }, [catalog, showSchema]);

  // Load tables when schema changes
  useEffect(() => {
    if (!catalog || !schema || !showTable) {
      setTables([]);
      return;
    }
    setLoadingTables(true);
    setTables([]);
    api.get<string[]>(`/catalogs/${catalog}/${schema}/tables`)
      .then((data) => setTables(data || []))
      .catch(() => setTables([]))
      .finally(() => setLoadingTables(false));
  }, [catalog, schema, showTable]);

  const selectClass =
    "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]";

  return (
    <div className="flex flex-col sm:flex-row gap-3 sm:gap-4 sm:items-end">
      {/* Catalog */}
      <div className="flex-1">
        <label className="text-sm font-medium mb-1 block">Catalog</label>
        {loadingCatalogs ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading catalogs...
          </div>
        ) : catalogs.length > 0 ? (
          <select className={selectClass} value={catalog} onChange={(e) => {
            onCatalogChange(e.target.value);
            onSchemaChange?.("");
            onTableChange?.("");
          }}>
            <option value="">Select catalog...</option>
            {catalogs.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        ) : (
          <input
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
          <label className="text-sm font-medium mb-1 block">{schemaLabel}</label>
          {loadingSchemas ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : schemas.length > 0 ? (
            <select className={selectClass} value={schema} onChange={(e) => {
              onSchemaChange?.(e.target.value);
              onTableChange?.("");
            }}>
              <option value="">All schemas</option>
              {schemas.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          ) : (
            <input
              className={selectClass}
              value={schema}
              onChange={(e) => onSchemaChange?.(e.target.value)}
              placeholder={catalog ? "No schemas found" : "Select catalog first"}
              disabled={!catalog}
            />
          )}
        </div>
      )}

      {/* Table */}
      {showTable && (
        <div className="flex-1">
          <label className="text-sm font-medium mb-1 block">{tableLabel}</label>
          {loadingTables ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : tables.length > 0 ? (
            <select className={selectClass} value={table} onChange={(e) => onTableChange?.(e.target.value)}>
              <option value="">All tables</option>
              {tables.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          ) : (
            <input
              className={selectClass}
              value={table}
              onChange={(e) => onTableChange?.(e.target.value)}
              placeholder={schema ? "No tables found" : "Select schema first"}
              disabled={!schema}
            />
          )}
        </div>
      )}
    </div>
  );
}
