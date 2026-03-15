// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import {
  RefreshCw, Loader2, CheckCircle, XCircle, ArrowRight,
  AlertTriangle, Database, Table2, Clock,
} from "lucide-react";

interface ChangedTable {
  table_name: string;
  reason: string;
  last_synced_version: number | null;
  current_version?: number;
  changes_since_sync?: number;
  operations?: string[];
}

interface SchemaCheck {
  schema: string;
  tables_needing_sync: number;
  tables: ChangedTable[];
  loading?: boolean;
  error?: string;
}

export default function IncrementalSyncPage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState("");
  const [schemaResults, setSchemaResults] = useState<SchemaCheck[]>([]);
  const [syncResult, setSyncResult] = useState<any>(null);
  // Track selected tables as "schema.table_name" keys
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());

  function tableKey(schema: string, table: string) { return `${schema}.${table}`; }

  // Fetch schemas for source catalog to iterate
  async function checkChanges() {
    setLoading(true);
    setError("");
    setSchemaResults([]);
    setSyncResult(null);
    setSelectedTables(new Set());

    try {
      if (sourceSchema) {
        // Single schema mode
        const data = await api.post("/incremental/check", {
          source_catalog: sourceCatalog,
          destination_catalog: destCatalog,
          schema_name: sourceSchema,
        });
        const result = { ...data, schema: sourceSchema };
        setSchemaResults([result]);
        // Auto-select all changed tables
        const keys = new Set((result.tables || []).map((t: ChangedTable) => tableKey(sourceSchema, t.table_name)));
        setSelectedTables(keys);
      } else {
        // All schemas mode — get schema list then check each
        const schemas = await api.get<string[]>(`/catalogs/${sourceCatalog}/schemas`);
        const filtered = (schemas || []).filter(
          (s) => s !== "information_schema" && s !== "default"
        );

        // Initialize with loading states
        const initial = filtered.map((s) => ({ schema: s, tables_needing_sync: 0, tables: [] as ChangedTable[], loading: true }));
        setSchemaResults(initial);

        // Fire all checks in parallel — each updates state as it resolves
        const promises = filtered.map(async (s, idx) => {
          try {
            const data = await api.post("/incremental/check", {
              source_catalog: sourceCatalog,
              destination_catalog: destCatalog,
              schema_name: s,
            });
            const result: SchemaCheck = { ...data, schema: s, loading: false };

            // Update this schema's result in-place
            setSchemaResults((prev) => prev.map((r, i) => i === idx ? result : r));

            // Auto-select changed tables as they come in
            if (result.tables?.length) {
              setSelectedTables((prev) => {
                const next = new Set(prev);
                result.tables.forEach((t: ChangedTable) => next.add(tableKey(s, t.table_name)));
                return next;
              });
            }
          } catch (err: any) {
            setSchemaResults((prev) =>
              prev.map((r, i) => i === idx ? { ...r, loading: false, error: err.message } : r)
            );
          }
        });

        await Promise.all(promises);
      }
    } catch (e: any) {
      setError(e.message || "Failed to check changes");
    } finally {
      setLoading(false);
    }
  }

  async function runSync() {
    setSyncing(true);
    setError("");
    setSyncResult(null);

    // Group selected tables by schema
    const bySchema = new Map<string, string[]>();
    selectedTables.forEach((key) => {
      const [schema, ...rest] = key.split(".");
      const tbl = rest.join(".");
      if (!bySchema.has(schema)) bySchema.set(schema, []);
      bySchema.get(schema)!.push(tbl);
    });

    const results: any[] = [];
    try {
      for (const [schema, tables] of bySchema.entries()) {
        const data = await api.post("/incremental/sync", {
          source_catalog: sourceCatalog,
          destination_catalog: destCatalog,
          schema_name: schema,
        });
        results.push({ schema, tables: tables.length, ...data });
      }
      setSyncResult({ schemas: results, total: selectedTables.size });
    } catch (e: any) {
      setError(e.message || "Sync failed");
    } finally {
      setSyncing(false);
    }
  }

  // Toggle a single table
  function toggleTable(schema: string, table: string) {
    const key = tableKey(schema, table);
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  // Toggle all tables in a schema
  function toggleSchema(schema: string, tables: ChangedTable[]) {
    const keys = tables.map((t) => tableKey(schema, t.table_name));
    const allSelected = keys.every((k) => selectedTables.has(k));
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (allSelected) {
        keys.forEach((k) => next.delete(k));
      } else {
        keys.forEach((k) => next.add(k));
      }
      return next;
    });
  }

  function selectAll() {
    const allKeys = new Set<string>();
    schemaResults.forEach((r) => {
      (r.tables || []).forEach((t) => allKeys.add(tableKey(r.schema, t.table_name)));
    });
    setSelectedTables(allKeys);
  }

  function deselectAll() {
    setSelectedTables(new Set());
  }

  const totalChanges = schemaResults.reduce((sum, r) => sum + r.tables_needing_sync, 0);
  const selectedCount = selectedTables.size;

  // How many tables selected in a given schema
  function schemaSelectedCount(schema: string, tables: ChangedTable[]) {
    return tables.filter((t) => selectedTables.has(tableKey(schema, t.table_name))).length;
  }

  function isSchemaAllSelected(schema: string, tables: ChangedTable[]) {
    return tables.length > 0 && tables.every((t) => selectedTables.has(tableKey(schema, t.table_name)));
  }

  function isSchemaSomeSelected(schema: string, tables: ChangedTable[]) {
    const count = schemaSelectedCount(schema, tables);
    return count > 0 && count < tables.length;
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Incremental Sync</h1>
        <p className="text-muted-foreground mt-1">
          Sync only changed tables using Delta table version history
        </p>
      </div>

      {/* Source & Destination Selection */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto_1fr] gap-4 items-end">
            {/* Source */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Source</p>
              <CatalogPicker
                catalog={sourceCatalog}
                schema={sourceSchema}
                onCatalogChange={(c) => { setSourceCatalog(c); setSourceSchema(""); }}
                onSchemaChange={setSourceSchema}
                showTable={false}
              />
            </div>

            <div className="flex items-center justify-center pb-2">
              <ArrowRight className="h-5 w-5 text-muted-foreground" />
            </div>

            {/* Destination */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">Destination</p>
              <CatalogPicker
                catalog={destCatalog}
                onCatalogChange={setDestCatalog}
                showSchema={false}
                showTable={false}
              />
            </div>
          </div>

          <div className="flex items-center gap-3 pt-2">
            <Button onClick={checkChanges} disabled={!sourceCatalog || !destCatalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {sourceSchema ? `Check ${sourceSchema}` : "Check All Schemas"}
            </Button>
            {!sourceSchema && sourceCatalog && (
              <span className="text-xs text-muted-foreground">
                Leave schema empty to scan all schemas
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Error */}
      {error && (
        <Card className="border-red-500/30 bg-red-500/5">
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-red-500">
              <XCircle className="h-5 w-5" /> {error}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Sync Result */}
      {syncResult && (
        <Card className="border-green-500/30 bg-green-500/5">
          <CardContent className="pt-6 space-y-2">
            <div className="flex items-center gap-2 text-green-500">
              <CheckCircle className="h-5 w-5" />
              Sync jobs submitted for {syncResult.total} schema(s)
            </div>
            {syncResult.schemas.map((s: any) => (
              <div key={s.schema} className="flex items-center gap-2 text-sm text-muted-foreground ml-7">
                <Database className="h-3 w-3" /> {s.schema}: Job {s.job_id}
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {/* Summary Bar */}
      {schemaResults.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <Database className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm text-foreground font-medium">
                    {schemaResults.filter((r) => !r.loading).length}/{schemaResults.length} schemas checked
                  </span>
                  {schemaResults.some((r) => r.loading) && (
                    <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
                  )}
                </div>
                <Badge variant={totalChanges > 0 ? "destructive" : "secondary"}>
                  {totalChanges} tables need sync
                </Badge>
                {selectedCount > 0 && (
                  <Badge variant="outline">
                    {selectedCount} table{selectedCount !== 1 ? "s" : ""} selected
                  </Badge>
                )}
              </div>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="sm" onClick={selectAll}>Select all</Button>
                <Button variant="ghost" size="sm" onClick={deselectAll}>Clear</Button>
                <Button
                  onClick={runSync}
                  disabled={selectedCount === 0 || syncing}
                >
                  {syncing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                  Sync {selectedCount} Table{selectedCount !== 1 ? "s" : ""}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Per-Schema Results */}
      {schemaResults.length > 0 && (
        <div className="space-y-3">
          {schemaResults.map((sr) => (
            <Card
              key={sr.schema}
              className={`bg-card border-border transition-all ${
                schemaSelectedCount(sr.schema, sr.tables || []) > 0 ? "ring-1 ring-[#FF3621]/40" : ""
              }`}
            >
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    {sr.tables_needing_sync > 0 && (
                      <input
                        type="checkbox"
                        checked={isSchemaAllSelected(sr.schema, sr.tables || [])}
                        ref={(el) => { if (el) el.indeterminate = isSchemaSomeSelected(sr.schema, sr.tables || []); }}
                        onChange={() => toggleSchema(sr.schema, sr.tables || [])}
                        className="h-4 w-4 rounded border-border"
                        title="Select/deselect all tables in this schema"
                      />
                    )}
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <CardTitle className="text-base">{sr.schema}</CardTitle>
                    {sr.tables_needing_sync > 0 && schemaSelectedCount(sr.schema, sr.tables || []) > 0 && (
                      <span className="text-xs text-muted-foreground">
                        ({schemaSelectedCount(sr.schema, sr.tables || [])}/{sr.tables_needing_sync} selected)
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {sr.loading ? (
                      <Badge variant="secondary"><Loader2 className="h-3 w-3 animate-spin mr-1" /> Checking...</Badge>
                    ) : sr.error ? (
                      <Badge variant="destructive">Error</Badge>
                    ) : sr.tables_needing_sync === 0 ? (
                      <Badge variant="secondary" className="text-green-500">
                        <CheckCircle className="h-3 w-3 mr-1" /> Up to date
                      </Badge>
                    ) : (
                      <Badge variant="destructive">
                        <AlertTriangle className="h-3 w-3 mr-1" /> {sr.tables_needing_sync} changed
                      </Badge>
                    )}
                  </div>
                </div>
              </CardHeader>

              {sr.tables && sr.tables.length > 0 && (
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-left text-muted-foreground">
                          <th className="py-2 px-3 w-8"></th>
                          <th className="py-2 px-3">Table</th>
                          <th className="py-2 px-3">Status</th>
                          <th className="py-2 px-3">Last Synced</th>
                          <th className="py-2 px-3">Current</th>
                          <th className="py-2 px-3">Changes</th>
                          <th className="py-2 px-3">Operations</th>
                        </tr>
                      </thead>
                      <tbody>
                        {sr.tables.map((t) => (
                          <tr
                            key={t.table_name}
                            className={`border-b border-border/50 hover:bg-muted/50 cursor-pointer ${
                              selectedTables.has(tableKey(sr.schema, t.table_name)) ? "bg-[#FF3621]/5" : ""
                            }`}
                            onClick={() => toggleTable(sr.schema, t.table_name)}
                          >
                            <td className="py-2 px-3">
                              <input
                                type="checkbox"
                                checked={selectedTables.has(tableKey(sr.schema, t.table_name))}
                                onChange={() => toggleTable(sr.schema, t.table_name)}
                                onClick={(e) => e.stopPropagation()}
                                className="h-4 w-4 rounded border-border"
                              />
                            </td>
                            <td className="py-2 px-3 font-mono text-foreground flex items-center gap-2">
                              <Table2 className="h-3 w-3 text-muted-foreground" /> {t.table_name}
                            </td>
                            <td className="py-2 px-3">
                              <Badge variant={t.reason === "never_synced" ? "destructive" : "secondary"} className="text-xs">
                                {t.reason === "never_synced" ? "New" : "Changed"}
                              </Badge>
                            </td>
                            <td className="py-2 px-3 text-muted-foreground font-mono text-xs">
                              {t.last_synced_version != null ? `v${t.last_synced_version}` : "—"}
                            </td>
                            <td className="py-2 px-3 text-muted-foreground font-mono text-xs">
                              {t.current_version != null ? `v${t.current_version}` : "—"}
                            </td>
                            <td className="py-2 px-3 text-muted-foreground">
                              {t.changes_since_sync != null ? t.changes_since_sync : "—"}
                            </td>
                            <td className="py-2 px-3">
                              {t.operations?.length ? (
                                <div className="flex flex-wrap gap-1">
                                  {t.operations.slice(0, 3).map((op, i) => (
                                    <Badge key={i} variant="outline" className="text-xs">{op}</Badge>
                                  ))}
                                  {t.operations.length > 3 && (
                                    <Badge variant="outline" className="text-xs">+{t.operations.length - 3}</Badge>
                                  )}
                                </div>
                              ) : "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              )}
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
