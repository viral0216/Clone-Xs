// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, HardDrive, Database, Trash2, Clock, AlertTriangle, Download } from "lucide-react";

function vacuumColor(pct: number) {
  if (pct >= 30) return "text-red-500";
  if (pct >= 10) return "text-yellow-500";
  return "text-green-500";
}

export default function StorageMetricsPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState<any>(null);

  async function analyze() {
    setLoading(true);
    setError("");
    setResults(null);
    try {
      const data = await api.post("/storage-metrics", {
        source_catalog: catalog,
        schema_filter: schema || undefined,
        table_filter: table || undefined,
      });
      setResults(data);
    } catch (e: any) {
      setError(e.message || "Storage metrics analysis failed");
    } finally {
      setLoading(false);
    }
  }

  function downloadFile(content: string, filename: string, type: string) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function exportCSV() {
    if (!results?.tables?.length) return;
    const headers = ["Schema", "Table", "Total Bytes", "Total", "Active Bytes", "Active", "Active %", "Vacuumable Bytes", "Vacuumable", "Vacuumable %", "Time Travel Bytes", "Time Travel", "Time Travel %", "Total Files", "Status"];
    const rows = results.tables.map((t: any) => [
      t.schema, t.table, t.total_bytes, t.total_display, t.active_bytes, t.active_display, t.active_pct,
      t.vacuumable_bytes, t.vacuumable_display, t.vacuumable_pct, t.time_travel_bytes, t.time_travel_display,
      t.time_travel_pct, t.num_total_files, t.error || "OK",
    ]);
    const csv = [headers, ...rows].map(r => r.map((v: any) => `"${v}"`).join(",")).join("\n");
    downloadFile(csv, `storage-metrics-${results.catalog}.csv`, "text/csv");
  }

  function exportJSON() {
    if (!results) return;
    downloadFile(JSON.stringify(results, null, 2), `storage-metrics-${results.catalog}.json`, "application/json");
  }

  const tables = results?.tables || [];
  const topVacuumable = results?.top_tables_by_vacuumable || [];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Storage Metrics</h1>
        <p className="text-muted-foreground mt-1">
          Analyze storage breakdown — active, vacuumable, and time-travel data per table
        </p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table={table}
              onCatalogChange={(v: string) => { setCatalog(v); setSchema(""); setTable(""); }}
              onSchemaChange={(v: string) => { setSchema(v); setTable(""); }}
              onTableChange={setTable}
              showSchema={true}
              showTable={true}
            />
            <Button onClick={analyze} disabled={!catalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <HardDrive className="h-4 w-4 mr-2" />}
              {loading ? "Analyzing..." : "Analyze Storage"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">
              Running ANALYZE TABLE ... COMPUTE STORAGE METRICS across {schema ? "schema" : "catalog"}...
            </p>
            <p className="text-xs text-muted-foreground mt-1">This may take a few minutes for large catalogs</p>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5 shrink-0" />{error}
          </CardContent>
        </Card>
      )}

      {results?.runtime_error && (
        <Card className="border-yellow-500/30 bg-card">
          <CardContent className="pt-6 flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-yellow-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-yellow-500 font-medium">Runtime Compatibility Issue</p>
              <p className="text-sm text-muted-foreground mt-1">{results.runtime_error}</p>
            </div>
          </CardContent>
        </Card>
      )}

      {results?.num_errors > 0 && !results?.runtime_error && (
        <Card className="border-yellow-500/30 bg-card">
          <CardContent className="pt-6 flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-yellow-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-yellow-500 font-medium">{results.num_errors} of {results.num_tables} table(s) had errors</p>
              <p className="text-sm text-muted-foreground mt-1">
                These tables may require Runtime 18.0+ or may have unsupported formats.
                Tables with errors show 0 B in the results below.
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {results && !results.runtime_error && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="bg-card border-border">
              <CardContent className="pt-6 text-center">
                <HardDrive className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
                <p className="text-2xl font-bold text-foreground">{results.total_display}</p>
                <p className="text-xs text-muted-foreground mt-1">Total Storage</p>
                <p className="text-xs text-muted-foreground">{results.num_total_files?.toLocaleString()} files</p>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-6 text-center">
                <Database className="h-5 w-5 mx-auto mb-1 text-blue-500" />
                <p className="text-2xl font-bold text-foreground">{results.active_display}</p>
                <p className="text-xs text-muted-foreground mt-1">Active Data</p>
                <Badge variant="outline" className="mt-1 text-blue-500 border-blue-500/30">{results.active_pct}%</Badge>
              </CardContent>
            </Card>
            <Card className="bg-card border-border border-yellow-500/20">
              <CardContent className="pt-6 text-center">
                <Trash2 className="h-5 w-5 mx-auto mb-1 text-yellow-500" />
                <p className="text-2xl font-bold text-yellow-500">{results.vacuumable_display}</p>
                <p className="text-xs text-muted-foreground mt-1">Vacuumable</p>
                <Badge variant="outline" className="mt-1 text-yellow-500 border-yellow-500/30">{results.vacuumable_pct}%</Badge>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-6 text-center">
                <Clock className="h-5 w-5 mx-auto mb-1 text-purple-500" />
                <p className="text-2xl font-bold text-foreground">{results.time_travel_display}</p>
                <p className="text-xs text-muted-foreground mt-1">Time Travel</p>
                <Badge variant="outline" className="mt-1 text-purple-500 border-purple-500/30">{results.time_travel_pct}%</Badge>
              </CardContent>
            </Card>
          </div>

          {/* Top Reclaimable Tables */}
          {topVacuumable.length > 0 && (
            <Card className="bg-card border-border border-yellow-500/20">
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center gap-2">
                  <Trash2 className="h-4 w-4 text-yellow-500" />
                  Top Reclaimable Tables
                  <span className="text-xs font-normal text-muted-foreground ml-1">run VACUUM to reclaim</span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                  {topVacuumable.map((t: any, i: number) => (
                    <div key={i} className="flex items-center justify-between px-3 py-2 rounded-lg bg-background border border-border">
                      <span className="text-sm text-foreground truncate">
                        <span className="text-muted-foreground">{t.schema}.</span>{t.table}
                      </span>
                      <span className="flex items-center gap-2 shrink-0 ml-2">
                        <span className={`text-sm font-medium ${vacuumColor(t.vacuumable_pct)}`}>{t.vacuumable_display}</span>
                        <Badge variant="outline" className={`text-xs ${vacuumColor(t.vacuumable_pct)}`}>{t.vacuumable_pct}%</Badge>
                      </span>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Detail Table */}
          {tables.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg">
                    All Tables ({tables.length})
                  </CardTitle>
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={exportCSV}>
                      <Download className="h-3.5 w-3.5 mr-1.5" />CSV
                    </Button>
                    <Button variant="outline" size="sm" onClick={exportJSON}>
                      <Download className="h-3.5 w-3.5 mr-1.5" />JSON
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto border border-border rounded">
                  <table className="w-full text-sm">
                    <thead className="bg-background">
                      <tr className="border-b border-border">
                        <th className="text-left py-2 px-3 font-medium text-foreground">Schema</th>
                        <th className="text-left py-2 px-3 font-medium text-foreground">Table</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Total</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Active</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Vacuumable</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Vacuum %</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Time Travel</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">TT %</th>
                        <th className="text-right py-2 px-3 font-medium text-foreground">Files</th>
                        <th className="text-center py-2 px-3 font-medium text-foreground">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tables.map((row: any, i: number) => (
                        <tr key={i} className={`border-b border-border ${row.error ? "opacity-60" : ""}`}>
                          <td className="py-2 px-3 text-muted-foreground">{row.schema}</td>
                          <td className="py-2 px-3 font-medium text-foreground">{row.table}</td>
                          <td className="py-2 px-3 text-right text-foreground">{row.total_display}</td>
                          <td className="py-2 px-3 text-right text-foreground">{row.active_display}</td>
                          <td className="py-2 px-3 text-right text-yellow-500">{row.vacuumable_display}</td>
                          <td className="py-2 px-3 text-right">
                            <span className={vacuumColor(row.vacuumable_pct)}>{row.vacuumable_pct}%</span>
                          </td>
                          <td className="py-2 px-3 text-right text-purple-500">{row.time_travel_display}</td>
                          <td className="py-2 px-3 text-right text-purple-500">{row.time_travel_pct}%</td>
                          <td className="py-2 px-3 text-right text-muted-foreground">{row.num_total_files?.toLocaleString()}</td>
                          <td className="py-2 px-3 text-center">
                            {row.error ? (
                              <span title={row.error}><XCircle className="h-4 w-4 text-red-500 inline" /></span>
                            ) : (
                              <span className="text-green-500 text-xs">OK</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
