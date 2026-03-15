// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, Columns, ArrowRight } from "lucide-react";

export default function PreviewPage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState<any>(null);

  async function preview() {
    setLoading(true);
    setError("");
    setResults(null);
    try {
      let data;
      if (destCatalog) {
        // Compare mode: source vs destination
        const compare = await api.post("/sample/compare", {
          source_catalog: sourceCatalog,
          destination_catalog: destCatalog,
          schema_name: schema,
          table_name: table,
          limit: 50,
        });
        // Fetch source rows for display
        const srcSample = await api.post("/sample", {
          catalog: sourceCatalog, schema_name: schema, table_name: table, limit: 50,
        });
        const dstSample = await api.post("/sample", {
          catalog: destCatalog, schema_name: schema, table_name: table, limit: 50,
        });
        data = {
          source_rows: srcSample.rows || [],
          dest_rows: dstSample.rows || [],
          source_count: compare.source_rows,
          dest_count: compare.dest_rows,
          differences: compare.sample_diffs || [],
          match: compare.match,
        };
      } else {
        // Single table preview
        const sample = await api.post("/sample", {
          catalog: sourceCatalog, schema_name: schema, table_name: table, limit: 50,
        });
        data = { source_rows: sample.rows || [], dest_rows: [], source_count: sample.rows?.length || 0, dest_count: 0, differences: [] };
      }
      setResults(data);
    } catch (e: any) {
      setError(e.message || "Failed to load preview");
    } finally {
      setLoading(false);
    }
  }

  const sourceRows = results?.source_rows || [];
  const destRows = results?.dest_rows || [];
  const columns = results?.columns || (sourceRows.length > 0 ? Object.keys(sourceRows[0]) : []);
  const diffs = results?.differences || [];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Data Preview</h1>
        <p className="text-muted-foreground mt-1">Compare source and destination data side by side</p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={sourceCatalog}
              schema={schema}
              table={table}
              onCatalogChange={setSourceCatalog}
              onSchemaChange={setSchema}
              onTableChange={setTable}
            />
            <div className="flex items-center text-muted-foreground pb-2"><ArrowRight className="h-5 w-5" /></div>
            <CatalogPicker
              catalog={destCatalog}
              schema=""
              table=""
              onCatalogChange={setDestCatalog}
              onSchemaChange={() => {}}
              onTableChange={() => {}}
              showSchema={false}
              showTable={false}
            />
            <Button onClick={preview} disabled={!sourceCatalog || !schema || !table || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Columns className="h-4 w-4 mr-2" />}
              {loading ? "Loading..." : "Preview"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {results && (
        <div className="grid grid-cols-2 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-4 text-center">
              <p className="text-2xl font-bold text-foreground">{results.source_count ?? sourceRows.length}</p>
              <p className="text-xs text-muted-foreground">Source Rows</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 text-center">
              <p className="text-2xl font-bold text-foreground">{results.dest_count ?? destRows.length}</p>
              <p className="text-xs text-muted-foreground">Dest Rows</p>
            </CardContent>
          </Card>
        </div>
      )}

      {results && (
        <div className="grid grid-cols-2 gap-4">
          {[{ title: "Source", rows: sourceRows }, { title: "Destination", rows: destRows }].map((side) => (
            <Card key={side.title} className="bg-card border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center justify-between">
                  {side.title}
                  <Badge variant="outline">{side.rows.length} rows (max 50)</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto max-h-80 overflow-y-auto border border-border rounded">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-background">
                      <tr className="border-b border-border">
                        {columns.map((col: string) => (
                          <th key={col} className="text-left py-2 px-3 font-medium text-foreground whitespace-nowrap">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {side.rows.slice(0, 50).map((row: any, i: number) => {
                        const hasDiff = diffs.some((d: any) => d.row_index === i && side.title === "Source");
                        return (
                          <tr key={i} className={`border-b border-border ${hasDiff ? "bg-yellow-50 dark:bg-yellow-950" : ""}`}>
                            {columns.map((col: string) => (
                              <td key={col} className="py-1.5 px-3 text-foreground whitespace-nowrap">{String(row[col] ?? "")}</td>
                            ))}
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {diffs.length > 0 && (
        <Card className="bg-card border-border border-yellow-300">
          <CardHeader className="pb-2"><CardTitle className="text-lg">Differences ({diffs.length})</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-2">
              {diffs.map((d: any, i: number) => (
                <div key={i} className="flex items-center gap-3 text-sm px-3 py-2 rounded border border-border bg-background">
                  <Badge variant="outline">Row {d.row_index ?? i}</Badge>
                  <span className="text-muted-foreground">{d.column}: </span>
                  <span className="text-red-600">{String(d.source_value ?? "")}</span>
                  <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  <span className="text-green-600">{String(d.dest_value ?? "")}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
