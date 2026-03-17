// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, BarChart3, CheckCircle, ScanSearch } from "lucide-react";
import PageHeader from "@/components/PageHeader";

export default function ProfilingPage() {
  const { job, run, isRunning } = usePageJob("profiling");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [schema, setSchema] = useState(job?.params?.schema || "");

  const results = job?.data as any;
  const columns = results?.columns || results?.results || [];
  const totalCols = results?.total_columns ?? columns.length;
  const nullRate = results?.null_rate ?? 0;
  const completeness = results?.completeness_score ?? (1 - nullRate);

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Profiling"
        icon={ScanSearch}
        breadcrumbs={["Analysis", "Profiling"]}
        description="Per-column data quality profiling — null rates, distinct counts, min/max values, and string length distributions. Helps assess data completeness before and after cloning."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-analyze-table"
        docsLabel="ANALYZE TABLE"
      />

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table=""
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
              onTableChange={() => {}}
              showTable={false}
            />
            <Button onClick={() => run({ catalog, schema }, () => api.post("/profile", { source_catalog: catalog, schema: schema || undefined }))} disabled={!catalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <BarChart3 className="h-4 w-4 mr-2" />}
              {isRunning ? "Profiling..." : "Run Profile"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {isRunning && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Running data profile...</p>
          </CardContent>
        </Card>
      )}

      {results && (
        <div className="grid grid-cols-3 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-foreground">{totalCols}</p>
              <p className="text-xs text-muted-foreground mt-1">Total Columns</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-foreground">{(nullRate * 100).toFixed(1)}%</p>
              <p className="text-xs text-muted-foreground mt-1">Null Rate</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <div className="flex items-center justify-center gap-1">
                <CheckCircle className="h-5 w-5 text-green-600" />
                <p className="text-2xl font-bold text-foreground">{(completeness * 100).toFixed(1)}%</p>
              </div>
              <p className="text-xs text-muted-foreground mt-1">Completeness Score</p>
            </CardContent>
          </Card>
        </div>
      )}

      {columns.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Column Statistics</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto max-h-96 overflow-y-auto border border-border rounded">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-background">
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 font-medium text-foreground">Column</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Type</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Nulls%</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Distinct</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Min</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Max</th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((col: any, i: number) => (
                    <tr key={i} className="border-b border-border hover:bg-background">
                      <td className="py-2 px-3 font-medium text-foreground">{col.column || col.name}</td>
                      <td className="py-2 px-3 text-muted-foreground">
                        <Badge variant="outline" className="text-xs">{col.type || col.data_type || "—"}</Badge>
                      </td>
                      <td className="py-2 px-3 text-right text-foreground">
                        {col.null_percent != null ? `${(col.null_percent * 100).toFixed(1)}%` : col.nulls_pct ?? "—"}
                      </td>
                      <td className="py-2 px-3 text-right text-foreground">{col.distinct_count ?? col.distinct ?? "—"}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{col.min ?? "—"}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{col.max ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {job?.status === "error" && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
