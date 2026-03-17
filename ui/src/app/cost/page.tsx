// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, DollarSign, HardDrive, Cpu, Calculator } from "lucide-react";
import PageHeader from "@/components/PageHeader";

export default function CostPage() {
  const { job, run, isRunning } = usePageJob("cost");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");

  const results = job?.data as any;
  const topTables = results?.top_tables || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost Estimator"
        icon={Calculator}
        breadcrumbs={["Analysis", "Cost Estimator"]}
        description="Estimate storage cost (GB x price) and compute cost (DBUs) for deep vs. shallow clone operations. Breaks down cost by schema and highlights the largest tables."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-describe-detail"
        docsLabel="DESCRIBE DETAIL"
      />

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema=""
              table=""
              onCatalogChange={setCatalog}
              onSchemaChange={() => {}}
              onTableChange={() => {}}
              showSchema={false}
              showTable={false}
            />
            <Button onClick={() => {
              const pricePerGb = (() => { try { return parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023; } catch { return 0.023; } })();
              run({ catalog }, () => api.post("/estimate", { source_catalog: catalog, price_per_gb: pricePerGb }));
            }} disabled={!catalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <DollarSign className="h-4 w-4 mr-2" />}
              {isRunning ? "Estimating..." : "Estimate Cost"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Stats */}
      {results && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <HardDrive className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{results.total_gb ?? 0} GB</p>
              <p className="text-xs text-muted-foreground mt-1">Total Size ({results.total_tb ?? 0} TB)</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <Cpu className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{results.table_count ?? 0}</p>
              <p className="text-xs text-muted-foreground mt-1">Tables Scanned</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <DollarSign className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">${results.monthly_cost_usd ?? 0}</p>
              <p className="text-xs text-muted-foreground mt-1">Monthly Cost (at ${results.price_per_gb}/GB)</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <DollarSign className="h-5 w-5 mx-auto mb-1 text-green-600" />
              <p className="text-2xl font-bold text-green-600">${results.yearly_cost_usd ?? 0}</p>
              <p className="text-xs text-muted-foreground mt-1">Yearly Cost</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Deep vs Shallow comparison */}
      {results && (
        <div className="grid grid-cols-2 gap-4">
          <Card className="bg-card border-border">
            <CardHeader className="pb-2"><CardTitle className="text-lg">DEEP Clone</CardTitle></CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-foreground">${results.monthly_cost_usd ?? 0}<span className="text-sm font-normal text-muted-foreground">/month</span></p>
              <p className="text-xs text-muted-foreground mt-1">Full data copy ({results.total_gb ?? 0} GB) — independent storage, supports time travel</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardHeader className="pb-2"><CardTitle className="text-lg">SHALLOW Clone</CardTitle></CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-green-600">~$0<span className="text-sm font-normal text-muted-foreground">/month</span></p>
              <p className="text-xs text-muted-foreground mt-1">Metadata-only reference — negligible additional storage, shares source data files</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Top Tables by Size */}
      {topTables.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Top {topTables.length} Largest Tables</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto border border-border rounded">
              <table className="w-full text-sm">
                <thead className="bg-background">
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 font-medium text-foreground">#</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Schema</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Table</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Size (GB)</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">% of Total</th>
                  </tr>
                </thead>
                <tbody>
                  {topTables.map((row: any, i: number) => {
                    const pct = results.total_gb > 0 ? ((row.size_gb / results.total_gb) * 100).toFixed(1) : 0;
                    return (
                      <tr key={i} className="border-b border-border">
                        <td className="py-2 px-3 text-muted-foreground">{i + 1}</td>
                        <td className="py-2 px-3 text-muted-foreground">{row.schema}</td>
                        <td className="py-2 px-3 font-medium text-foreground">{row.table}</td>
                        <td className="py-2 px-3 text-right text-foreground">{row.size_gb?.toFixed(2)}</td>
                        <td className="py-2 px-3 text-right">
                          <div className="flex items-center justify-end gap-2">
                            <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden">
                              <div className="h-full bg-blue-600 rounded-full" style={{ width: `${pct}%` }} />
                            </div>
                            <span className="text-xs text-muted-foreground w-10 text-right">{pct}%</span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
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
