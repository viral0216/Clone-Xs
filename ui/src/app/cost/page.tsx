// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, DollarSign, HardDrive, Cpu, Calculator } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { useCurrency } from "@/hooks/useSettings";

export default function CostPage() {
  const { job, run, isRunning } = usePageJob("cost");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [pricePerGb, setPricePerGb] = useState(0.023);
  const { symbol: currSymbol } = useCurrency();

  // Load pricing from backend config
  useEffect(() => {
    api.get<any>("/config").then((config) => {
      if (config?.price_per_gb != null) setPricePerGb(config.price_per_gb);
    }).catch(() => {
      try { setPricePerGb(parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023); } catch {}
    });
  }, []);

  const results = job?.data as any;
  const topTables = results?.top_tables || [];

  return (
    <div className="space-y-4">
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
              <span className="text-muted-foreground font-bold text-lg block mb-1">{currSymbol}</span>
              <p className="text-2xl font-bold text-foreground">{currSymbol}{results.monthly_cost_usd ?? 0}</p>
              <p className="text-xs text-muted-foreground mt-1">Monthly Cost (at {currSymbol}{results.price_per_gb}/GB)</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <span className="text-foreground font-bold text-lg block mb-1">{currSymbol}</span>
              <p className="text-2xl font-bold text-foreground">{currSymbol}{results.yearly_cost_usd ?? 0}</p>
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
              <p className="text-3xl font-bold text-foreground">~$0<span className="text-sm font-normal text-muted-foreground">/month</span></p>
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
            <DataTable
              data={topTables.map((row: any) => ({
                ...row,
                pct: results.total_gb > 0 ? (row.size_gb / results.total_gb) * 100 : 0,
              }))}
              columns={[
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-muted-foreground">{v}</span> },
                { key: "table", label: "Table", sortable: true, render: (v: string) => <span className="font-medium text-foreground">{v}</span> },
                { key: "size_gb", label: "Size (GB)", sortable: true, align: "right", render: (v: number) => <span className="text-foreground">{v?.toFixed(2)}</span> },
                {
                  key: "pct",
                  label: "% of Total",
                  sortable: true,
                  align: "right",
                  render: (v: number) => {
                    const pctStr = v.toFixed(1);
                    return (
                      <div className="flex items-center justify-end gap-2">
                        <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden">
                          <div className="h-full bg-[#E8453C] rounded-full" style={{ width: `${pctStr}%` }} />
                        </div>
                        <span className="text-xs text-muted-foreground w-10 text-right">{pctStr}%</span>
                      </div>
                    );
                  },
                },
              ] as Column[]}
              searchable
              searchKeys={["schema", "table"]}
              pageSize={25}
              compact
              tableId="cost-top-tables"
              emptyMessage="No table data available."
            />
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
