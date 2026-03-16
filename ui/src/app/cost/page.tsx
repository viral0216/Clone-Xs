// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, DollarSign, HardDrive, Cpu, Calculator } from "lucide-react";
import PageHeader from "@/components/PageHeader";

export default function CostPage() {
  const [catalog, setCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState<any>(null);

  async function estimate() {
    setLoading(true);
    setError("");
    setResults(null);
    try {
      const data = await api.post("/estimate", { source_catalog: catalog });
      setResults(data);
    } catch (e: any) {
      setError(e.message || "Failed to estimate cost");
    } finally {
      setLoading(false);
    }
  }

  const breakdown = results?.schema_breakdown || results?.breakdown || [];
  const deepCost = results?.deep_clone_cost ?? results?.deep_cost;
  const shallowCost = results?.shallow_clone_cost ?? results?.shallow_cost;

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
            <Button onClick={estimate} disabled={!catalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <DollarSign className="h-4 w-4 mr-2" />}
              {loading ? "Estimating..." : "Estimate Cost"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {results && (
        <div className="grid grid-cols-3 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <HardDrive className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{results.total_size || "—"}</p>
              <p className="text-xs text-muted-foreground mt-1">Total Size</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <Cpu className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{results.estimated_dbus ?? "—"}</p>
              <p className="text-xs text-muted-foreground mt-1">Estimated DBUs</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <DollarSign className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{results.total_cost ?? "—"}</p>
              <p className="text-xs text-muted-foreground mt-1">Estimated Total Cost</p>
            </CardContent>
          </Card>
        </div>
      )}

      {(deepCost != null || shallowCost != null) && (
        <div className="grid grid-cols-2 gap-4">
          <Card className="bg-card border-border">
            <CardHeader className="pb-2"><CardTitle className="text-lg">DEEP Clone</CardTitle></CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-foreground">{deepCost ?? "—"}</p>
              <p className="text-xs text-muted-foreground mt-1">Full data copy - higher cost, independent storage</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardHeader className="pb-2"><CardTitle className="text-lg">SHALLOW Clone</CardTitle></CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-green-600">{shallowCost ?? "—"}</p>
              <p className="text-xs text-muted-foreground mt-1">Reference only - lower cost, shared storage</p>
            </CardContent>
          </Card>
        </div>
      )}

      {breakdown.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Cost Breakdown by Schema</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto border border-border rounded">
              <table className="w-full text-sm">
                <thead className="bg-background">
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 font-medium text-foreground">Schema</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Tables</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Size</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">DBUs</th>
                    <th className="text-right py-2 px-3 font-medium text-foreground">Cost</th>
                  </tr>
                </thead>
                <tbody>
                  {breakdown.map((row: any, i: number) => (
                    <tr key={i} className="border-b border-border">
                      <td className="py-2 px-3 font-medium text-foreground">{row.schema || row.name}</td>
                      <td className="py-2 px-3 text-right text-foreground">{row.table_count ?? row.tables ?? "—"}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{row.size ?? "—"}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{row.dbus ?? "—"}</td>
                      <td className="py-2 px-3 text-right text-foreground">
                        <Badge variant="outline">{row.cost ?? "—"}</Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
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
