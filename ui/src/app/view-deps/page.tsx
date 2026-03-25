// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { GitFork, Loader2, XCircle, ArrowRight, List } from "lucide-react";

interface DepsResult {
  catalog: string;
  schema: string;
  dependencies: Record<string, string[]>;
}

interface OrderResult {
  catalog: string;
  schema: string;
  creation_order: string[];
}

export default function ViewDepsPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [viewDeps, setViewDeps] = useState<DepsResult | null>(null);
  const [funcDeps, setFuncDeps] = useState<DepsResult | null>(null);
  const [order, setOrder] = useState<OrderResult | null>(null);

  async function analyze() {
    setLoading(true);
    setError("");
    setViewDeps(null);
    setFuncDeps(null);
    setOrder(null);
    try {
      const payload = { catalog, schema_name: schema };
      const [views, funcs, ord] = await Promise.all([
        api.post<DepsResult>("/dependencies/views", payload),
        api.post<DepsResult>("/dependencies/functions", payload),
        api.post<OrderResult>("/dependencies/order", payload),
      ]);
      setViewDeps(views);
      setFuncDeps(funcs);
      setOrder(ord);
    } catch (e: any) {
      setError(e.message || "Failed to analyze dependencies");
    } finally {
      setLoading(false);
    }
  }

  const renderDepGraph = (deps: Record<string, string[]>, title: string) => {
    const entries = Object.entries(deps);
    if (entries.length === 0) return null;

    return (
      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-lg">{title} ({entries.length})</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {entries.map(([name, depList]) => (
              <div key={name} className="flex items-start gap-3 py-2 px-3 rounded hover:bg-muted/50">
                <GitFork className="h-4 w-4 mt-0.5 text-muted-foreground shrink-0" />
                <div>
                  <span className="font-mono text-sm text-foreground">{name}</span>
                  {depList.length > 0 ? (
                    <div className="flex flex-wrap gap-1 mt-1">
                      <ArrowRight className="h-3 w-3 text-muted-foreground mt-0.5" />
                      {depList.map((dep) => (
                        <Badge key={dep} variant="secondary" className="text-xs font-mono">
                          {dep}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    <span className="text-xs text-muted-foreground ml-2">(no dependencies)</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-3xl font-bold text-foreground">View Dependencies</h1>
        <p className="text-muted-foreground mt-1">Map view and function dependencies within a schema — shows which views depend on which tables, and computes the correct creation order for cloning.</p>
        <p className="text-xs text-muted-foreground mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-view" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Views</a>
        </p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
            />
            <Button onClick={analyze} disabled={!catalog || !schema || loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <GitFork className="h-4 w-4 mr-2" />}
              Analyze
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-red-500/30 bg-red-500/5">
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-red-500">
              <XCircle className="h-5 w-5" /> {error}
            </div>
          </CardContent>
        </Card>
      )}

      {order && order.creation_order.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <List className="h-5 w-5" />
              Recommended Creation Order
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2">
              {order.creation_order.map((name, i) => (
                <div key={name} className="flex items-center gap-2">
                  <Badge variant="outline" className="font-mono">
                    <span className="text-muted-foreground mr-1">{i + 1}.</span> {name}
                  </Badge>
                  {i < order.creation_order.length - 1 && (
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {viewDeps && renderDepGraph(viewDeps.dependencies, "View Dependencies")}
      {funcDeps && renderDepGraph(funcDeps.dependencies, "Function Dependencies")}

      {viewDeps && funcDeps && Object.keys(viewDeps.dependencies).length === 0 && Object.keys(funcDeps.dependencies).length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center text-muted-foreground">
            No views or functions found in {catalog}.{schema}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
