// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import {
  AlertTriangle, Loader2, XCircle, Network, Eye, FunctionSquare, Database,
} from "lucide-react";

function riskBadge(level: string) {
  switch (level?.toUpperCase()) {
    case "HIGH": return <Badge variant="destructive">{level}</Badge>;
    case "MEDIUM": return <Badge className="bg-yellow-500 text-white">{level}</Badge>;
    case "LOW": return <Badge className="bg-green-600 text-white">{level}</Badge>;
    default: return <Badge variant="outline">{level || "Unknown"}</Badge>;
  }
}

export default function ImpactPage() {
  const { job, run, isRunning } = usePageJob("impact");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [schema, setSchema] = useState(job?.params?.schema || "");
  const [table, setTable] = useState(job?.params?.table || "");

  const results = job?.data as any;
  const views = results?.affected_views || [];
  const functions = results?.affected_functions || [];
  const downstream = results?.downstream_tables || [];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Impact Analysis</h1>
        <p className="text-muted-foreground mt-1">Assess the blast radius of schema changes — shows which views, functions, and downstream consumers would be affected before you modify a table.</p>
        <p className="text-xs text-muted-foreground mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Unity Catalog lineage</a>
        </p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table={table}
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
              onTableChange={setTable}
            />
            <Button onClick={() => run({ catalog, schema, table }, () => api.post("/impact", { catalog, schema: schema || undefined, table: table || undefined }))} disabled={!catalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Network className="h-4 w-4 mr-2" />}
              {isRunning ? "Analyzing..." : "Analyze Impact"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {!results && !isRunning && job?.status !== "error" && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center text-muted-foreground py-12">
            <Network className="h-10 w-10 mx-auto mb-3 opacity-40" />
            <p>Enter a catalog to analyze impact</p>
          </CardContent>
        </Card>
      )}

      {results?.risk_level && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <AlertTriangle className="h-6 w-6 text-muted-foreground" />
              <span className="font-semibold text-foreground">Overall Risk Level</span>
            </div>
            {riskBadge(results.risk_level)}
          </CardContent>
        </Card>
      )}

      {results && (
        <div className="grid grid-cols-3 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <Eye className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{views.length}</p>
              <p className="text-xs text-muted-foreground mt-1">Affected Views</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <FunctionSquare className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{functions.length}</p>
              <p className="text-xs text-muted-foreground mt-1">Affected Functions</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 text-center">
              <Database className="h-5 w-5 mx-auto mb-1 text-muted-foreground" />
              <p className="text-2xl font-bold text-foreground">{downstream.length}</p>
              <p className="text-xs text-muted-foreground mt-1">Downstream Tables</p>
            </CardContent>
          </Card>
        </div>
      )}

      {[{ title: "Affected Views", items: views }, { title: "Affected Functions", items: functions }, { title: "Downstream Tables", items: downstream }].map(
        (section) =>
          section.items.length > 0 && (
            <Card key={section.title} className="bg-card border-border">
              <CardHeader className="pb-3"><CardTitle className="text-lg">{section.title}</CardTitle></CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {section.items.map((item: any, i: number) => (
                    <div key={i} className="flex items-center gap-2 px-3 py-1.5 rounded border border-border bg-background text-sm">
                      <span className="text-foreground">{typeof item === "string" ? item : item.name || JSON.stringify(item)}</span>
                      {item.risk && riskBadge(item.risk)}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )
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
