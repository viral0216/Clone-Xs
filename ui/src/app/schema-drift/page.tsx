// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import {
  GitBranch, Loader2, XCircle, ArrowRight, Plus, Minus, RefreshCw,
} from "lucide-react";

function changeColor(changeType: string) {
  switch (changeType?.toUpperCase()) {
    case "ADDED": return "text-foreground bg-muted/20 border-border";
    case "REMOVED": return "text-red-700 bg-red-50 border-red-200";
    case "MODIFIED": return "text-muted-foreground bg-muted/20 border-border";
    default: return "text-gray-700 bg-gray-50 border-gray-200";
  }
}

function changeIcon(changeType: string) {
  switch (changeType?.toUpperCase()) {
    case "ADDED": return <Plus className="h-4 w-4 text-foreground" />;
    case "REMOVED": return <Minus className="h-4 w-4 text-red-500" />;
    case "MODIFIED": return <RefreshCw className="h-4 w-4 text-muted-foreground" />;
    default: return null;
  }
}

export default function SchemaDriftPage() {
  const { job, run, isRunning } = usePageJob("schema-drift");
  const [source, setSource] = useState(job?.params?.source || "");
  const [dest, setDest] = useState(job?.params?.dest || "");
  const data = job?.data as any;

  const summary = data?.summary;
  const drifts = data?.drifts || data?.results || [];

  // Group drifts by schema.table
  const grouped: Record<string, any[]> = {};
  drifts.forEach((d: any) => {
    const key = `${d.schema}.${d.table}`;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(d);
  });

  const addedCount = summary?.added_columns ?? drifts.filter((d: any) => d.change_type?.toUpperCase() === "ADDED").length;
  const removedCount = summary?.removed_columns ?? drifts.filter((d: any) => d.change_type?.toUpperCase() === "REMOVED").length;
  const modifiedCount = summary?.modified_columns ?? drifts.filter((d: any) => d.change_type?.toUpperCase() === "MODIFIED").length;
  const totalCount = summary?.total_drift_count ?? drifts.length;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-3xl font-bold">Schema Drift</h1>
        <p className="text-gray-500 mt-1">Detect schema drift between source and destination — added, removed, or modified columns, data type changes, and nullability differences across all tables.</p>
        <p className="text-xs text-gray-400 mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema" target="_blank" rel="noopener noreferrer" className="text-[#E8453C] hover:underline">Schema evolution</a>
        </p>
      </div>

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={source}
              onCatalogChange={setSource}
              showSchema={false}
              showTable={false}
            />
            <div className="flex items-center text-gray-400 pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <CatalogPicker
              catalog={dest}
              onCatalogChange={setDest}
              showSchema={false}
              showTable={false}
            />
            <Button
              onClick={() => run({ source, dest }, () => api.post("/schema-drift", { source_catalog: source, destination_catalog: dest }))}
              disabled={!source || !dest || isRunning}
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <GitBranch className="h-4 w-4 mr-2" />}
              {isRunning ? "Detecting..." : "Detect Drift"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {data && (
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-foreground">{addedCount}</p>
              <p className="text-xs text-gray-500 mt-1">Added Columns</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-red-700">{removedCount}</p>
              <p className="text-xs text-gray-500 mt-1">Removed Columns</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-muted-foreground">{modifiedCount}</p>
              <p className="text-xs text-gray-500 mt-1">Modified Columns</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-[#E8453C]">{totalCount}</p>
              <p className="text-xs text-gray-500 mt-1">Total Drift</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Drift Results grouped by table */}
      {Object.keys(grouped).length > 0 && (
        <div className="space-y-4">
          {Object.entries(grouped).map(([tableKey, rows]) => (
            <Card key={tableKey}>
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center gap-2">
                  <GitBranch className="h-4 w-4 text-gray-500" />
                  {tableKey}
                  <Badge variant="outline" className="ml-auto text-xs">{rows.length} changes</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto border rounded">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-gray-50">
                        <th className="text-left py-2 px-3 font-medium w-8"></th>
                        <th className="text-left py-2 px-3 font-medium">Column</th>
                        <th className="text-left py-2 px-3 font-medium">Change Type</th>
                        <th className="text-left py-2 px-3 font-medium">Old Type</th>
                        <th className="text-left py-2 px-3 font-medium">New Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row: any, i: number) => (
                        <tr key={i} className={`border-b ${row.change_type?.toUpperCase() === "ADDED" ? "bg-muted/20/50" : row.change_type?.toUpperCase() === "REMOVED" ? "bg-red-50/50" : "bg-muted/20/50"}`}>
                          <td className="py-2 px-3">{changeIcon(row.change_type)}</td>
                          <td className="py-2 px-3 font-medium">{row.column || row.column_name}</td>
                          <td className="py-2 px-3">
                            <Badge variant="outline" className={`text-xs ${changeColor(row.change_type)}`}>
                              {row.change_type?.toUpperCase()}
                            </Badge>
                          </td>
                          <td className="py-2 px-3 text-gray-600">{row.old_type || "—"}</td>
                          <td className="py-2 px-3 text-gray-600">{row.new_type || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Error */}
      {job?.status === "error" && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
