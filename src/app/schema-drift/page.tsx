// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useSchemaDrift } from "@/hooks/useApi";
import {
  GitBranch, Loader2, XCircle, ArrowRight, Plus, Minus, RefreshCw,
} from "lucide-react";

function changeColor(changeType: string) {
  switch (changeType?.toUpperCase()) {
    case "ADDED": return "text-green-700 bg-green-50 border-green-200";
    case "REMOVED": return "text-red-700 bg-red-50 border-red-200";
    case "MODIFIED": return "text-yellow-700 bg-yellow-50 border-yellow-200";
    default: return "text-gray-700 bg-gray-50 border-gray-200";
  }
}

function changeIcon(changeType: string) {
  switch (changeType?.toUpperCase()) {
    case "ADDED": return <Plus className="h-4 w-4 text-green-500" />;
    case "REMOVED": return <Minus className="h-4 w-4 text-red-500" />;
    case "MODIFIED": return <RefreshCw className="h-4 w-4 text-yellow-500" />;
    default: return null;
  }
}

export default function SchemaDriftPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const schemaDrift = useSchemaDrift();
  const data = schemaDrift.data as any;

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
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Schema Drift</h1>
        <p className="text-gray-500 mt-1">Detect column-level schema differences between catalogs</p>
      </div>

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Source Catalog</label>
              <Input value={source} onChange={(e) => setSource(e.target.value)} placeholder="production" />
            </div>
            <div className="flex items-center text-gray-400 pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input value={dest} onChange={(e) => setDest(e.target.value)} placeholder="staging" />
            </div>
            <Button
              onClick={() => schemaDrift.mutate({ source_catalog: source, destination_catalog: dest })}
              disabled={!source || !dest || schemaDrift.isPending}
            >
              {schemaDrift.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <GitBranch className="h-4 w-4 mr-2" />}
              {schemaDrift.isPending ? "Detecting..." : "Detect Drift"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {data && (
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-green-700">{addedCount}</p>
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
              <p className="text-2xl font-bold text-yellow-700">{modifiedCount}</p>
              <p className="text-xs text-gray-500 mt-1">Modified Columns</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-blue-700">{totalCount}</p>
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
                        <tr key={i} className={`border-b ${row.change_type?.toUpperCase() === "ADDED" ? "bg-green-50/50" : row.change_type?.toUpperCase() === "REMOVED" ? "bg-red-50/50" : "bg-yellow-50/50"}`}>
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
      {schemaDrift.isError && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {(schemaDrift.error as Error).message}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
