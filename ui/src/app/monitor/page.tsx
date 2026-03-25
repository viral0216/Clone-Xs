import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import { Activity, RefreshCw, CheckCircle, AlertTriangle } from "lucide-react";
import PageHeader from "@/components/PageHeader";

export default function MonitorPage() {
  const { job, run, isRunning } = usePageJob("monitor");
  const [source, setSource] = useState(job?.params?.source || "");
  const [dest, setDest] = useState(job?.params?.dest || "");

  const result = job?.data as Record<string, unknown> | null;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Monitor"
        icon={Activity}
        description="Continuous monitoring of catalog sync status — compares source and destination in real-time, tracks drift, and shows sync freshness for each table."
        breadcrumbs={["Management", "Monitor"]}
      />

      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={source}
              onCatalogChange={setSource}
              showSchema={false}
              showTable={false}
            />
            <CatalogPicker
              catalog={dest}
              onCatalogChange={setDest}
              showSchema={false}
              showTable={false}
            />
            <Button onClick={() => run({ source, dest }, () => api.post("/monitor", { source_catalog: source, destination_catalog: dest, check_drift: true, check_counts: false }))} disabled={!source || !dest || isRunning}>
              <RefreshCw className={`h-4 w-4 mr-2 ${isRunning ? "animate-spin" : ""}`} />
              Check Now
            </Button>
          </div>
        </CardContent>
      </Card>

      {result && (() => {
        const r = result as {
          in_sync?: boolean;
          source_catalog?: string;
          destination_catalog?: string;
          total_tables?: number;
          matched_tables?: number;
          missing_tables?: string[];
          extra_tables?: string[];
          drifted_tables?: { table: string; source_hash?: string; dest_hash?: string; reason?: string }[];
          checked_at?: string;
        };
        return (
          <>
            {/* Summary Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <Card>
                <CardContent className="pt-4 text-center">
                  <div className="flex items-center justify-center gap-2 mb-1">
                    {r.in_sync ? (
                      <CheckCircle className="h-5 w-5 text-foreground" />
                    ) : (
                      <AlertTriangle className="h-5 w-5 text-muted-foreground" />
                    )}
                  </div>
                  <p className={`text-lg font-bold ${r.in_sync ? "text-foreground" : "text-muted-foreground"}`}>
                    {r.in_sync ? "In Sync" : "Out of Sync"}
                  </p>
                  <p className="text-xs text-gray-500">Status</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-[#E8453C]">{r.total_tables ?? "N/A"}</p>
                  <p className="text-xs text-gray-500">Total Tables</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-foreground">{r.matched_tables ?? "N/A"}</p>
                  <p className="text-xs text-gray-500">Matched</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-red-700">
                    {(r.missing_tables?.length ?? 0) + (r.extra_tables?.length ?? 0) + (r.drifted_tables?.length ?? 0)}
                  </p>
                  <p className="text-xs text-gray-500">Issues</p>
                </CardContent>
              </Card>
            </div>

            {r.checked_at && (
              <p className="text-xs text-gray-400">
                Checked at {new Date(r.checked_at).toLocaleString()}
              </p>
            )}

            {/* Missing Tables */}
            {r.missing_tables && r.missing_tables.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm flex items-center gap-2">
                    <AlertTriangle className="h-4 w-4 text-red-500" />
                    Missing in Destination ({r.missing_tables.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b bg-gray-50">
                          <th className="text-left py-2 px-3 font-medium">#</th>
                          <th className="text-left py-2 px-3 font-medium">Table</th>
                        </tr>
                      </thead>
                      <tbody>
                        {r.missing_tables.map((t, i) => (
                          <tr key={i} className="border-b hover:bg-gray-50">
                            <td className="py-2 px-3 text-gray-400">{i + 1}</td>
                            <td className="py-2 px-3 font-mono text-sm">{t}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Extra Tables */}
            {r.extra_tables && r.extra_tables.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm flex items-center gap-2">
                    <AlertTriangle className="h-4 w-4 text-muted-foreground" />
                    Extra in Destination ({r.extra_tables.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b bg-gray-50">
                          <th className="text-left py-2 px-3 font-medium">#</th>
                          <th className="text-left py-2 px-3 font-medium">Table</th>
                        </tr>
                      </thead>
                      <tbody>
                        {r.extra_tables.map((t, i) => (
                          <tr key={i} className="border-b hover:bg-gray-50">
                            <td className="py-2 px-3 text-gray-400">{i + 1}</td>
                            <td className="py-2 px-3 font-mono text-sm">{t}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Drifted Tables */}
            {r.drifted_tables && r.drifted_tables.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Activity className="h-4 w-4 text-muted-foreground" />
                    Drifted Tables ({r.drifted_tables.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b bg-gray-50">
                          <th className="text-left py-2 px-3 font-medium">#</th>
                          <th className="text-left py-2 px-3 font-medium">Table</th>
                          <th className="text-left py-2 px-3 font-medium">Reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {r.drifted_tables.map((t, i) => (
                          <tr key={i} className="border-b hover:bg-gray-50">
                            <td className="py-2 px-3 text-gray-400">{i + 1}</td>
                            <td className="py-2 px-3 font-mono text-sm">{t.table}</td>
                            <td className="py-2 px-3 text-gray-500 text-xs">{t.reason ?? "Schema or data drift"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Fallback: show raw JSON if no structured fields are present */}
            {!r.missing_tables && !r.extra_tables && !r.drifted_tables && r.total_tables === undefined && (
              <details className="mt-2">
                <summary className="text-sm text-gray-500 cursor-pointer hover:text-gray-700">Show raw response</summary>
                <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-64 mt-2">
                  {JSON.stringify(result, null, 2)}
                </pre>
              </details>
            )}
          </>
        );
      })()}

      {job?.status === "error" && (
        <Card className="border-red-200">
          <CardContent className="pt-6 text-red-600">{job.error}</CardContent>
        </Card>
      )}
    </div>
  );
}
