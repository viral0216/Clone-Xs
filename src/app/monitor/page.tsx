import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { Activity, RefreshCw, CheckCircle, AlertTriangle } from "lucide-react";

export default function MonitorPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const [result, setResult] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const runCheck = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.post<Record<string, unknown>>("/monitor", {
        source_catalog: source,
        destination_catalog: dest,
        check_drift: true,
        check_counts: false,
      });
      setResult(res);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Monitor</h1>
        <p className="text-gray-500 mt-1">Check sync status between catalogs</p>
      </div>

      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Source Catalog</label>
              <Input value={source} onChange={(e) => setSource(e.target.value)} placeholder="production" />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input value={dest} onChange={(e) => setDest(e.target.value)} placeholder="staging" />
            </div>
            <Button onClick={runCheck} disabled={!source || !dest || loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
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
                      <CheckCircle className="h-5 w-5 text-green-600" />
                    ) : (
                      <AlertTriangle className="h-5 w-5 text-yellow-600" />
                    )}
                  </div>
                  <p className={`text-lg font-bold ${r.in_sync ? "text-green-700" : "text-yellow-700"}`}>
                    {r.in_sync ? "In Sync" : "Out of Sync"}
                  </p>
                  <p className="text-xs text-gray-500">Status</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-blue-700">{r.total_tables ?? "N/A"}</p>
                  <p className="text-xs text-gray-500">Total Tables</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 text-center">
                  <p className="text-2xl font-bold text-green-700">{r.matched_tables ?? "N/A"}</p>
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
                    <AlertTriangle className="h-4 w-4 text-yellow-500" />
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
                    <Activity className="h-4 w-4 text-orange-500" />
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
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Raw Response</CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-96">
                    {JSON.stringify(result, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            )}
          </>
        );
      })()}

      {error && (
        <Card className="border-red-200">
          <CardContent className="pt-6 text-red-600">{error}</CardContent>
        </Card>
      )}
    </div>
  );
}
