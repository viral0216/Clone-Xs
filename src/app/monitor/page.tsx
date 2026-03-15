// @ts-nocheck
"use client";

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

      {result && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {(result as { in_sync?: boolean }).in_sync ? (
                <><CheckCircle className="h-5 w-5 text-green-600" /> In Sync</>
              ) : (
                <><AlertTriangle className="h-5 w-5 text-yellow-600" /> Out of Sync</>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-96">
              {JSON.stringify(result, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-red-200">
          <CardContent className="pt-6 text-red-600">{error}</CardContent>
        </Card>
      )}
    </div>
  );
}
