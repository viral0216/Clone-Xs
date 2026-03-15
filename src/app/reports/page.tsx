// @ts-nocheck
"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useCloneJobs } from "@/hooks/useApi";
import { api } from "@/lib/api-client";
import { FileText, DollarSign, Download, History } from "lucide-react";

export default function ReportsPage() {
  const jobs = useCloneJobs();
  const [catalog, setCatalog] = useState("");
  const [costResult, setCostResult] = useState<Record<string, unknown> | null>(null);
  const [rollbackLogs, setRollbackLogs] = useState<Array<Record<string, unknown>>>([]);
  const [loading, setLoading] = useState(false);

  const estimateCost = async () => {
    setLoading(true);
    try {
      const res = await api.post<Record<string, unknown>>("/estimate", { source_catalog: catalog });
      setCostResult(res);
    } catch (e) {
      alert((e as Error).message);
    }
    setLoading(false);
  };

  const loadRollbackLogs = async () => {
    try {
      const logs = await api.get<Array<Record<string, unknown>>>("/rollback/logs");
      setRollbackLogs(logs);
    } catch (e) {
      alert((e as Error).message);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Reports</h1>
        <p className="text-gray-500 mt-1">Clone history, cost estimation, and rollback logs</p>
      </div>

      {/* Clone History */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <History className="h-5 w-5" />
            Clone History
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!jobs.data?.length ? (
            <p className="text-gray-400">No clone jobs recorded.</p>
          ) : (
            <div className="space-y-2">
              {jobs.data.map((job) => (
                <div key={job.job_id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <Badge
                      variant={job.status === "completed" ? "default" : job.status === "failed" ? "destructive" : "outline"}
                      className={job.status === "completed" ? "bg-green-600" : ""}
                    >
                      {job.status}
                    </Badge>
                    <span className="text-sm">{job.source_catalog} &rarr; {job.destination_catalog}</span>
                    <span className="text-xs text-gray-400">{job.clone_type}</span>
                  </div>
                  <span className="text-xs text-gray-400">
                    {job.created_at ? new Date(job.created_at).toLocaleString() : ""}
                  </span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Cost Estimation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <DollarSign className="h-5 w-5" />
            Cost Estimation
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Catalog</label>
              <Input value={catalog} onChange={(e) => setCatalog(e.target.value)} placeholder="production" />
            </div>
            <Button onClick={estimateCost} disabled={!catalog || loading}>
              <DollarSign className="h-4 w-4 mr-2" />
              Estimate
            </Button>
          </div>
          {costResult && (
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-64">
              {JSON.stringify(costResult, null, 2)}
            </pre>
          )}
        </CardContent>
      </Card>

      {/* Rollback Logs */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            Rollback Logs
          </CardTitle>
          <Button variant="outline" size="sm" onClick={loadRollbackLogs}>Load</Button>
        </CardHeader>
        <CardContent>
          {rollbackLogs.length === 0 ? (
            <p className="text-gray-400">Click Load to fetch rollback logs.</p>
          ) : (
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-64">
              {JSON.stringify(rollbackLogs, null, 2)}
            </pre>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
