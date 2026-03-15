// @ts-nocheck
import { useState } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import CatalogPicker from "@/components/CatalogPicker";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { useCloneJobs } from "@/hooks/useApi";
import { api } from "@/lib/api-client";
import {
  DollarSign, History, FileText, Loader2, HardDrive,
  Table2, TrendingUp, Download, Clock, CheckCircle, XCircle,
  Undo2, Camera, FileDown,
} from "lucide-react";

function formatBytes(bytes: number): string {
  if (bytes >= 1e12) return `${(bytes / 1e12).toFixed(2)} TB`;
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(2)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(2)} MB`;
  if (bytes >= 1e3) return `${(bytes / 1e3).toFixed(1)} KB`;
  return `${bytes} B`;
}

export default function ReportsPage() {
  const jobs = useCloneJobs();
  const [catalog, setCatalog] = useState("");
  const [costResult, setCostResult] = useState<any>(null);
  const [rollbackLogs, setRollbackLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [rbLoading, setRbLoading] = useState(false);

  // Rollback Execute state
  const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
  const [selectedLog, setSelectedLog] = useState<any>(null);
  const [dropCatalog, setDropCatalog] = useState(false);
  const [rollbackExecuting, setRollbackExecuting] = useState(false);
  const [rollbackResult, setRollbackResult] = useState<any>(null);

  // Catalog Snapshot state
  const [snapshotCatalog, setSnapshotCatalog] = useState("");
  const [snapshotOutputPath, setSnapshotOutputPath] = useState("");
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [snapshotResult, setSnapshotResult] = useState<any>(null);

  // Export Metadata state
  const [exportCatalog, setExportCatalog] = useState("");
  const [exportFormat, setExportFormat] = useState("csv");
  const [exportLoading, setExportLoading] = useState(false);
  const [exportResult, setExportResult] = useState<any>(null);

  const estimateCost = async () => {
    setLoading(true);
    try {
      const res = await api.post("/estimate", { source_catalog: catalog });
      setCostResult(res);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  const loadRollbackLogs = async () => {
    setRbLoading(true);
    try {
      const logs = await api.get<any[]>("/rollback/logs");
      setRollbackLogs(logs);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setRbLoading(false);
  };

  const openRollbackDialog = (log: any) => {
    setSelectedLog(log);
    setDropCatalog(false);
    setRollbackResult(null);
    setRollbackDialogOpen(true);
  };

  const executeRollback = async () => {
    if (!selectedLog) return;
    setRollbackExecuting(true);
    try {
      const res = await api.post("/rollback", {
        log_file: selectedLog.file,
        drop_catalog: dropCatalog,
      });
      setRollbackResult(res);
      toast.success("Rollback executed successfully");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setRollbackExecuting(false);
  };

  const createSnapshot = async () => {
    setSnapshotLoading(true);
    try {
      const payload: any = { source_catalog: snapshotCatalog };
      if (snapshotOutputPath) payload.output_path = snapshotOutputPath;
      const res = await api.post("/snapshot", payload);
      setSnapshotResult(res);
      toast.success("Snapshot created successfully");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setSnapshotLoading(false);
  };

  const exportMetadata = async () => {
    setExportLoading(true);
    try {
      const res = await api.post("/export", {
        source_catalog: exportCatalog,
        format: exportFormat,
      });
      setExportResult(res);
      toast.success("Export completed successfully");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setExportLoading(false);
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
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3 font-medium">Status</th>
                    <th className="text-left py-2 px-3 font-medium">Source</th>
                    <th className="text-left py-2 px-3 font-medium">Destination</th>
                    <th className="text-left py-2 px-3 font-medium">Type</th>
                    <th className="text-left py-2 px-3 font-medium">Started</th>
                    <th className="text-left py-2 px-3 font-medium">Duration</th>
                    <th className="text-left py-2 px-3 font-medium">Job ID</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.data.map((job) => {
                    const duration = job.started_at && job.completed_at
                      ? Math.round((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 1000)
                      : null;
                    return (
                      <tr key={job.job_id} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3">
                          <Badge
                            variant={job.status === "completed" ? "default" : job.status === "failed" ? "destructive" : "secondary"}
                            className={job.status === "completed" ? "bg-green-600" : job.status === "running" ? "bg-blue-600" : ""}
                          >
                            {job.status}
                          </Badge>
                        </td>
                        <td className="py-2 px-3 font-medium">{job.source_catalog}</td>
                        <td className="py-2 px-3">{job.destination_catalog}</td>
                        <td className="py-2 px-3">
                          <Badge variant="outline" className="text-xs">{job.clone_type}</Badge>
                        </td>
                        <td className="py-2 px-3 text-gray-500 text-xs">
                          {job.created_at ? new Date(job.created_at).toLocaleString() : "\u2014"}
                        </td>
                        <td className="py-2 px-3 text-gray-500 text-xs">
                          {duration != null ? `${duration}s` : job.status === "running" ? "..." : "\u2014"}
                        </td>
                        <td className="py-2 px-3 text-gray-400 font-mono text-xs">{job.job_id}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
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
              <CatalogPicker catalog={catalog} onCatalogChange={setCatalog} showSchema={false} showTable={false} />
            </div>
            <Button onClick={estimateCost} disabled={!catalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <DollarSign className="h-4 w-4 mr-2" />}
              {loading ? "Estimating..." : "Estimate"}
            </Button>
          </div>

          {costResult && (
            <>
              {/* Cost Summary Cards */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <Card>
                  <CardContent className="pt-4 text-center">
                    <div className="flex items-center justify-center gap-2 mb-1">
                      <HardDrive className="h-4 w-4 text-blue-600" />
                    </div>
                    <p className="text-2xl font-bold text-blue-700">{costResult.total_gb?.toFixed(2)} GB</p>
                    <p className="text-xs text-gray-500">Total Size</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 text-center">
                    <div className="flex items-center justify-center gap-2 mb-1">
                      <Table2 className="h-4 w-4 text-purple-600" />
                    </div>
                    <p className="text-2xl font-bold text-purple-700">{costResult.table_count}</p>
                    <p className="text-xs text-gray-500">Tables</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 text-center">
                    <div className="flex items-center justify-center gap-2 mb-1">
                      <DollarSign className="h-4 w-4 text-green-600" />
                    </div>
                    <p className="text-2xl font-bold text-green-700">${costResult.monthly_cost_usd?.toFixed(2)}</p>
                    <p className="text-xs text-gray-500">Monthly Cost</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 text-center">
                    <div className="flex items-center justify-center gap-2 mb-1">
                      <TrendingUp className="h-4 w-4 text-orange-600" />
                    </div>
                    <p className="text-2xl font-bold text-orange-700">${costResult.yearly_cost_usd?.toFixed(2)}</p>
                    <p className="text-xs text-gray-500">Yearly Cost</p>
                  </CardContent>
                </Card>
              </div>

              <p className="text-xs text-gray-400">
                Price: ${costResult.price_per_gb}/GB &middot; Storage cost only — does not include compute (DBU) costs for running the clone
              </p>

              {/* Top Tables by Size */}
              {costResult.top_tables && costResult.top_tables.length > 0 && (
                <div>
                  <p className="text-sm font-medium mb-2">Top Tables by Size</p>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b bg-gray-50">
                          <th className="text-left py-2 px-3 font-medium">#</th>
                          <th className="text-left py-2 px-3 font-medium">Schema</th>
                          <th className="text-left py-2 px-3 font-medium">Table</th>
                          <th className="text-right py-2 px-3 font-medium">Size</th>
                          <th className="text-right py-2 px-3 font-medium">% of Total</th>
                        </tr>
                      </thead>
                      <tbody>
                        {costResult.top_tables.map((t: any, i: number) => {
                          const pct = costResult.total_bytes > 0
                            ? ((t.size_bytes / costResult.total_bytes) * 100).toFixed(1)
                            : "0";
                          return (
                            <tr key={i} className="border-b hover:bg-gray-50">
                              <td className="py-2 px-3 text-gray-400">{i + 1}</td>
                              <td className="py-2 px-3 text-gray-600">{t.schema}</td>
                              <td className="py-2 px-3 font-medium">{t.table}</td>
                              <td className="py-2 px-3 text-right">{formatBytes(t.size_bytes)}</td>
                              <td className="py-2 px-3 text-right">
                                <div className="flex items-center justify-end gap-2">
                                  <div className="w-16 bg-gray-200 rounded-full h-2">
                                    <div
                                      className="bg-blue-600 h-2 rounded-full"
                                      style={{ width: `${Math.min(parseFloat(pct), 100)}%` }}
                                    />
                                  </div>
                                  <span className="text-xs text-gray-500 w-10 text-right">{pct}%</span>
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
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
          <Button variant="outline" size="sm" onClick={loadRollbackLogs} disabled={rbLoading}>
            {rbLoading ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : null}
            {rbLoading ? "Loading..." : "Load"}
          </Button>
        </CardHeader>
        <CardContent>
          {rollbackLogs.length === 0 ? (
            <p className="text-gray-400 text-sm">Click Load to fetch rollback logs.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3 font-medium">Destination</th>
                    <th className="text-left py-2 px-3 font-medium">Timestamp</th>
                    <th className="text-left py-2 px-3 font-medium">Objects</th>
                    <th className="text-left py-2 px-3 font-medium">File</th>
                    <th className="text-left py-2 px-3 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {rollbackLogs.map((log: any, i: number) => (
                    <tr key={i} className="border-b hover:bg-gray-50">
                      <td className="py-2 px-3 font-medium">{log.destination_catalog || "\u2014"}</td>
                      <td className="py-2 px-3 text-gray-500 text-xs">
                        <div className="flex items-center gap-1">
                          <Clock className="h-3 w-3" />
                          {log.timestamp ? new Date(log.timestamp).toLocaleString() : "\u2014"}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <Badge variant="outline" className="text-xs">{log.total_objects ?? 0} objects</Badge>
                      </td>
                      <td className="py-2 px-3 text-gray-400 font-mono text-xs">{log.file}</td>
                      <td className="py-2 px-3">
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={() => openRollbackDialog(log)}
                        >
                          <Undo2 className="h-3 w-3 mr-1" />
                          Rollback
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Rollback Confirmation Dialog */}
      <Dialog open={rollbackDialogOpen} onOpenChange={setRollbackDialogOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Confirm Rollback</DialogTitle>
            <DialogDescription>
              Are you sure? This will drop cloned objects from{" "}
              <strong>{selectedLog?.destination_catalog}</strong>.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            {selectedLog && (
              <div className="text-sm text-gray-600">
                <p>Log file: <span className="font-mono text-xs">{selectedLog.file}</span></p>
                <p>Objects: {selectedLog.total_objects ?? 0}</p>
              </div>
            )}
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={dropCatalog}
                onChange={(e) => setDropCatalog(e.target.checked)}
                className="rounded border-gray-300"
              />
              Drop entire catalog after rollback
            </label>
            {rollbackResult && (
              <div className="rounded-lg bg-green-50 border border-green-200 p-3 text-sm">
                <div className="flex items-center gap-2 text-green-700 font-medium mb-1">
                  <CheckCircle className="h-4 w-4" />
                  Rollback Complete
                </div>
                <p className="text-green-600">
                  Objects dropped: {rollbackResult.objects_dropped ?? rollbackResult.dropped_count ?? "N/A"}
                </p>
              </div>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setRollbackDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={executeRollback}
              disabled={rollbackExecuting}
            >
              {rollbackExecuting ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Undo2 className="h-4 w-4 mr-2" />
              )}
              {rollbackExecuting ? "Executing..." : "Execute Rollback"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Catalog Snapshot */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Camera className="h-5 w-5" />
            Catalog Snapshot
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Catalog Name</label>
              <CatalogPicker catalog={snapshotCatalog} onCatalogChange={setSnapshotCatalog} showSchema={false} showTable={false} />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Output Path (optional)</label>
              <Input
                value={snapshotOutputPath}
                onChange={(e) => setSnapshotOutputPath(e.target.value)}
                placeholder="Auto-generated if empty"
              />
            </div>
            <Button onClick={createSnapshot} disabled={!snapshotCatalog || snapshotLoading}>
              {snapshotLoading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Camera className="h-4 w-4 mr-2" />
              )}
              {snapshotLoading ? "Creating..." : "Create Snapshot"}
            </Button>
          </div>

          {snapshotResult && (
            <div className="rounded-lg bg-blue-50 border border-blue-200 p-4 space-y-2">
              <div className="flex items-center gap-2 text-blue-700 font-medium">
                <CheckCircle className="h-4 w-4" />
                Snapshot Created
              </div>
              <div className="text-sm text-blue-600 space-y-1">
                <p>
                  Output: <span className="font-mono text-xs">{snapshotResult.output_path ?? snapshotResult.output_file ?? "N/A"}</span>
                </p>
                {snapshotResult.schemas !== undefined && (
                  <p>Schemas: {snapshotResult.schemas}</p>
                )}
                {snapshotResult.tables !== undefined && (
                  <p>Tables: {snapshotResult.tables}</p>
                )}
                {snapshotResult.views !== undefined && (
                  <p>Views: {snapshotResult.views}</p>
                )}
                {snapshotResult.total_objects !== undefined && (
                  <p>Total Objects: {snapshotResult.total_objects}</p>
                )}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Export Metadata */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileDown className="h-5 w-5" />
            Export Metadata
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Catalog Name</label>
              <CatalogPicker catalog={exportCatalog} onCatalogChange={setExportCatalog} showSchema={false} showTable={false} />
            </div>
            <div>
              <label className="text-sm font-medium">Format</label>
              <select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value)}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                <option value="csv">CSV</option>
                <option value="json">JSON</option>
              </select>
            </div>
            <Button onClick={exportMetadata} disabled={!exportCatalog || exportLoading}>
              {exportLoading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Download className="h-4 w-4 mr-2" />
              )}
              {exportLoading ? "Exporting..." : "Export"}
            </Button>
          </div>

          {exportResult && (
            <div className="rounded-lg bg-green-50 border border-green-200 p-4 space-y-2">
              <div className="flex items-center gap-2 text-green-700 font-medium">
                <CheckCircle className="h-4 w-4" />
                Export Complete
              </div>
              <div className="text-sm text-green-600">
                <p>
                  Output: <span className="font-mono text-xs">{exportResult.output_path ?? exportResult.output_file ?? "N/A"}</span>
                </p>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
