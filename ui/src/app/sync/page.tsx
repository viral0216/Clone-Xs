// @ts-nocheck
import { useState, useEffect, useRef } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  RefreshCw, Loader2, XCircle, ArrowRight, Plus, Minus, CheckCircle,
  AlertTriangle, Pencil, Clock, Download, ClipboardCopy, Check,
} from "lucide-react";

function actionColor(action: string) {
  switch (action?.toUpperCase()) {
    case "ADD": return "text-foreground bg-muted/20 border-border";
    case "UPDATE": return "text-muted-foreground bg-muted/20 border-border";
    case "REMOVE": return "text-red-700 bg-red-50 border-red-200";
    default: return "text-gray-700 bg-gray-50 border-gray-200";
  }
}

function actionIcon(action: string) {
  switch (action?.toUpperCase()) {
    case "ADD": return <Plus className="h-4 w-4 text-foreground" />;
    case "UPDATE": return <Pencil className="h-4 w-4 text-muted-foreground" />;
    case "REMOVE": return <Minus className="h-4 w-4 text-red-500" />;
    default: return null;
  }
}

function SyncJobProgress({ jobId }: { jobId: string }) {
  const [job, setJob] = useState<any>(null);
  const [copied, setCopied] = useState(false);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${jobId}`);
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch {}
    };
    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [jobId]);

  if (!job) {
    return (
      <div className="flex items-center gap-2 text-gray-500">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading job status...
      </div>
    );
  }

  const statusColor = {
    queued: "bg-muted/40 text-muted-foreground",
    running: "bg-[#E8453C]/10 text-[#E8453C]",
    completed: "bg-muted/40 text-foreground",
    failed: "bg-red-100 text-red-800",
  }[job.status] || "bg-gray-100 text-gray-800";

  const statusIcon = {
    queued: <Clock className="h-5 w-5 text-muted-foreground" />,
    running: <Loader2 className="h-5 w-5 text-[#E8453C] animate-spin" />,
    completed: <CheckCircle className="h-5 w-5 text-foreground" />,
    failed: <XCircle className="h-5 w-5 text-red-600" />,
  }[job.status];

  const result = job.result;
  const tables = result?.tables || result?.results || [];
  const summary = result?.summary;
  const addCount = summary?.tables_to_add ?? tables.filter((t: any) => t.action?.toUpperCase() === "ADD").length;
  const updateCount = summary?.tables_to_update ?? tables.filter((t: any) => t.action?.toUpperCase() === "UPDATE").length;
  const removeCount = summary?.tables_to_remove ?? tables.filter((t: any) => t.action?.toUpperCase() === "REMOVE").length;

  return (
    <div className="space-y-4">
      {/* Status Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {statusIcon}
          <div>
            <p className="font-semibold text-lg">
              {job.source_catalog} <ArrowRight className="inline h-4 w-4 mx-1" /> {job.destination_catalog}
            </p>
            <p className="text-sm text-gray-500">Job {jobId} &middot; sync</p>
          </div>
        </div>
        <Badge className={statusColor}>{job.status.toUpperCase()}</Badge>
      </div>

      {/* Running */}
      {job.status === "running" && (
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Sync in progress...
          </div>
          <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
            <div className="h-full bg-[#E8453C] rounded-full animate-pulse" style={{ width: "60%" }} />
          </div>
        </div>
      )}

      {/* Timing */}
      <div className="flex gap-6 text-xs text-gray-500">
        {job.started_at && <span>Started: {new Date(job.started_at).toLocaleTimeString()}</span>}
        {job.completed_at && <span>Completed: {new Date(job.completed_at).toLocaleTimeString()}</span>}
        {job.started_at && job.completed_at && (
          <span>Duration: {Math.round((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 1000)}s</span>
        )}
      </div>

      {/* Results */}
      {job.status === "completed" && result && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-3 gap-4">
            <Card>
              <CardContent className="pt-4 text-center">
                <p className="text-2xl font-bold text-foreground">{addCount}</p>
                <p className="text-xs text-gray-500">Tables to Add</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4 text-center">
                <p className="text-2xl font-bold text-muted-foreground">{updateCount}</p>
                <p className="text-xs text-gray-500">Tables to Update</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4 text-center">
                <p className="text-2xl font-bold text-red-700">{removeCount}</p>
                <p className="text-xs text-gray-500">Tables to Remove</p>
              </CardContent>
            </Card>
          </div>

          {/* Table details */}
          {tables.length > 0 && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center gap-2">
                  Sync Details
                  {result?.dry_run && <Badge variant="outline" className="text-xs">DRY RUN</Badge>}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto max-h-80 overflow-y-auto border rounded">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-white">
                      <tr className="border-b bg-gray-50">
                        <th className="text-left py-2 px-3 font-medium w-8"></th>
                        <th className="text-left py-2 px-3 font-medium">Schema</th>
                        <th className="text-left py-2 px-3 font-medium">Table</th>
                        <th className="text-left py-2 px-3 font-medium">Action</th>
                        <th className="text-left py-2 px-3 font-medium">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tables.map((row: any, i: number) => (
                        <tr key={i} className={`border-b ${
                          row.action?.toUpperCase() === "ADD" ? "bg-muted/20" :
                          row.action?.toUpperCase() === "REMOVE" ? "bg-red-50/50" :
                          row.action?.toUpperCase() === "UPDATE" ? "bg-muted/20" : ""
                        }`}>
                          <td className="py-2 px-3">{actionIcon(row.action)}</td>
                          <td className="py-2 px-3 text-gray-600">{row.schema}</td>
                          <td className="py-2 px-3 font-medium">{row.table}</td>
                          <td className="py-2 px-3">
                            <Badge variant="outline" className={`text-xs ${actionColor(row.action)}`}>
                              {row.action?.toUpperCase()}
                            </Badge>
                          </td>
                          <td className="py-2 px-3">
                            {row.status?.toUpperCase() === "DONE" || row.status?.toUpperCase() === "SUCCESS" ? (
                              <span className="flex items-center gap-1 text-foreground">
                                <CheckCircle className="h-3.5 w-3.5" /> {row.status}
                              </span>
                            ) : (
                              <span className="text-gray-500">{row.status || "pending"}</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Download */}
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={() => {
              const blob = new Blob([JSON.stringify(result, null, 2)], { type: "application/json" });
              const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
              a.download = `sync-result-${jobId}.json`; a.click();
            }}>
              <Download className="h-3 w-3 mr-1" /> Download JSON
            </Button>
          </div>
        </>
      )}

      {/* Error */}
      {job.status === "failed" && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="pt-4">
            <div className="flex items-center gap-2 mb-2">
              <XCircle className="h-5 w-5 text-red-600" />
              <span className="font-medium text-red-800">Sync failed</span>
            </div>
            <pre className="bg-white p-3 rounded text-sm text-red-700 overflow-auto max-h-48 whitespace-pre-wrap">
              {job.error}
            </pre>
          </CardContent>
        </Card>
      )}

      {/* Logs */}
      {job.logs && job.logs.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center justify-between">
              <span className="flex items-center gap-2">
                {job.status === "running" && <Loader2 className="h-3 w-3 animate-spin" />}
                Logs
              </span>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="text-xs">{job.logs.length} lines</Badge>
                <Button variant="ghost" size="sm" className="h-7 px-2" onClick={async () => {
                  await navigator.clipboard.writeText(job.logs.join("\n"));
                  setCopied(true); setTimeout(() => setCopied(false), 2000);
                }}>
                  {copied ? <Check className="h-3 w-3 text-foreground" /> : <ClipboardCopy className="h-3 w-3" />}
                  <span className="ml-1 text-xs">{copied ? "Copied" : "Copy"}</span>
                </Button>
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="bg-gray-900 text-gray-300 p-3 rounded-lg font-mono text-xs max-h-48 overflow-y-auto"
              ref={(el) => { if (el && job.status === "running") el.scrollTop = el.scrollHeight; }}>
              {job.logs.map((line: string, i: number) => (
                <div key={i} className={
                  line.includes("ERROR") ? "text-red-400" :
                  line.includes("WARNING") ? "text-gray-400" :
                  line.includes("completed") || line.includes("success") ? "text-gray-300" :
                  line.includes("Syncing") || line.includes("Scanning") ? "text-gray-400" : ""
                }>{line}</div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

export default function SyncPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [dropExtra, setDropExtra] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleSync = async () => {
    if (!dryRun && !showConfirm) {
      setShowConfirm(true);
      return;
    }
    setShowConfirm(false);
    setSubmitting(true);
    try {
      const res = await api.post<any>("/sync", {
        source_catalog: source,
        destination_catalog: dest,
        dry_run: dryRun,
        drop_extra: dropExtra,
      });
      setActiveJobId(res.job_id);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setSubmitting(false);
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Sync"
        icon={RefreshCw}
        description="Two-way synchronization between catalogs — detects missing, extra, or modified tables and applies changes. Preserves permissions, tags, and properties during sync."
        breadcrumbs={["Operations", "Sync"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables"
        docsLabel="Unity Catalog tables"
      />

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Source Catalog</label>
              <CatalogPicker catalog={source} onCatalogChange={setSource} showSchema={false} showTable={false} />
            </div>
            <div className="flex items-center text-gray-400 pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Destination Catalog</label>
              <CatalogPicker catalog={dest} onCatalogChange={setDest} showSchema={false} showTable={false} />
            </div>
            <div className="flex items-center gap-4 pb-2">
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={dryRun}
                  onChange={(e) => { setDryRun(e.target.checked); setShowConfirm(false); }} />
                Dry Run
              </label>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={dropExtra} onChange={(e) => setDropExtra(e.target.checked)} />
                Drop Extra
              </label>
            </div>
            <Button onClick={handleSync} disabled={!source || !dest || submitting}
              variant={!dryRun ? "destructive" : "default"}>
              {submitting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {submitting ? "Submitting..." : dryRun ? "Preview Sync" : "Sync"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Confirmation */}
      {showConfirm && (
        <Card className="border-border bg-muted/20">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <AlertTriangle className="h-6 w-6 text-muted-foreground" />
              <div className="flex-1">
                <p className="font-semibold">Are you sure you want to sync?</p>
                <p className="text-sm text-gray-600">
                  This will make changes to <strong>{dest}</strong>.
                  {dropExtra && " Extra tables in destination will be dropped."}
                </p>
              </div>
              <div className="flex gap-2">
                <Button variant="outline" onClick={() => setShowConfirm(false)}>Cancel</Button>
                <Button variant="destructive" onClick={handleSync}>Confirm Sync</Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Job Progress */}
      {activeJobId && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <RefreshCw className="h-5 w-5" /> Sync Job
            </CardTitle>
          </CardHeader>
          <CardContent>
            <SyncJobProgress jobId={activeJobId} />
            <div className="mt-4">
              <Button variant="outline" onClick={() => { setActiveJobId(null); }}>New Sync</Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
