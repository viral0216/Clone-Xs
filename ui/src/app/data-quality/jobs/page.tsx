// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import StatusBadge from "@/components/StatusBadge";
import EmptyState from "@/components/EmptyState";
import LoadingState from "@/components/LoadingState";
import {
  Layers, RefreshCw, XCircle, Clock, Loader2,
  CheckCircle, AlertTriangle, Play, Trash2,
} from "lucide-react";

interface Job {
  job_id: string;
  job_type: string;
  status: string;
  source_catalog: string | null;
  destination_catalog: string | null;
  clone_type: string | null;
  progress: any;
  error: string | null;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
}

function timeSince(ts: string | null) {
  if (!ts) return "—";
  const diff = (Date.now() - new Date(ts).getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function duration(start: string | null, end: string | null) {
  if (!start) return "—";
  const endTs = end ? new Date(end).getTime() : Date.now();
  const secs = (endTs - new Date(start).getTime()) / 1000;
  if (secs < 60) return `${secs.toFixed(1)}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${Math.floor(secs % 60)}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

function ProgressBar({ value, max, label }: { value: number; max: number; label: string }) {
  const pct = max > 0 ? Math.round((value / max) * 100) : 0;
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>{label}</span>
        <span>{value}/{max} ({pct}%)</span>
      </div>
      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
        <div className="h-full bg-[#E8453C] rounded-full transition-all" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

export default function ActiveJobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);

  async function loadJobs() {
    try {
      const data = await api.get("/clone/jobs");
      setJobs(Array.isArray(data) ? data : []);
    } catch {
      setJobs([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadJobs();
    const interval = setInterval(loadJobs, 3000);
    return () => clearInterval(interval);
  }, []);

  async function cancelJob(jobId: string) {
    try {
      await api.delete(`/clone/${jobId}`);
      toast.success(`Job ${jobId} cancelled.`);
      loadJobs();
    } catch (e: any) {
      toast.error(e?.message || "Failed to cancel job.");
    }
  }

  const active = jobs.filter(j => j.status === "running" || j.status === "queued");
  const completed = jobs.filter(j => j.status !== "running" && j.status !== "queued");

  const columns: Column[] = [
    {
      key: "job_id", label: "Job ID", sortable: true,
      render: (v) => <span className="font-mono text-xs">{v}</span>,
    },
    {
      key: "job_type", label: "Type", sortable: true,
      render: (v) => <Badge variant="outline" className="text-[10px]">{v || "clone"}</Badge>,
    },
    {
      key: "status", label: "Status", sortable: true,
      render: (v) => <StatusBadge status={v} />,
    },
    {
      key: "source_catalog", label: "Source → Dest", sortable: true,
      render: (_, row) => (
        <span className="text-xs">
          {row.source_catalog || "—"} <span className="text-muted-foreground">→</span> {row.destination_catalog || "—"}
        </span>
      ),
    },
    {
      key: "started_at", label: "Duration", sortable: true,
      render: (_, row) => (
        <span className="text-xs tabular-nums">
          {row.status === "running" ? (
            <span className="text-[#E8453C]">{duration(row.started_at, null)}</span>
          ) : (
            duration(row.started_at, row.completed_at)
          )}
        </span>
      ),
    },
    {
      key: "created_at", label: "Created", sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{timeSince(v)}</span>,
    },
    {
      key: "_actions", label: "", sortable: false, align: "right",
      render: (_, row) => (
        row.status === "queued" ? (
          <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); cancelJob(row.job_id); }}>
            <XCircle className="h-3.5 w-3.5 text-red-500" />
          </Button>
        ) : null
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Active Jobs"
        description="Monitor running and queued jobs across all operations — clone, reconciliation, monitoring."
        icon={Layers}
        breadcrumbs={["Data Quality", "Jobs"]}
      />

      {/* Active Jobs Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2">
              <Loader2 className={`h-4 w-4 ${active.filter(j => j.status === "running").length > 0 ? "animate-spin text-[#E8453C]" : "text-muted-foreground"}`} />
              <div>
                <p className="text-2xl font-bold">{active.filter(j => j.status === "running").length}</p>
                <p className="text-xs text-muted-foreground">Running</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-blue-500" />
              <div>
                <p className="text-2xl font-bold">{active.filter(j => j.status === "queued").length}</p>
                <p className="text-xs text-muted-foreground">Queued</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2">
              <CheckCircle className="h-4 w-4 text-green-500" />
              <div>
                <p className="text-2xl font-bold">{completed.filter(j => j.status === "completed").length}</p>
                <p className="text-xs text-muted-foreground">Completed</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2">
              <XCircle className="h-4 w-4 text-red-500" />
              <div>
                <p className="text-2xl font-bold">{completed.filter(j => j.status === "failed").length}</p>
                <p className="text-xs text-muted-foreground">Failed</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Active / Running Jobs */}
      {active.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-[#E8453C]" />
                Active Jobs ({active.length})
              </CardTitle>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            {active.map(job => (
              <Card key={job.job_id} className="border-[#E8453C]/20 bg-[#E8453C]/5">
                <CardContent className="pt-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <StatusBadge status={job.status} />
                      <Badge variant="outline" className="text-[10px]">{job.job_type || "clone"}</Badge>
                      <span className="font-mono text-xs text-muted-foreground">{job.job_id}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs tabular-nums text-[#E8453C]">{duration(job.started_at, null)}</span>
                      {job.status === "queued" && (
                        <Button variant="outline" size="sm" onClick={() => cancelJob(job.job_id)}>
                          <XCircle className="h-3 w-3 mr-1" /> Cancel
                        </Button>
                      )}
                    </div>
                  </div>
                  {job.source_catalog && (
                    <p className="text-xs text-muted-foreground">
                      {job.source_catalog} → {job.destination_catalog}
                    </p>
                  )}
                  {job.progress && (
                    <div className="space-y-2">
                      {job.progress.total_tables != null && (
                        <ProgressBar value={job.progress.completed_tables || 0} max={job.progress.total_tables} label="Tables" />
                      )}
                      {job.progress.current_table && (
                        <p className="text-[10px] text-muted-foreground font-mono truncate">
                          Current: {job.progress.current_table}
                        </p>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </CardContent>
        </Card>
      )}

      {/* All Jobs Table */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">Job History ({jobs.length})</CardTitle>
            <Button variant="outline" size="sm" onClick={loadJobs}>
              <RefreshCw className="h-3 w-3 mr-1" /> Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <LoadingState message="Loading jobs..." />
          ) : jobs.length === 0 ? (
            <EmptyState icon={Layers} title="No jobs" description="Submit a clone, reconciliation, or monitoring job to see it here." />
          ) : (
            <DataTable
              data={jobs}
              columns={columns}
              searchable
              searchKeys={["job_id", "job_type", "status", "source_catalog", "destination_catalog"]}
              pageSize={25}
              compact
              tableId="active-jobs"
              emptyMessage="No jobs found."
              rowClassName={(row) =>
                row.status === "running" ? "bg-[#E8453C]/5" :
                row.status === "failed" ? "bg-red-500/5" : ""
              }
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
