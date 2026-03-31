// @ts-nocheck
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { api } from "@/lib/api-client";
import {
  Briefcase, Copy, Search, Play, Clock, Download, Upload,
  GitCompare, Loader2, Globe, X, ChevronRight,
} from "lucide-react";

function useJobs(nameFilter = "") {
  return useQuery({ queryKey: ["jobs", nameFilter], queryFn: () => api.get(`/jobs?name_filter=${encodeURIComponent(nameFilter)}`), retry: 1 });
}

function useJobDetail(jobId: number | null) {
  return useQuery({ queryKey: ["job", jobId], queryFn: () => api.get(`/jobs/${jobId}`), enabled: !!jobId, retry: 1 });
}

export default function JobsPage() {
  const [search, setSearch] = useState("");
  const [selectedJob, setSelectedJob] = useState<number | null>(null);
  const [cloneModal, setCloneModal] = useState<{ jobId: number; name: string } | null>(null);
  const [crossWorkspace, setCrossWorkspace] = useState(false);
  const [cloneName, setCloneName] = useState("");
  const [destHost, setDestHost] = useState("");
  const [destToken, setDestToken] = useState("");
  const [diffModal, setDiffModal] = useState(false);
  const [diffJobA, setDiffJobA] = useState("");
  const [diffJobB, setDiffJobB] = useState("");
  const qc = useQueryClient();

  const { data: jobs, isLoading } = useJobs(search);
  const { data: detail } = useJobDetail(selectedJob);

  const cloneJob = useMutation({
    mutationFn: (data: any) => api.post(crossWorkspace ? "/jobs/clone-cross-workspace" : "/jobs/clone", data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["jobs"] }); setCloneModal(null); },
  });

  const diffJobs = useMutation({
    mutationFn: (data: any) => api.post("/jobs/diff", data),
  });

  const backupJobs = useMutation({
    mutationFn: (jobIds: number[]) => api.post("/jobs/backup", { job_ids: jobIds }),
    onSuccess: (data) => {
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a"); a.href = url; a.download = "jobs_backup.json"; a.click();
      URL.revokeObjectURL(url);
    },
  });

  const jobList = Array.isArray(jobs) ? jobs : [];

  return (
    <div className="space-y-4">
      <PageHeader title="Databricks Jobs" icon={Briefcase} breadcrumbs={["Automation", "Jobs"]}
        description="List, clone, compare, and backup Databricks Jobs across workspaces." />

      {/* Actions */}
      <div className="flex items-center gap-2 flex-wrap">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <input className="w-full pl-8 pr-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Filter jobs by name..."
            value={search} onChange={e => setSearch(e.target.value)} />
        </div>
        <Button size="sm" variant="outline" onClick={() => setDiffModal(true)}><GitCompare className="h-3 w-3 mr-1" /> Diff</Button>
        <Button size="sm" variant="outline" disabled={jobList.length === 0 || backupJobs.isPending}
          onClick={() => backupJobs.mutate(jobList.map(j => j.job_id))}>
          <Download className="h-3 w-3 mr-1" /> Backup All
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Jobs List */}
        <div className="md:col-span-2">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Workspace Jobs ({jobList.length})</CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <div className="space-y-2">{[1, 2, 3, 4].map(i => <Skeleton key={i} className="h-12 w-full" />)}</div>
              ) : jobList.length === 0 ? (
                <div className="text-center py-8 text-sm text-muted-foreground">No jobs found</div>
              ) : (
                <div className="space-y-1">
                  {jobList.map(job => (
                    <div key={job.job_id}
                      className={`flex items-center justify-between px-3 py-2.5 rounded-lg transition-colors cursor-pointer ${selectedJob === job.job_id ? "bg-[#E8453C]/5 border border-[#E8453C]/20" : "hover:bg-muted/30"}`}
                      onClick={() => setSelectedJob(job.job_id)}>
                      <div className="flex items-center gap-3 min-w-0">
                        <Briefcase className="h-4 w-4 text-muted-foreground shrink-0" />
                        <div className="min-w-0">
                          <p className="text-sm font-medium truncate">{job.name}</p>
                          <div className="flex items-center gap-2 mt-0.5">
                            <span className="text-[10px] text-muted-foreground">#{job.job_id}</span>
                            {job.task_count > 0 && <Badge variant="outline" className="text-[9px]">{job.task_count} tasks</Badge>}
                            {job.schedule && <Badge variant="outline" className="text-[9px]"><Clock className="h-2.5 w-2.5 mr-0.5" />{job.schedule.slice(0, 20)}</Badge>}
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center gap-1.5 shrink-0">
                        <Button size="sm" variant="ghost" className="h-7 text-xs" onClick={(e) => { e.stopPropagation(); setCloneModal({ jobId: job.job_id, name: job.name }); setCrossWorkspace(false); setCloneName(`${job.name}_clone`); }}>
                          <Copy className="h-3 w-3 mr-1" /> Clone
                        </Button>
                        <ChevronRight className="h-3 w-3 text-muted-foreground" />
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Detail Panel */}
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Job Detail</CardTitle></CardHeader>
          <CardContent>
            {selectedJob && detail && !detail.error ? (
              <div className="space-y-3">
                <div>
                  <p className="text-sm font-medium">{detail.name}</p>
                  <p className="text-xs text-muted-foreground">#{detail.job_id}</p>
                </div>
                {detail.creator_user_name && <p className="text-xs text-muted-foreground">Creator: {detail.creator_user_name}</p>}

                {/* Settings summary */}
                {detail.settings && (
                  <div>
                    <p className="text-[10px] font-semibold text-muted-foreground uppercase mb-1">Configuration</p>
                    <div className="space-y-1 text-xs">
                      {detail.settings.tasks && <p>{detail.settings.tasks.length} task(s)</p>}
                      {detail.settings.schedule && <p>Schedule: {detail.settings.schedule.quartz_cron_expression}</p>}
                      {detail.settings.max_retries && <p>Max retries: {detail.settings.max_retries}</p>}
                      {detail.settings.timeout_seconds && <p>Timeout: {detail.settings.timeout_seconds}s</p>}
                    </div>
                  </div>
                )}

                {/* Recent runs */}
                {detail.recent_runs?.length > 0 && (
                  <div>
                    <p className="text-[10px] font-semibold text-muted-foreground uppercase mb-1">Recent Runs</p>
                    <div className="space-y-1">
                      {detail.recent_runs.map(run => (
                        <div key={run.run_id} className="flex items-center justify-between text-xs">
                          <Badge variant="outline" className={`text-[9px] ${run.result_state?.includes("SUCCESS") ? "text-foreground" : run.result_state?.includes("FAIL") ? "text-red-500" : "text-muted-foreground"}`}>
                            {run.result_state || run.state}
                          </Badge>
                          <span className="text-muted-foreground">{run.run_duration ? `${Math.round(run.run_duration / 1000)}s` : ""}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div className="flex gap-1.5 pt-2">
                  <Button size="sm" variant="outline" className="h-7 text-xs flex-1" onClick={() => { setCloneModal({ jobId: selectedJob, name: detail.name }); setCrossWorkspace(false); setCloneName(`${detail.name}_clone`); }}>
                    <Copy className="h-3 w-3 mr-1" /> Clone
                  </Button>
                  <Button size="sm" variant="outline" className="h-7 text-xs flex-1" onClick={() => { setCloneModal({ jobId: selectedJob, name: detail.name }); setCrossWorkspace(true); setCloneName(`${detail.name}_clone`); }}>
                    <Globe className="h-3 w-3 mr-1" /> Cross-WS
                  </Button>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground text-center py-8">Select a job to see details</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Clone Modal */}
      {cloneModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          <div className="absolute inset-0 bg-black/40" onClick={() => setCloneModal(null)} />
          <Card className="relative z-10 w-full max-w-md">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm">{crossWorkspace ? "Clone to Another Workspace" : "Clone Job"}</CardTitle>
                <button onClick={() => setCloneModal(null)}><X className="h-4 w-4" /></button>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-xs text-muted-foreground">Source: {cloneModal.name} (#{cloneModal.jobId})</p>
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">New Name</label>
                <input className="w-full mt-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" value={cloneName} onChange={e => setCloneName(e.target.value)} />
              </div>
              {crossWorkspace && (
                <>
                  <div>
                    <label className="text-[10px] font-medium text-muted-foreground uppercase">Destination Host</label>
                    <input className="w-full mt-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="https://adb-xxx.azuredatabricks.net" value={destHost} onChange={e => setDestHost(e.target.value)} />
                  </div>
                  <div>
                    <label className="text-[10px] font-medium text-muted-foreground uppercase">Destination Token</label>
                    <input type="password" className="w-full mt-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="dapi..." value={destToken} onChange={e => setDestToken(e.target.value)} />
                  </div>
                </>
              )}
              <Button className="w-full" disabled={cloneJob.isPending || !cloneName}
                onClick={() => {
                  if (crossWorkspace) {
                    cloneJob.mutate({ job_id: cloneModal.jobId, dest_host: destHost, dest_token: destToken, new_name: cloneName });
                  } else {
                    cloneJob.mutate({ job_id: cloneModal.jobId, new_name: cloneName });
                  }
                }}>
                {cloneJob.isPending ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Copy className="h-3 w-3 mr-1" />}
                Clone Job
              </Button>
              {cloneJob.isSuccess && <Badge className="bg-muted/40 text-foreground">Job cloned: #{cloneJob.data?.new_job_id}</Badge>}
              {cloneJob.isError && <p className="text-xs text-red-500">Clone failed</p>}
            </CardContent>
          </Card>
        </div>
      )}

      {/* Diff Modal */}
      {diffModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          <div className="absolute inset-0 bg-black/40" onClick={() => setDiffModal(false)} />
          <Card className="relative z-10 w-full max-w-2xl max-h-[80vh] overflow-y-auto">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm">Compare Jobs</CardTitle>
                <button onClick={() => setDiffModal(false)}><X className="h-4 w-4" /></button>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center gap-2">
                <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Job A ID" value={diffJobA} onChange={e => setDiffJobA(e.target.value)} />
                <span className="text-muted-foreground">vs</span>
                <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Job B ID" value={diffJobB} onChange={e => setDiffJobB(e.target.value)} />
                <Button size="sm" disabled={!diffJobA || !diffJobB || diffJobs.isPending}
                  onClick={() => diffJobs.mutate({ job_id_a: parseInt(diffJobA), job_id_b: parseInt(diffJobB) })}>
                  {diffJobs.isPending ? <Loader2 className="h-3 w-3 animate-spin" /> : "Compare"}
                </Button>
              </div>

              {diffJobs.data && !diffJobs.data.error && (
                <div>
                  <p className="text-xs text-muted-foreground mb-2">{diffJobs.data.differences} differences out of {diffJobs.data.total_fields} fields</p>
                  {diffJobs.data.diffs?.length === 0 ? (
                    <p className="text-sm text-center py-4 text-muted-foreground">Jobs are identical</p>
                  ) : (
                    <div className="rounded-md border border-border overflow-hidden">
                      <div className="grid grid-cols-3 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                        <span>Field</span>
                        <span>Job A ({diffJobs.data.job_a?.name})</span>
                        <span>Job B ({diffJobs.data.job_b?.name})</span>
                      </div>
                      {diffJobs.data.diffs?.map((d, i) => (
                        <div key={i} className="grid grid-cols-3 px-3 py-2 border-t border-border bg-red-500/5">
                          <span className="text-xs font-mono font-medium">{d.field}</span>
                          <span className="text-xs break-all">{d.job_a}</span>
                          <span className="text-xs break-all">{d.job_b}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
