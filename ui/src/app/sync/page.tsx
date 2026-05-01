// @ts-nocheck
import { useState, useEffect, useMemo } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import FieldLabel, { FieldLabelSmall, InfoDot } from "@/components/FieldLabel";
import {
  RefreshCw, Loader2, XCircle, ArrowRight, Plus, Minus, CheckCircle,
  AlertTriangle, Pencil, Clock, Download, ClipboardCopy, Check,
  Zap, GitCompare, History, CalendarClock, Pause, Play, Trash2,
} from "lucide-react";
import {
  useStartSync, useIncrementalCheck, useStartIncrementalSync,
  useSchemaEvolutionDetect, useSyncJobs,
  useCdfCheck, useSchedules, useCreateSchedule,
  usePauseSchedule, useResumeSchedule, useDeleteSchedule,
} from "@/hooks/useApi";
import { useDurableJob } from "@/hooks/useDurableJob";

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
  // Polling lives in JobContext via useDurableJob, so navigating away mid-run
  // and returning resumes from the latest server state instead of the local
  // useState resetting to null.
  const tracker = useDurableJob({
    key: `sync-job-${jobId}`,
    pollUrl: (id) => `/clone/${id}`,
    pollInterval: 2000,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
    notificationTitle: "Sync complete",
  });
  // Seed the tracker the first time we render this jobId — useDurableJob
  // won't poll until it has a jobId in its entry, and we get jobId via prop
  // (not via the user clicking submit).
  useEffect(() => {
    if (!tracker.jobId && jobId) {
      tracker.start({}, async () => jobId).catch(() => {});
    }
  }, [jobId, tracker]);
  const job = tracker.entry?.data ?? null;
  const [copied, setCopied] = useState(false);

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

type SyncMode = "two_way" | "incremental";
type IncrementalStrategy = "auto" | "cdf" | "version";

export default function SyncPage() {
  const [mode, setMode] = useState<SyncMode>("two_way");

  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const [dryRun, setDryRun] = useState(true);

  // Two-way
  const [dropExtra, setDropExtra] = useState(false);

  // Incremental
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schemaName, setSchemaName] = useState("");
  const [syncStrategy, setSyncStrategy] = useState<IncrementalStrategy>("auto");
  const [cloneType, setCloneType] = useState<"DEEP" | "SHALLOW">("DEEP");

  const [showConfirm, setShowConfirm] = useState(false);
  // Active sync job_id — persisted in sessionStorage so navigating away
  // mid-sync and coming back keeps the progress card visible. The full
  // server-side job state is fetched fresh on remount by SyncJobProgress.
  const [activeJobId, _setActiveJobId] = useState<string | null>(() => {
    try { return sessionStorage.getItem("clxs-sync-active-job") || null; } catch { return null; }
  });
  const setActiveJobId = (id: string | null) => {
    _setActiveJobId(id);
    try {
      if (id) sessionStorage.setItem("clxs-sync-active-job", id);
      else sessionStorage.removeItem("clxs-sync-active-job");
    } catch {}
  };

  const startSync = useStartSync();
  const incCheck = useIncrementalCheck();
  const startIncSync = useStartIncrementalSync();
  const schemaEv = useSchemaEvolutionDetect();
  const syncJobs = useSyncJobs();
  const cdfCheck = useCdfCheck();
  const schedules = useSchedules();
  const createSchedule = useCreateSchedule();
  const pauseSchedule = usePauseSchedule();
  const resumeSchedule = useResumeSchedule();
  const deleteSchedule = useDeleteSchedule();

  // Map of "schema.table" → { cdf_enabled, change_summary }
  const [cdfByTable, setCdfByTable] = useState<Record<string, any>>({});
  const [showScheduleModal, setShowScheduleModal] = useState(false);

  // Load schemas for the source catalog when in incremental mode
  useEffect(() => {
    if (mode !== "incremental" || !source) return;
    api.get<string[]>(`/catalogs/${encodeURIComponent(source)}/schemas`)
      .then((data) => {
        setSchemas(data || []);
        if (data && data.length && !schemaName) setSchemaName(data[0]);
      })
      .catch(() => setSchemas([]));
  }, [mode, source]);

  // Reset schema selection when source changes
  useEffect(() => {
    setSchemaName("");
  }, [source]);

  const submitting =
    startSync.isPending || incCheck.isPending || startIncSync.isPending;

  const canSubmit = useMemo(() => {
    if (!source || !dest || submitting) return false;
    if (mode === "incremental" && !schemaName) return false;
    return true;
  }, [source, dest, mode, schemaName, submitting]);

  const handleSync = () => {
    if (!dryRun && !showConfirm) {
      setShowConfirm(true);
      return;
    }
    setShowConfirm(false);

    if (mode === "two_way") {
      startSync.mutate(
        {
          source_catalog: source,
          destination_catalog: dest,
          dry_run: dryRun,
          drop_extra: dropExtra,
        },
        {
          onSuccess: (res: any) => {
            setActiveJobId(res.job_id);
            syncJobs.refetch();
          },
          onError: (e: any) => toast.error(e?.message || "Sync failed"),
        },
      );
    } else {
      startIncSync.mutate(
        {
          source_catalog: source,
          destination_catalog: dest,
          schema_name: schemaName,
          dry_run: dryRun,
          sync_mode: syncStrategy,
          clone_type: cloneType,
        },
        {
          onSuccess: (res: any) => {
            setActiveJobId(res.job_id);
            syncJobs.refetch();
          },
          onError: (e: any) => toast.error(e?.message || "Incremental sync failed"),
        },
      );
    }
  };

  const handleCheckChanges = () => {
    if (!source || !dest || !schemaName) return;
    setCdfByTable({});
    incCheck.mutate(
      {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: schemaName,
      },
      {
        onSuccess: async (data: any) => {
          // Fan out CDF checks for every table in the preview.
          // Best-effort parallel probe — failures just leave the badge unknown.
          const tables = (data?.tables || []).slice(0, 50);
          for (const t of tables) {
            cdfCheck.mutate(
              {
                source_catalog: source,
                destination_catalog: dest,
                schema_name: schemaName,
                table_name: t.table_name,
              },
              {
                onSuccess: (r: any) => {
                  setCdfByTable((prev) => ({
                    ...prev,
                    [`${schemaName}.${t.table_name}`]: r,
                  }));
                },
              },
            );
          }
        },
        onError: (e: any) => toast.error(e?.message || "Check failed"),
      },
    );
  };

  const handleDetectSchemaDrift = (schema: string, table: string) => {
    schemaEv.mutate(
      {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: schema,
        table_name: table,
      },
      {
        onError: (e: any) => toast.error(e?.message || "Detect failed"),
      },
    );
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Sync"
        icon={RefreshCw}
        description="Two-way synchronization or Delta-aware incremental sync. Preview the plan before executing, then watch live progress."
        breadcrumbs={["Operations", "Sync"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables"
        docsLabel="Unity Catalog tables"
      />

      {/* Mode toggle */}
      <Card>
        <CardContent className="pt-6 space-y-4">
          <div className="flex items-center gap-2">
            <FieldLabelSmall hint="Two-way: detect + apply missing/extra/modified tables at the structural level. Incremental: re-copy only tables whose Delta version advanced since the last sync.">
              Sync Mode
            </FieldLabelSmall>
          </div>
          <div className="flex gap-2">
            <Button
              type="button"
              size="sm"
              variant={mode === "two_way" ? "default" : "outline"}
              onClick={() => { setMode("two_way"); setActiveJobId(null); }}
            >
              <RefreshCw className="h-4 w-4 mr-1.5" /> Two-way Sync
            </Button>
            <Button
              type="button"
              size="sm"
              variant={mode === "incremental" ? "default" : "outline"}
              onClick={() => { setMode("incremental"); setActiveJobId(null); }}
            >
              <Zap className="h-4 w-4 mr-1.5" /> Incremental Sync
            </Button>
          </div>

          {/* Catalog inputs */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <FieldLabel hint="Catalog whose state drives the sync.">Source Catalog</FieldLabel>
              <CatalogPicker catalog={source} onCatalogChange={setSource} showSchema={false} showTable={false} />
            </div>
            <div>
              <FieldLabel hint="Catalog being kept in sync with the source.">Destination Catalog</FieldLabel>
              <CatalogPicker catalog={dest} onCatalogChange={setDest} showSchema={false} showTable={false} />
            </div>
          </div>

          {/* Incremental-specific */}
          {mode === "incremental" && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <FieldLabelSmall hint="Incremental sync works one schema at a time. Pick the schema whose tables should be checked for version advancement.">
                  Schema
                </FieldLabelSmall>
                <select
                  className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1A73E8]/30 focus:border-[#1A73E8]"
                  value={schemaName}
                  onChange={(e) => setSchemaName(e.target.value)}
                  disabled={!source || schemas.length === 0}
                >
                  <option value="">Select a schema…</option>
                  {schemas.map((s) => (
                    <option key={s} value={s}>{s}</option>
                  ))}
                </select>
              </div>
              <div>
                <FieldLabelSmall hint="Auto: CDF if source table has delta.enableChangeDataFeed=true AND destination has a PK, else Version. CDF: row-level MERGE (requires PK). Version: full re-clone of changed tables.">
                  Sync Strategy
                </FieldLabelSmall>
                <div className="flex gap-1 mt-1">
                  {(["auto", "cdf", "version"] as const).map((s) => (
                    <Button
                      key={s}
                      type="button"
                      size="sm"
                      variant={syncStrategy === s ? "default" : "outline"}
                      onClick={() => setSyncStrategy(s)}
                    >
                      {s.toUpperCase()}
                    </Button>
                  ))}
                </div>
              </div>
              <div>
                <FieldLabelSmall hint="When Version mode applies: DEEP re-copies all data files, SHALLOW re-writes only the metadata pointer.">
                  Clone Type (fallback)
                </FieldLabelSmall>
                <div className="flex gap-1 mt-1">
                  {(["DEEP", "SHALLOW"] as const).map((s) => (
                    <Button
                      key={s}
                      type="button"
                      size="sm"
                      variant={cloneType === s ? "default" : "outline"}
                      onClick={() => setCloneType(s)}
                    >
                      {s}
                    </Button>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Options row */}
          <div className="flex items-center gap-4 flex-wrap">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={dryRun}
                onChange={(e) => { setDryRun(e.target.checked); setShowConfirm(false); }}
              />
              Dry Run
              <InfoDot hint="Compute and show the planned operations without applying changes." />
            </label>

            {mode === "two_way" && (
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={dropExtra} onChange={(e) => setDropExtra(e.target.checked)} />
                Drop Extra
                <InfoDot hint="Drop tables on the destination that no longer exist on the source. Off = additive only." />
              </label>
            )}

            <div className="ml-auto flex items-center gap-2">
              {mode === "incremental" && (
                <Button
                  type="button"
                  variant="outline"
                  onClick={handleCheckChanges}
                  disabled={!canSubmit}
                >
                  {incCheck.isPending ? (
                    <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Checking…</>
                  ) : (
                    <><GitCompare className="h-4 w-4 mr-1" /> Check changes</>
                  )}
                </Button>
              )}
              <Button
                type="button"
                variant="outline"
                onClick={() => setShowScheduleModal(true)}
                disabled={!source || !dest || (mode === "incremental" && !schemaName)}
                title="Schedule this sync to run on a cron"
              >
                <CalendarClock className="h-4 w-4 mr-1" /> Schedule…
              </Button>
              <Button
                onClick={handleSync}
                disabled={!canSubmit}
                variant={!dryRun ? "destructive" : "default"}
              >
                {submitting ? (
                  <><Loader2 className="h-4 w-4 animate-spin mr-2" /></>
                ) : (
                  <><RefreshCw className="h-4 w-4 mr-2" /></>
                )}
                {submitting
                  ? "Submitting…"
                  : dryRun
                  ? (mode === "incremental" ? "Preview Incremental Sync" : "Preview Sync")
                  : (mode === "incremental" ? "Run Incremental Sync" : "Run Sync")}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Incremental check preview */}
      {mode === "incremental" && incCheck.data && (
        <IncrementalCheckPanel
          data={incCheck.data}
          onDetectSchemaDrift={handleDetectSchemaDrift}
          schemaEvolution={schemaEv.data}
          schemaEvolutionLoading={schemaEv.isPending}
          cdfByTable={cdfByTable}
        />
      )}

      {/* Destructive-sync confirmation */}
      {showConfirm && (
        <Card className="border-border bg-muted/20">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <AlertTriangle className="h-6 w-6 text-muted-foreground" />
              <div className="flex-1">
                <p className="font-semibold">Are you sure you want to sync?</p>
                <p className="text-sm text-gray-600">
                  This will make changes to <strong>{dest}</strong>.
                  {mode === "two_way" && dropExtra && " Extra tables in destination will be dropped."}
                  {mode === "incremental" && ` Only tables in schema '${schemaName}' whose Delta version advanced will be touched.`}
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

      {/* Active job progress */}
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
              <Button variant="outline" onClick={() => setActiveJobId(null)}>New Sync</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Scheduled syncs */}
      <ScheduledSyncsPanel
        schedules={schedules.data || []}
        isLoading={schedules.isLoading}
        isFetching={schedules.isFetching}
        onRefresh={() => schedules.refetch()}
        onPause={(id) => pauseSchedule.mutate(id)}
        onResume={(id) => resumeSchedule.mutate(id)}
        onDelete={(id) => {
          if (confirm("Delete this schedule?")) deleteSchedule.mutate(id);
        }}
      />

      {/* Recent syncs */}
      <RecentSyncsPanel
        jobs={syncJobs.data || []}
        isLoading={syncJobs.isLoading}
        onRefresh={() => syncJobs.refetch()}
        isFetching={syncJobs.isFetching}
        onSelectJob={(id) => setActiveJobId(id)}
      />

      {/* Schedule modal */}
      {showScheduleModal && (
        <ScheduleModal
          mode={mode}
          source={source}
          dest={dest}
          schemaName={schemaName}
          syncStrategy={syncStrategy}
          dropExtra={dropExtra}
          onClose={() => setShowScheduleModal(false)}
          onSubmit={(name, cron) => {
            const payload: Record<string, unknown> = {
              name,
              source_catalog: source,
              destination_catalog: dest,
              cron,
              clone_type: cloneType,
              job_type: mode === "incremental" ? "incremental_sync" : "sync",
              dry_run: false,
            };
            if (mode === "incremental") {
              payload.schema_name = schemaName;
              payload.sync_mode = syncStrategy;
            } else {
              payload.drop_extra = dropExtra;
            }
            createSchedule.mutate(payload, {
              onSuccess: () => {
                toast.success(`Schedule "${name}" created`);
                setShowScheduleModal(false);
              },
              onError: (e: any) => toast.error(e?.message || "Create failed"),
            });
          }}
          isCreating={createSchedule.isPending}
        />
      )}
    </div>
  );
}

function IncrementalCheckPanel({
  data, onDetectSchemaDrift, schemaEvolution, schemaEvolutionLoading, cdfByTable,
}: {
  data: any;
  onDetectSchemaDrift: (schema: string, table: string) => void;
  schemaEvolution: any;
  schemaEvolutionLoading: boolean;
  cdfByTable: Record<string, any>;
}) {
  const tables = data?.tables || [];
  const schema = data?.schema;
  if (!tables.length) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <GitCompare className="h-4 w-4" /> Incremental Check Preview — {schema}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground italic">
            No tables need syncing in <code>{schema}</code> — destination already matches the source versions.
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <GitCompare className="h-4 w-4" />
          Incremental Check Preview — {schema}
          <Badge className="ml-1">{tables.length} need sync</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-auto max-h-96 border rounded">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-muted/40">
              <tr className="border-b">
                <th className="text-left py-2 px-3 font-medium">Table</th>
                <th className="text-left py-2 px-3 font-medium">CDF</th>
                <th className="text-left py-2 px-3 font-medium">Reason</th>
                <th className="text-left py-2 px-3 font-medium">Last synced</th>
                <th className="text-left py-2 px-3 font-medium">Current</th>
                <th className="text-left py-2 px-3 font-medium">Changes</th>
                <th className="text-left py-2 px-3 font-medium">Operations</th>
                <th className="text-right py-2 px-3 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {tables.map((t: any, i: number) => {
                const cdf = cdfByTable[`${schema}.${t.table_name}`];
                return (
                <tr key={i} className="border-b">
                  <td className="py-1.5 px-3 font-medium">{t.table_name}</td>
                  <td className="py-1.5 px-3">
                    {cdf === undefined ? (
                      <span className="text-[10px] text-muted-foreground">…</span>
                    ) : cdf.cdf_enabled ? (
                      <Badge className="text-[10px] bg-green-100 text-green-700 dark:bg-green-950/50 dark:text-green-400 border-green-200">
                        CDF
                      </Badge>
                    ) : (
                      <Badge variant="outline" className="text-[10px] text-muted-foreground">
                        no CDF
                      </Badge>
                    )}
                  </td>
                  <td className="py-1.5 px-3">
                    <Badge variant="outline" className="text-xs">
                      {t.reason === "never_synced" ? "NEVER SYNCED" : "CHANGED"}
                    </Badge>
                  </td>
                  <td className="py-1.5 px-3 text-xs text-muted-foreground">
                    {t.last_synced_version ?? "—"}
                  </td>
                  <td className="py-1.5 px-3 text-xs">{t.current_version ?? "—"}</td>
                  <td className="py-1.5 px-3 text-xs">{t.changes_since_sync ?? "—"}</td>
                  <td className="py-1.5 px-3 text-xs text-muted-foreground">
                    {(t.operations || []).slice(0, 5).join(", ")}
                    {t.operations && t.operations.length > 5 && ` +${t.operations.length - 5}`}
                  </td>
                  <td className="py-1.5 px-3 text-right">
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      onClick={() => onDetectSchemaDrift(schema, t.table_name)}
                    >
                      {schemaEvolutionLoading ? (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      ) : (
                        <>Detect schema drift</>
                      )}
                    </Button>
                  </td>
                </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Schema evolution result */}
        {schemaEvolution && (
          <div className="mt-3 border rounded-md p-3 bg-muted/30 text-sm">
            <div className="font-medium mb-2 flex items-center gap-2">
              <Pencil className="h-3.5 w-3.5" />
              Column-level drift
            </div>
            <div className="grid grid-cols-3 gap-2 text-xs">
              <div>
                <div className="font-medium text-green-700 dark:text-green-400">
                  Added ({(schemaEvolution.added_columns || []).length})
                </div>
                <ul className="list-disc pl-4 max-h-28 overflow-auto">
                  {(schemaEvolution.added_columns || []).map((c: any, i: number) => (
                    <li key={i}>{c.column} <span className="text-muted-foreground">({c.data_type})</span></li>
                  ))}
                </ul>
              </div>
              <div>
                <div className="font-medium text-red-700 dark:text-red-400">
                  Removed ({(schemaEvolution.removed_columns || []).length})
                </div>
                <ul className="list-disc pl-4 max-h-28 overflow-auto">
                  {(schemaEvolution.removed_columns || []).map((c: any, i: number) => (
                    <li key={i}>{c.column}</li>
                  ))}
                </ul>
              </div>
              <div>
                <div className="font-medium text-amber-700 dark:text-amber-400">
                  Changed ({(schemaEvolution.changed_columns || []).length})
                </div>
                <ul className="list-disc pl-4 max-h-28 overflow-auto">
                  {(schemaEvolution.changed_columns || []).map((c: any, i: number) => (
                    <li key={i}>{c.column}: {c.dest_type} → {c.source_type}</li>
                  ))}
                </ul>
              </div>
            </div>
            {schemaEvolution.is_compatible && (
              <div className="text-xs text-green-700 dark:text-green-400 mt-2 flex items-center gap-1">
                <CheckCircle className="h-3 w-3" /> Changes are compatible — can be applied via ALTER TABLE.
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function RecentSyncsPanel({
  jobs, isLoading, onRefresh, isFetching, onSelectJob,
}: {
  jobs: any[];
  isLoading: boolean;
  onRefresh: () => void;
  isFetching: boolean;
  onSelectJob: (id: string) => void;
}) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <History className="h-4 w-4" /> Recent Syncs
          <Button
            type="button"
            size="sm"
            variant="ghost"
            className="ml-auto"
            onClick={onRefresh}
            disabled={isFetching}
          >
            {isFetching ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading && <div className="text-sm text-muted-foreground">Loading…</div>}
        {!isLoading && jobs.length === 0 && (
          <div className="text-sm text-muted-foreground italic">
            No sync jobs yet. Run one above to populate this list.
          </div>
        )}
        {jobs.length > 0 && (
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left border-b">
                  <th className="py-1.5 pr-3">Job ID</th>
                  <th className="py-1.5 pr-3">Source → Dest</th>
                  <th className="py-1.5 pr-3">Type</th>
                  <th className="py-1.5 pr-3">Status</th>
                  <th className="py-1.5 pr-3">Duration</th>
                  <th className="py-1.5 pr-3">Started</th>
                </tr>
              </thead>
              <tbody>
                {jobs.slice(0, 10).map((j) => {
                  const dur =
                    j.started_at && j.completed_at
                      ? Math.round(
                          (new Date(j.completed_at).getTime() -
                            new Date(j.started_at).getTime()) / 1000,
                        )
                      : null;
                  return (
                    <tr key={j.job_id} className="border-b hover:bg-muted/20">
                      <td className="py-1.5 pr-3">
                        <button
                          type="button"
                          className="text-[#E8453C] hover:underline"
                          onClick={() => onSelectJob(j.job_id)}
                        >
                          {j.job_id}
                        </button>
                      </td>
                      <td className="py-1.5 pr-3">
                        <code className="text-xs">
                          {j.source_catalog} → {j.destination_catalog}
                        </code>
                      </td>
                      <td className="py-1.5 pr-3">
                        <Badge variant="outline" className="text-xs">{j.job_type}</Badge>
                      </td>
                      <td className="py-1.5 pr-3">
                        <Badge
                          className={
                            j.status === "completed"
                              ? "bg-green-100 text-green-700"
                              : j.status === "failed"
                              ? "bg-red-100 text-red-700"
                              : j.status === "running"
                              ? "bg-[#E8453C]/10 text-[#E8453C]"
                              : "bg-muted/40 text-muted-foreground"
                          }
                        >
                          {j.status}
                        </Badge>
                      </td>
                      <td className="py-1.5 pr-3 text-xs">
                        {dur != null ? `${dur}s` : "—"}
                      </td>
                      <td className="py-1.5 pr-3 text-xs text-muted-foreground">
                        {j.started_at ? new Date(j.started_at).toLocaleString() : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ScheduleModal({
  mode, source, dest, schemaName, syncStrategy, dropExtra,
  onClose, onSubmit, isCreating,
}: {
  mode: SyncMode;
  source: string;
  dest: string;
  schemaName: string;
  syncStrategy: string;
  dropExtra: boolean;
  onClose: () => void;
  onSubmit: (name: string, cron: string) => void;
  isCreating: boolean;
}) {
  const [name, setName] = useState(
    mode === "incremental"
      ? `${source}-${schemaName}-to-${dest}-incremental`
      : `${source}-to-${dest}-sync`,
  );
  const [cron, setCron] = useState("0 3 * * *"); // Daily at 03:00 UTC
  const [preset, setPreset] = useState<string>("daily_3am");

  const presets: Record<string, { cron: string; label: string }> = {
    hourly: { cron: "0 * * * *", label: "Every hour" },
    every_4h: { cron: "0 */4 * * *", label: "Every 4 hours" },
    daily_3am: { cron: "0 3 * * *", label: "Daily at 03:00" },
    weekly_sun: { cron: "0 4 * * 0", label: "Weekly (Sun 04:00)" },
    custom: { cron, label: "Custom" },
  };

  const applyPreset = (key: string) => {
    setPreset(key);
    if (key !== "custom") setCron(presets[key].cron);
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={onClose}
    >
      <div
        className="bg-background rounded-lg shadow-xl max-w-lg w-full p-6 space-y-4 border"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2">
          <CalendarClock className="h-5 w-5" />
          <h2 className="text-lg font-semibold">Schedule Sync</h2>
        </div>
        <p className="text-sm text-muted-foreground">
          Create a cron-backed schedule. When a Databricks warehouse is attached, a matching Databricks Job is also created so the cron fires workspace-side even when Clone-Xs is offline.
        </p>

        <div className="space-y-3">
          <div>
            <FieldLabelSmall hint="Friendly name shown in the schedules list + Databricks Job name.">Name</FieldLabelSmall>
            <Input value={name} onChange={(e) => setName(e.target.value)} />
          </div>

          <div>
            <FieldLabelSmall hint="Pick a common cadence or write your own 5-field cron expression.">Cadence</FieldLabelSmall>
            <div className="flex flex-wrap gap-1 mt-1">
              {Object.entries(presets).map(([key, p]) => (
                <Button
                  key={key}
                  type="button"
                  size="sm"
                  variant={preset === key ? "default" : "outline"}
                  onClick={() => applyPreset(key)}
                >
                  {p.label}
                </Button>
              ))}
            </div>
            <Input
              className="mt-2 font-mono"
              value={cron}
              onChange={(e) => { setPreset("custom"); setCron(e.target.value); }}
              placeholder="0 3 * * *"
            />
            <p className="text-xs text-muted-foreground mt-1">
              Standard 5-field cron (min hr day month weekday). UTC.
            </p>
          </div>

          <div className="border rounded-md p-3 bg-muted/40 text-xs space-y-1">
            <div><span className="font-medium">Mode:</span> {mode === "incremental" ? "Incremental Sync" : "Two-way Sync"}</div>
            <div><span className="font-medium">Source → Dest:</span> {source} → {dest}</div>
            {mode === "incremental" && (
              <>
                <div><span className="font-medium">Schema:</span> {schemaName}</div>
                <div><span className="font-medium">Strategy:</span> {syncStrategy.toUpperCase()}</div>
              </>
            )}
            {mode === "two_way" && dropExtra && (
              <div className="text-amber-700 dark:text-amber-400">⚠ Drop Extra is on — scheduled runs will drop destination-only tables each fire.</div>
            )}
          </div>
        </div>

        <div className="flex justify-end gap-2 pt-2">
          <Button type="button" variant="outline" onClick={onClose} disabled={isCreating}>Cancel</Button>
          <Button
            type="button"
            onClick={() => onSubmit(name, cron)}
            disabled={isCreating || !name.trim() || !cron.trim()}
          >
            {isCreating ? (
              <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Creating…</>
            ) : (
              <><CalendarClock className="h-4 w-4 mr-1" /> Create schedule</>
            )}
          </Button>
        </div>
      </div>
    </div>
  );
}

function ScheduledSyncsPanel({
  schedules, isLoading, isFetching, onRefresh, onPause, onResume, onDelete,
}: {
  schedules: any[];
  isLoading: boolean;
  isFetching: boolean;
  onRefresh: () => void;
  onPause: (id: string) => void;
  onResume: (id: string) => void;
  onDelete: (id: string) => void;
}) {
  // Only show schedules whose job_type is sync-family — filter client-side
  // since the backend stores the type inside the config blob rather than on
  // the schedule row itself. Accept anything for now.
  const rows = schedules;
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <CalendarClock className="h-4 w-4" /> Scheduled Syncs
          <Button
            type="button"
            size="sm"
            variant="ghost"
            className="ml-auto"
            onClick={onRefresh}
            disabled={isFetching}
          >
            {isFetching ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading && <div className="text-sm text-muted-foreground">Loading…</div>}
        {!isLoading && rows.length === 0 && (
          <div className="text-sm text-muted-foreground italic">
            No schedules yet. Configure a sync above, then click <span className="font-medium">Schedule…</span>.
          </div>
        )}
        {rows.length > 0 && (
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left border-b">
                  <th className="py-1.5 pr-3">Name</th>
                  <th className="py-1.5 pr-3">Source → Dest</th>
                  <th className="py-1.5 pr-3">Cron</th>
                  <th className="py-1.5 pr-3">Status</th>
                  <th className="py-1.5 pr-3">Next run</th>
                  <th className="py-1.5 pr-3">Last run</th>
                  <th className="py-1.5 pr-3">Databricks job</th>
                  <th className="py-1.5"></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((s) => (
                  <tr key={s.id} className="border-b hover:bg-muted/20">
                    <td className="py-1.5 pr-3 font-medium">{s.name}</td>
                    <td className="py-1.5 pr-3 text-xs">
                      <code>{s.source_catalog} → {s.destination_catalog}</code>
                    </td>
                    <td className="py-1.5 pr-3">
                      <code className="text-xs">{s.cron}</code>
                    </td>
                    <td className="py-1.5 pr-3">
                      <Badge
                        className={
                          s.status === "active"
                            ? "bg-green-100 text-green-700"
                            : "bg-muted/40 text-muted-foreground"
                        }
                      >
                        {s.status}
                      </Badge>
                    </td>
                    <td className="py-1.5 pr-3 text-xs text-muted-foreground">{s.next_run || "—"}</td>
                    <td className="py-1.5 pr-3 text-xs text-muted-foreground">{s.last_run || "—"}</td>
                    <td className="py-1.5 pr-3 text-xs">
                      {s.job_url ? (
                        <a href={s.job_url} target="_blank" rel="noreferrer" className="text-[#E8453C] hover:underline">
                          {s.job_id}
                        </a>
                      ) : (
                        <span className="text-muted-foreground">local</span>
                      )}
                    </td>
                    <td className="py-1.5 text-right whitespace-nowrap">
                      {s.status === "active" ? (
                        <Button type="button" size="sm" variant="ghost" onClick={() => onPause(s.id)}>
                          <Pause className="h-3.5 w-3.5" />
                        </Button>
                      ) : (
                        <Button type="button" size="sm" variant="ghost" onClick={() => onResume(s.id)}>
                          <Play className="h-3.5 w-3.5" />
                        </Button>
                      )}
                      <Button type="button" size="sm" variant="ghost" onClick={() => onDelete(s.id)}>
                        <Trash2 className="h-3.5 w-3.5 text-red-600" />
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
  );
}
