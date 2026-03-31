// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import {
  Zap, Play, Square, Copy, Loader2, RefreshCw, CheckCircle, XCircle,
  AlertTriangle, Activity, GitFork, Clock, Eye, Heart, Database, Globe, Lock,
} from "lucide-react";

function stateBadge(state: string) {
  if (!state) return <Badge variant="outline" className="text-[12px]">Unknown</Badge>;
  const s = state.toUpperCase();
  const m: Record<string, string> = {
    RUNNING: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
    IDLE: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
    FAILED: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
    STOPPING: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300",
  };
  return <Badge variant="outline" className={`text-[12px] font-semibold border ${m[s] || ""}`}>{s}</Badge>;
}

function healthBadge(health: string) {
  if (!health) return null;
  const h = health.toUpperCase();
  if (h.includes("HEALTHY") && !h.includes("UN")) return <Badge variant="outline" className="text-[11px] border-emerald-300 text-emerald-600">Healthy</Badge>;
  if (h.includes("UNHEALTHY")) return <Badge variant="outline" className="text-[11px] border-red-300 text-red-600">Unhealthy</Badge>;
  return <Badge variant="outline" className="text-[11px]">{health}</Badge>;
}

function levelBadge(level: string) {
  if (!level) return null;
  const l = level.toUpperCase();
  const m: Record<string, string> = {
    ERROR: "bg-red-100 text-red-700", WARN: "bg-amber-100 text-amber-700",
    INFO: "bg-blue-100 text-blue-700", DEBUG: "bg-gray-100 text-gray-500",
  };
  return <Badge variant="outline" className={`text-[11px] ${m[l] || ""}`}>{l}</Badge>;
}

const PIPE_COLS: Column[] = [
  { key: "name", label: "Pipeline Name", sortable: true, width: "25%", render: (v: string) => <span className="font-medium">{v}</span> },
  { key: "state", label: "State", sortable: true, width: "12%", render: (v: string) => stateBadge(v) },
  { key: "health", label: "Health", sortable: true, width: "10%", render: (v: string) => healthBadge(v) },
  { key: "creator", label: "Creator", sortable: true, width: "18%" },
  { key: "pipeline_id", label: "Pipeline ID", sortable: true, width: "20%", render: (v: string) => <code className="text-xs font-mono bg-muted px-1.5 py-0.5 rounded">{v?.slice(0, 12)}...</code> },
];

const EVENT_COLS: Column[] = [
  { key: "timestamp", label: "Time", sortable: true, width: "18%", render: (v: string) => v ? <span className="text-xs text-muted-foreground">{new Date(v).toLocaleString()}</span> : "-" },
  { key: "level", label: "Level", sortable: true, width: "8%", render: (v: string) => levelBadge(v) },
  { key: "event_type", label: "Type", sortable: true, width: "15%" },
  { key: "pipeline_name", label: "Pipeline", sortable: true, width: "15%" },
  { key: "message", label: "Message", sortable: false, width: "44%", render: (v: string) => <span className="text-xs text-muted-foreground">{v?.slice(0, 120)}{v?.length > 120 ? "..." : ""}</span> },
];

const DATASET_COLS: Column[] = [
  { key: "fqn", label: "Table FQN", sortable: true, width: "40%", render: (v: string) => <code className="text-xs font-mono">{v}</code> },
  { key: "type", label: "Type", sortable: true, width: "15%" },
  { key: "format", label: "Format", sortable: true, width: "15%" },
  { key: "comment", label: "Comment", sortable: false, width: "30%", render: (v: string) => <span className="text-xs text-muted-foreground">{v}</span> },
];

export default function DltPage() {
  const [dashboard, setDashboard] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [selectedPipeline, setSelectedPipeline] = useState<any>(null);
  const [events, setEvents] = useState<any[]>([]);
  const [lineage, setLineage] = useState<any>(null);
  const [updates, setUpdates] = useState<any[]>([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [activeTab, setActiveTab] = useState("dashboard");

  // Clone modal state
  const [cloneModal, setCloneModal] = useState<{ open: boolean; pipeline: any } | null>(null);
  const [cloneName, setCloneName] = useState("");
  const [cloneMode, setCloneMode] = useState<"same" | "cross">("same");
  const [destHost, setDestHost] = useState("");
  const [destToken, setDestToken] = useState("");
  const [cloneDryRun, setCloneDryRun] = useState(false);
  const [cloning, setCloning] = useState(false);
  const [cloneResult, setCloneResult] = useState<any>(null);
  const [cloneError, setCloneError] = useState("");

  useEffect(() => { loadDashboard(); }, []);

  async function loadDashboard() {
    setLoading(true);
    try { setDashboard(await api.get("/dlt/dashboard")); } catch {}
    setLoading(false);
  }

  async function selectPipeline(row: any) {
    setSelectedPipeline(row);
    setActiveTab("detail");
    setLoadingDetail(true);
    try {
      const [ev, lin, upd] = await Promise.all([
        api.get(`/dlt/pipelines/${row.pipeline_id}/events?max_events=50`),
        api.get(`/dlt/pipelines/${row.pipeline_id}/lineage`),
        api.get(`/dlt/pipelines/${row.pipeline_id}/updates`),
      ]);
      setEvents(ev?.events || []);
      setLineage(lin);
      setUpdates(upd || []);
    } catch {}
    setLoadingDetail(false);
  }

  async function handleTrigger(id: string, fullRefresh: boolean = false) {
    try { await api.post(`/dlt/pipelines/${id}/trigger`, { full_refresh: fullRefresh }); loadDashboard(); } catch {}
  }

  async function handleStop(id: string) {
    try { await api.post(`/dlt/pipelines/${id}/stop`, {}); loadDashboard(); } catch {}
  }

  function openCloneModal(pipeline: any) {
    setCloneModal({ open: true, pipeline });
    setCloneName(`Copy of ${pipeline.name || "Pipeline"}`);
    setCloneMode("same");
    setDestHost(""); setDestToken(""); setCloneDryRun(false); setCloneResult(null); setCloneError("");
  }

  async function handleClone() {
    if (!cloneModal || !cloneName) return;
    setCloning(true); setCloneResult(null); setCloneError("");
    try {
      let result;
      if (cloneMode === "cross") {
        result = await api.post(`/dlt/pipelines/${cloneModal.pipeline.pipeline_id}/clone-to-workspace`, {
          new_name: cloneName, dest_host: destHost, dest_token: destToken, dry_run: cloneDryRun,
        });
      } else {
        result = await api.post(`/dlt/pipelines/${cloneModal.pipeline.pipeline_id}/clone`, {
          new_name: cloneName, dry_run: cloneDryRun,
        });
      }
      setCloneResult(result);
      if (!cloneDryRun) { loadDashboard(); }
    } catch (e: any) {
      const detail = e?.response?.data?.detail || e?.message || "Clone failed. Check workspace URL and token.";
      setCloneError(typeof detail === "string" ? detail : JSON.stringify(detail));
    }
    setCloning(false);
  }

  const summary = dashboard?.summary || {};
  const pipelines = dashboard?.pipelines || [];
  const recentEvents = dashboard?.recent_events || [];

  return (
    <div className="space-y-6">
      <PageHeader title="Delta Live Tables" description="Discover, clone, monitor, and manage DLT pipelines across your Databricks workspace" breadcrumbs={["Operations", "DLT"]} />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-3 max-w-md">
          <TabsTrigger value="dashboard" className="gap-1.5"><Zap className="h-3.5 w-3.5" />Dashboard</TabsTrigger>
          <TabsTrigger value="pipelines" className="gap-1.5"><Activity className="h-3.5 w-3.5" />Pipelines</TabsTrigger>
          <TabsTrigger value="detail" className="gap-1.5"><Eye className="h-3.5 w-3.5" />Detail</TabsTrigger>
        </TabsList>

        {/* Dashboard */}
        <TabsContent value="dashboard" className="space-y-5 mt-5">
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            {[{ l: "Total", v: summary.total || 0, c: "", i: Database },
              { l: "Running", v: summary.running || 0, c: "text-emerald-600", i: Play },
              { l: "Failed", v: summary.failed || 0, c: "text-red-600", i: XCircle },
              { l: "Idle", v: summary.idle || 0, c: "text-blue-600", i: Clock },
              { l: "Healthy", v: summary.healthy || 0, c: "text-emerald-600", i: Heart },
              { l: "Unhealthy", v: summary.unhealthy || 0, c: "text-red-600", i: AlertTriangle },
            ].map(s => (
              <Card key={s.l}>
                <CardContent className="pt-4">
                  <div className="flex items-center justify-between">
                    <div><p className="text-xs text-muted-foreground uppercase tracking-wider">{s.l}</p><p className={`text-2xl font-bold mt-1 ${s.c}`}>{s.v}</p></div>
                    <s.i className="h-5 w-5 text-muted-foreground" />
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {recentEvents.length > 0 && (
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-base">Recent Events</CardTitle></CardHeader>
              <CardContent>
                <DataTable data={recentEvents} columns={EVENT_COLS} pageSize={10} emptyMessage="No recent events" />
              </CardContent>
            </Card>
          )}

          <div className="flex justify-end">
            <Button variant="outline" size="sm" onClick={loadDashboard} disabled={loading}>
              {loading ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />} Refresh
            </Button>
          </div>
        </TabsContent>

        {/* Pipelines list */}
        <TabsContent value="pipelines" className="mt-5">
          <Card>
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <div><CardTitle className="text-base">All DLT Pipelines</CardTitle><CardDescription>{pipelines.length} pipeline{pipelines.length !== 1 ? "s" : ""}</CardDescription></div>
              <Button variant="outline" size="sm" onClick={loadDashboard} disabled={loading}><RefreshCw className="h-3.5 w-3.5 mr-1.5" />Refresh</Button>
            </CardHeader>
            <CardContent>
              {pipelines.length === 0 ? (
                <div className="py-12 text-center text-muted-foreground">
                  <Zap className="h-10 w-10 mx-auto text-muted-foreground/30 mb-3" />
                  <p className="font-semibold">No DLT pipelines found in this workspace.</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {pipelines.map((p: any) => (
                    <div key={p.pipeline_id} className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-muted/30 transition-colors cursor-pointer" onClick={() => selectPipeline(p)}>
                      <div className="flex items-center gap-3 min-w-0">
                        <Zap className="h-4 w-4 text-[#E8453C] shrink-0" />
                        <div className="min-w-0">
                          <div className="font-medium text-sm truncate">{p.name}</div>
                          <div className="text-xs text-muted-foreground font-mono">{p.pipeline_id?.slice(0, 16)}...</div>
                        </div>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        {stateBadge(p.state)}
                        {healthBadge(p.health)}
                        <Button size="sm" variant="outline" className="h-7 px-2" onClick={(e) => { e.stopPropagation(); openCloneModal(p); }} title="Clone pipeline">
                          <Copy className="h-3.5 w-3.5 mr-1" /> Clone
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Pipeline Detail */}
        <TabsContent value="detail" className="space-y-4 mt-5">
          {!selectedPipeline ? (
            <Card className="border-dashed"><CardContent className="py-12 text-center text-muted-foreground">
              <Zap className="h-10 w-10 mx-auto text-muted-foreground/30 mb-3" />
              <h3 className="font-semibold mb-1">No Pipeline Selected</h3>
              <p className="text-sm">Click a pipeline in the Pipelines tab to view details.</p>
            </CardContent></Card>
          ) : (
            <>
              {/* Header */}
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base flex items-center gap-2">
                      <Zap className="h-4 w-4 text-[#E8453C]" /> {selectedPipeline.name}
                    </CardTitle>
                    <div className="flex items-center gap-2">{stateBadge(selectedPipeline.state)} {healthBadge(selectedPipeline.health)}</div>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                    <div><span className="text-xs text-muted-foreground uppercase block">Pipeline ID</span><code className="text-xs font-mono">{selectedPipeline.pipeline_id}</code></div>
                    <div><span className="text-xs text-muted-foreground uppercase block">Creator</span>{selectedPipeline.creator || "-"}</div>
                    <div><span className="text-xs text-muted-foreground uppercase block">State</span>{selectedPipeline.state || "-"}</div>
                    <div><span className="text-xs text-muted-foreground uppercase block">Health</span>{selectedPipeline.health || "-"}</div>
                  </div>
                </CardContent>
              </Card>

              {/* Actions */}
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Actions</CardTitle></CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex flex-wrap gap-2">
                    <Button size="sm" onClick={() => handleTrigger(selectedPipeline.pipeline_id)}><Play className="h-4 w-4 mr-1.5" /> Run</Button>
                    <Button size="sm" variant="outline" onClick={() => handleTrigger(selectedPipeline.pipeline_id, true)}><RefreshCw className="h-4 w-4 mr-1.5" /> Full Refresh</Button>
                    <Button size="sm" variant="outline" className="text-red-600" onClick={() => handleStop(selectedPipeline.pipeline_id)}><Square className="h-4 w-4 mr-1.5" /> Stop</Button>
                  </div>
                  <div>
                    <Button size="sm" variant="outline" onClick={() => openCloneModal(selectedPipeline)}>
                      <Copy className="h-4 w-4 mr-1.5" /> Clone Pipeline
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Lineage */}
              {lineage?.datasets?.length > 0 && (
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-base flex items-center gap-2"><GitFork className="h-4 w-4 text-muted-foreground" /> DLT Datasets ({lineage.total_datasets})</CardTitle>
                    <CardDescription>Unity Catalog tables managed by this pipeline in {lineage.catalog}.{lineage.target_schema}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <DataTable data={lineage.datasets} columns={DATASET_COLS} pageSize={15} emptyMessage="No datasets found" />
                  </CardContent>
                </Card>
              )}

              {/* Events */}
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Event Log</CardTitle><CardDescription>{events.length} events</CardDescription></CardHeader>
                <CardContent>
                  <DataTable data={events} columns={EVENT_COLS} pageSize={25} emptyMessage={loadingDetail ? "Loading..." : "No events"} />
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>

      {/* ── Clone Modal ─────────────────────────────── */}
      {cloneModal?.open && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onClick={() => setCloneModal(null)}>
          <div className="bg-background border rounded-xl shadow-2xl max-w-lg w-full mx-4 p-6 space-y-5" onClick={(e) => e.stopPropagation()}>
            <div>
              <h3 className="text-lg font-semibold flex items-center gap-2"><Copy className="h-5 w-5 text-[#E8453C]" /> Clone DLT Pipeline</h3>
              <p className="text-sm text-muted-foreground mt-1">Clone <strong>{cloneModal.pipeline.name}</strong> to a new pipeline</p>
            </div>

            {/* Mode toggle */}
            <div className="grid grid-cols-2 gap-2">
              <button onClick={() => setCloneMode("same")}
                className={`text-left px-3 py-2.5 rounded-lg border-2 text-sm transition-all ${cloneMode === "same" ? "border-[#E8453C] bg-[#E8453C]/5 dark:bg-[#E8453C]/10" : "border-border hover:border-muted-foreground/30"}`}>
                <div className={`font-semibold flex items-center gap-1.5 ${cloneMode === "same" ? "text-[#E8453C]" : ""}`}><Copy className="h-3.5 w-3.5" /> Same Workspace</div>
                <div className="text-xs text-muted-foreground mt-0.5">Clone within this workspace</div>
              </button>
              <button onClick={() => setCloneMode("cross")}
                className={`text-left px-3 py-2.5 rounded-lg border-2 text-sm transition-all ${cloneMode === "cross" ? "border-[#E8453C] bg-[#E8453C]/5 dark:bg-[#E8453C]/10" : "border-border hover:border-muted-foreground/30"}`}>
                <div className={`font-semibold flex items-center gap-1.5 ${cloneMode === "cross" ? "text-[#E8453C]" : ""}`}><Globe className="h-3.5 w-3.5" /> Different Workspace</div>
                <div className="text-xs text-muted-foreground mt-0.5">Clone to another Databricks workspace</div>
              </button>
            </div>

            {/* New name */}
            <div className="space-y-2">
              <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">New Pipeline Name</Label>
              <Input className="h-11" value={cloneName} onChange={(e) => setCloneName(e.target.value)} placeholder="e.g., My Pipeline - DR Copy" />
            </div>

            {/* Cross-workspace fields */}
            {cloneMode === "cross" && (
              <div className="space-y-3 p-3 rounded-lg border border-amber-200 dark:border-amber-900 bg-amber-50/30 dark:bg-amber-950/10">
                <div className="text-xs font-semibold text-amber-700 dark:text-amber-400 flex items-center gap-1.5">
                  <Globe className="h-3.5 w-3.5" /> Destination Workspace
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Workspace URL</Label>
                  <div className="relative">
                    <Globe className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-10" placeholder="https://adb-xxx.azuredatabricks.net" value={destHost} onChange={(e) => setDestHost(e.target.value)} />
                  </div>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Personal Access Token</Label>
                  <div className="relative">
                    <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-10" type="password" placeholder="dapi..." value={destToken} onChange={(e) => setDestToken(e.target.value)} />
                  </div>
                </div>
              </div>
            )}

            {/* Dry run toggle */}
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={cloneDryRun} onChange={(e) => setCloneDryRun(e.target.checked)} className="rounded" />
              <span className="text-muted-foreground">Dry run — preview without creating</span>
            </label>

            {/* Result */}
            {cloneResult && (
              <div className={`p-3 rounded-lg border text-sm ${cloneResult.dry_run ? "border-amber-200 bg-amber-50/30 dark:border-amber-900 dark:bg-amber-950/10" : "border-emerald-200 bg-emerald-50/30 dark:border-emerald-900 dark:bg-emerald-950/10"}`}>
                {cloneResult.dry_run ? (
                  <div>
                    <div className="font-semibold text-amber-700 dark:text-amber-400 mb-1">Dry Run Preview</div>
                    <div className="text-xs space-y-0.5 text-muted-foreground">
                      <div>Catalog: <strong>{cloneResult.catalog || "—"}</strong></div>
                      <div>Target: <strong>{cloneResult.target || "—"}</strong></div>
                      <div>Libraries: {cloneResult.libraries_count || 0} | Clusters: {cloneResult.clusters_count || 0}</div>
                      {cloneResult.dest_workspace && <div>Dest: <strong>{cloneResult.dest_workspace}</strong></div>}
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 text-emerald-700 dark:text-emerald-400">
                    <CheckCircle className="h-4 w-4" />
                    <span>Pipeline cloned! {cloneResult.dest_pipeline_id ? `ID: ${cloneResult.dest_pipeline_id.slice(0, 12)}...` : cloneResult.new_pipeline_id ? `ID: ${cloneResult.new_pipeline_id.slice(0, 12)}...` : ""}</span>
                  </div>
                )}
              </div>
            )}

            {/* Error */}
            {cloneError && (
              <div className="p-3 rounded-lg border border-red-200 bg-red-50/50 dark:border-red-900 dark:bg-red-950/20 text-sm">
                <div className="font-semibold text-red-700 dark:text-red-400 flex items-center gap-1.5 mb-1">
                  <XCircle className="h-4 w-4" /> Clone Failed
                </div>
                <div className="text-red-600 dark:text-red-400 text-xs break-all">{cloneError}</div>
              </div>
            )}

            {/* Buttons */}
            <div className="flex justify-end gap-2 pt-1">
              <Button variant="outline" onClick={() => setCloneModal(null)}>Cancel</Button>
              <Button onClick={handleClone} disabled={cloning || !cloneName || (cloneMode === "cross" && (!destHost || !destToken))}>
                {cloning ? <Loader2 className="animate-spin h-4 w-4 mr-2" /> : <Copy className="h-4 w-4 mr-2" />}
                {cloneDryRun ? "Preview" : "Clone Pipeline"}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
