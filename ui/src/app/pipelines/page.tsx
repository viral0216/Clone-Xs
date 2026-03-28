// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import {
  GitBranch, Play, Plus, Trash2, Loader2, CheckCircle, XCircle,
  RefreshCw, Copy, Shield, Eye, RotateCcw, Bell, Zap, Database,
  LayoutTemplate, Clock, ArrowRight, Pause,
} from "lucide-react";

const STEP_ICONS: Record<string, any> = {
  clone: Copy, mask: Shield, validate: CheckCircle, notify: Bell,
  vacuum: RotateCcw, custom_sql: Database,
};

function statusBadge(s: string) {
  const m: Record<string, string> = {
    created: "bg-blue-100 text-blue-700", running: "bg-amber-100 text-amber-700",
    completed: "bg-emerald-100 text-emerald-700", failed: "bg-red-100 text-red-700",
    cancelled: "bg-gray-100 text-gray-500", pending: "bg-gray-100 text-gray-600",
    skipped: "bg-gray-100 text-gray-400",
  };
  return <Badge variant="outline" className={`text-[12px] font-semibold border ${m[s] || ""}`}>{s}</Badge>;
}

const PIPE_COLS: Column[] = [
  { key: "pipeline_id", label: "ID", sortable: true, width: "15%", render: (v: string) => <code className="text-xs font-mono bg-muted px-1.5 py-0.5 rounded">{v?.slice(0, 8)}</code> },
  { key: "name", label: "Name", sortable: true, width: "25%" },
  { key: "description", label: "Description", sortable: false, width: "30%", render: (v: string) => <span className="text-muted-foreground text-sm">{v}</span> },
  { key: "created_at", label: "Created", sortable: true, width: "15%", render: (v: string) => v ? new Date(v).toLocaleDateString() : "-" },
  { key: "created_by", label: "By", sortable: true, width: "15%" },
];

const RUN_COLS: Column[] = [
  { key: "run_id", label: "Run ID", sortable: true, width: "15%", render: (v: string) => <code className="text-xs font-mono bg-muted px-1.5 py-0.5 rounded">{v?.slice(0, 8)}</code> },
  { key: "pipeline_name", label: "Pipeline", sortable: true, width: "20%" },
  { key: "status", label: "Status", sortable: true, width: "12%", render: (v: string) => statusBadge(v) },
  { key: "total_steps", label: "Steps", sortable: true, width: "8%", align: "right" },
  { key: "completed_steps", label: "Done", sortable: true, width: "8%", align: "right" },
  { key: "started_at", label: "Started", sortable: true, width: "15%", render: (v: string) => v ? new Date(v).toLocaleString() : "-" },
  { key: "triggered_by", label: "Triggered By", sortable: true, width: "12%" },
];

export default function PipelinesPage() {
  const [pipelines, setPipelines] = useState<any[]>([]);
  const [runs, setRuns] = useState<any[]>([]);
  const [templates, setTemplates] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("pipelines");

  // Create form
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [newSteps, setNewSteps] = useState<any[]>([{ type: "clone", name: "Clone catalog", config: {}, on_failure: "abort" }]);
  const [creating, setCreating] = useState(false);

  useEffect(() => { loadAll(); }, []);
  async function loadAll() {
    setLoading(true);
    try {
      const [p, r, t] = await Promise.all([
        api.get("/pipelines/pipelines"), api.get("/pipelines/runs"), api.get("/pipelines/templates"),
      ]);
      setPipelines(p || []); setRuns(r || []); setTemplates(t || []);
    } catch {}
    setLoading(false);
  }

  async function handleCreate() {
    setCreating(true);
    try {
      await api.post("/pipelines/pipelines", { name: newName, description: newDesc, steps: newSteps });
      setNewName(""); setNewDesc(""); setNewSteps([{ type: "clone", name: "Clone catalog", config: {}, on_failure: "abort" }]);
      loadAll();
    } catch {}
    setCreating(false);
  }

  async function handleRun(id: string) {
    try { await api.post(`/pipelines/pipelines/${id}/run`, {}); loadAll(); } catch {}
  }

  async function handleUseTemplate(name: string) {
    try { await api.post(`/pipelines/templates/${name}/create`, { template_name: name }); loadAll(); } catch {}
  }

  async function handleDelete(id: string) {
    try { await api.delete(`/pipelines/pipelines/${id}`); loadAll(); } catch {}
  }

  function addStep() {
    setNewSteps([...newSteps, { type: "validate", name: `Step ${newSteps.length + 1}`, config: {}, on_failure: "abort" }]);
  }
  function removeStep(i: number) {
    setNewSteps(newSteps.filter((_, idx) => idx !== i));
  }
  function updateStep(i: number, field: string, value: any) {
    const updated = [...newSteps];
    updated[i] = { ...updated[i], [field]: value };
    setNewSteps(updated);
  }

  return (
    <div className="space-y-6">
      <PageHeader title="Clone Pipelines" description="Chain multiple operations into reusable workflows — clone, mask, validate, notify, and more" breadcrumbs={["Operations", "Pipelines"]} />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-4 max-w-lg">
          <TabsTrigger value="pipelines" className="gap-1.5"><GitBranch className="h-3.5 w-3.5" />Pipelines</TabsTrigger>
          <TabsTrigger value="create" className="gap-1.5"><Plus className="h-3.5 w-3.5" />Create</TabsTrigger>
          <TabsTrigger value="runs" className="gap-1.5"><Play className="h-3.5 w-3.5" />Runs</TabsTrigger>
          <TabsTrigger value="templates" className="gap-1.5"><LayoutTemplate className="h-3.5 w-3.5" />Templates</TabsTrigger>
        </TabsList>

        <TabsContent value="pipelines" className="mt-5">
          <Card>
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <div><CardTitle className="text-base">My Pipelines</CardTitle><CardDescription>{pipelines.length} pipeline{pipelines.length !== 1 ? "s" : ""}</CardDescription></div>
              <Button variant="outline" size="sm" onClick={loadAll} disabled={loading}>{loading ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}Refresh</Button>
            </CardHeader>
            <CardContent>
              {pipelines.length === 0 && !loading ? (
                <div className="py-12 text-center">
                  <GitBranch className="h-10 w-10 mx-auto text-muted-foreground/30 mb-3" />
                  <h3 className="font-semibold mb-1">No Pipelines Yet</h3>
                  <p className="text-sm text-muted-foreground mb-3">Create a pipeline or use a template to get started.</p>
                  <div className="flex gap-2 justify-center">
                    <Button onClick={() => setActiveTab("create")}>Create Pipeline</Button>
                    <Button variant="outline" onClick={() => setActiveTab("templates")}>Use Template</Button>
                  </div>
                </div>
              ) : (
                <div className="space-y-3">
                  {pipelines.map((p: any) => (
                    <div key={p.pipeline_id} className="flex items-center justify-between p-3 rounded-lg border border-border">
                      <div>
                        <div className="font-semibold text-sm">{p.name}</div>
                        <div className="text-xs text-muted-foreground">{p.description} {p.template_name && <Badge variant="outline" className="text-[10px] ml-1">from: {p.template_name}</Badge>}</div>
                      </div>
                      <div className="flex gap-2">
                        <Button size="sm" onClick={() => handleRun(p.pipeline_id)}><Play className="h-3.5 w-3.5 mr-1" /> Run</Button>
                        <Button size="sm" variant="ghost" className="text-red-600" onClick={() => handleDelete(p.pipeline_id)}><Trash2 className="h-3.5 w-3.5" /></Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="create" className="space-y-5 mt-5">
          <Card>
            <CardHeader className="pb-3"><CardTitle className="text-base flex items-center gap-2"><Plus className="h-4 w-4 text-muted-foreground" /> Create Pipeline</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Pipeline Name</Label>
                  <Input className="h-11" placeholder="e.g., Production to Dev Refresh" value={newName} onChange={e => setNewName(e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Description</Label>
                  <Input className="h-11" placeholder="What does this pipeline do?" value={newDesc} onChange={e => setNewDesc(e.target.value)} />
                </div>
              </div>

              <div className="space-y-2">
                <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Steps</Label>
                <div className="space-y-2">
                  {newSteps.map((step, i) => {
                    const Icon = STEP_ICONS[step.type] || Zap;
                    return (
                      <div key={i} className="flex items-center gap-3 p-3 rounded-lg border border-border">
                        <div className="shrink-0 w-7 h-7 rounded-full bg-[#E8453C] text-white flex items-center justify-center text-xs font-bold">{i + 1}</div>
                        <Icon className="h-4 w-4 text-muted-foreground shrink-0" />
                        <Select value={step.type} onValueChange={v => updateStep(i, "type", v)}>
                          <SelectTrigger className="w-32 h-9"><SelectValue /></SelectTrigger>
                          <SelectContent>
                            {["clone","mask","validate","notify","vacuum","custom_sql"].map(t => <SelectItem key={t} value={t}>{t}</SelectItem>)}
                          </SelectContent>
                        </Select>
                        <Input className="h-9 flex-1" placeholder="Step name" value={step.name} onChange={e => updateStep(i, "name", e.target.value)} />
                        <Select value={step.on_failure} onValueChange={v => updateStep(i, "on_failure", v)}>
                          <SelectTrigger className="w-24 h-9"><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="abort">Abort</SelectItem>
                            <SelectItem value="skip">Skip</SelectItem>
                            <SelectItem value="retry">Retry</SelectItem>
                          </SelectContent>
                        </Select>
                        <Button size="sm" variant="ghost" className="text-red-600" onClick={() => removeStep(i)} disabled={newSteps.length <= 1}><XCircle className="h-4 w-4" /></Button>
                      </div>
                    );
                  })}
                </div>
                <Button variant="outline" size="sm" onClick={addStep}><Plus className="h-3.5 w-3.5 mr-1" /> Add Step</Button>
              </div>

              <Button size="lg" onClick={handleCreate} disabled={creating || !newName || newSteps.length === 0}>
                {creating ? <Loader2 className="animate-spin h-4 w-4 mr-2" /> : <GitBranch className="h-4 w-4 mr-2" />} Create Pipeline
              </Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="runs" className="mt-5">
          <Card>
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <div><CardTitle className="text-base">Run History</CardTitle><CardDescription>{runs.length} run{runs.length !== 1 ? "s" : ""}</CardDescription></div>
              <Button variant="outline" size="sm" onClick={loadAll} disabled={loading}><RefreshCw className="h-3.5 w-3.5 mr-1.5" />Refresh</Button>
            </CardHeader>
            <CardContent>
              <DataTable data={runs} columns={RUN_COLS} searchable pageSize={25} emptyMessage="No pipeline runs yet." />
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="templates" className="space-y-4 mt-5">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {templates.map((t: any) => (
              <Card key={t.name} className="hover:border-[#E8453C]/30 transition-colors">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base flex items-center gap-2"><LayoutTemplate className="h-4 w-4 text-[#E8453C]" /> {t.name}</CardTitle>
                  <CardDescription>{t.description}</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex items-center gap-1 mb-3 flex-wrap">
                    {t.step_types?.map((s: string, i: number) => {
                      const Icon = STEP_ICONS[s] || Zap;
                      return (
                        <span key={i} className="flex items-center">
                          {i > 0 && <ArrowRight className="h-3 w-3 mx-1 text-muted-foreground/40" />}
                          <Badge variant="outline" className="text-[11px] gap-1"><Icon className="h-3 w-3" />{s}</Badge>
                        </span>
                      );
                    })}
                  </div>
                  <Button size="sm" onClick={() => handleUseTemplate(t.name)}>Use Template</Button>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
