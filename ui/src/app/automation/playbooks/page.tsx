// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import {
  Zap, Plus, Trash2, Play, Loader2, ChevronDown, ChevronUp,
  Clock, Copy, ToggleLeft, ToggleRight,
} from "lucide-react";

const TRIGGER_TYPES = [
  { value: "dq_failure", label: "DQ Failure" },
  { value: "anomaly", label: "Anomaly Detected" },
  { value: "sla_breach", label: "SLA Breach" },
  { value: "freshness_stale", label: "Freshness Stale" },
  { value: "schema_drift", label: "Schema Drift" },
];

function triggerColor(t: string) {
  const map: Record<string, string> = {
    dq_failure: "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400",
    anomaly: "text-purple-600 bg-purple-50 border-purple-200 dark:bg-purple-950/30 dark:text-purple-400",
    sla_breach: "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400",
    freshness_stale: "text-orange-600 bg-orange-50 border-orange-200 dark:bg-orange-950/30 dark:text-orange-400",
    schema_drift: "text-sky-600 bg-sky-50 border-sky-200 dark:bg-sky-950/30 dark:text-sky-400",
  };
  return map[t] || "text-gray-600 bg-gray-50 border-gray-200";
}

export default function PlaybooksPage() {
  const [playbooks, setPlaybooks] = useState<any[]>([]);
  const [templates, setTemplates] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [executing, setExecuting] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [history, setHistory] = useState<Record<string, any[]>>({});
  const [historyLoading, setHistoryLoading] = useState<string | null>(null);

  const [form, setForm] = useState({
    name: "",
    description: "",
    trigger_type: "dq_failure",
    actions: "[]",
    max_executions_per_hour: 10,
    enabled: true,
  });

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const [pb, tpl] = await Promise.all([
        api.get("/playbooks/"),
        api.get("/playbooks/templates"),
      ]);
      setPlaybooks(Array.isArray(pb) ? pb : []);
      setTemplates(Array.isArray(tpl) ? tpl : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to load playbooks");
    }
    setLoading(false);
  }

  async function createPlaybook() {
    if (!form.name.trim()) { toast.error("Name is required"); return; }
    try {
      JSON.parse(form.actions);
    } catch {
      toast.error("Actions must be valid JSON"); return;
    }
    try {
      await api.post("/playbooks/", {
        ...form,
        actions: JSON.parse(form.actions),
        max_executions_per_hour: Number(form.max_executions_per_hour),
      });
      toast.success("Playbook created");
      setShowForm(false);
      setForm({ name: "", description: "", trigger_type: "dq_failure", actions: "[]", max_executions_per_hour: 10, enabled: true });
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to create playbook");
    }
  }

  async function deletePlaybook(id: string) {
    if (!confirm("Delete this playbook?")) return;
    try {
      await api.delete(`/playbooks/${id}`);
      toast.success("Playbook deleted");
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to delete");
    }
  }

  async function executePlaybook(id: string) {
    setExecuting(id);
    try {
      await api.post(`/playbooks/${id}/execute`);
      toast.success("Playbook executed");
      load();
    } catch (e: any) {
      toast.error(e.message || "Execution failed");
    }
    setExecuting(null);
  }

  async function toggleExpand(id: string) {
    if (expandedId === id) { setExpandedId(null); return; }
    setExpandedId(id);
    if (!history[id]) {
      setHistoryLoading(id);
      try {
        const h = await api.get(`/playbooks/${id}/history`);
        setHistory((prev) => ({ ...prev, [id]: Array.isArray(h) ? h : [] }));
      } catch { setHistory((prev) => ({ ...prev, [id]: [] })); }
      setHistoryLoading(null);
    }
  }

  async function createFromTemplate(tpl: any) {
    try {
      await api.post("/playbooks/", {
        name: tpl.name,
        description: tpl.description || "",
        trigger_type: tpl.trigger_type,
        actions: tpl.actions || [],
        max_executions_per_hour: tpl.max_executions_per_hour || 10,
        enabled: true,
      });
      toast.success(`Playbook created from template "${tpl.name}"`);
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to create from template");
    }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <PageHeader
        title="Remediation Playbooks"
        description="Automated if-this-then-that remediation workflows"
        icon={Zap}
        actions={
          <Button size="sm" onClick={() => setShowForm(!showForm)}>
            <Plus className="h-4 w-4 mr-1" /> New Playbook
          </Button>
        }
      />

      {/* Create Form */}
      {showForm && (
        <Card>
          <CardHeader><CardTitle className="text-sm">New Playbook</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <input
                className="border rounded px-3 py-2 text-sm bg-background"
                placeholder="Playbook name"
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
              />
              <select
                className="border rounded px-3 py-2 text-sm bg-background"
                value={form.trigger_type}
                onChange={(e) => setForm({ ...form, trigger_type: e.target.value })}
              >
                {TRIGGER_TYPES.map((t) => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
            </div>
            <textarea
              className="border rounded px-3 py-2 text-sm w-full bg-background"
              placeholder="Description"
              rows={2}
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
            />
            <textarea
              className="border rounded px-3 py-2 text-sm w-full font-mono bg-background"
              placeholder='Actions JSON, e.g. [{"type": "notify", "channel": "#alerts"}]'
              rows={3}
              value={form.actions}
              onChange={(e) => setForm({ ...form, actions: e.target.value })}
            />
            <div className="flex items-center gap-3">
              <input
                type="number"
                className="border rounded px-3 py-2 text-sm w-48 bg-background"
                placeholder="Max executions/hour"
                value={form.max_executions_per_hour}
                onChange={(e) => setForm({ ...form, max_executions_per_hour: Number(e.target.value) })}
              />
              <span className="text-xs text-muted-foreground">Max executions per hour</span>
            </div>
            <div className="flex gap-2">
              <Button size="sm" onClick={createPlaybook}>Create Playbook</Button>
              <Button size="sm" variant="outline" onClick={() => setShowForm(false)}>Cancel</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Playbooks List */}
      {!loading && (
        <div className="space-y-3">
          {playbooks.length === 0 && (
            <p className="text-sm text-muted-foreground text-center py-8">No playbooks configured yet. Create one above or use a template below.</p>
          )}
          {playbooks.map((pb) => (
            <Card key={pb.id}>
              <CardContent className="py-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3 min-w-0">
                    <Zap className="h-4 w-4 text-amber-500 shrink-0" />
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm truncate">{pb.name}</span>
                        <Badge variant="outline" className={`text-[11px] ${triggerColor(pb.trigger_type)}`}>
                          {pb.trigger_type}
                        </Badge>
                        {pb.enabled ? (
                          <Badge variant="outline" className="text-[11px] text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400">Enabled</Badge>
                        ) : (
                          <Badge variant="outline" className="text-[11px] text-gray-500 bg-gray-50 border-gray-200">Disabled</Badge>
                        )}
                      </div>
                      <div className="text-xs text-muted-foreground mt-0.5 flex items-center gap-3">
                        <span>{Array.isArray(pb.actions) ? pb.actions.length : 0} action(s)</span>
                        {pb.last_executed && (
                          <span className="flex items-center gap-1">
                            <Clock className="h-3 w-3" /> Last: {new Date(pb.last_executed).toLocaleString()}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-1.5 shrink-0">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => executePlaybook(pb.id)}
                      disabled={executing === pb.id}
                    >
                      {executing === pb.id ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => toggleExpand(pb.id)}>
                      {expandedId === pb.id ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                    </Button>
                    <Button size="sm" variant="outline" className="text-red-600" onClick={() => deletePlaybook(pb.id)}>
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>

                {/* Execution History */}
                {expandedId === pb.id && (
                  <div className="mt-4 border-t pt-3">
                    <h4 className="text-xs font-medium mb-2">Execution History</h4>
                    {historyLoading === pb.id ? (
                      <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                    ) : (history[pb.id] || []).length === 0 ? (
                      <p className="text-xs text-muted-foreground">No executions yet.</p>
                    ) : (
                      <div className="space-y-1.5 max-h-48 overflow-y-auto">
                        {(history[pb.id] || []).map((h, i) => (
                          <div key={i} className="flex items-center justify-between text-xs border rounded px-3 py-1.5">
                            <span className="text-muted-foreground">{new Date(h.executed_at || h.timestamp).toLocaleString()}</span>
                            <Badge variant="outline" className={`text-[10px] ${h.status === "success" ? "text-green-600 bg-green-50" : "text-red-600 bg-red-50"}`}>
                              {h.status}
                            </Badge>
                            {h.error && <span className="text-red-500 truncate max-w-[200px]">{h.error}</span>}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Templates Gallery */}
      {!loading && templates.length > 0 && (
        <div>
          <h3 className="text-sm font-medium mb-3">Templates Gallery</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {templates.map((tpl, i) => (
              <Card key={i}>
                <CardContent className="py-4 space-y-2">
                  <div className="flex items-center gap-2">
                    <Copy className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium text-sm">{tpl.name}</span>
                  </div>
                  {tpl.description && <p className="text-xs text-muted-foreground">{tpl.description}</p>}
                  <Badge variant="outline" className={`text-[11px] ${triggerColor(tpl.trigger_type)}`}>
                    {tpl.trigger_type}
                  </Badge>
                  <div>
                    <Button size="sm" variant="outline" onClick={() => createFromTemplate(tpl)}>
                      <Plus className="h-3.5 w-3.5 mr-1" /> Create from Template
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
