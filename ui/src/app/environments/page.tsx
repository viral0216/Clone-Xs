// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  Server, Plus, Loader2, Trash2, Clock, DollarSign,
  Copy, Timer, Shield, AlertTriangle, RefreshCw,
} from "lucide-react";

const MASKING_PROFILES = [
  { value: "none", label: "None" },
  { value: "basic", label: "Basic (email, phone)" },
  { value: "full", label: "Full (all PII)" },
];

const CLONE_TYPES = [
  { value: "SHALLOW", label: "Shallow Clone" },
  { value: "DEEP", label: "Deep Clone" },
];

function statusColor(s: string) {
  const map: Record<string, string> = {
    active: "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400",
    provisioning: "text-blue-600 bg-blue-50 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400",
    expired: "text-gray-600 bg-gray-50 border-gray-200 dark:bg-gray-950/30 dark:text-gray-400",
    destroying: "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400",
    error: "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400",
  };
  return map[s] || "text-gray-600 bg-gray-50 border-gray-200";
}

function formatTTL(expiresAt: string | null) {
  if (!expiresAt) return "No TTL";
  const diff = new Date(expiresAt).getTime() - Date.now();
  if (diff <= 0) return "Expired";
  const hours = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);
  if (hours > 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
  return `${hours}h ${mins}m`;
}

function costPercent(cost: number, budget: number) {
  if (!budget || budget <= 0) return 0;
  return Math.min(100, Math.round((cost / budget) * 100));
}

export default function EnvironmentsPage() {
  const [environments, setEnvironments] = useState<any[]>([]);
  const [templates, setTemplates] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [destroying, setDestroying] = useState<string | null>(null);
  const [extending, setExtending] = useState<string | null>(null);
  const [cleaningUp, setCleaningUp] = useState(false);

  const [form, setForm] = useState({
    name: "",
    source_catalog: "",
    tables: "",
    masking_profile: "none",
    ttl_hours: 24,
    cost_budget: 100,
    clone_type: "SHALLOW",
  });

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const [envs, tpls] = await Promise.all([
        api.get("/environments/"),
        api.get("/environments/templates").catch(() => []),
      ]);
      setEnvironments(Array.isArray(envs) ? envs : []);
      setTemplates(Array.isArray(tpls) ? tpls : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to load environments");
    }
    setLoading(false);
  }

  async function createEnvironment() {
    if (!form.name.trim()) { toast.error("Name is required"); return; }
    if (!form.source_catalog.trim()) { toast.error("Source catalog is required"); return; }
    try {
      await api.post("/environments/", {
        name: form.name,
        source_catalog: form.source_catalog,
        tables: form.tables.split(",").map((t) => t.trim()).filter(Boolean),
        masking_profile: form.masking_profile,
        ttl_hours: Number(form.ttl_hours),
        cost_budget: Number(form.cost_budget),
        clone_type: form.clone_type,
      });
      toast.success("Environment created");
      setShowForm(false);
      setForm({ name: "", source_catalog: "", tables: "", masking_profile: "none", ttl_hours: 24, cost_budget: 100, clone_type: "SHALLOW" });
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to create environment");
    }
  }

  async function destroyEnvironment(id: string) {
    if (!confirm("Destroy this environment? This cannot be undone.")) return;
    setDestroying(id);
    try {
      await api.delete(`/environments/${id}`);
      toast.success("Environment destroyed");
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to destroy environment");
    }
    setDestroying(null);
  }

  async function extendEnvironment(id: string) {
    setExtending(id);
    try {
      await api.post(`/environments/${id}/extend`, { hours: 24 });
      toast.success("Extended by 24 hours");
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to extend");
    }
    setExtending(null);
  }

  async function cleanupExpired() {
    setCleaningUp(true);
    try {
      const result = await api.post("/environments/cleanup");
      toast.success(`Cleanup complete${result?.cleaned ? `: ${result.cleaned} removed` : ""}`);
      load();
    } catch (e: any) {
      toast.error(e.message || "Cleanup failed");
    }
    setCleaningUp(false);
  }

  async function createFromTemplate(tpl: any) {
    try {
      await api.post("/environments/", {
        name: `${tpl.name}-${Date.now().toString(36)}`,
        source_catalog: tpl.source_catalog,
        tables: tpl.tables || [],
        masking_profile: tpl.masking_profile || "none",
        ttl_hours: tpl.ttl_hours || 24,
        cost_budget: tpl.cost_budget || 100,
        clone_type: tpl.clone_type || "SHALLOW",
      });
      toast.success(`Environment created from template "${tpl.name}"`);
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to create from template");
    }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <PageHeader
        title="Data Environments"
        description="Ephemeral sandbox environments with auto-masking and TTL"
        icon={Server}
        actions={
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" onClick={cleanupExpired} disabled={cleaningUp}>
              {cleaningUp ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              Cleanup Expired
            </Button>
            <Button size="sm" onClick={() => setShowForm(!showForm)}>
              <Plus className="h-4 w-4 mr-1" /> New Environment
            </Button>
          </div>
        }
      />

      {/* Create Form */}
      {showForm && (
        <Card>
          <CardHeader><CardTitle className="text-sm">New Environment</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <input className="border rounded px-3 py-2 text-sm bg-background" placeholder="Environment name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
              <div className="flex-1 max-w-md">
                <CatalogPicker
                  catalog={form.source_catalog}
                  onCatalogChange={(c) => setForm({ ...form, source_catalog: c })}
                  showSchema={false}
                  showTable={false}
                />
              </div>
            </div>
            <input className="border rounded px-3 py-2 text-sm w-full bg-background" placeholder="Tables (comma-separated, e.g. schema.table1, schema.table2)" value={form.tables} onChange={(e) => setForm({ ...form, tables: e.target.value })} />
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Masking Profile</label>
                <select className="border rounded px-3 py-2 text-sm w-full bg-background" value={form.masking_profile} onChange={(e) => setForm({ ...form, masking_profile: e.target.value })}>
                  {MASKING_PROFILES.map((m) => <option key={m.value} value={m.value}>{m.label}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">TTL (hours)</label>
                <input type="number" className="border rounded px-3 py-2 text-sm w-full bg-background" value={form.ttl_hours} onChange={(e) => setForm({ ...form, ttl_hours: Number(e.target.value) })} />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Cost Budget ($)</label>
                <input type="number" className="border rounded px-3 py-2 text-sm w-full bg-background" value={form.cost_budget} onChange={(e) => setForm({ ...form, cost_budget: Number(e.target.value) })} />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Clone Type</label>
                <select className="border rounded px-3 py-2 text-sm w-full bg-background" value={form.clone_type} onChange={(e) => setForm({ ...form, clone_type: e.target.value })}>
                  {CLONE_TYPES.map((c) => <option key={c.value} value={c.value}>{c.label}</option>)}
                </select>
              </div>
            </div>
            <div className="flex gap-2">
              <Button size="sm" onClick={createEnvironment} disabled={!form.source_catalog}>Create Environment</Button>
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

      {/* Active Environments */}
      {!loading && (
        <div>
          <h3 className="text-sm font-medium mb-3">Active Environments ({environments.filter((e) => e.status === "active" || e.status === "provisioning").length})</h3>
          {environments.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No environments. Create one above or use a template below.</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {environments.map((env) => {
                const pct = costPercent(env.cost_current || 0, env.cost_budget || 0);
                const costBarColor = pct >= 90 ? "bg-red-500" : pct >= 70 ? "bg-amber-500" : "bg-green-500";

                return (
                  <Card key={env.id}>
                    <CardContent className="py-4 space-y-3">
                      <div className="flex items-start justify-between">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <Server className="h-4 w-4 text-muted-foreground shrink-0" />
                            <span className="font-medium text-sm truncate">{env.name}</span>
                          </div>
                          <div className="text-xs text-muted-foreground mt-0.5">
                            {env.source_catalog} → {env.target_catalog || "sandbox"}
                          </div>
                        </div>
                        <Badge variant="outline" className={`text-[11px] shrink-0 ${statusColor(env.status)}`}>
                          {env.status}
                        </Badge>
                      </div>

                      {/* TTL Countdown */}
                      <div className="flex items-center gap-2 text-xs">
                        <Timer className="h-3.5 w-3.5 text-muted-foreground" />
                        <span className={`font-medium ${formatTTL(env.expires_at) === "Expired" ? "text-red-600" : ""}`}>
                          TTL: {formatTTL(env.expires_at)}
                        </span>
                      </div>

                      {/* Masking */}
                      {env.masking_profile && env.masking_profile !== "none" && (
                        <div className="flex items-center gap-1 text-xs text-muted-foreground">
                          <Shield className="h-3 w-3" /> Masking: {env.masking_profile}
                        </div>
                      )}

                      {/* Cost vs Budget Progress */}
                      {env.cost_budget > 0 && (
                        <div className="space-y-1">
                          <div className="flex items-center justify-between text-xs">
                            <span className="flex items-center gap-1 text-muted-foreground">
                              <DollarSign className="h-3 w-3" /> Cost
                            </span>
                            <span>${env.cost_current?.toFixed(2) || "0.00"} / ${env.cost_budget?.toFixed(2)}</span>
                          </div>
                          <div className="h-2 rounded-full bg-muted overflow-hidden">
                            <div className={`h-full rounded-full transition-all ${costBarColor}`} style={{ width: `${pct}%` }} />
                          </div>
                          {pct >= 90 && (
                            <div className="flex items-center gap-1 text-[10px] text-red-600">
                              <AlertTriangle className="h-3 w-3" /> Budget nearly exhausted
                            </div>
                          )}
                        </div>
                      )}

                      {/* Actions */}
                      <div className="flex items-center gap-2 pt-1 border-t">
                        <Button
                          size="sm"
                          variant="outline"
                          className="text-xs"
                          disabled={extending === env.id}
                          onClick={() => extendEnvironment(env.id)}
                        >
                          {extending === env.id ? <Loader2 className="h-3 w-3 animate-spin mr-1" /> : <Clock className="h-3 w-3 mr-1" />}
                          +24h
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="text-xs text-red-600"
                          disabled={destroying === env.id}
                          onClick={() => destroyEnvironment(env.id)}
                        >
                          {destroying === env.id ? <Loader2 className="h-3 w-3 animate-spin mr-1" /> : <Trash2 className="h-3 w-3 mr-1" />}
                          Destroy
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Templates */}
      {!loading && templates.length > 0 && (
        <div>
          <h3 className="text-sm font-medium mb-3">Environment Templates</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {templates.map((tpl, i) => (
              <Card key={i}>
                <CardContent className="py-4 space-y-2">
                  <div className="flex items-center gap-2">
                    <Copy className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium text-sm">{tpl.name}</span>
                  </div>
                  {tpl.description && <p className="text-xs text-muted-foreground">{tpl.description}</p>}
                  <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                    {tpl.source_catalog && <span>Catalog: {tpl.source_catalog}</span>}
                    {tpl.clone_type && <Badge variant="outline" className="text-[10px]">{tpl.clone_type}</Badge>}
                    {tpl.masking_profile && tpl.masking_profile !== "none" && (
                      <Badge variant="outline" className="text-[10px]"><Shield className="h-2.5 w-2.5 mr-0.5" />{tpl.masking_profile}</Badge>
                    )}
                  </div>
                  <Button size="sm" variant="outline" onClick={() => createFromTemplate(tpl)}>
                    <Plus className="h-3.5 w-3.5 mr-1" /> Create from Template
                  </Button>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
