// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { FileText, Plus, Play, CheckCircle2, XCircle, AlertTriangle, Loader2 } from "lucide-react";

export default function ContractsPage() {
  const [contracts, setContracts] = useState<any[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [validating, setValidating] = useState<string | null>(null);
  const [validationResult, setValidationResult] = useState<any>(null);
  const [form, setForm] = useState({ name: "", table_fqn: "", producer_team: "", consumer_teams: "", freshness_sla_hours: 24, row_count_min: 0, row_count_max: 0, status: "draft" });

  useEffect(() => { load(); }, []);
  async function load() { try { const d = await api.get("/governance/contracts"); setContracts(Array.isArray(d) ? d : []); } catch {} }
  async function addContract() {
    try {
      await api.post("/governance/contracts", { ...form, consumer_teams: form.consumer_teams.split(",").map(t => t.trim()).filter(Boolean) });
      toast.success("Contract created"); setShowForm(false); load();
    } catch (e: any) { toast.error(e.message); }
  }
  async function validate(id: string) {
    setValidating(id); setValidationResult(null);
    try { const r = await api.post(`/governance/contracts/${id}/validate`, {}); setValidationResult(r); } catch (e: any) { toast.error(e.message); }
    setValidating(null);
  }

  return (
    <div className="space-y-6">
      <PageHeader title="Data Contracts" icon={FileText} breadcrumbs={["Governance", "Data Contracts"]} description="Define expected schema, quality, and freshness between producer and consumer teams." />
      <Button onClick={() => setShowForm(!showForm)}><Plus className="h-4 w-4 mr-2" />New Contract</Button>

      {showForm && (
        <Card><CardContent className="pt-4 space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <Input placeholder="Contract name *" value={form.name} onChange={e => setForm({...form, name: e.target.value})} />
            <Input placeholder="Table FQN *" value={form.table_fqn} onChange={e => setForm({...form, table_fqn: e.target.value})} />
            <Input placeholder="Producer team" value={form.producer_team} onChange={e => setForm({...form, producer_team: e.target.value})} />
          </div>
          <div className="grid grid-cols-4 gap-3">
            <Input placeholder="Consumer teams (comma-separated)" value={form.consumer_teams} onChange={e => setForm({...form, consumer_teams: e.target.value})} className="col-span-2" />
            <Input type="number" placeholder="Freshness SLA (hours)" value={form.freshness_sla_hours} onChange={e => setForm({...form, freshness_sla_hours: parseInt(e.target.value) || 24})} />
            <select value={form.status} onChange={e => setForm({...form, status: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="draft">Draft</option><option value="active">Active</option>
            </select>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Input type="number" placeholder="Min row count (0 = no min)" value={form.row_count_min} onChange={e => setForm({...form, row_count_min: parseInt(e.target.value) || 0})} />
            <Input type="number" placeholder="Max row count (0 = no max)" value={form.row_count_max} onChange={e => setForm({...form, row_count_max: parseInt(e.target.value) || 0})} />
          </div>
          <div className="flex gap-2"><Button onClick={addContract} disabled={!form.name || !form.table_fqn}>Create Contract</Button><Button variant="ghost" onClick={() => setShowForm(false)}>Cancel</Button></div>
        </CardContent></Card>
      )}

      <div className="space-y-4">
        {contracts.map(c => (
          <Card key={c.contract_id}>
            <CardContent className="pt-4 space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <FileText className="h-5 w-5 text-blue-500" />
                  <span className="font-medium">{c.name}</span>
                  <Badge variant="outline" className="font-mono text-xs">{c.table_fqn}</Badge>
                  <Badge className={c.status === "active" ? "bg-green-100 text-green-800" : c.status === "violated" ? "bg-red-100 text-red-800" : "bg-gray-100 text-gray-800"}>{c.status}</Badge>
                </div>
                <Button variant="outline" size="sm" onClick={() => validate(c.contract_id)} disabled={validating === c.contract_id}>
                  {validating === c.contract_id ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Play className="h-4 w-4 mr-1" />}Validate
                </Button>
              </div>
              <div className="flex gap-6 text-xs text-muted-foreground">
                <span>Producer: {c.producer_team}</span>
                <span>Consumers: {Array.isArray(c.consumer_teams) ? c.consumer_teams.join(", ") : c.consumer_teams}</span>
                <span>Freshness: {c.freshness_sla_hours}h</span>
                {c.row_count_min > 0 && <span>Min rows: {Number(c.row_count_min).toLocaleString()}</span>}
              </div>

              {validationResult?.contract_id === c.contract_id && (
                <div className={`rounded-lg p-3 ${validationResult.compliant ? "bg-green-50 dark:bg-green-950/20 border border-green-200" : "bg-red-50 dark:bg-red-950/20 border border-red-200"}`}>
                  <div className="flex items-center gap-2 mb-2">
                    {validationResult.compliant ? <CheckCircle2 className="h-4 w-4 text-green-600" /> : <XCircle className="h-4 w-4 text-red-600" />}
                    <span className="text-sm font-medium">{validationResult.compliant ? "Compliant" : "Violations Detected"}</span>
                  </div>
                  {validationResult.violations?.length > 0 && (
                    <div className="space-y-1">{validationResult.violations.map((v: any, i: number) => (
                      <div key={i} className="flex items-center gap-2 text-xs text-red-600">
                        <AlertTriangle className="h-3 w-3" />{v.type}: {JSON.stringify(v)}
                      </div>
                    ))}</div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        ))}
        {contracts.length === 0 && <div className="text-center py-12 text-muted-foreground">No contracts defined. Click "New Contract" to create your first data contract.</div>}
      </div>
    </div>
  );
}
