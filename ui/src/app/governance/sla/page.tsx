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
import { Clock, Plus, Play, Loader2, CheckCircle2, XCircle, AlertTriangle } from "lucide-react";
import DataTable, { Column } from "@/components/DataTable";

export default function SLAPage() {
  const [rules, setRules] = useState<any[]>([]);
  const [status, setStatus] = useState<any>({});
  const [showForm, setShowForm] = useState(false);
  const [running, setRunning] = useState(false);
  const [form, setForm] = useState({ table_fqn: "", metric: "freshness", threshold_hours: 24, severity: "warning", owner_team: "" });

  useEffect(() => { load(); }, []);
  async function load() {
    try { const [r, s] = await Promise.all([api.get("/governance/sla/rules"), api.get("/governance/sla/status")]); setRules(Array.isArray(r) ? r : []); setStatus(s || {}); } catch {}
  }
  async function addRule() { try { await api.post("/governance/sla/rules", form); toast.success("SLA rule created"); setShowForm(false); load(); } catch (e: any) { toast.error(e.message); } }
  async function runCheck() { setRunning(true); try { await api.post("/governance/sla/check", {}); toast.success("SLA check complete"); load(); } catch (e: any) { toast.error(e.message); } setRunning(false); }

  const health = status.health_pct ?? 100;
  const checks = status.checks || [];

  return (
    <div className="space-y-4">
      <PageHeader title="SLA Dashboard" icon={Clock} breadcrumbs={["Governance", "SLA & Freshness"]} description="Monitor data freshness, row counts, and schema stability against SLA thresholds." />
      <div className="flex gap-3">
        <Button onClick={() => setShowForm(!showForm)}><Plus className="h-4 w-4 mr-2" />Add SLA Rule</Button>
        <Button variant="outline" onClick={runCheck} disabled={running}>{running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}Run SLA Check</Button>
      </div>

      <div className="grid grid-cols-3 gap-4">
        <Card><CardContent className="pt-4 text-center"><p className="text-3xl font-bold">{status.total_rules || rules.length}</p><p className="text-xs text-muted-foreground">Total SLAs</p></CardContent></Card>
        <Card><CardContent className="pt-4 text-center"><p className="text-3xl font-bold text-foreground">{status.passed || 0}</p><p className="text-xs text-muted-foreground">Passing</p></CardContent></Card>
        <Card><CardContent className="pt-4 text-center"><p className={`text-3xl font-bold ${(status.failed || 0) > 0 ? "text-red-600" : "text-foreground"}`}>{status.failed || 0}</p><p className="text-xs text-muted-foreground">Failing</p></CardContent></Card>
      </div>

      {health < 100 && (
        <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3">
          <div className={`h-3 rounded-full transition-all ${health >= 90 ? "bg-muted/200" : health >= 70 ? "bg-muted/200" : "bg-red-500"}`} style={{ width: `${health}%` }} />
        </div>
      )}

      {showForm && (
        <Card><CardContent className="pt-4 space-y-3">
          <div className="grid grid-cols-4 gap-3">
            <Input placeholder="Table FQN *" value={form.table_fqn} onChange={e => setForm({...form, table_fqn: e.target.value})} />
            <select value={form.metric} onChange={e => setForm({...form, metric: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="freshness">Freshness</option><option value="row_count">Row Count</option><option value="schema_stability">Schema Stability</option>
            </select>
            <Input type="number" placeholder="Threshold (hours)" value={form.threshold_hours} onChange={e => setForm({...form, threshold_hours: parseInt(e.target.value) || 24})} />
            <select value={form.severity} onChange={e => setForm({...form, severity: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="critical">Critical</option><option value="warning">Warning</option><option value="info">Info</option>
            </select>
          </div>
          <Input placeholder="Owner team" value={form.owner_team} onChange={e => setForm({...form, owner_team: e.target.value})} />
          <div className="flex gap-2"><Button onClick={addRule} disabled={!form.table_fqn}>Create SLA</Button><Button variant="ghost" onClick={() => setShowForm(false)}>Cancel</Button></div>
        </CardContent></Card>
      )}

      {checks.length > 0 && (
        <Card><CardHeader className="pb-2"><CardTitle className="text-base">SLA Check Results</CardTitle></CardHeader>
          <CardContent>
            <DataTable
              data={checks}
              columns={[
                {
                  key: "passed",
                  label: "Status",
                  sortable: true,
                  render: (v: any) => {
                    const pass = v === true || v === "true";
                    return pass ? <CheckCircle2 className="h-4 w-4 text-foreground" /> : <XCircle className="h-4 w-4 text-red-600" />;
                  },
                },
                { key: "table_fqn", label: "Table", sortable: true, render: (v: any) => <span className="font-mono text-sm">{v}</span> },
                { key: "metric", label: "Metric", sortable: true, render: (v: any) => <Badge variant="outline">{v}</Badge> },
                { key: "current_value", label: "Current", sortable: true, render: (v: any) => <span className="text-xs">{v}</span> },
                { key: "threshold", label: "Threshold", sortable: true, render: (v: any) => <span className="text-xs text-muted-foreground">{v}</span> },
                {
                  key: "severity",
                  label: "Severity",
                  sortable: true,
                  render: (v: any) => <Badge className={v === "critical" ? "bg-red-100 text-red-800" : "bg-muted/40 text-foreground"}>{v}</Badge>,
                },
              ] as Column[]}
              searchable
              searchKeys={["table_fqn", "metric", "severity"]}
              pageSize={25}
              compact
              tableId="sla-check-results"
              emptyMessage="No SLA check results."
            />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
