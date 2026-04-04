// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import { ShieldCheck, Plus, Play, Trash2, Loader2, CheckCircle2, XCircle } from "lucide-react";

const RULE_TYPES = ["not_null", "unique", "range", "regex", "custom_sql", "freshness", "row_count", "referential"];
const SEVERITIES = ["critical", "warning", "info"];

export default function DQRulesPage() {
  const [rules, setRules] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [running, setRunning] = useState(false);
  const [runResults, setRunResults] = useState<any[]>([]);
  const [form, setForm] = useState({ name: "", table_fqn: "", column: "", rule_type: "not_null", expression: "", params: {}, threshold: 0, severity: "warning", schedule: "manual" });

  useEffect(() => { load(); }, []);
  async function load() { setLoading(true); try { const d = await api.get("/governance/dq/rules"); setRules(Array.isArray(d) ? d : []); } catch {} setLoading(false); }

  async function addRule() {
    try { await api.post("/governance/dq/rules", form); toast.success("Rule created"); setShowForm(false); load(); } catch (e: any) { toast.error(e.message); }
  }
  async function deleteRule(id: string) { if (!confirm("Delete?")) return; try { await api.delete(`/governance/dq/rules/${id}`); toast.success("Deleted"); load(); } catch (e: any) { toast.error(e.message); } }
  async function runAll() { setRunning(true); try { const r = await api.post("/governance/dq/run", {}); setRunResults(Array.isArray(r) ? r : []); toast.success(`Ran ${r.length} rules`); } catch (e: any) { toast.error(e.message); } setRunning(false); }
  async function runOne(id: string) { try { const r = await api.post("/governance/dq/run", { rule_ids: [id] }); if (r[0]) { toast[r[0].passed ? "success" : "error"](`${r[0].rule_name}: ${r[0].passed ? "PASSED" : "FAILED"} (${r[0].failure_rate * 100}%)`); } } catch (e: any) { toast.error(e.message); } }

  const sevColor = (s: string) => s === "critical" ? "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-400" : s === "warning" ? "bg-muted/40 text-foreground dark:bg-white/5 dark:text-gray-400" : "bg-muted/50 text-foreground";

  const rulesColumns: Column[] = [
    { key: "name", label: "Name", sortable: true, render: (v) => <span className="font-medium">{v}</span> },
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "column_name", label: "Column", sortable: true, render: (v) => <span className="font-mono text-xs">{v || "—"}</span> },
    { key: "rule_type", label: "Type", sortable: true, render: (v) => <Badge variant="outline">{v}</Badge> },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge className={sevColor(v)}>{v}</Badge> },
    { key: "schedule", label: "Schedule", sortable: true, render: (v) => <span className="text-xs">{v}</span> },
    { key: "rule_id", label: "Actions", align: "right", render: (v, r) => (
      <>
        <Button variant="ghost" size="sm" onClick={() => runOne(r.rule_id)}><Play className="h-3.5 w-3.5" /></Button>
        <Button variant="ghost" size="sm" onClick={() => deleteRule(r.rule_id)}><Trash2 className="h-3.5 w-3.5 text-red-500" /></Button>
      </>
    )},
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="Data Quality Rules" icon={ShieldCheck} breadcrumbs={["Governance", "DQ Rules"]} description="Define and execute data quality expectations — not_null, unique, range, regex, freshness, and custom SQL rules." />
      <div className="flex gap-3">
        <Button onClick={() => setShowForm(!showForm)}><Plus className="h-4 w-4 mr-2" />Add Rule</Button>
        <Button variant="outline" onClick={runAll} disabled={running}>{running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}Run All Rules</Button>
      </div>

      {showForm && (
        <Card><CardContent className="pt-4 space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <Input placeholder="Rule name *" value={form.name} onChange={e => setForm({...form, name: e.target.value})} />
            <Input placeholder="Table FQN (catalog.schema.table) *" value={form.table_fqn} onChange={e => setForm({...form, table_fqn: e.target.value})} />
            <Input placeholder="Column (optional)" value={form.column} onChange={e => setForm({...form, column: e.target.value})} />
          </div>
          <div className="grid grid-cols-4 gap-3">
            <select value={form.rule_type} onChange={e => setForm({...form, rule_type: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              {RULE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
            <select value={form.severity} onChange={e => setForm({...form, severity: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              {SEVERITIES.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <Input type="number" placeholder="Threshold (0-1)" value={form.threshold} onChange={e => setForm({...form, threshold: parseFloat(e.target.value) || 0})} step="0.01" min="0" max="1" />
            <select value={form.schedule} onChange={e => setForm({...form, schedule: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="manual">Manual</option><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="hourly">Hourly</option>
            </select>
          </div>
          {form.rule_type === "custom_sql" && <textarea placeholder="Custom SQL (must return total and failures columns)" value={form.expression} onChange={e => setForm({...form, expression: e.target.value})} className="w-full border rounded px-3 py-2 text-sm bg-background font-mono min-h-[60px]" />}
          {form.rule_type === "regex" && <Input placeholder="Regex pattern" value={form.expression} onChange={e => setForm({...form, expression: e.target.value})} />}
          <div className="flex gap-2">
            <Button onClick={addRule} disabled={!form.name || !form.table_fqn}>Create Rule</Button>
            <Button variant="ghost" onClick={() => setShowForm(false)}>Cancel</Button>
          </div>
        </CardContent></Card>
      )}

      <Card><CardContent className="pt-4">
        <DataTable
          data={rules}
          columns={rulesColumns}
          searchable
          searchKeys={["name", "table_fqn", "column_name", "rule_type", "severity"]}
          pageSize={25}
          compact
          tableId="dq-rules-table"
          emptyMessage='No rules defined. Click "Add Rule" to create your first DQ rule.'
        />
      </CardContent></Card>

      {runResults.length > 0 && (
        <Card className="border-border dark:border-border"><CardHeader className="pb-2"><CardTitle className="text-base">Run Results ({runResults.length})</CardTitle></CardHeader>
          <CardContent><div className="space-y-2">
            {runResults.map((r, i) => (
              <div key={i} className={`flex items-center gap-3 py-2 px-3 rounded border ${r.passed ? "border-border bg-muted/20 dark:bg-white/5" : "border-red-200 bg-red-50/50 dark:bg-red-950/20"}`}>
                {r.passed ? <CheckCircle2 className="h-4 w-4 text-foreground" /> : <XCircle className="h-4 w-4 text-red-600" />}
                <span className="font-medium text-sm">{r.rule_name}</span>
                <Badge variant="outline" className="text-xs">{r.rule_type}</Badge>
                <span className="text-xs text-muted-foreground">Failed: {r.failed_rows}/{r.total_rows} ({(r.failure_rate * 100).toFixed(2)}%)</span>
                <span className="text-xs text-muted-foreground ml-auto">{r.execution_time_ms}ms</span>
              </div>
            ))}
          </div></CardContent>
        </Card>
      )}
    </div>
  );
}
