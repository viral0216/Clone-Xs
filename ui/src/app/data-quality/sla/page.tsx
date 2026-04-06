// @ts-nocheck
import { useState, useEffect } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  Clock, Plus, Play, Loader2, CheckCircle2, XCircle, Trash2,
  TrendingUp, ShieldCheck, RefreshCw,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from "recharts";

const METRICS = [
  { value: "freshness", label: "Freshness (hours)" },
  { value: "row_count", label: "Row Count (min)" },
  { value: "schema_stability", label: "Schema Stability" },
  { value: "completeness", label: "Completeness (%)" },
  { value: "accuracy", label: "Accuracy (%)" },
];

const SEVERITIES = ["critical", "warning", "info"];

function sevColor(s: string) {
  return s === "critical" ? "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400" :
    s === "warning" ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400" :
    "text-sky-600 bg-sky-50 border-sky-200 dark:bg-sky-950/30 dark:text-sky-400";
}

export default function SLAManagementPage() {
  const [rules, setRules] = usePersistedState<any[]>("dq-sla-rules", []);
  const [status, setStatus] = usePersistedState<any>("dq-sla-status", {});
  const [trend, setTrend] = usePersistedState<any[]>("dq-sla-trend", []);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [showForm, setShowForm] = useState(false);

  // Form state
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [form, setForm] = useState({
    metric: "freshness",
    threshold_hours: 24,
    threshold_value: 0,
    severity: "warning",
    owner_team: "",
    enabled: true,
  });

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const [r, s, t] = await Promise.all([
        api.get("/governance/sla/rules"),
        api.get("/governance/sla/status"),
        api.get("/governance/sla/compliance-trend?days=30"),
      ]);
      setRules(Array.isArray(r) ? r : []);
      setStatus(s || {});
      setTrend(Array.isArray(t) ? t : []);
    } catch { }
    setLoading(false);
  }

  async function createRule() {
    if (!catalog || !table) { toast.error("Select a table."); return; }
    const tableFqn = `${catalog}.${schema}.${table}`;
    const body = {
      table_fqn: tableFqn,
      metric: form.metric,
      threshold_hours: form.metric === "freshness" ? form.threshold_hours : 0,
      threshold_value: form.metric !== "freshness" ? form.threshold_value : 0,
      severity: form.severity,
      owner_team: form.owner_team,
      enabled: form.enabled,
    };
    try {
      await api.post("/governance/sla/rules", body);
      toast.success("SLA rule created.");
      setShowForm(false);
      load();
    } catch (e: any) { toast.error(e.message || "Failed to create rule."); }
  }

  async function deleteRule(slaId: string) {
    try {
      await api.delete(`/governance/sla/rules/${slaId}`);
      toast.success("SLA rule deleted.");
      setRules((prev) => prev.filter((r) => r.sla_id !== slaId));
    } catch (e: any) { toast.error(e.message || "Failed to delete."); }
  }

  async function runChecks() {
    setRunning(true);
    try {
      await api.post("/governance/sla/check", {});
      toast.success("SLA check complete.");
      load();
    } catch (e: any) { toast.error(e.message || "SLA check failed."); }
    setRunning(false);
  }

  const health = status.health_pct ?? 100;
  const checks = status.checks || [];
  const passing = status.passed || 0;
  const failing = status.failed || 0;
  const total = status.total_rules || rules.length;
  const compColor = health >= 90 ? "text-green-500" : health >= 70 ? "text-amber-500" : "text-red-500";
  const compBg = health >= 90 ? "bg-green-500" : health >= 70 ? "bg-amber-500" : "bg-red-500";

  const rulesColumns: Column[] = [
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "metric", label: "Metric", sortable: true, render: (v) => <Badge variant="outline" className="text-[10px]">{METRICS.find((m) => m.value === v)?.label || v}</Badge> },
    { key: "threshold_hours", label: "Threshold (h)", sortable: true, render: (v, row) => <span className="text-xs">{row.metric === "freshness" ? `${v}h` : row.threshold_value}</span> },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge variant="outline" className={`text-[10px] ${sevColor(v)}`}>{v}</Badge> },
    { key: "owner_team", label: "Owner", sortable: true, render: (v) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
    { key: "enabled", label: "Enabled", render: (v) => v ? <CheckCircle2 className="h-3.5 w-3.5 text-green-500" /> : <XCircle className="h-3.5 w-3.5 text-muted-foreground" /> },
    { key: "sla_id", label: "", render: (_, row) => <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => deleteRule(row.sla_id)}><Trash2 className="h-3.5 w-3.5 text-destructive" /></Button> },
  ];

  const checksColumns: Column[] = [
    { key: "passed", label: "Status", sortable: true, render: (v) => v ? <CheckCircle2 className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" /> },
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "metric", label: "Metric", sortable: true, render: (v) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
    { key: "current_value", label: "Current", sortable: true, render: (v) => <span className="text-xs font-mono">{typeof v === "number" ? v.toFixed(1) : v}</span> },
    { key: "threshold", label: "Threshold", sortable: true, render: (v) => <span className="text-xs text-muted-foreground">{v}</span> },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge variant="outline" className={`text-[10px] ${sevColor(v)}`}>{v}</Badge> },
    { key: "owner_team", label: "Owner", render: (v) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="SLA Management"
        icon={ShieldCheck}
        breadcrumbs={["Data Quality", "Governance", "SLA Management"]}
        description="Define freshness, row count, and quality SLAs per table. Track compliance and get alerted on breaches."
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{total}</p>
            <p className="text-xs text-muted-foreground mt-1">Total Rules</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-green-500">{passing}</p>
            <p className="text-xs text-muted-foreground mt-1">Passing</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold ${failing > 0 ? "text-red-500" : "text-foreground"}`}>{failing}</p>
            <p className="text-xs text-muted-foreground mt-1">Failing</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold ${compColor}`}>{health.toFixed(1)}%</p>
            <div className="w-20 h-1.5 bg-muted rounded-full overflow-hidden mx-auto mt-1">
              <div className={`h-full rounded-full ${compBg}`} style={{ width: `${health}%` }} />
            </div>
            <p className="text-xs text-muted-foreground mt-1">Compliance</p>
          </CardContent>
        </Card>
      </div>

      {/* Compliance Trend Chart */}
      {trend.length > 1 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <TrendingUp className="h-4 w-4" /> Compliance Trend (30 days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trend} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="date" tick={{ fontSize: 10, fill: "var(--muted-foreground)" }} tickFormatter={(d) => d.slice(5)} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 10, fill: "var(--muted-foreground)" }} />
                  <Tooltip contentStyle={{ fontSize: 11, borderRadius: 8, border: "1px solid var(--border)", background: "var(--popover)" }} formatter={(v: number) => [`${v.toFixed(1)}%`, "Compliance"]} />
                  <ReferenceLine y={90} stroke="#22c55e" strokeDasharray="4 4" label={{ value: "90%", position: "right", fontSize: 9, fill: "#22c55e" }} />
                  <Line type="monotone" dataKey="compliance_pct" stroke="#E8453C" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Actions */}
      <div className="flex gap-3">
        <Button onClick={() => setShowForm(!showForm)}>
          <Plus className="h-4 w-4 mr-2" />{showForm ? "Cancel" : "Add SLA Rule"}
        </Button>
        <Button variant="outline" onClick={runChecks} disabled={running}>
          {running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
          Run All Checks
        </Button>
        <Button variant="outline" onClick={load} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
        </Button>
      </div>

      {/* Create Form */}
      {showForm && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">New SLA Rule</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-3 items-end flex-wrap">
              <CatalogPicker
                catalog={catalog}
                schema={schema}
                table={table}
                onCatalogChange={(v) => { setCatalog(v); setSchema(""); setTable(""); }}
                onSchemaChange={(v) => { setSchema(v); setTable(""); }}
                onTableChange={setTable}
              />
            </div>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Metric</label>
                <select value={form.metric} onChange={(e) => setForm({ ...form, metric: e.target.value })}
                  className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm">
                  {METRICS.map((m) => <option key={m.value} value={m.value}>{m.label}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">
                  {form.metric === "freshness" ? "Threshold (hours)" : "Threshold Value"}
                </label>
                <Input
                  type="number"
                  value={form.metric === "freshness" ? form.threshold_hours : form.threshold_value}
                  onChange={(e) => form.metric === "freshness"
                    ? setForm({ ...form, threshold_hours: parseInt(e.target.value) || 0 })
                    : setForm({ ...form, threshold_value: parseFloat(e.target.value) || 0 })
                  }
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Severity</label>
                <select value={form.severity} onChange={(e) => setForm({ ...form, severity: e.target.value })}
                  className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm">
                  {SEVERITIES.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Owner Team</label>
                <Input value={form.owner_team} onChange={(e) => setForm({ ...form, owner_team: e.target.value })} placeholder="e.g. data-eng" />
              </div>
              <div className="flex items-end">
                <Button onClick={createRule} disabled={!catalog || !table}>Create SLA</Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Rules List */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">SLA Rules ({rules.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && rules.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : rules.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No SLA rules configured. Click "Add SLA Rule" to get started.</p>
          ) : (
            <DataTable data={rules} columns={rulesColumns} searchable searchKeys={["table_fqn", "metric", "owner_team"]} pageSize={15} compact tableId="sla-rules" />
          )}
        </CardContent>
      </Card>

      {/* Check Results */}
      {checks.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Latest Check Results ({checks.length})</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable data={checks} columns={checksColumns} searchable searchKeys={["table_fqn", "metric", "severity"]} pageSize={25} compact tableId="sla-checks" />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
