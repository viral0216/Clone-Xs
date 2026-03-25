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
import CatalogPicker from "@/components/CatalogPicker";
import {
  Zap, Plus, Play, Trash2, Search, Loader2, CheckCircle2, XCircle,
  BarChart3, Database, Settings, ToggleLeft, ToggleRight, Wand2,
  AlertTriangle, Clock, Download, Upload, PlayCircle, Filter,
  ChevronDown, ChevronRight, Pencil,
} from "lucide-react";
import LogPanel from "@/components/LogPanel";

export default function DQXPage() {
  const [tab, setTab] = useState<"dashboard" | "checks" | "profile" | "results" | "functions">("dashboard");
  const [dashboard, setDashboard] = useState<any>({});
  const [checks, setChecks] = useState<any[]>([]);
  const [results, setResults] = useState<any[]>([]);
  const [functions, setFunctions] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterTable, setFilterTable] = useState("");
  const [filterCategory, setFilterCategory] = useState("");

  // Profile state
  const [profileScope, setProfileScope] = useState<"table" | "schema" | "catalog">("table");
  const [profileCatalog, setProfileCatalog] = useState("");
  const [profileSchema, setProfileSchema] = useState("");
  const [profileTable, setProfileTable] = useState("");
  const [profiling, setProfiling] = useState(false);
  const [profileResult, setProfileResult] = useState<any>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [profileOpts, setProfileOpts] = useState({ sample_fraction: 0.3, max_in_count: 10, max_null_ratio: 0.01, remove_outliers: true, max_parallelism: 4 });

  // Create check state
  const [showCreate, setShowCreate] = useState(false);
  const [newCheck, setNewCheck] = useState({ table_fqn: "", name: "", check_function: "", arguments: {} as any, criticality: "error", filter_expr: "" });
  const [selectedFunc, setSelectedFunc] = useState<any>(null);

  // Import YAML state
  const [showImport, setShowImport] = useState(false);
  const [importYaml, setImportYaml] = useState("");
  const [importTable, setImportTable] = useState("");

  // Run state
  const [running, setRunning] = useState<string | null>(null);
  const [selectedChecks, setSelectedChecks] = useState<Set<string>>(new Set());
  const [deleting, setDeleting] = useState(false);
  const [editingCheck, setEditingCheck] = useState<any>(null);
  const [runningAll, setRunningAll] = useState(false);

  // Spark status
  const [sparkStatus, setSparkStatus] = useState<any>({});
  const [sparkConfiguring, setSparkConfiguring] = useState(false);
  const [sparkClusterId, setSparkClusterId] = useState("");
  const [sparkServerless, setSparkServerless] = useState(true);

  // Function browser expand
  const [expandedCat, setExpandedCat] = useState<string | null>(null);

  useEffect(() => { loadAll(); }, []);

  async function loadAll() {
    setLoading(true);
    const [d, c, r, f, ss] = await Promise.allSettled([
      api.get("/governance/dqx/dashboard"),
      api.get("/governance/dqx/checks"),
      api.get("/governance/dqx/results"),
      api.get("/governance/dqx/functions"),
      api.get("/governance/dqx/spark-status"),
    ]);
    if (d.status === "fulfilled") setDashboard(d.value || {});
    if (c.status === "fulfilled") setChecks(Array.isArray(c.value) ? c.value : []);
    if (r.status === "fulfilled") setResults(Array.isArray(r.value) ? r.value : []);
    if (f.status === "fulfilled") setFunctions(Array.isArray(f.value) ? f.value : []);
    if (ss.status === "fulfilled") {
      setSparkStatus(ss.value || {});
      if (ss.value?.cluster_id) setSparkClusterId(ss.value.cluster_id);
      if (ss.value?.serverless) setSparkServerless(ss.value.serverless);
    }
    setLoading(false);
  }

  function buildLogs(res: any, scope: string, target: string): string[] {
    const ts = new Date().toLocaleTimeString();
    const lines: string[] = [];
    lines.push(`[${ts}] DQX profiling completed for ${scope}: ${target}`);

    if (res.error) {
      lines.push(`[${ts}] ERROR: ${res.error}`);
      return lines;
    }

    // Single table
    if (res.checks && !res.tables && !res.schemas) {
      lines.push(`[${ts}] Generated ${res.count || res.checks.length} checks for ${res.table_fqn}`);
      for (const c of res.checks) {
        lines.push(`[${ts}]   + ${c.check_function} on ${c.column || 'table'} (${c.criticality})`);
      }
    }

    // Schema-level
    if (res.tables) {
      const total = res.tables_processed || res.tables.length;
      const ok = res.tables.filter((t: any) => !t.error).length;
      const failed = total - ok;
      lines.push(`[${ts}] Processed ${total} tables in parallel — ${res.total_checks || 0} total checks (${ok} succeeded, ${failed} failed)`);
      let idx = 0;
      for (const t of res.tables) {
        idx++;
        if (t.error) lines.push(`[${ts}]   [${idx}/${total}] FAILED ${t.table_fqn}: ${t.error}`);
        else lines.push(`[${ts}]   [${idx}/${total}] OK ${t.table_fqn}: ${t.count || 0} checks generated`);
      }
    }

    // Catalog-level
    if (res.schemas) {
      const totalSchemas = res.schemas_processed || res.schemas.length;
      const totalTables = res.schemas.reduce((s: number, sch: any) => s + (sch.tables_processed || sch.tables?.length || 0), 0);
      lines.push(`[${ts}] Processed ${totalSchemas} schemas, ${totalTables} tables in parallel — ${res.total_checks || 0} total checks`);
      let schIdx = 0;
      for (const s of res.schemas) {
        schIdx++;
        const schTables = s.tables_processed || s.tables?.length || 0;
        const schOk = (s.tables || []).filter((t: any) => !t.error).length;
        lines.push(`[${ts}]   [${schIdx}/${totalSchemas}] Schema ${s.schema}: ${s.total_checks || 0} checks from ${schTables} tables (${schOk} ok)`);
        let tblIdx = 0;
        for (const t of (s.tables || [])) {
          tblIdx++;
          if (t.error) lines.push(`[${ts}]     [${tblIdx}/${schTables}] FAILED ${t.table_fqn}: ${t.error}`);
          else lines.push(`[${ts}]     [${tblIdx}/${schTables}] OK ${t.table_fqn}: ${t.count || 0} checks`);
        }
      }
    }

    lines.push(`[${ts}] Done.`);
    return lines;
  }

  async function profileAction() {
    setProfiling(true);
    setProfileResult(null);
    const ts = new Date().toLocaleTimeString();
    const parallelNote = profileScope !== "table" ? ` (parallelism: ${profileOpts.max_parallelism})` : "";
    setLogs([`[${ts}] Starting DQX profiling (${profileScope})${parallelNote}...`, `[${ts}] Connecting to Spark and running profiler — this may take a moment...`]);
    try {
      let res: any;
      let target = "";
      if (profileScope === "table") {
        if (!profileCatalog || !profileSchema || !profileTable) { toast.error("Select a table"); setProfiling(false); return; }
        target = `${profileCatalog}.${profileSchema}.${profileTable}`;
        res = await api.post("/governance/dqx/profile", { table_fqn: target, auto_generate_checks: true, ...profileOpts });
      } else if (profileScope === "schema") {
        if (!profileCatalog || !profileSchema) { toast.error("Select catalog and schema"); setProfiling(false); return; }
        target = `${profileCatalog}.${profileSchema}`;
        res = await api.post("/governance/dqx/profile-schema", { catalog: profileCatalog, schema_name: profileSchema, ...profileOpts });
      } else {
        if (!profileCatalog) { toast.error("Select a catalog"); setProfiling(false); return; }
        target = profileCatalog;
        res = await api.post("/governance/dqx/profile-catalog", { catalog: profileCatalog, ...profileOpts });
      }
      setProfileResult(res);
      setLogs(prev => [...prev, ...buildLogs(res, profileScope, target)]);
      if (res.error) toast.error(res.error);
      else toast.success(`${res.total_checks || res.count || 0} checks generated`);
      loadAll();
    } catch (e: any) {
      setLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ERROR: ${e.message}`]);
      toast.error(e.message);
    }
    setProfiling(false);
  }

  async function runChecks(tableFqn: string) {
    setRunning(tableFqn);
    try {
      const res = await api.post("/governance/dqx/run", { table_fqn: tableFqn });
      if (res.error) toast.error(res.error);
      else toast.success(`${tableFqn}: ${res.pass_rate}% pass rate`);
      loadAll();
    } catch (e: any) { toast.error(e.message); }
    setRunning(null);
  }

  async function runAllChecks() {
    setRunningAll(true);
    const ts = new Date().toLocaleTimeString();
    setLogs([`[${ts}] Running DQX checks on all monitored tables...`]);
    try {
      const res = await api.post("/governance/dqx/run-all", {});
      const ts2 = new Date().toLocaleTimeString();
      const newLogs: string[] = [];
      if (res.error) {
        newLogs.push(`[${ts2}] ERROR: ${res.error}`);
        toast.error(res.error);
      } else {
        newLogs.push(`[${ts2}] Completed: ${res.tables_checked} tables checked`);
        for (const r of (res.results || [])) {
          if (r.error) newLogs.push(`[${ts2}]   FAILED ${r.table_fqn}: ${r.error}`);
          else newLogs.push(`[${ts2}]   ${parseFloat(r.pass_rate) >= 95 ? "OK" : "WARN"} ${r.table_fqn}: ${r.pass_rate}% pass rate (${Number(r.valid_rows).toLocaleString()}/${Number(r.total_rows).toLocaleString()} rows, ${r.execution_time_ms}ms)`);
        }
        newLogs.push(`[${ts2}] Summary: ${res.passed} passed, ${res.failed} failed`);
        toast.success(`${res.tables_checked} tables: ${res.passed} passed, ${res.failed} failed`);
      }
      setLogs(prev => [...prev, ...newLogs]);
      loadAll();
    } catch (e: any) {
      setLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ERROR: ${e.message}`]);
      toast.error(e.message);
    }
    setRunningAll(false);
  }

  async function toggleCheck(checkId: string, enabled: boolean) {
    try { await api.post(`/governance/dqx/checks/${checkId}/toggle`, { enabled }); loadAll(); } catch (e: any) { toast.error(e.message); }
  }

  async function deleteCheck(checkId: string) {
    if (!confirm("Delete this check?")) return;
    try { await api.delete(`/governance/dqx/checks/${checkId}`); toast.success("Deleted"); loadAll(); } catch (e: any) { toast.error(e.message); }
  }

  async function saveEdit() {
    if (!editingCheck) return;
    try {
      const res = await api.put(`/governance/dqx/checks/${editingCheck.check_id}`, {
        name: editingCheck.name,
        criticality: editingCheck.criticality,
        check_function: editingCheck.check_function,
        arguments: editingCheck.arguments,
        filter_expr: editingCheck.filter_expr || "",
      });
      if (res.error) toast.error(res.error);
      else { toast.success("Check updated"); setEditingCheck(null); loadAll(); }
    } catch (e: any) { toast.error(e.message); }
  }

  async function createCheck() {
    if (!newCheck.table_fqn || !newCheck.check_function) { toast.error("Table and function required"); return; }
    try {
      await api.post("/governance/dqx/checks", newCheck);
      toast.success("Check created");
      setShowCreate(false);
      setNewCheck({ table_fqn: "", name: "", check_function: "", arguments: {}, criticality: "error", filter_expr: "" });
      setSelectedFunc(null);
      loadAll();
    } catch (e: any) { toast.error(e.message); }
  }

  async function exportChecks() {
    try {
      const params = filterTable ? `?table_fqn=${filterTable}` : "";
      const headers: Record<string, string> = {};
      const host = sessionStorage.getItem("dbx_host"); if (host) headers["X-Databricks-Host"] = host;
      const token = sessionStorage.getItem("dbx_token"); if (token) headers["X-Databricks-Token"] = token;
      const wh = localStorage.getItem("dbx_warehouse_id"); if (wh) headers["X-Databricks-Warehouse"] = wh;
      const res = await fetch(`/api/governance/dqx/checks/export${params}`, { headers });
      const text = await res.text();
      const blob = new Blob([text], { type: "text/yaml" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a"); a.href = url; a.download = "dqx-checks.yaml"; a.click();
      URL.revokeObjectURL(url);
      toast.success("Checks exported as YAML");
    } catch (e: any) { toast.error(e.message); }
  }

  async function doImport() {
    if (!importTable || !importYaml.trim()) { toast.error("Table FQN and YAML required"); return; }
    try {
      const res = await api.post("/governance/dqx/checks/import", { table_fqn: importTable, yaml_content: importYaml });
      if (res.error) toast.error(res.error);
      else { toast.success(`${res.imported} checks imported`); setShowImport(false); setImportYaml(""); loadAll(); }
    } catch (e: any) { toast.error(e.message); }
  }

  const filteredChecks = checks.filter(c => {
    if (filterTable && !c.table_fqn?.includes(filterTable)) return false;
    return true;
  });
  const uniqueTables = [...new Set(checks.map(c => c.table_fqn).filter(Boolean))];
  const categories = [...new Set(functions.map(f => f.category).filter(Boolean))].sort();

  return (
    <div className="space-y-6">
      <PageHeader title="DQX Quality Engine" icon={Zap} breadcrumbs={["Governance", "DQX"]}
        description="Data quality powered by databricks-labs-dqx — 57+ row-level checks, dataset validation, profiling, PII detection, and geospatial checks." />

      {/* Tabs */}
      <div className="flex gap-1 border-b pb-1 flex-wrap">
        {[
          { key: "dashboard", label: "Dashboard", icon: BarChart3 },
          { key: "checks", label: "Checks", icon: Settings, count: checks.length },
          { key: "profile", label: "Profile & Generate", icon: Wand2 },
          { key: "results", label: "Run History", icon: Clock, count: results.length },
          { key: "functions", label: "Function Catalog", icon: Zap, count: functions.length },
        ].map((t) => (
          <button key={t.key} onClick={() => setTab(t.key as any)}
            className={`flex items-center gap-1.5 px-4 py-2 text-sm rounded-t transition-colors ${tab === t.key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground hover:bg-muted"}`}>
            <t.icon className="h-4 w-4" />{t.label}
            {t.count !== undefined && <Badge variant="outline" className="ml-1 text-[10px] px-1.5">{t.count}</Badge>}
          </button>
        ))}
      </div>

      {loading ? <div className="flex justify-center py-12"><Loader2 className="h-6 w-6 animate-spin" /></div> : (
        <>
          {/* ============ DASHBOARD ============ */}
          {tab === "dashboard" && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                {[
                  { label: "Total Checks", value: dashboard.total_checks || 0, icon: Settings, color: "text-blue-600 bg-blue-100 dark:bg-blue-950" },
                  { label: "Enabled", value: dashboard.enabled_checks || 0, icon: CheckCircle2, color: "text-green-600 bg-green-100 dark:bg-green-950" },
                  { label: "Pass Rate", value: `${dashboard.overall_pass_rate || 100}%`, icon: BarChart3, color: (dashboard.overall_pass_rate || 100) >= 90 ? "text-green-600 bg-green-100 dark:bg-green-950" : "text-red-600 bg-red-100 dark:bg-red-950" },
                  { label: "Tables Monitored", value: dashboard.tables_monitored || 0, icon: Database, color: "text-purple-600 bg-purple-100 dark:bg-purple-950" },
                  { label: "Profiled Tables", value: dashboard.profiled_tables || 0, icon: Wand2, color: "text-amber-600 bg-amber-100 dark:bg-amber-950" },
                ].map((s, i) => (
                  <Card key={i}><CardContent className="pt-4">
                    <div className="flex items-center gap-3">
                      <div className={`p-2 rounded-lg ${s.color}`}><s.icon className="h-5 w-5" /></div>
                      <div><p className="text-2xl font-bold">{s.value}</p><p className="text-xs text-muted-foreground">{s.label}</p></div>
                    </div>
                  </CardContent></Card>
                ))}
              </div>

              {/* Spark Connection */}
              <Card className={sparkStatus.available ? "border-green-200 dark:border-green-800" : "border-amber-200 dark:border-amber-800"}>
                <CardContent className="pt-4">
                  <div className="flex items-center gap-3">
                    <div className={`p-2 rounded-lg ${sparkStatus.available ? "bg-green-100 dark:bg-green-950 text-green-600" : "bg-amber-100 dark:bg-amber-950 text-amber-600"}`}>
                      <Zap className="h-5 w-5" />
                    </div>
                    <div className="flex-1">
                      <p className="text-sm font-medium">{sparkStatus.available ? "Spark Connected" : "Spark Not Connected"}</p>
                      <p className="text-xs text-muted-foreground">
                        {sparkStatus.available ? `${sparkStatus.serverless ? "Serverless" : `Cluster: ${sparkStatus.cluster_id}`}` : (sparkStatus.error || "Configure cluster to enable profiling & check execution")}
                      </p>
                    </div>
                    <Input placeholder="Cluster ID" className="w-44 h-8 text-xs" value={sparkClusterId} onChange={(e) => setSparkClusterId(e.target.value)} />
                    <label className="flex items-center gap-1 text-xs whitespace-nowrap"><input type="checkbox" checked={sparkServerless} onChange={(e) => setSparkServerless(e.target.checked)} /> Serverless</label>
                    <Button size="sm" variant="outline" disabled={sparkConfiguring} onClick={async () => {
                      setSparkConfiguring(true);
                      try { const res = await api.post("/governance/dqx/spark-configure", { cluster_id: sparkClusterId, serverless: sparkServerless }); setSparkStatus(res); toast.success(res.available ? "Connected!" : "Failed: " + (res.error || "")); } catch (e: any) { toast.error(e.message); }
                      setSparkConfiguring(false);
                    }}>{sparkConfiguring ? <Loader2 className="h-3 w-3 animate-spin" /> : "Connect"}</Button>
                  </div>
                </CardContent>
              </Card>

              {/* Run All + Latest Runs */}
              <div className="flex items-center gap-3">
                <Button onClick={runAllChecks} disabled={runningAll || !sparkStatus.available}>
                  {runningAll ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <PlayCircle className="h-4 w-4 mr-2" />}
                  Run All Checks
                </Button>
                <span className="text-xs text-muted-foreground">{uniqueTables.length} table(s) with checks</span>
                <Button variant="outline" size="sm" className="ml-auto text-red-600 border-red-300 hover:bg-red-50 dark:hover:bg-red-950"
                  onClick={async () => {
                    if (!confirm("Clear ALL DQX data? This will delete all checks, profiles, and run history. This cannot be undone.")) return;
                    try {
                      const res = await api.post("/governance/dqx/clear-all", {});
                      if (res.errors?.length) toast.error(`Some tables failed: ${res.errors.map((e: any) => e.table).join(", ")}`);
                      else toast.success(`Cleared ${res.cleared?.length || 0} DQX tables`);
                      loadAll();
                    } catch (e: any) { toast.error(e.message); }
                  }}>
                  <Trash2 className="h-4 w-4 mr-1" />Clear All DQX Data
                </Button>
              </div>

              {logs.length > 0 && (
                <LogPanel logs={logs} isRunning={runningAll} title="Execution Logs" maxHeight="max-h-48" collapsible={true} defaultExpanded={true} />
              )}

              {(dashboard.latest_runs || []).length > 0 && (
                <Card><CardHeader className="pb-2"><CardTitle className="text-base">Latest Runs</CardTitle></CardHeader>
                  <CardContent>
                    <div className="space-y-1">
                      {dashboard.latest_runs.map((r: any, i: number) => (
                        <div key={i} className="flex items-center gap-3 text-sm p-2 rounded hover:bg-muted/30">
                          {parseFloat(r.pass_rate) >= 95 ? <CheckCircle2 className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
                          <span className="font-mono text-xs flex-1">{r.table_fqn}</span>
                          <Badge className={`text-xs ${parseFloat(r.pass_rate) >= 95 ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>{r.pass_rate}%</Badge>
                          <span className="text-xs text-muted-foreground w-20 text-right">{Number(r.total_rows).toLocaleString()} rows</span>
                          <span className="text-xs text-muted-foreground w-16 text-right">{r.execution_time_ms}ms</span>
                          <Button variant="ghost" size="sm" onClick={() => runChecks(r.table_fqn)} disabled={running === r.table_fqn}>
                            {running === r.table_fqn ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                          </Button>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* ============ CHECKS ============ */}
          {tab === "checks" && (
            <div className="space-y-4">
              {/* Summary cards */}
              {checks.length > 0 && (
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                  <Card><CardContent className="pt-3 pb-3">
                    <p className="text-2xl font-bold">{checks.length}</p>
                    <p className="text-xs text-muted-foreground">Total Checks</p>
                  </CardContent></Card>
                  <Card><CardContent className="pt-3 pb-3">
                    <p className="text-2xl font-bold">{uniqueTables.length}</p>
                    <p className="text-xs text-muted-foreground">Tables</p>
                  </CardContent></Card>
                  <Card><CardContent className="pt-3 pb-3">
                    <p className="text-2xl font-bold text-green-600">{checks.filter(c => c.enabled === "true" || c.enabled === true).length}</p>
                    <p className="text-xs text-muted-foreground">Enabled</p>
                  </CardContent></Card>
                  <Card><CardContent className="pt-3 pb-3">
                    <p className="text-2xl font-bold text-red-600">{checks.filter(c => c.criticality === "error").length}</p>
                    <p className="text-xs text-muted-foreground">Error Level</p>
                  </CardContent></Card>
                  <Card><CardContent className="pt-3 pb-3">
                    <p className="text-2xl font-bold text-amber-600">{checks.filter(c => c.criticality === "warn").length}</p>
                    <p className="text-xs text-muted-foreground">Warning Level</p>
                  </CardContent></Card>
                </div>
              )}

              {/* Actions bar */}
              <div className="flex items-center gap-2 flex-wrap">
                <Button size="sm" onClick={() => setShowCreate(!showCreate)}><Plus className="h-4 w-4 mr-1" />New Check</Button>
                <Button size="sm" variant="outline" onClick={() => setShowImport(!showImport)}><Upload className="h-4 w-4 mr-1" />Import YAML</Button>
                <Button size="sm" variant="outline" onClick={exportChecks}><Download className="h-4 w-4 mr-1" />Export YAML</Button>

                {/* Bulk delete */}
                {selectedChecks.size > 0 && (
                  <Button size="sm" variant="destructive" disabled={deleting} onClick={async () => {
                    if (!confirm(`Delete ${selectedChecks.size} selected check(s)?`)) return;
                    setDeleting(true);
                    try {
                      const res = await api.post("/governance/dqx/checks/delete-bulk", { check_ids: [...selectedChecks] });
                      if (res.error) toast.error(res.error);
                      else { toast.success(`${res.deleted} check(s) deleted`); setSelectedChecks(new Set()); loadAll(); }
                    } catch (e: any) { toast.error(e.message); }
                    setDeleting(false);
                  }}>
                    {deleting ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Trash2 className="h-4 w-4 mr-1" />}
                    Delete Selected ({selectedChecks.size})
                  </Button>
                )}

                <div className="ml-auto flex items-center gap-2">
                  <select value={filterTable} onChange={(e) => { setFilterTable(e.target.value); setSelectedChecks(new Set()); }} className="border rounded px-3 py-1.5 text-sm bg-background">
                    <option value="">All Tables ({checks.length})</option>
                    {uniqueTables.map(t => <option key={t} value={t}>{t} ({checks.filter(c => c.table_fqn === t).length})</option>)}
                  </select>
                  {filterTable && (
                    <>
                      <Button size="sm" variant="outline" onClick={() => runChecks(filterTable)} disabled={!!running}>
                        {running === filterTable ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Play className="h-4 w-4 mr-1" />}Run
                      </Button>
                      <Button size="sm" variant="outline" className="text-red-600 border-red-300 hover:bg-red-50" disabled={deleting} onClick={async () => {
                        if (!confirm(`Delete ALL checks for ${filterTable}?`)) return;
                        setDeleting(true);
                        try {
                          const res = await api.post("/governance/dqx/checks/delete-bulk", { table_fqn: filterTable, delete_all: true });
                          if (res.error) toast.error(res.error);
                          else { toast.success(`All checks deleted for ${filterTable}`); setFilterTable(""); setSelectedChecks(new Set()); loadAll(); }
                        } catch (e: any) { toast.error(e.message); }
                        setDeleting(false);
                      }}>
                        <Trash2 className="h-4 w-4 mr-1" />Delete All
                      </Button>
                    </>
                  )}
                  {!filterTable && checks.length > 0 && (
                    <Button size="sm" variant="outline" className="text-red-600 border-red-300 hover:bg-red-50" disabled={deleting} onClick={async () => {
                      if (!confirm(`Delete ALL ${checks.length} checks? This cannot be undone.`)) return;
                      setDeleting(true);
                      try {
                        const res = await api.post("/governance/dqx/checks/delete-bulk", { delete_all: true });
                        if (res.error) toast.error(res.error);
                        else { toast.success("All checks deleted"); setSelectedChecks(new Set()); loadAll(); }
                      } catch (e: any) { toast.error(e.message); }
                      setDeleting(false);
                    }}>
                      <Trash2 className="h-4 w-4 mr-1" />Delete All ({checks.length})
                    </Button>
                  )}
                </div>
              </div>

              {/* Import YAML panel */}
              {showImport && (
                <Card className="border-blue-200 dark:border-blue-800"><CardContent className="pt-4 space-y-3">
                  <p className="text-sm font-medium">Import DQX Checks from YAML</p>
                  <Input placeholder="Table FQN (catalog.schema.table) *" value={importTable} onChange={(e) => setImportTable(e.target.value)} />
                  <textarea className="w-full h-48 border rounded p-3 font-mono text-xs bg-muted/30" placeholder={"- criticality: error\n  check:\n    function: is_not_null\n    arguments:\n      column: id"} value={importYaml} onChange={(e) => setImportYaml(e.target.value)} />
                  <div className="flex gap-2">
                    <Button size="sm" onClick={doImport}><Upload className="h-4 w-4 mr-1" />Import</Button>
                    <Button size="sm" variant="ghost" onClick={() => setShowImport(false)}>Cancel</Button>
                  </div>
                </CardContent></Card>
              )}

              {/* Create check panel */}
              {showCreate && (
                <Card className="border-green-200 dark:border-green-800"><CardContent className="pt-4 space-y-3">
                  <p className="text-sm font-medium">Create DQX Check</p>
                  <div className="grid grid-cols-3 gap-3">
                    <div><label className="text-xs text-muted-foreground">Table FQN *</label>
                      <Input value={newCheck.table_fqn} onChange={(e) => setNewCheck({...newCheck, table_fqn: e.target.value})} placeholder="catalog.schema.table" /></div>
                    <div><label className="text-xs text-muted-foreground">Check Function *</label>
                      <select value={newCheck.check_function} onChange={(e) => {
                        const func = functions.find(f => f.name === e.target.value);
                        setSelectedFunc(func);
                        setNewCheck({...newCheck, check_function: e.target.value, name: e.target.value, arguments: {}});
                      }} className="w-full border rounded px-3 py-2 text-sm bg-background">
                        <option value="">Select...</option>
                        {functions.map(f => <option key={f.name} value={f.name}>{f.name}</option>)}
                      </select></div>
                    <div><label className="text-xs text-muted-foreground">Criticality</label>
                      <select value={newCheck.criticality} onChange={(e) => setNewCheck({...newCheck, criticality: e.target.value})} className="w-full border rounded px-3 py-2 text-sm bg-background">
                        <option value="error">Error</option><option value="warn">Warning</option>
                      </select></div>
                  </div>
                  {selectedFunc && (
                    <div className="p-2 bg-muted/30 rounded text-xs space-y-2">
                      <p><strong>{selectedFunc.name}</strong> ({selectedFunc.level}) — {selectedFunc.description}</p>
                      {selectedFunc.args && (
                        <div className="grid grid-cols-3 gap-2">
                          {Object.entries(selectedFunc.args).map(([key, type]: [string, any]) => (
                            <div key={key}><label className="text-xs text-muted-foreground">{key} <span className="opacity-50">({type})</span></label>
                              <Input className="h-7 text-xs" value={newCheck.arguments[key] || ""} onChange={(e) => {
                                const val = e.target.value;
                                const args = {...newCheck.arguments};
                                // Auto-parse lists and numbers
                                if (type === "list" && val.includes(",")) args[key] = val.split(",").map(s => s.trim());
                                else if (type === "number" || type === "integer") args[key] = val ? Number(val) : "";
                                else args[key] = val;
                                setNewCheck({...newCheck, arguments: args});
                              }} placeholder={key} /></div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                  <div className="grid grid-cols-2 gap-3">
                    <div><label className="text-xs text-muted-foreground">Name (optional)</label>
                      <Input value={newCheck.name} onChange={(e) => setNewCheck({...newCheck, name: e.target.value})} /></div>
                    <div><label className="text-xs text-muted-foreground">Filter (optional SQL WHERE)</label>
                      <Input value={newCheck.filter_expr} onChange={(e) => setNewCheck({...newCheck, filter_expr: e.target.value})} placeholder="status = 'active'" /></div>
                  </div>
                  <div className="flex gap-2">
                    <Button size="sm" onClick={createCheck} disabled={!newCheck.table_fqn || !newCheck.check_function}>Create</Button>
                    <Button size="sm" variant="ghost" onClick={() => { setShowCreate(false); setSelectedFunc(null); }}>Cancel</Button>
                  </div>
                </CardContent></Card>
              )}

              {/* Edit check panel */}
              {editingCheck && (
                <Card className="border-amber-200 dark:border-amber-800"><CardContent className="pt-4 space-y-3">
                  <div className="flex items-center gap-2">
                    <Pencil className="h-4 w-4 text-amber-600" />
                    <p className="text-sm font-medium">Edit Check: {editingCheck.name || editingCheck.check_function}</p>
                    <Badge variant="outline" className="text-xs ml-auto">{editingCheck.check_id}</Badge>
                  </div>
                  <div className="grid grid-cols-4 gap-3">
                    <div><label className="text-xs text-muted-foreground">Name</label>
                      <Input value={editingCheck.name || ""} onChange={(e) => setEditingCheck({...editingCheck, name: e.target.value})} /></div>
                    <div><label className="text-xs text-muted-foreground">Function</label>
                      <select value={editingCheck.check_function || ""} onChange={(e) => setEditingCheck({...editingCheck, check_function: e.target.value})} className="w-full border rounded px-3 py-2 text-sm bg-background">
                        {functions.map(f => <option key={f.name} value={f.name}>{f.name}</option>)}
                      </select></div>
                    <div><label className="text-xs text-muted-foreground">Criticality</label>
                      <select value={editingCheck.criticality || "error"} onChange={(e) => setEditingCheck({...editingCheck, criticality: e.target.value})} className="w-full border rounded px-3 py-2 text-sm bg-background">
                        <option value="error">Error</option><option value="warn">Warning</option>
                      </select></div>
                    <div><label className="text-xs text-muted-foreground">Filter (SQL WHERE)</label>
                      <Input value={editingCheck.filter_expr || ""} onChange={(e) => setEditingCheck({...editingCheck, filter_expr: e.target.value})} placeholder="Optional" /></div>
                  </div>
                  <div><label className="text-xs text-muted-foreground">Arguments (JSON)</label>
                    <Input value={JSON.stringify(editingCheck.arguments || {})} onChange={(e) => { try { setEditingCheck({...editingCheck, arguments: JSON.parse(e.target.value)}); } catch {} }} className="font-mono text-xs" /></div>
                  <div className="flex gap-2">
                    <Button size="sm" onClick={saveEdit}><CheckCircle2 className="h-4 w-4 mr-1" />Save</Button>
                    <Button size="sm" variant="ghost" onClick={() => setEditingCheck(null)}>Cancel</Button>
                  </div>
                </CardContent></Card>
              )}

              {/* Checks table */}
              {filteredChecks.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground"><Settings className="h-10 w-10 mx-auto mb-2 opacity-30" /><p>No checks. Profile a table or create manually.</p></div>
              ) : (
                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50">
                      <tr>
                        <th className="px-3 py-2 w-8">
                          <input type="checkbox"
                            checked={selectedChecks.size === filteredChecks.length && filteredChecks.length > 0}
                            onChange={(e) => {
                              if (e.target.checked) setSelectedChecks(new Set(filteredChecks.map(c => c.check_id)));
                              else setSelectedChecks(new Set());
                            }} />
                        </th>
                        <th className="text-left px-3 py-2 font-medium">Name</th>
                        <th className="text-left px-3 py-2 font-medium">Table</th>
                        <th className="text-left px-3 py-2 font-medium">Function</th>
                        <th className="text-left px-3 py-2 font-medium">Arguments</th>
                        <th className="text-center px-3 py-2 font-medium">Level</th>
                        <th className="text-center px-3 py-2 font-medium">Enabled</th>
                        <th className="text-right px-3 py-2 font-medium">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredChecks.map((c) => (
                        <tr key={c.check_id} className={`border-t hover:bg-muted/30 ${selectedChecks.has(c.check_id) ? "bg-blue-50 dark:bg-blue-950/20" : ""}`}>
                          <td className="px-3 py-2">
                            <input type="checkbox" checked={selectedChecks.has(c.check_id)}
                              onChange={(e) => {
                                const next = new Set(selectedChecks);
                                if (e.target.checked) next.add(c.check_id); else next.delete(c.check_id);
                                setSelectedChecks(next);
                              }} />
                          </td>
                          <td className="px-3 py-2">
                            <span className="font-medium text-xs">{c.name || c.check_function}</span>
                            <Badge className={`ml-2 text-[10px] ${c.criticality === "error" ? "bg-red-100 text-red-800" : "bg-amber-100 text-amber-800"}`}>{c.criticality}</Badge>
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-muted-foreground">{c.table_fqn?.split(".").pop()}</td>
                          <td className="px-3 py-2"><Badge variant="outline" className="text-xs">{c.check_function}</Badge></td>
                          <td className="px-3 py-2 text-xs text-muted-foreground max-w-48 truncate">{typeof c.arguments === "object" ? Object.entries(c.arguments).map(([k,v]) => `${k}=${JSON.stringify(v)}`).join(", ") : ""}</td>
                          <td className="px-3 py-2 text-center">
                            <Badge variant="outline" className="text-[10px]">{functions.find(f => f.name === c.check_function)?.level || "row"}</Badge>
                          </td>
                          <td className="px-3 py-2 text-center">
                            <button onClick={() => toggleCheck(c.check_id, c.enabled === "false" || c.enabled === false)}>
                              {c.enabled === "true" || c.enabled === true ? <ToggleRight className="h-5 w-5 text-green-500" /> : <ToggleLeft className="h-5 w-5 text-muted-foreground" />}
                            </button>
                          </td>
                          <td className="px-3 py-2 text-right">
                            <Button variant="ghost" size="sm" onClick={() => setEditingCheck({...c, arguments: typeof c.arguments === "object" ? c.arguments : {}})}><Pencil className="h-3.5 w-3.5" /></Button>
                            <Button variant="ghost" size="sm" onClick={() => deleteCheck(c.check_id)}><Trash2 className="h-3.5 w-3.5 text-red-500" /></Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* ============ PROFILE & GENERATE ============ */}
          {tab === "profile" && (
            <div className="space-y-4">
              <Card><CardContent className="pt-4 space-y-4">
                <h2 className="text-sm font-medium flex items-center gap-2" style={{ fontSize: '16px' }}><Wand2 className="h-4 w-4 text-purple-600" />Profile & Auto-Generate DQX Checks</h2>
                <p className="text-xs text-muted-foreground">DQX Profiler analyzes your data to discover patterns (null rates, value distributions, ranges, cardinality) and auto-generates quality checks.</p>
                <div className="flex gap-3">
                  {(["table", "schema", "catalog"] as const).map((s) => (
                    <button key={s} onClick={() => { setProfileScope(s); setProfileSchema(""); setProfileTable(""); setProfileResult(null); }}
                      className={`px-4 py-2 rounded border text-sm transition-colors ${profileScope === s ? "bg-purple-100 border-purple-400 text-purple-800 dark:bg-purple-950 dark:border-purple-600 dark:text-purple-300" : "border-border hover:bg-muted"}`}>
                      <Database className="h-4 w-4 inline mr-1.5" />
                      {s === "table" ? "Single Table" : s === "schema" ? "Entire Schema" : "Entire Catalog"}
                    </button>
                  ))}
                </div>
                <CatalogPicker catalog={profileCatalog} schema={profileSchema} table={profileTable}
                  onCatalogChange={(v) => { setProfileCatalog(v); setProfileSchema(""); setProfileTable(""); }}
                  onSchemaChange={(v) => { setProfileSchema(v); setProfileTable(""); }}
                  onTableChange={setProfileTable}
                  showSchema={profileScope !== "catalog"} showTable={profileScope === "table"} />

                {/* Profiling Options */}
                <div className="grid grid-cols-4 gap-3 p-3 bg-muted/30 rounded-lg border">
                  <div>
                    <label className="text-xs text-muted-foreground font-medium">Sample Fraction</label>
                    <Input type="number" step="0.05" min="0.05" max="1.0" className="h-8 text-xs mt-1"
                      value={profileOpts.sample_fraction}
                      onChange={(e) => setProfileOpts({...profileOpts, sample_fraction: parseFloat(e.target.value) || 0.3})} />
                    <p className="text-[10px] text-muted-foreground mt-0.5">Fraction of data to sample (0.05–1.0)</p>
                  </div>
                  <div>
                    <label className="text-xs text-muted-foreground font-medium">Max In-List Count</label>
                    <Input type="number" min="1" max="100" className="h-8 text-xs mt-1"
                      value={profileOpts.max_in_count}
                      onChange={(e) => setProfileOpts({...profileOpts, max_in_count: parseInt(e.target.value) || 10})} />
                    <p className="text-[10px] text-muted-foreground mt-0.5">Generate is_in_list when distinct values &lt; this</p>
                  </div>
                  <div>
                    <label className="text-xs text-muted-foreground font-medium">Max Null Ratio</label>
                    <Input type="number" step="0.01" min="0" max="1.0" className="h-8 text-xs mt-1"
                      value={profileOpts.max_null_ratio}
                      onChange={(e) => setProfileOpts({...profileOpts, max_null_ratio: parseFloat(e.target.value) || 0.01})} />
                    <p className="text-[10px] text-muted-foreground mt-0.5">Generate is_not_null when null ratio &lt; this</p>
                  </div>
                  <div>
                    <label className="text-xs text-muted-foreground font-medium">Remove Outliers</label>
                    <div className="flex items-center gap-2 mt-2">
                      <input type="checkbox" checked={profileOpts.remove_outliers}
                        onChange={(e) => setProfileOpts({...profileOpts, remove_outliers: e.target.checked})} />
                      <span className="text-xs">Filter outliers from min/max</span>
                    </div>
                  </div>
                  {profileScope !== "table" && (
                    <div>
                      <label className="text-xs text-muted-foreground font-medium">Parallelism</label>
                      <Input type="number" min="1" max="16" className="h-8 text-xs mt-1"
                        value={profileOpts.max_parallelism}
                        onChange={(e) => setProfileOpts({...profileOpts, max_parallelism: parseInt(e.target.value) || 4})} />
                      <p className="text-[10px] text-muted-foreground mt-0.5">Tables profiled in parallel (1–16)</p>
                    </div>
                  )}
                </div>

                <Button onClick={profileAction} disabled={profiling || !profileCatalog || (profileScope !== "catalog" && !profileSchema) || (profileScope === "table" && !profileTable)}>
                  {profiling ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Wand2 className="h-4 w-4 mr-2" />}
                  {profileScope === "table" ? "Profile & Generate Checks" : profileScope === "schema" ? "Profile Entire Schema" : "Profile Entire Catalog"}
                </Button>
              </CardContent></Card>

              {/* Logs */}
              {logs.length > 0 && (
                <LogPanel logs={logs} isRunning={profiling} title="Profiling Logs" maxHeight="max-h-64" collapsible={true} defaultExpanded={true} />
              )}

              {profileResult && (
                <Card><CardContent className="pt-4 space-y-3">
                  {profileResult.error ? (
                    <div className="flex items-center gap-2 text-red-600"><XCircle className="h-5 w-5" />{profileResult.error}</div>
                  ) : profileScope === "table" ? (
                    <>
                      <div className="flex items-center gap-2"><CheckCircle2 className="h-5 w-5 text-green-500" /><span className="font-medium">{profileResult.count || 0} checks generated for {profileResult.table_fqn}</span></div>
                      {profileResult.checks?.length > 0 && (
                        <div className="border rounded max-h-80 overflow-auto">
                          {profileResult.checks.map((c: any, i: number) => (
                            <div key={i} className="flex items-center gap-2 px-3 py-2 border-b last:border-0 text-sm">
                              <Badge variant="outline" className="text-xs">{c.check_function}</Badge>
                              <span className="text-xs">{c.name}</span>
                              <span className="text-xs text-muted-foreground ml-auto">{c.column}</span>
                              <Badge className={`text-[10px] ${c.criticality === "error" ? "bg-red-100 text-red-800" : "bg-amber-100 text-amber-800"}`}>{c.criticality}</Badge>
                            </div>
                          ))}
                        </div>
                      )}
                    </>
                  ) : (
                    <>
                      <div className="flex items-center gap-2"><CheckCircle2 className="h-5 w-5 text-green-500" />
                        <span className="font-medium">{profileResult.total_checks || 0} checks across {profileResult.tables_processed || profileResult.schemas_processed || 0} {profileScope === "schema" ? "table(s)" : "schema(s)"}</span>
                      </div>
                      <div className="border rounded max-h-80 overflow-auto">
                        {profileResult.tables?.map((t: any, i: number) => (
                          <div key={i} className="flex items-center gap-2 px-3 py-2 border-b last:border-0 text-sm">
                            {t.error ? <XCircle className="h-4 w-4 text-red-500" /> : <CheckCircle2 className="h-4 w-4 text-green-500" />}
                            <span className="font-mono text-xs">{t.table_fqn}</span>
                            {t.error ? <span className="text-xs text-red-500 ml-auto">{t.error}</span> : <Badge className="ml-auto text-xs bg-green-100 text-green-800">{t.count} checks</Badge>}
                          </div>
                        ))}
                        {profileResult.schemas?.map((s: any, i: number) => (
                          <div key={i} className="border-b last:border-0">
                            <div className="flex items-center gap-2 px-3 py-2 bg-muted/30"><Database className="h-4 w-4 text-purple-500" /><span className="font-medium text-sm">{s.schema}</span><Badge className="ml-auto text-xs bg-purple-100 text-purple-800">{s.total_checks} checks</Badge></div>
                            {(s.tables || []).map((t: any, j: number) => (
                              <div key={j} className="flex items-center gap-2 px-3 pl-8 py-1.5 text-xs">
                                {t.error ? <XCircle className="h-3.5 w-3.5 text-red-500" /> : <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />}
                                <span className="font-mono">{t.table_fqn}</span>
                                {t.error ? <span className="text-red-500 ml-auto">{t.error}</span> : <span className="text-muted-foreground ml-auto">{t.count} checks</span>}
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </CardContent></Card>
              )}
            </div>
          )}

          {/* ============ RUN HISTORY ============ */}
          {tab === "results" && (
            <div className="space-y-4">
              {results.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground"><Clock className="h-10 w-10 mx-auto mb-2 opacity-30" /><p>No run history yet.</p></div>
              ) : (
                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50">
                      <tr>
                        <th className="text-left px-3 py-2 font-medium">Table</th>
                        <th className="text-center px-3 py-2 font-medium">Pass Rate</th>
                        <th className="text-right px-3 py-2 font-medium">Valid</th>
                        <th className="text-right px-3 py-2 font-medium">Invalid</th>
                        <th className="text-right px-3 py-2 font-medium">Total</th>
                        <th className="text-right px-3 py-2 font-medium">Checks</th>
                        <th className="text-right px-3 py-2 font-medium">Time</th>
                        <th className="text-right px-3 py-2 font-medium">Executed</th>
                        <th className="text-right px-3 py-2 font-medium">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {results.map((r, i) => (
                        <tr key={i} className="border-t hover:bg-muted/30">
                          <td className="px-3 py-2 font-mono text-xs">{r.table_fqn}</td>
                          <td className="px-3 py-2 text-center">
                            <Badge className={`text-xs ${parseFloat(r.pass_rate) >= 95 ? "bg-green-100 text-green-800" : parseFloat(r.pass_rate) >= 80 ? "bg-amber-100 text-amber-800" : "bg-red-100 text-red-800"}`}>{r.pass_rate}%</Badge>
                          </td>
                          <td className="px-3 py-2 text-right text-green-600">{Number(r.valid_rows).toLocaleString()}</td>
                          <td className="px-3 py-2 text-right text-red-600">{Number(r.invalid_rows).toLocaleString()}</td>
                          <td className="px-3 py-2 text-right">{Number(r.total_rows).toLocaleString()}</td>
                          <td className="px-3 py-2 text-right">{r.checks_applied}</td>
                          <td className="px-3 py-2 text-right text-muted-foreground">{r.execution_time_ms}ms</td>
                          <td className="px-3 py-2 text-right text-xs text-muted-foreground">{r.executed_at?.slice(0, 16)}</td>
                          <td className="px-3 py-2 text-right">
                            <Button variant="ghost" size="sm" onClick={() => runChecks(r.table_fqn)} disabled={running === r.table_fqn}>
                              {running === r.table_fqn ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                            </Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* ============ FUNCTION CATALOG ============ */}
          {tab === "functions" && (
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">Complete catalog of {functions.length} DQX check functions across {categories.length} categories.</p>
              <div className="flex gap-2 flex-wrap">
                <button onClick={() => setFilterCategory("")} className={`px-3 py-1 rounded text-xs border ${!filterCategory ? "bg-primary text-primary-foreground" : "hover:bg-muted"}`}>All ({functions.length})</button>
                {categories.map(cat => (
                  <button key={cat} onClick={() => setFilterCategory(cat)} className={`px-3 py-1 rounded text-xs border ${filterCategory === cat ? "bg-primary text-primary-foreground" : "hover:bg-muted"}`}>
                    {cat} ({functions.filter(f => f.category === cat).length})
                  </button>
                ))}
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                {functions.filter(f => !filterCategory || f.category === filterCategory).map((f, i) => (
                  <Card key={i} className="hover:border-blue-300 dark:hover:border-blue-700 transition-colors">
                    <CardContent className="pt-3 pb-3">
                      <div className="flex items-start gap-2">
                        <Zap className="h-4 w-4 text-amber-500 mt-0.5 shrink-0" />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="font-mono text-sm font-medium">{f.name}</span>
                            <Badge variant="outline" className="text-[10px]">{f.category}</Badge>
                            <Badge variant="outline" className="text-[10px]">{f.level}</Badge>
                          </div>
                          <p className="text-xs text-muted-foreground mt-0.5">{f.description}</p>
                          {f.args && (
                            <div className="flex gap-1 mt-1 flex-wrap">
                              {Object.entries(f.args).map(([k, v]: [string, any]) => (
                                <Badge key={k} variant="outline" className="text-[10px] font-mono">{k}: {v}</Badge>
                              ))}
                            </div>
                          )}
                        </div>
                        <Button size="sm" variant="ghost" className="shrink-0" onClick={() => {
                          setNewCheck({...newCheck, check_function: f.name, name: f.name, arguments: {}});
                          setSelectedFunc(f);
                          setShowCreate(true);
                          setTab("checks");
                        }}><Plus className="h-3.5 w-3.5" /></Button>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
