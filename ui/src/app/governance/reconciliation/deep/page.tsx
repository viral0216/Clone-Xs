// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Rows3, Loader2, CheckCircle, XCircle, AlertTriangle,
  ArrowLeftRight, Zap, Eye, ChevronDown, ChevronUp,
  Minus, Plus, Pencil, Search, Download, FileJson, FileText,
  TrendingUp, Columns3, Wrench, Bell, Trash2, Copy, ShieldAlert,
} from "lucide-react";
import SqlWorkbench from "@/components/sql/SqlWorkbench";
import {
  PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, LineChart, Line, CartesianGrid,
} from "recharts";

function SummaryCard({ label, value, color, icon: Icon }: { label: string; value: string | number; color?: string; icon?: any }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : color === "blue" ? "text-blue-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-5 pb-4">
        <div className="flex items-center gap-2 mb-1">
          {Icon && <Icon className={`h-3.5 w-3.5 ${colorClass || "text-muted-foreground"}`} />}
          <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</p>
        </div>
        <p className={`text-xl font-bold ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  );
}

const CHART_COLORS = {
  matched: "#22c55e",
  missing: "#ef4444",
  extra: "#3b82f6",
  modified: "#f59e0b",
};

function exportCsv(results: any) {
  const rows: string[] = [];
  const details = results.details || [];
  for (const d of details) {
    const fqn = `${d.schema}.${d.table}`;
    // Missing rows
    for (const row of d.missing_sample || []) {
      const cols = Object.keys(row);
      if (rows.length === 0) rows.push(["type", "table", ...cols].join(","));
      rows.push(["missing", fqn, ...cols.map((c) => `"${String(row[c] ?? "").replace(/"/g, '""')}"`)] .join(","));
    }
    // Extra rows
    for (const row of d.extra_sample || []) {
      const cols = Object.keys(row);
      if (rows.length === 0) rows.push(["type", "table", ...cols].join(","));
      rows.push(["extra", fqn, ...cols.map((c) => `"${String(row[c] ?? "").replace(/"/g, '""')}"`)] .join(","));
    }
    // Modified rows
    for (const m of d.modified_sample || []) {
      for (const diff of m.diffs || []) {
        if (rows.length === 0) rows.push("type,table,key,column,source_value,dest_value");
        const keyStr = Object.entries(m.key || {}).map(([k, v]) => `${k}=${v}`).join(";");
        rows.push(["modified", fqn, `"${keyStr}"`, diff.column, `"${String(diff.source ?? "NULL")}"`, `"${String(diff.dest ?? "NULL")}"`].join(","));
      }
    }
  }
  if (rows.length === 0) { toast.info("No sample data to export"); return; }
  const blob = new Blob([rows.join("\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `deep-reconciliation-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function exportJson(results: any) {
  const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `deep-reconciliation-${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

export default function DeepReconciliationPage() {
  // ── Catalog/Table selection ────────────────────────────────────
  const [source, setSource] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [dest, setDest] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [destTable, setDestTable] = useState("");

  // ── Spark ──────────────────────────────────────────────────────
  const [sparkStatus, setSparkStatus] = useState<any>({ available: false });
  const [sparkClusterId, setSparkClusterId] = useState("");
  const [sparkServerless, setSparkServerless] = useState(true);
  const [sparkConfiguring, setSparkConfiguring] = useState(false);

  // ── Preview ────────────────────────────────────────────────────
  const [preview, setPreview] = useState<any>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // ── Column selection ───────────────────────────────────────────
  const [selectedCols, setSelectedCols] = useState<Set<string>>(new Set());
  const [ignoredCols, setIgnoredCols] = useState<Set<string>>(new Set());
  const [keyColumns, setKeyColumns] = useState<string[]>([]);

  // ── Comparison options ────────────────────────────────────────
  const [ignoreNulls, setIgnoreNulls] = useState(false);
  const [ignoreCase, setIgnoreCase] = useState(false);
  const [ignoreWhitespace, setIgnoreWhitespace] = useState(false);
  const [decimalPrecision, setDecimalPrecision] = useState(0);

  // ── Deep reconciliation results ────────────────────────────────
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<any>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [sampleDiffs, setSampleDiffs] = useState(10);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  // ── History / Trend ──────────────────────────────────────────
  const [history, setHistory] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  // ── Remediation ─────────────────────────────────────────────
  const [remediationSql, setRemediationSql] = useState<Record<string, string[]>>({});
  const [remediationLoading, setRemediationLoading] = useState<Record<string, boolean>>({});

  // ── Alert Rules ─────────────────────────────────────────────
  const [alertRules, setAlertRules] = useState<any[]>([]);
  const [alertRulesLoading, setAlertRulesLoading] = useState(false);
  const [firedAlerts, setFiredAlerts] = useState<any[]>([]);
  const [alertRulesExpanded, setAlertRulesExpanded] = useState(false);
  const [newRule, setNewRule] = useState({ name: "", metric: "match_rate", operator: "<", threshold: 0, severity: "warning" });

  // ── Init ───────────────────────────────────────────────────────
  useEffect(() => {
    api.get("/reconciliation/spark-status").then((s) => {
      setSparkStatus(s);
      if (s.cluster_id) setSparkClusterId(s.cluster_id);
      if (s.serverless != null) setSparkServerless(s.serverless);
    }).catch(() => {});
  }, []);

  // ── Auto-load preview when table is selected ───────────────────
  useEffect(() => {
    if (source && sourceSchema && sourceTable && dest) {
      loadPreview();
    } else {
      setPreview(null);
    }
  }, [source, sourceSchema, sourceTable, dest, destSchema]);

  async function loadPreview() {
    setPreviewLoading(true);
    setPreview(null);
    setResults(null);
    try {
      const data = await api.post("/reconciliation/preview", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: sourceSchema,
        table_name: sourceTable,
      });
      setPreview(data);
      // Auto-select all matching columns
      const matching = (data.column_match || [])
        .filter((c: any) => c.status === "match" || c.status === "type_mismatch")
        .map((c: any) => c.column);
      setSelectedCols(new Set(matching));
      setIgnoredCols(new Set());
      setKeyColumns(data.key_columns || []);
    } catch (e: any) {
      toast.error("Preview failed: " + (e.message || "Unknown error"));
    } finally {
      setPreviewLoading(false);
    }
  }

  // ── Run deep reconciliation ────────────────────────────────────
  async function runDeepReconciliation() {
    if (!source || !dest || !sourceSchema || !sourceTable) return;
    setLoading(true);
    setResults(null);
    setExpandedTable(null);
    const t = () => new Date().toLocaleTimeString();
    setLogs([
      `[${t()}] Starting deep reconciliation: ${source}.${sourceSchema}.${sourceTable} → ${dest}`,
      `[${t()}] Key columns: ${keyColumns.length > 0 ? keyColumns.join(", ") : "(auto-detect / hash match)"}`,
      `[${t()}] Columns: ${selectedCols.size} selected, ${ignoredCols.size} ignored`,
    ]);

    try {
      const data = await api.post("/reconciliation/deep-validate", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: sourceSchema,
        table_name: sourceTable,
        key_columns: keyColumns,
        include_columns: [...selectedCols],
        ignore_columns: [...ignoredCols],
        sample_diffs: sampleDiffs,
        ignore_nulls: ignoreNulls,
        ignore_case: ignoreCase,
        ignore_whitespace: ignoreWhitespace,
        decimal_precision: decimalPrecision,
      });
      const details = data.details || [];
      const newLogs: string[] = [];
      for (const d of details) {
        newLogs.push(
          `[${t()}] ${d.schema}.${d.table}: matched=${d.matched_rows} missing=${d.missing_in_dest} extra=${d.extra_in_dest} modified=${d.modified_rows}${d.error ? ` ERROR: ${d.error}` : ""}`
        );
      }
      newLogs.push(`[${t()}] Completed in ${data.duration_seconds ?? 0}s`);
      if (data.run_id) newLogs.push(`[${t()}] Stored in Delta → run_id: ${data.run_id}`);
      // Check for fired alerts from the response
      if (data.fired_alerts && data.fired_alerts.length > 0) {
        setFiredAlerts(data.fired_alerts);
        for (const alert of data.fired_alerts) {
          newLogs.push(`[${t()}] ALERT [${(alert.severity || "warning").toUpperCase()}]: ${alert.rule_name || alert.name} — ${alert.message || `${alert.metric} ${alert.operator} ${alert.threshold}`}`);
        }
      }
      setLogs((prev) => [...prev, ...newLogs]);
      setResults(data);
      // Fetch history after a successful run
      fetchHistory();
    } catch (e: any) {
      setLogs((prev) => [...prev, `[${t()}] ERROR: ${e.message}`]);
      toast.error(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function fetchHistory() {
    setHistoryLoading(true);
    try {
      const data = await api.get("/reconciliation/history?limit=10");
      setHistory(Array.isArray(data) ? data : data.runs || []);
    } catch {
      // History endpoint may not exist yet — silently ignore
    } finally {
      setHistoryLoading(false);
    }
  }

  // ── Alert rule CRUD ──────────────────────────────────────────
  async function fetchAlertRules() {
    setAlertRulesLoading(true);
    try {
      const data = await api.get("/reconciliation/alerts/rules");
      setAlertRules(Array.isArray(data) ? data : data.rules || []);
    } catch {
      // endpoint may not exist yet
    } finally {
      setAlertRulesLoading(false);
    }
  }

  async function createAlertRule() {
    if (!newRule.name.trim()) { toast.error("Rule name is required"); return; }
    try {
      await api.post("/reconciliation/alerts/rules", newRule);
      toast.success("Alert rule created");
      setNewRule({ name: "", metric: "match_rate", operator: "<", threshold: 0, severity: "warning" });
      fetchAlertRules();
    } catch (e: any) {
      toast.error("Failed to create rule: " + (e.message || "Unknown error"));
    }
  }

  async function deleteAlertRule(ruleId: string) {
    try {
      await api.delete(`/reconciliation/alerts/rules/${ruleId}`);
      toast.success("Rule deleted");
      fetchAlertRules();
    } catch (e: any) {
      toast.error("Failed to delete rule: " + (e.message || "Unknown error"));
    }
  }

  // ── Remediation ─────────────────────────────────────────────
  async function generateFixSql(d: any) {
    const fqn = `${d.schema}.${d.table}`;
    setRemediationLoading((prev) => ({ ...prev, [fqn]: true }));
    try {
      const data = await api.post("/reconciliation/remediate", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: d.schema,
        table_name: d.table,
        key_columns: keyColumns,
      });
      setRemediationSql((prev) => ({ ...prev, [fqn]: data.sql_statements || data.statements || [data.sql || "-- No SQL returned"] }));
    } catch (e: any) {
      toast.error("Remediation failed: " + (e.message || "Unknown error"));
    } finally {
      setRemediationLoading((prev) => ({ ...prev, [fqn]: false }));
    }
  }

  // Load alert rules on mount
  useEffect(() => { fetchAlertRules(); }, []);

  const toggleCol = (col: string) => {
    setSelectedCols((prev) => {
      const next = new Set(prev);
      if (next.has(col)) { next.delete(col); setIgnoredCols((ig) => new Set(ig).add(col)); }
      else { next.add(col); setIgnoredCols((ig) => { const n = new Set(ig); n.delete(col); return n; }); }
      return next;
    });
  };

  const toggleKey = (col: string) => {
    setKeyColumns((prev) => prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]);
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Deep Reconciliation"
        description="PySpark-based row-level diff — identifies missing, extra, and modified rows with column-level details."
        icon={Search}
        breadcrumbs={["Data Quality", "Reconciliation", "Deep"]}
      />

      {/* ── Catalog/Table Selection ─────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Source</label>
              <CatalogPicker catalog={source} schema={sourceSchema} table={sourceTable} onCatalogChange={setSource} onSchemaChange={setSourceSchema} onTableChange={setSourceTable} showTable idPrefix="deep-src" />
            </div>
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Destination</label>
              <CatalogPicker catalog={dest} schema={destSchema} table={destTable} onCatalogChange={setDest} onSchemaChange={setDestSchema} onTableChange={setDestTable} showTable idPrefix="deep-dst" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* ── Spark Connect ──────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-4">
          <div className="flex items-center gap-3 flex-wrap">
            <div className={`p-2 rounded-lg ${sparkStatus.available ? "bg-muted/40 text-foreground" : "bg-muted/40 text-muted-foreground"}`}>
              <Zap className="h-5 w-5" />
            </div>
            <div className="flex-1 min-w-[140px]">
              <p className="text-sm font-medium">{sparkStatus.available ? "Spark Connected" : "Spark Required"}</p>
              <p className="text-xs text-muted-foreground">{sparkStatus.available ? (sparkStatus.serverless ? "Serverless" : `Cluster: ${sparkStatus.cluster_id}`) : "Deep reconciliation requires Spark Connect"}</p>
            </div>
            <Input placeholder="Cluster ID" className="w-44 h-8 text-xs" value={sparkClusterId} onChange={(e) => setSparkClusterId(e.target.value)} />
            <label className="flex items-center gap-1 text-xs whitespace-nowrap"><input type="checkbox" checked={sparkServerless} onChange={(e) => setSparkServerless(e.target.checked)} /> Serverless</label>
            <Button size="sm" variant="outline" disabled={sparkConfiguring} onClick={async () => {
              setSparkConfiguring(true);
              try { const res = await api.post("/reconciliation/spark-configure", { cluster_id: sparkClusterId, serverless: sparkServerless }); setSparkStatus(res); toast.success(res.available ? "Connected!" : "Failed"); } catch (e: any) { toast.error(e.message); }
              setSparkConfiguring(false);
            }}>{sparkConfiguring ? <Loader2 className="h-3 w-3 animate-spin" /> : "Connect"}</Button>
          </div>
        </CardContent>
      </Card>

      {/* ── Preview ────────────────────────────────────────────────── */}
      {previewLoading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading preview...</div>
      )}

      {preview && !preview.error && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base flex items-center gap-2">
              <Eye className="h-4 w-4" /> Preview
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Scope summary */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Source</p>
                <p className="font-mono text-xs">{preview.source_table}</p>
                <p className="text-lg font-bold mt-1">{preview.source_count?.toLocaleString()} rows</p>
                <p className="text-xs text-muted-foreground">{preview.source_columns?.length} columns</p>
              </div>
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Destination</p>
                <p className="font-mono text-xs">{preview.dest_table}</p>
                <p className="text-lg font-bold mt-1">{preview.dest_count?.toLocaleString()} rows</p>
                <p className="text-xs text-muted-foreground">{preview.dest_columns?.length} columns</p>
              </div>
            </div>

            {/* Key columns */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Key Columns (click to toggle)</p>
              <div className="flex flex-wrap gap-1.5">
                {(preview.source_columns || []).map((c: any) => (
                  <Badge
                    key={c.name}
                    variant={keyColumns.includes(c.name) ? "default" : "outline"}
                    className={`text-xs cursor-pointer ${keyColumns.includes(c.name) ? "bg-[#E8453C] text-white" : ""}`}
                    onClick={() => toggleKey(c.name)}
                  >
                    {c.name}
                  </Badge>
                ))}
              </div>
              <p className="text-[10px] text-muted-foreground mt-1">{keyColumns.length > 0 ? `Join on: ${keyColumns.join(", ")}` : "No keys selected — will use full-row hash matching"}</p>
            </div>

            {/* Column match status + selection */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Columns (click to include/exclude)</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-1">
                {(preview.column_match || []).map((c: any) => {
                  const included = selectedCols.has(c.column);
                  const isKey = keyColumns.includes(c.column);
                  return (
                    <button
                      key={c.column}
                      onClick={() => toggleCol(c.column)}
                      className={`flex items-center gap-2 px-2.5 py-1.5 rounded text-xs text-left transition-colors ${
                        included ? "bg-muted/40 text-foreground" : "bg-transparent text-muted-foreground line-through opacity-50"
                      }`}
                    >
                      {c.status === "match" ? <CheckCircle className="h-3 w-3 text-green-500 shrink-0" /> :
                       c.status === "type_mismatch" ? <AlertTriangle className="h-3 w-3 text-amber-500 shrink-0" /> :
                       c.status === "missing_in_dest" ? <XCircle className="h-3 w-3 text-red-500 shrink-0" /> :
                       <Plus className="h-3 w-3 text-blue-500 shrink-0" />}
                      <span className="font-mono truncate">{c.column}</span>
                      {isKey && <Badge variant="outline" className="text-[8px] ml-auto shrink-0">KEY</Badge>}
                      {c.status === "type_mismatch" && (
                        <span className="text-[9px] text-amber-500 ml-auto shrink-0">{c.source_type}→{c.dest_type}</span>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Sample rows */}
            {preview.source_sample?.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Sample Rows (Source)</p>
                <div className="overflow-x-auto max-h-32 border rounded bg-background">
                  <table className="w-full text-xs font-mono">
                    <thead><tr className="border-b">{Object.keys(preview.source_sample[0]).map((col) => <th key={col} className="px-2 py-1 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                    <tbody>{preview.source_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[120px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Comparison Options */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-2">Options</p>
              <div className="flex flex-wrap items-center gap-4">
                <label className="flex items-center gap-1.5 text-xs">
                  <input type="checkbox" checked={ignoreNulls} onChange={(e) => setIgnoreNulls(e.target.checked)} />
                  Ignore NULLs
                  <span className="text-[10px] text-muted-foreground">(treat NULL == NULL as match)</span>
                </label>
                <label className="flex items-center gap-1.5 text-xs">
                  <input type="checkbox" checked={ignoreCase} onChange={(e) => setIgnoreCase(e.target.checked)} />
                  Ignore Case
                  <span className="text-[10px] text-muted-foreground">(case-insensitive string comparison)</span>
                </label>
                <label className="flex items-center gap-1.5 text-xs">
                  <input type="checkbox" checked={ignoreWhitespace} onChange={(e) => setIgnoreWhitespace(e.target.checked)} />
                  Ignore Whitespace
                  <span className="text-[10px] text-muted-foreground">(trim before comparing)</span>
                </label>
                <div className="flex items-center gap-1.5">
                  <label className="text-xs whitespace-nowrap">Decimal Precision:</label>
                  <Input
                    type="number"
                    min={0}
                    max={18}
                    className="w-16 h-7 text-xs"
                    value={decimalPrecision}
                    onChange={(e) => setDecimalPrecision(Number(e.target.value))}
                  />
                  <span className="text-[10px] text-muted-foreground">(0 = exact)</span>
                </div>
              </div>
            </div>

            {/* Run button */}
            <div className="flex items-center gap-3 pt-2">
              <div className="w-28">
                <label className="text-xs text-muted-foreground mb-1 block">Max Diff Samples</label>
                <Input type="number" min={1} max={100} value={sampleDiffs} onChange={(e) => setSampleDiffs(Number(e.target.value))} />
              </div>
              <Button
                onClick={runDeepReconciliation}
                disabled={loading || !sparkStatus.available || selectedCols.size === 0}
                className="mt-4"
              >
                {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <ArrowLeftRight className="h-4 w-4 mr-2" />}
                {loading ? "Running Deep Reconciliation..." : "Run Deep Reconciliation"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {preview?.error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
          <p className="font-medium">Preview error</p>
          <p className="mt-1 font-mono text-xs">{preview.error}</p>
        </div>
      )}

      {/* ── Log Panel ──────────────────────────────────────────────── */}
      {logs.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              {loading && <Loader2 className="h-3 w-3 animate-spin" />} Reconciliation Log
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="bg-muted/50 dark:bg-muted/30 border border-border rounded-lg p-3 max-h-48 overflow-y-auto font-mono text-xs leading-relaxed">
              {logs.map((line, i) => (
                <div key={i} className={line.includes("ERROR") ? "text-red-500" : line.includes("Completed") || line.includes("Stored") ? "text-blue-500" : "text-foreground/70"}>{line}</div>
              ))}
              {loading && <div className="text-muted-foreground animate-pulse mt-1">Running...</div>}
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Deep Results ───────────────────────────────────────────── */}
      {results && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            <SummaryCard label="Source Rows" value={(results.source_rows ?? 0).toLocaleString()} icon={Rows3} />
            <SummaryCard label="Dest Rows" value={(results.dest_rows ?? 0).toLocaleString()} icon={Rows3} />
            <SummaryCard label="Matched" value={(results.matched_rows ?? 0).toLocaleString()} color="green" icon={CheckCircle} />
            <SummaryCard label="Missing in Dest" value={(results.missing_in_dest ?? 0).toLocaleString()} color="red" icon={Minus} />
            <SummaryCard label="Extra in Dest" value={(results.extra_in_dest ?? 0).toLocaleString()} color="blue" icon={Plus} />
            <SummaryCard label="Modified" value={(results.modified_rows ?? 0).toLocaleString()} color="amber" icon={Pencil} />
          </div>

          {/* Export Toolbar */}
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" onClick={() => exportCsv(results)}>
              <Download className="h-3.5 w-3.5 mr-1.5" /> Export CSV
            </Button>
            <Button size="sm" variant="outline" onClick={() => exportJson(results)}>
              <FileJson className="h-3.5 w-3.5 mr-1.5" /> Export JSON
            </Button>
            <Button size="sm" variant="outline" onClick={() => window.print()}>
              <FileText className="h-3.5 w-3.5 mr-1.5" /> Export PDF
            </Button>
          </div>

          {/* Distribution Charts */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`grid gap-6 ${(results.details || []).length > 1 ? "grid-cols-1 md:grid-cols-2" : "grid-cols-1 max-w-md mx-auto"}`}>
                {/* Pie Chart */}
                <div>
                  <p className="text-xs text-muted-foreground text-center mb-2">Overall Breakdown</p>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie
                        data={[
                          { name: "Matched", value: results.matched_rows ?? 0 },
                          { name: "Missing", value: results.missing_in_dest ?? 0 },
                          { name: "Extra", value: results.extra_in_dest ?? 0 },
                          { name: "Modified", value: results.modified_rows ?? 0 },
                        ].filter((d) => d.value > 0)}
                        cx="50%"
                        cy="50%"
                        innerRadius={50}
                        outerRadius={90}
                        paddingAngle={2}
                        dataKey="value"
                        label={({ name, percent }) => `${name} ${(percent * 100).toFixed(1)}%`}
                      >
                        <Cell fill={CHART_COLORS.matched} />
                        <Cell fill={CHART_COLORS.missing} />
                        <Cell fill={CHART_COLORS.extra} />
                        <Cell fill={CHART_COLORS.modified} />
                      </Pie>
                      <Tooltip />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </div>

                {/* Bar Chart — only shown when there are multiple tables */}
                {(results.details || []).length > 1 && (
                  <div>
                    <p className="text-xs text-muted-foreground text-center mb-2">Per-Table Breakdown</p>
                    <ResponsiveContainer width="100%" height={260}>
                      <BarChart
                        data={(results.details || []).map((d: any) => ({
                          table: `${d.schema}.${d.table}`,
                          Matched: d.matched_rows ?? 0,
                          Missing: d.missing_in_dest ?? 0,
                          Extra: d.extra_in_dest ?? 0,
                          Modified: d.modified_rows ?? 0,
                        }))}
                      >
                        <XAxis dataKey="table" tick={{ fontSize: 10 }} />
                        <YAxis tick={{ fontSize: 10 }} />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="Matched" stackId="a" fill={CHART_COLORS.matched} />
                        <Bar dataKey="Missing" stackId="a" fill={CHART_COLORS.missing} />
                        <Bar dataKey="Extra" stackId="a" fill={CHART_COLORS.extra} />
                        <Bar dataKey="Modified" stackId="a" fill={CHART_COLORS.modified} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Per-table details */}
          {(results.details || []).map((d: any) => {
            const fqn = `${d.schema}.${d.table}`;
            const isExpanded = expandedTable === fqn;
            return (
              <Card key={fqn}>
                <CardHeader className="pb-2 cursor-pointer" onClick={() => setExpandedTable(isExpanded ? null : fqn)}>
                  <CardTitle className="text-sm flex items-center justify-between">
                    <span className="font-mono">{fqn}</span>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-green-500 border-green-500/30 text-[10px]">{d.matched_rows} matched</Badge>
                      {d.missing_in_dest > 0 && <Badge variant="outline" className="text-red-500 border-red-500/30 text-[10px]">{d.missing_in_dest} missing</Badge>}
                      {d.extra_in_dest > 0 && <Badge variant="outline" className="text-blue-500 border-blue-500/30 text-[10px]">{d.extra_in_dest} extra</Badge>}
                      {d.modified_rows > 0 && <Badge variant="outline" className="text-amber-500 border-amber-500/30 text-[10px]">{d.modified_rows} modified</Badge>}
                      {isExpanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
                    </div>
                  </CardTitle>
                </CardHeader>

                {isExpanded && (
                  <CardContent>
                    <Tabs defaultValue="missing">
                      <TabsList>
                        <TabsTrigger value="missing">Missing in Dest ({d.missing_in_dest})</TabsTrigger>
                        <TabsTrigger value="extra">Extra in Dest ({d.extra_in_dest})</TabsTrigger>
                        <TabsTrigger value="modified">Modified ({d.modified_rows})</TabsTrigger>
                        {d.column_impact && Object.keys(d.column_impact).length > 0 && (
                          <TabsTrigger value="column_impact">
                            <Columns3 className="h-3 w-3 mr-1" /> Column Impact
                          </TabsTrigger>
                        )}
                      </TabsList>

                      {/* Missing rows */}
                      <TabsContent value="missing">
                        {d.missing_sample?.length > 0 ? (
                          <div className="overflow-x-auto border rounded max-h-64">
                            <table className="w-full text-xs font-mono">
                              <thead><tr className="border-b bg-red-500/5">{Object.keys(d.missing_sample[0]).map((col) => <th key={col} className="px-2 py-1.5 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                              <tbody>{d.missing_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No missing rows found.</p>}
                      </TabsContent>

                      {/* Extra rows */}
                      <TabsContent value="extra">
                        {d.extra_sample?.length > 0 ? (
                          <div className="overflow-x-auto border rounded max-h-64">
                            <table className="w-full text-xs font-mono">
                              <thead><tr className="border-b bg-blue-500/5">{Object.keys(d.extra_sample[0]).map((col) => <th key={col} className="px-2 py-1.5 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                              <tbody>{d.extra_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No extra rows found.</p>}
                      </TabsContent>

                      {/* Modified rows with column diffs */}
                      <TabsContent value="modified">
                        {d.modified_sample?.length > 0 ? (
                          <div className="space-y-3">
                            {d.modified_sample.map((m: any, i: number) => (
                              <div key={i} className="border rounded-lg overflow-hidden">
                                <div className="bg-amber-500/5 px-3 py-1.5 text-xs font-mono">
                                  Key: {Object.entries(m.key || {}).map(([k, v]) => `${k}=${v}`).join(", ")}
                                </div>
                                <table className="w-full text-xs">
                                  <thead><tr className="border-b"><th className="px-3 py-1.5 text-left text-muted-foreground">Column</th><th className="px-3 py-1.5 text-left text-muted-foreground">Source</th><th className="px-3 py-1.5 text-left text-muted-foreground">Destination</th></tr></thead>
                                  <tbody>
                                    {(m.diffs || []).map((diff: any, j: number) => (
                                      <tr key={j} className="border-b border-border/30 bg-amber-500/5">
                                        <td className="px-3 py-1 font-mono font-medium">{diff.column}</td>
                                        <td className="px-3 py-1 font-mono text-red-400">{diff.source ?? "NULL"}</td>
                                        <td className="px-3 py-1 font-mono text-blue-400">{diff.dest ?? "NULL"}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            ))}
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No modified rows found (or no key columns defined).</p>}
                      </TabsContent>

                      {/* Column Impact heatmap */}
                      {d.column_impact && Object.keys(d.column_impact).length > 0 && (
                        <TabsContent value="column_impact">
                          <div className="space-y-1.5">
                            <p className="text-xs text-muted-foreground mb-2">Number of rows where each column differed between source and destination.</p>
                            {(() => {
                              const entries = Object.entries(d.column_impact as Record<string, number>).sort(([, a], [, b]) => (b as number) - (a as number));
                              const maxCount = Math.max(...entries.map(([, v]) => v as number), 1);
                              return entries.map(([col, count]) => {
                                const ratio = (count as number) / maxCount;
                                // Interpolate from light red to dark red
                                const r = 239;
                                const g = Math.round(68 + (1 - ratio) * 130);
                                const b = Math.round(68 + (1 - ratio) * 130);
                                const bgColor = `rgb(${r}, ${g}, ${b})`;
                                return (
                                  <div key={col} className="flex items-center gap-2 text-xs">
                                    <span className="font-mono w-36 truncate text-right text-muted-foreground shrink-0">{col}</span>
                                    <div className="flex-1 h-5 bg-muted/30 rounded overflow-hidden relative">
                                      <div
                                        className="h-full rounded transition-all"
                                        style={{
                                          width: `${Math.max(ratio * 100, 2)}%`,
                                          backgroundColor: bgColor,
                                        }}
                                      />
                                    </div>
                                    <span className="font-mono w-12 text-right font-medium shrink-0">{(count as number).toLocaleString()}</span>
                                  </div>
                                );
                              });
                            })()}
                          </div>
                        </TabsContent>
                      )}
                    </Tabs>

                    {/* Remediation: Generate Fix SQL */}
                    <div className="mt-4 border-t pt-4">
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={remediationLoading[fqn]}
                        onClick={() => generateFixSql(d)}
                      >
                        {remediationLoading[fqn] ? <Loader2 className="h-3 w-3 animate-spin mr-1.5" /> : <Wrench className="h-3.5 w-3.5 mr-1.5" />}
                        Generate Fix SQL
                      </Button>
                      {remediationSql[fqn] && remediationSql[fqn].length > 0 && (
                        <div className="mt-3 space-y-2">
                          <p className="text-xs text-muted-foreground font-semibold uppercase">Remediation SQL</p>
                          {remediationSql[fqn].map((sql, idx) => (
                            <div key={idx} className="relative group">
                              <pre className="bg-muted/50 border rounded-lg p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">{sql}</pre>
                              <Button
                                size="sm"
                                variant="ghost"
                                className="absolute top-1.5 right-1.5 h-6 w-6 p-0 opacity-0 group-hover:opacity-100 transition-opacity"
                                onClick={() => {
                                  navigator.clipboard.writeText(sql);
                                  toast.success("SQL copied to clipboard");
                                }}
                              >
                                <Copy className="h-3 w-3" />
                              </Button>
                            </div>
                          ))}
                          <p className="text-[10px] text-muted-foreground">Copy the SQL above and paste it into the SQL Workbench below to execute.</p>
                        </div>
                      )}
                    </div>
                  </CardContent>
                )}
              </Card>
            );
          })}
        </>
      )}

      {/* ── History / Trend ──────────────────────────────────────── */}
      {results && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <TrendingUp className="h-4 w-4" /> History
            </CardTitle>
          </CardHeader>
          <CardContent>
            {historyLoading && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading history...
              </div>
            )}
            {!historyLoading && history.length === 0 && (
              <p className="text-sm text-muted-foreground py-4">No history data available yet. Run reconciliations to build trend data.</p>
            )}
            {!historyLoading && history.length > 0 && (
              <ResponsiveContainer width="100%" height={240}>
                <LineChart
                  data={history.map((h: any) => ({
                    executed_at: h.executed_at ? new Date(h.executed_at).toLocaleDateString() : "",
                    match_rate_pct: h.match_rate_pct ?? 0,
                  }))}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.1} />
                  <XAxis dataKey="executed_at" tick={{ fontSize: 10 }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} unit="%" />
                  <Tooltip formatter={(v: number) => `${v.toFixed(1)}%`} />
                  <Line
                    type="monotone"
                    dataKey="match_rate_pct"
                    stroke={CHART_COLORS.matched}
                    strokeWidth={2}
                    dot={{ r: 4 }}
                    name="Match Rate"
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      )}

      {/* ── Alert Rules ──────────────────────────────────────────── */}
      <Card>
        <CardHeader className="pb-2 cursor-pointer" onClick={() => { setAlertRulesExpanded(!alertRulesExpanded); if (!alertRulesExpanded) fetchAlertRules(); }}>
          <CardTitle className="text-sm flex items-center justify-between">
            <span className="flex items-center gap-2"><Bell className="h-4 w-4" /> Alert Rules</span>
            {alertRulesExpanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
          </CardTitle>
        </CardHeader>
        {alertRulesExpanded && (
          <CardContent className="space-y-4">
            {/* Fired alerts from last run */}
            {firedAlerts.length > 0 && (
              <div className="border border-amber-500/30 bg-amber-500/5 rounded-lg p-3 space-y-1.5">
                <p className="text-xs font-semibold flex items-center gap-1.5 text-amber-500"><ShieldAlert className="h-3.5 w-3.5" /> Fired Alerts (last run)</p>
                {firedAlerts.map((alert, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <Badge variant="outline" className={`text-[9px] ${alert.severity === "critical" ? "text-red-500 border-red-500/40" : alert.severity === "warning" ? "text-amber-500 border-amber-500/40" : "text-blue-500 border-blue-500/40"}`}>
                      {(alert.severity || "info").toUpperCase()}
                    </Badge>
                    <span className="font-medium">{alert.rule_name || alert.name}</span>
                    <span className="text-muted-foreground">{alert.message || `${alert.metric} ${alert.operator} ${alert.threshold}`}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Existing rules */}
            {alertRulesLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading rules...</div>
            ) : alertRules.length === 0 ? (
              <p className="text-sm text-muted-foreground">No alert rules configured yet.</p>
            ) : (
              <div className="space-y-1.5">
                <p className="text-xs font-semibold text-muted-foreground uppercase">Existing Rules</p>
                {alertRules.map((rule) => (
                  <div key={rule.id || rule.name} className="flex items-center gap-2 bg-muted/30 rounded-lg px-3 py-2 text-xs">
                    <Badge variant="outline" className={`text-[9px] shrink-0 ${rule.severity === "critical" ? "text-red-500 border-red-500/40" : rule.severity === "warning" ? "text-amber-500 border-amber-500/40" : "text-blue-500 border-blue-500/40"}`}>
                      {(rule.severity || "info").toUpperCase()}
                    </Badge>
                    <span className="font-medium">{rule.name}</span>
                    <span className="text-muted-foreground font-mono">{rule.metric} {rule.operator} {rule.threshold}</span>
                    <Button size="sm" variant="ghost" className="ml-auto h-6 w-6 p-0 text-muted-foreground hover:text-red-500" onClick={() => deleteAlertRule(rule.id || rule.name)}>
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </div>
                ))}
              </div>
            )}

            {/* Create new rule form */}
            <div className="border rounded-lg p-3 space-y-2">
              <p className="text-xs font-semibold text-muted-foreground uppercase">Create New Rule</p>
              <div className="flex flex-wrap items-end gap-2">
                <div>
                  <label className="text-[10px] text-muted-foreground block mb-0.5">Name</label>
                  <Input className="h-7 text-xs w-40" placeholder="Rule name" value={newRule.name} onChange={(e) => setNewRule({ ...newRule, name: e.target.value })} />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground block mb-0.5">Metric</label>
                  <select className="h-7 text-xs rounded border bg-background px-2" value={newRule.metric} onChange={(e) => setNewRule({ ...newRule, metric: e.target.value })}>
                    <option value="match_rate">Match Rate (%)</option>
                    <option value="missing">Missing Rows</option>
                    <option value="extra">Extra Rows</option>
                    <option value="modified">Modified Rows</option>
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground block mb-0.5">Operator</label>
                  <select className="h-7 text-xs rounded border bg-background px-2" value={newRule.operator} onChange={(e) => setNewRule({ ...newRule, operator: e.target.value })}>
                    <option value="<">&lt;</option>
                    <option value=">">&gt;</option>
                    <option value="==">==</option>
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground block mb-0.5">Threshold</label>
                  <Input type="number" className="h-7 text-xs w-20" value={newRule.threshold} onChange={(e) => setNewRule({ ...newRule, threshold: Number(e.target.value) })} />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground block mb-0.5">Severity</label>
                  <select className="h-7 text-xs rounded border bg-background px-2" value={newRule.severity} onChange={(e) => setNewRule({ ...newRule, severity: e.target.value })}>
                    <option value="info">Info</option>
                    <option value="warning">Warning</option>
                    <option value="critical">Critical</option>
                  </select>
                </div>
                <Button size="sm" className="h-7 text-xs" onClick={createAlertRule}>
                  <Plus className="h-3 w-3 mr-1" /> Add Rule
                </Button>
              </div>
            </div>
          </CardContent>
        )}
      </Card>

      {/* ── SQL Workbench (floating bottom panel) ──────────────── */}
      <SqlWorkbench />
    </div>
  );
}
