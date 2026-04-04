// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { BarChart3, CheckCircle2, XCircle, Play, Loader2, ShieldCheck, AlertTriangle } from "lucide-react";
import { toast } from "sonner";

export default function DQDashboardPage() {
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);

  useEffect(() => { load(); }, []);
  async function load() { setLoading(true); try { const d = await api.get("/governance/dq/results"); setResults(Array.isArray(d) ? d : []); } catch {} setLoading(false); }
  async function runAll() { setRunning(true); try { await api.post("/governance/dq/run", {}); toast.success("Rules executed"); load(); } catch (e: any) { toast.error(e.message); } setRunning(false); }

  const passed = results.filter(r => r.passed === true || r.passed === "true").length;
  const failed = results.length - passed;
  const passRate = results.length > 0 ? Math.round((passed / results.length) * 100) : 100;
  const critical = results.filter(r => (r.passed === false || r.passed === "false") && r.severity === "critical").length;

  const columns: Column[] = [
    { key: "passed", label: "Status", sortable: true, render: (v) => {
      const pass = v === true || v === "true";
      return pass ? <CheckCircle2 className="h-4 w-4 text-foreground" /> : <XCircle className="h-4 w-4 text-red-600" />;
    }},
    { key: "rule_name", label: "Rule", sortable: true, render: (v) => <span className="font-medium">{v}</span> },
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge className={v === "critical" ? "bg-red-100 text-red-800" : v === "warning" ? "bg-muted/40 text-foreground" : "bg-muted/50 text-foreground"}>{v}</Badge> },
    { key: "total_rows", label: "Total", sortable: true, align: "right", render: (v) => Number(v || 0).toLocaleString() },
    { key: "failed_rows", label: "Failed", sortable: true, align: "right", render: (v) => <span className="font-medium text-red-600">{Number(v || 0).toLocaleString()}</span> },
    { key: "failure_rate", label: "Rate", sortable: true, align: "right", render: (v) => `${((Number(v) || 0) * 100).toFixed(2)}%` },
    { key: "execution_time_ms", label: "Time", sortable: true, align: "right", render: (v) => <span className="text-xs text-muted-foreground">{v}ms</span> },
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="DQ Dashboard" icon={BarChart3} breadcrumbs={["Governance", "DQ Dashboard"]} description="Data quality health at a glance — pass rates, failing rules, and severity breakdown." />
      <div className="flex gap-3">
        <Button onClick={runAll} disabled={running}>{running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}Run All Rules</Button>
        <Button variant="ghost" onClick={load}>Refresh</Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4 text-center"><p className="text-3xl font-bold">{results.length}</p><p className="text-xs text-muted-foreground">Total Checks</p></CardContent></Card>
        <Card><CardContent className="pt-4 text-center"><p className="text-3xl font-bold text-foreground">{passed}</p><p className="text-xs text-muted-foreground">Passing</p></CardContent></Card>
        <Card><CardContent className="pt-4 text-center"><p className="text-3xl font-bold text-red-600">{failed}</p><p className="text-xs text-muted-foreground">Failing</p></CardContent></Card>
        <Card><CardContent className="pt-4 text-center"><p className={`text-3xl font-bold ${passRate >= 90 ? "text-foreground" : passRate >= 70 ? "text-muted-foreground" : "text-red-600"}`}>{passRate}%</p><p className="text-xs text-muted-foreground">Pass Rate</p></CardContent></Card>
      </div>

      {critical > 0 && (
        <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800">
          <AlertTriangle className="h-5 w-5 text-red-600" />
          <p className="text-sm font-medium text-red-800 dark:text-red-300">{critical} critical rule{critical > 1 ? "s" : ""} failing — immediate attention required</p>
        </div>
      )}

      <Card><CardHeader className="pb-2"><CardTitle className="text-base">Latest Results</CardTitle></CardHeader>
        <CardContent>
          <DataTable
            data={results}
            columns={columns}
            searchable
            searchKeys={["rule_name", "table_fqn", "severity"]}
            pageSize={25}
            compact
            tableId="dq-dashboard-table"
            emptyMessage="No results. Run DQ rules to see results here."
            rowClassName={(r) => {
              const pass = r.passed === true || r.passed === "true";
              return !pass ? "bg-red-50/50 dark:bg-red-950/10" : "";
            }}
          />
        </CardContent>
      </Card>
    </div>
  );
}
