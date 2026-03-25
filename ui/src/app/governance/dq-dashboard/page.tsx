// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
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
        <CardContent><div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="border-b border-border">
              <th className="text-left py-2 px-3">Status</th><th className="text-left py-2 px-3">Rule</th><th className="text-left py-2 px-3">Table</th>
              <th className="text-left py-2 px-3">Severity</th><th className="text-right py-2 px-3">Total</th><th className="text-right py-2 px-3">Failed</th>
              <th className="text-right py-2 px-3">Rate</th><th className="text-right py-2 px-3">Time</th>
            </tr></thead>
            <tbody>
              {results.map((r, i) => {
                const pass = r.passed === true || r.passed === "true";
                return (
                  <tr key={i} className={`border-b border-border ${!pass ? "bg-red-50/50 dark:bg-red-950/10" : ""}`}>
                    <td className="py-2 px-3">{pass ? <CheckCircle2 className="h-4 w-4 text-foreground" /> : <XCircle className="h-4 w-4 text-red-600" />}</td>
                    <td className="py-2 px-3 font-medium">{r.rule_name}</td>
                    <td className="py-2 px-3 font-mono text-xs">{r.table_fqn}</td>
                    <td className="py-2 px-3"><Badge className={r.severity === "critical" ? "bg-red-100 text-red-800" : r.severity === "warning" ? "bg-muted/40 text-foreground" : "bg-muted/50 text-foreground"}>{r.severity}</Badge></td>
                    <td className="py-2 px-3 text-right">{Number(r.total_rows || 0).toLocaleString()}</td>
                    <td className="py-2 px-3 text-right font-medium text-red-600">{Number(r.failed_rows || 0).toLocaleString()}</td>
                    <td className="py-2 px-3 text-right">{((Number(r.failure_rate) || 0) * 100).toFixed(2)}%</td>
                    <td className="py-2 px-3 text-right text-xs text-muted-foreground">{r.execution_time_ms}ms</td>
                  </tr>
                );
              })}
              {results.length === 0 && <tr><td colSpan={8} className="text-center py-8 text-muted-foreground">No results. Run DQ rules to see results here.</td></tr>}
            </tbody>
          </table>
        </div></CardContent>
      </Card>
    </div>
  );
}
