// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { ClipboardCheck, CheckCircle2, XCircle, Search } from "lucide-react";

export default function DQResultsPage() {
  const [results, setResults] = useState<any[]>([]);
  const [filter, setFilter] = useState("");
  const [sevFilter, setSevFilter] = useState("");

  useEffect(() => { api.get("/governance/dq/results").then(d => setResults(Array.isArray(d) ? d : [])).catch(() => {}); }, []);

  const filtered = results.filter(r => (!filter || r.table_fqn?.includes(filter) || r.rule_name?.includes(filter)) && (!sevFilter || r.severity === sevFilter));

  return (
    <div className="space-y-6">
      <PageHeader title="Validation Results" icon={ClipboardCheck} breadcrumbs={["Governance", "DQ Results"]} description="Detailed data quality validation results with per-rule pass/fail status." />
      <div className="flex gap-3">
        <div className="relative flex-1 max-w-md"><Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" /><Input value={filter} onChange={e => setFilter(e.target.value)} placeholder="Filter by table or rule..." className="pl-10" /></div>
        <select value={sevFilter} onChange={e => setSevFilter(e.target.value)} className="border rounded px-3 py-2 text-sm bg-background"><option value="">All Severities</option><option value="critical">Critical</option><option value="warning">Warning</option><option value="info">Info</option></select>
      </div>
      <Card><CardContent className="pt-4"><div className="overflow-x-auto"><table className="w-full text-sm"><thead><tr className="border-b"><th className="text-left py-2 px-3">Status</th><th className="text-left py-2 px-3">Rule</th><th className="text-left py-2 px-3">Table</th><th className="text-left py-2 px-3">Column</th><th className="text-left py-2 px-3">Type</th><th className="text-left py-2 px-3">Severity</th><th className="text-right py-2 px-3">Total</th><th className="text-right py-2 px-3">Failed</th><th className="text-right py-2 px-3">Rate</th><th className="text-right py-2 px-3">Time</th><th className="text-left py-2 px-3">Timestamp</th></tr></thead>
        <tbody>{filtered.map((r, i) => { const pass = r.passed === true || r.passed === "true"; return (
          <tr key={i} className={`border-b hover:bg-accent/30 ${!pass && r.severity === "critical" ? "bg-red-50/50 dark:bg-red-950/10" : !pass ? "bg-amber-50/30 dark:bg-amber-950/10" : ""}`}>
            <td className="py-2 px-3">{pass ? <CheckCircle2 className="h-4 w-4 text-green-600" /> : <XCircle className="h-4 w-4 text-red-600" />}</td>
            <td className="py-2 px-3 font-medium">{r.rule_name}</td><td className="py-2 px-3 font-mono text-xs">{r.table_fqn}</td><td className="py-2 px-3 font-mono text-xs">{r.column_name || "—"}</td>
            <td className="py-2 px-3"><Badge variant="outline" className="text-xs">{r.rule_type}</Badge></td>
            <td className="py-2 px-3"><Badge className={r.severity === "critical" ? "bg-red-100 text-red-800" : r.severity === "warning" ? "bg-amber-100 text-amber-800" : "bg-blue-100 text-blue-800"}>{r.severity}</Badge></td>
            <td className="py-2 px-3 text-right">{Number(r.total_rows||0).toLocaleString()}</td><td className="py-2 px-3 text-right text-red-600">{Number(r.failed_rows||0).toLocaleString()}</td>
            <td className="py-2 px-3 text-right">{((Number(r.failure_rate)||0)*100).toFixed(2)}%</td><td className="py-2 px-3 text-right text-xs">{r.execution_time_ms}ms</td>
            <td className="py-2 px-3 text-xs text-muted-foreground">{r.executed_at?.slice(0,16)}</td>
          </tr>); })}{filtered.length === 0 && <tr><td colSpan={11} className="text-center py-8 text-muted-foreground">No results</td></tr>}</tbody></table></div></CardContent></Card>
    </div>
  );
}
