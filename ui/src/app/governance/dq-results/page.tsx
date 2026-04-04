// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { ClipboardCheck, CheckCircle2, XCircle } from "lucide-react";

export default function DQResultsPage() {
  const [results, setResults] = useState<any[]>([]);
  const [sevFilter, setSevFilter] = useState("");

  useEffect(() => { api.get("/governance/dq/results").then(d => setResults(Array.isArray(d) ? d : [])).catch(() => {}); }, []);

  const filtered = results.filter(r => (!sevFilter || r.severity === sevFilter));

  const columns: Column[] = [
    { key: "passed", label: "Status", sortable: true, render: (v) => {
      const pass = v === true || v === "true";
      return pass ? <CheckCircle2 className="h-4 w-4 text-foreground" /> : <XCircle className="h-4 w-4 text-red-600" />;
    }},
    { key: "rule_name", label: "Rule", sortable: true, render: (v) => <span className="font-medium">{v}</span> },
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "column_name", label: "Column", sortable: true, render: (v) => <span className="font-mono text-xs">{v || "—"}</span> },
    { key: "rule_type", label: "Type", sortable: true, render: (v) => <Badge variant="outline" className="text-xs">{v}</Badge> },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge className={v === "critical" ? "bg-red-100 text-red-800" : v === "warning" ? "bg-muted/40 text-foreground" : "bg-muted/50 text-foreground"}>{v}</Badge> },
    { key: "total_rows", label: "Total", sortable: true, align: "right", render: (v) => Number(v || 0).toLocaleString() },
    { key: "failed_rows", label: "Failed", sortable: true, align: "right", render: (v) => <span className="text-red-600">{Number(v || 0).toLocaleString()}</span> },
    { key: "failure_rate", label: "Rate", sortable: true, align: "right", render: (v) => `${((Number(v) || 0) * 100).toFixed(2)}%` },
    { key: "execution_time_ms", label: "Time", sortable: true, align: "right", render: (v) => <span className="text-xs">{v}ms</span> },
    { key: "executed_at", label: "Timestamp", sortable: true, render: (v) => <span className="text-xs text-muted-foreground">{v?.slice(0, 16)}</span> },
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="Validation Results" icon={ClipboardCheck} breadcrumbs={["Governance", "DQ Results"]} description="Detailed data quality validation results with per-rule pass/fail status." />
      <div className="flex gap-3">
        <select value={sevFilter} onChange={e => setSevFilter(e.target.value)} className="border rounded px-3 py-2 text-sm bg-background"><option value="">All Severities</option><option value="critical">Critical</option><option value="warning">Warning</option><option value="info">Info</option></select>
      </div>
      <Card><CardContent className="pt-4">
        <DataTable
          data={filtered}
          columns={columns}
          searchable
          searchKeys={["rule_name", "table_fqn", "column_name", "rule_type", "severity"]}
          pageSize={25}
          compact
          tableId="dq-results-table"
          emptyMessage="No results"
          rowClassName={(r) => {
            const pass = r.passed === true || r.passed === "true";
            return !pass && r.severity === "critical" ? "bg-red-50/50 dark:bg-red-950/10" : !pass ? "bg-muted/20 dark:bg-white/5" : "";
          }}
        />
      </CardContent></Card>
    </div>
  );
}
