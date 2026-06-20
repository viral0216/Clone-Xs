// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AlertTriangle, Download, Loader2, ChevronDown, ChevronUp } from "lucide-react";
import { usePersistedState } from "@/hooks/usePersistedState";

function SeverityBadge({ severity }: { severity: string }) {
  const cls: Record<string, string> = {
    critical: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    high: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300",
    medium: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    low: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium capitalize ${cls[severity?.toLowerCase()] ?? "bg-muted text-muted-foreground"}`}>
      {severity}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  const cls: Record<string, string> = {
    FAIL: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    WARN: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    PASS: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300",
    NOT_APPLICABLE: "bg-muted text-muted-foreground",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls[status?.toUpperCase()] ?? "bg-muted text-muted-foreground"}`}>
      {status}
    </span>
  );
}

function ExpandRow({ finding }: { finding: any }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button
        onClick={() => setOpen(v => !v)}
        className="flex items-center gap-1 text-xs text-primary hover:underline"
      >
        {open ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        {open ? "Collapse" : "Details"}
      </button>
      {open && (
        <div className="mt-2 space-y-1.5 text-xs text-muted-foreground bg-muted/30 rounded-md p-3">
          {finding.description && (
            <p>
              <span className="font-medium text-foreground">Description:</span>{" "}
              {typeof finding.description === "string" ? finding.description : JSON.stringify(finding.description)}
            </p>
          )}
          {finding.recommendation && (
            <p>
              <span className="font-medium text-foreground">Recommendation:</span>{" "}
              {typeof finding.recommendation === "string" ? finding.recommendation : JSON.stringify(finding.recommendation)}
            </p>
          )}
          {finding.current_state && (
            <div>
              <span className="font-medium text-foreground">Current state:</span>{" "}
              {typeof finding.current_state === "string"
                ? <span>{finding.current_state}</span>
                : <pre className="mt-1 whitespace-pre-wrap break-all text-xs font-mono">{JSON.stringify(finding.current_state, null, 2)}</pre>}
            </div>
          )}
          {finding.effort && (
            <p>
              <span className="font-medium text-foreground">Effort:</span>{" "}
              {typeof finding.effort === "string" ? finding.effort : JSON.stringify(finding.effort)}
            </p>
          )}
          {finding.reference_url && (
            <p>
              <a href={finding.reference_url} target="_blank" rel="noreferrer" className="text-primary hover:underline">
                Reference docs →
              </a>
            </p>
          )}
        </div>
      )}
    </div>
  );
}

const SEVERITIES = ["critical", "high", "medium", "low"];
const STATUSES = ["FAIL", "WARN", "PASS", "NOT_APPLICABLE"];

export default function FindingsPage() {
  const [findings, setFindings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [severity, setSeverity] = usePersistedState<string>("assessment-filter-severity", "");
  const [status, setStatus] = usePersistedState<string>("assessment-filter-status", "FAIL,WARN");

  async function load() {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (severity) params.set("severity", severity);
      if (status) params.set("status", status);
      const data = await api.get(`/assessment/findings?${params}`);
      setFindings(Array.isArray(data) ? data : []);
    } catch {}
    setLoading(false);
  }

  useEffect(() => { load(); }, [severity, status]);

  const columns = [
    {
      key: "check_id",
      label: "Check ID",
      width: "100px",
      render: (v: string) => <span className="font-mono text-xs">{v}</span>,
    },
    {
      key: "title",
      label: "Finding",
      render: (v: string, row: any) => (
        <div>
          <p className="text-sm font-medium leading-tight">{v}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{row.category}</p>
        </div>
      ),
    },
    {
      key: "severity",
      label: "Severity",
      width: "90px",
      render: (v: string) => <SeverityBadge severity={v} />,
    },
    {
      key: "status",
      label: "Status",
      width: "100px",
      render: (v: string) => <StatusBadge status={v} />,
    },
    {
      key: "_expand",
      label: "",
      width: "80px",
      render: (_: any, row: any) => <ExpandRow finding={row} />,
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Findings"
        icon={AlertTriangle}
        breadcrumbs={["Assessment", "Findings"]}
        description="All security check results — filter by severity and status to focus on what matters most."
        actions={
          <Button
            size="sm"
            variant="outline"
            onClick={() => window.open("/api/assessment/export/csv", "_blank")}
          >
            <Download className="h-4 w-4 mr-1.5" />
            Export CSV
          </Button>
        }
      />

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-muted-foreground">Severity:</label>
          <div className="flex gap-1">
            <button
              onClick={() => setSeverity("")}
              className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${!severity ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
            >
              All
            </button>
            {SEVERITIES.map(s => (
              <button
                key={s}
                onClick={() => setSeverity(severity === s ? "" : s)}
                className={`px-2.5 py-1 rounded text-xs font-medium capitalize transition-colors ${severity === s ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-muted-foreground">Status:</label>
          <div className="flex gap-1">
            {[
              { val: "FAIL,WARN", label: "Issues" },
              { val: "FAIL", label: "FAIL" },
              { val: "WARN", label: "WARN" },
              { val: "PASS", label: "PASS" },
              { val: "", label: "All" },
            ].map(({ val, label }) => (
              <button
                key={label}
                onClick={() => setStatus(val)}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${status === val ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
      </div>

      <DataTable
        data={findings}
        columns={columns}
        searchable
        pageSize={25}
        tableId="assessment-findings"
        emptyMessage={loading ? "Loading findings…" : "No findings match the current filters."}
      />
    </div>
  );
}
