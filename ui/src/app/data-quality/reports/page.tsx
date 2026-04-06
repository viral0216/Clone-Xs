// @ts-nocheck
import { useState, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  FileSpreadsheet, Loader2, Download, Play, CheckCircle2, XCircle,
  AlertTriangle, ShieldCheck, Activity, Clock, RefreshCw,
} from "lucide-react";

/* ── Types ────────────────────────────────────────────── */

interface ReportData {
  generatedAt: string;
  catalog: string;
  healthScore: any;
  slaStatus: any;
  incidents: any[];
  dqResults: any[];
}

/* ── Helpers ──────────────────────────────────────────── */

function downloadFile(data: string, filename: string, mimeType: string) {
  const blob = new Blob([data], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function toCSV(rows: any[], columns: string[]): string {
  const header = columns.join(",");
  const body = rows.map((row) =>
    columns.map((col) => {
      const val = row[col] ?? "";
      const str = String(val).replace(/"/g, '""');
      return `"${str}"`;
    }).join(",")
  );
  return [header, ...body].join("\n");
}

function scoreColor(score: number) {
  if (score >= 90) return "text-green-500";
  if (score >= 70) return "text-amber-500";
  return "text-red-500";
}

function scoreBg(score: number) {
  if (score >= 90) return "bg-green-500";
  if (score >= 70) return "bg-amber-500";
  return "bg-red-500";
}

/* ── Component ────────────────────────────────────────── */

export default function ReportsPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [report, setReport] = useState<ReportData | null>(null);

  /* ── Generate report ───────────────────────────────── */

  const generateReport = useCallback(async () => {
    if (!catalog) {
      toast.error("Select a catalog to scope the report.");
      return;
    }
    setLoading(true);
    setReport(null);
    try {
      const [healthScore, slaStatus, incidentsRaw, dqResults] = await Promise.all([
        api.get(`/data-quality/health-score/${encodeURIComponent(catalog)}`).catch(() => null),
        api.get("/governance/sla/status").catch(() => null),
        api.get("/data-quality/incidents", { limit: "100" }).catch(() => []),
        api.get("/governance/dq/results").catch(() => []),
      ]);

      const incidents = Array.isArray(incidentsRaw)
        ? incidentsRaw
        : incidentsRaw?.incidents ?? [];

      setReport({
        generatedAt: new Date().toISOString(),
        catalog,
        healthScore: healthScore || {},
        slaStatus: slaStatus || {},
        incidents: Array.isArray(incidents) ? incidents : [],
        dqResults: Array.isArray(dqResults) ? dqResults : [],
      });
      toast.success("Report generated successfully.");
    } catch (e: any) {
      toast.error(e.message || "Failed to generate report.");
    }
    setLoading(false);
  }, [catalog]);

  /* ── Export handlers ───────────────────────────────── */

  function exportCSV() {
    if (!report) return;
    const timestamp = report.generatedAt.replace(/[:.]/g, "-");

    // Incidents CSV
    if (report.incidents.length > 0) {
      const cols = ["id", "title", "severity", "status", "source", "detected_at", "table_fqn"];
      const csv = toCSV(report.incidents, cols);
      downloadFile(csv, `dq-report-incidents-${timestamp}.csv`, "text/csv");
    }

    // DQ Results CSV
    if (report.dqResults.length > 0) {
      const cols = Object.keys(report.dqResults[0]);
      const csv = toCSV(report.dqResults, cols);
      downloadFile(csv, `dq-report-rules-${timestamp}.csv`, "text/csv");
    }

    if (report.incidents.length === 0 && report.dqResults.length === 0) {
      toast.info("No tabular data to export as CSV.");
      return;
    }
    toast.success("CSV file(s) downloaded.");
  }

  function exportJSON() {
    if (!report) return;
    const timestamp = report.generatedAt.replace(/[:.]/g, "-");
    downloadFile(JSON.stringify(report, null, 2), `dq-report-${timestamp}.json`, "application/json");
    toast.success("JSON report downloaded.");
  }

  /* ── Derived data ──────────────────────────────────── */

  const health = report?.healthScore?.overall_score ?? report?.healthScore?.score ?? null;
  const sla = report?.slaStatus || {};
  const slaPassed = sla.passed ?? 0;
  const slaFailed = sla.failed ?? 0;
  const slaTotal = sla.total_rules ?? slaPassed + slaFailed;
  const slaHealth = sla.health_pct ?? (slaTotal > 0 ? ((slaPassed / slaTotal) * 100) : 100);

  const openIncidents = report?.incidents.filter((i) => i.status === "open" || !i.status) || [];
  const criticalIncidents = openIncidents.filter((i) => i.severity === "critical");

  const dqPassed = report?.dqResults.filter((r) => r.passed || r.status === "passed") || [];
  const dqFailed = report?.dqResults.filter((r) => !r.passed && r.status !== "passed") || [];

  /* ── Incident columns ──────────────────────────────── */

  const incidentColumns: Column[] = [
    {
      key: "title",
      label: "Title",
      sortable: true,
      render: (v) => <span className="text-xs font-medium">{v}</span>,
    },
    {
      key: "severity",
      label: "Severity",
      sortable: true,
      render: (v) => (
        <Badge
          variant="outline"
          className={`text-[10px] ${
            v === "critical"
              ? "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400"
              : v === "warning"
              ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
              : "text-sky-600 bg-sky-50 border-sky-200 dark:bg-sky-950/30 dark:text-sky-400"
          }`}
        >
          {v}
        </Badge>
      ),
    },
    {
      key: "status",
      label: "Status",
      sortable: true,
      render: (v) => (
        <Badge variant="outline" className={`text-[10px] ${v === "resolved" ? "text-green-600" : "text-amber-600"}`}>
          {v || "open"}
        </Badge>
      ),
    },
    {
      key: "source",
      label: "Source",
      sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{v}</span>,
    },
    {
      key: "table_fqn",
      label: "Table",
      sortable: true,
      render: (v) => <span className="text-xs font-mono text-muted-foreground">{v || "N/A"}</span>,
    },
    {
      key: "detected_at",
      label: "Detected",
      sortable: true,
      render: (v) => (
        <span className="text-xs text-muted-foreground">
          {v ? new Date(v).toLocaleDateString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "N/A"}
        </span>
      ),
    },
  ];

  /* ── DQ rule columns ───────────────────────────────── */

  const dqColumns: Column[] = [
    {
      key: "rule_name",
      label: "Rule",
      sortable: true,
      render: (v, row) => <span className="text-xs font-medium">{v || row.name || row.expectation_type || "Unnamed"}</span>,
    },
    {
      key: "table_fqn",
      label: "Table",
      sortable: true,
      render: (v) => <span className="text-xs font-mono text-muted-foreground">{v || "N/A"}</span>,
    },
    {
      key: "passed",
      label: "Result",
      sortable: true,
      render: (v, row) => {
        const ok = v === true || row.status === "passed";
        return ok ? (
          <span className="flex items-center gap-1 text-green-600 text-xs">
            <CheckCircle2 className="h-3.5 w-3.5" /> Passed
          </span>
        ) : (
          <span className="flex items-center gap-1 text-red-600 text-xs">
            <XCircle className="h-3.5 w-3.5" /> Failed
          </span>
        );
      },
    },
  ];

  /* ── Render ────────────────────────────────────────── */

  return (
    <div className="space-y-4">
      <PageHeader
        title="DQ Reports"
        icon={FileSpreadsheet}
        breadcrumbs={["Data Quality", "Discovery", "Reports"]}
        description="Generate and export data quality reports covering health scores, SLA compliance, incidents, and DQ rule results."
      />

      {/* Scope & generate */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table={table}
              onCatalogChange={(v) => { setCatalog(v); setSchema(""); setTable(""); }}
              onSchemaChange={(v) => { setSchema(v); setTable(""); }}
              onTableChange={setTable}
            />
            <Button onClick={generateReport} disabled={loading || !catalog}>
              {loading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              {loading ? "Generating..." : "Generate Report"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Loading state */}
      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3 text-sm">Generating report...</p>
          </CardContent>
        </Card>
      )}

      {/* Report preview */}
      {report && !loading && (
        <>
          {/* Timestamp & export buttons */}
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-muted-foreground" />
              <span className="text-xs text-muted-foreground">
                Report generated:{" "}
                {new Date(report.generatedAt).toLocaleString("en-US", {
                  dateStyle: "medium",
                  timeStyle: "short",
                })}
              </span>
              <Badge variant="outline" className="text-[10px]">
                {report.catalog}
              </Badge>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={exportCSV} className="text-xs gap-1.5">
                <Download className="h-3.5 w-3.5" /> Export CSV
              </Button>
              <Button variant="outline" size="sm" onClick={exportJSON} className="text-xs gap-1.5">
                <Download className="h-3.5 w-3.5" /> Export JSON
              </Button>
            </div>
          </div>

          {/* Summary KPI cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {/* Health score */}
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                {health !== null ? (
                  <>
                    <p className={`text-3xl font-bold ${scoreColor(health)}`}>{health}</p>
                    <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden mx-auto mt-1.5">
                      <div
                        className={`h-full rounded-full ${scoreBg(health)}`}
                        style={{ width: `${Math.min(health, 100)}%` }}
                      />
                    </div>
                  </>
                ) : (
                  <p className="text-2xl font-bold text-muted-foreground">N/A</p>
                )}
                <p className="text-xs text-muted-foreground mt-1">Health Score</p>
              </CardContent>
            </Card>

            {/* SLA compliance */}
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className={`text-3xl font-bold ${scoreColor(slaHealth)}`}>
                  {slaHealth.toFixed(1)}%
                </p>
                <p className="text-[10px] text-muted-foreground mt-1">
                  {slaPassed}/{slaTotal} passing
                </p>
                <p className="text-xs text-muted-foreground mt-0.5">SLA Compliance</p>
              </CardContent>
            </Card>

            {/* Open incidents */}
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className={`text-3xl font-bold ${openIncidents.length > 0 ? "text-amber-500" : "text-green-500"}`}>
                  {openIncidents.length}
                </p>
                {criticalIncidents.length > 0 && (
                  <p className="text-[10px] text-red-500 mt-0.5">
                    {criticalIncidents.length} critical
                  </p>
                )}
                <p className="text-xs text-muted-foreground mt-1">Open Incidents</p>
              </CardContent>
            </Card>

            {/* DQ pass/fail */}
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <div className="flex items-center justify-center gap-3">
                  <div>
                    <p className="text-xl font-bold text-green-500">{dqPassed.length}</p>
                    <p className="text-[10px] text-muted-foreground">Passed</p>
                  </div>
                  <div className="w-px h-8 bg-border" />
                  <div>
                    <p className={`text-xl font-bold ${dqFailed.length > 0 ? "text-red-500" : "text-foreground"}`}>
                      {dqFailed.length}
                    </p>
                    <p className="text-[10px] text-muted-foreground">Failed</p>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground mt-1">DQ Rules</p>
              </CardContent>
            </Card>
          </div>

          {/* Top incidents table */}
          {report.incidents.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" /> Top Incidents ({report.incidents.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <DataTable
                  data={report.incidents.slice(0, 50)}
                  columns={incidentColumns}
                  searchable
                  searchKeys={["title", "severity", "source", "table_fqn"]}
                  pageSize={10}
                  compact
                  tableId="report-incidents"
                />
              </CardContent>
            </Card>
          )}

          {/* DQ rule results */}
          {report.dqResults.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  <Activity className="h-4 w-4" /> DQ Rule Results ({report.dqResults.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <DataTable
                  data={report.dqResults}
                  columns={dqColumns}
                  searchable
                  searchKeys={["rule_name", "table_fqn", "name", "expectation_type"]}
                  pageSize={10}
                  compact
                  tableId="report-dq-results"
                />
              </CardContent>
            </Card>
          )}

          {/* Empty state */}
          {report.incidents.length === 0 && report.dqResults.length === 0 && health === null && (
            <Card className="bg-card border-border">
              <CardContent className="py-10 text-center">
                <CheckCircle2 className="h-8 w-8 mx-auto text-green-500 mb-2" />
                <p className="text-foreground font-medium">No data quality issues found</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Run monitoring, SLA checks, and DQ rules first to populate report data.
                </p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
