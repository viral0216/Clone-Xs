// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import {
  Loader2, XCircle, ShieldCheck, Download, FileText, CheckCircle,
  AlertTriangle, Shield, Lock, Tag, Users, ChevronDown, ChevronUp,
  ClipboardCheck, BarChart3,
} from "lucide-react";

/* ── helpers ─────────────────────────────────────────────── */

function statusBadge(status: string) {
  switch (status?.toUpperCase()) {
    case "COMPLIANT":
      return <Badge className="bg-green-600 text-white">{status}</Badge>;
    case "NON_COMPLIANT": case "NON-COMPLIANT":
      return <Badge variant="destructive">NON COMPLIANT</Badge>;
    case "WARNING":
      return <Badge className="bg-yellow-500 text-white">{status}</Badge>;
    case "ERROR":
      return <Badge variant="destructive">ERROR</Badge>;
    default:
      return <Badge variant="outline">{status || "Unknown"}</Badge>;
  }
}

function scoreColor(score: number) {
  if (score >= 80) return "text-green-600 bg-green-50 border-green-200";
  if (score >= 50) return "text-yellow-600 bg-yellow-50 border-yellow-200";
  return "text-red-600 bg-red-50 border-red-200";
}

function statusIcon(status: string, size = "h-7 w-7") {
  switch (status?.toUpperCase()) {
    case "COMPLIANT":
      return <CheckCircle className={`${size} text-green-600`} />;
    case "WARNING":
      return <AlertTriangle className={`${size} text-yellow-600`} />;
    case "NON_COMPLIANT": case "NON-COMPLIANT": case "ERROR":
      return <XCircle className={`${size} text-red-600`} />;
    default:
      return <ShieldCheck className={`${size} text-muted-foreground`} />;
  }
}

function sectionBorder(status: string) {
  switch (status?.toUpperCase()) {
    case "COMPLIANT": return "border-l-4 border-l-green-500";
    case "WARNING": return "border-l-4 border-l-yellow-500";
    case "NON_COMPLIANT": case "NON-COMPLIANT": return "border-l-4 border-l-red-500";
    default: return "border-l-4 border-l-gray-300";
  }
}

function scoreBadge(score: number) {
  if (score >= 80) return <Badge className="bg-green-100 text-green-700 font-bold">{score}%</Badge>;
  if (score >= 50) return <Badge className="bg-yellow-100 text-yellow-700 font-bold">{score}%</Badge>;
  return <Badge className="bg-red-100 text-red-700 font-bold">{score}%</Badge>;
}

/* ── report types ────────────────────────────────────────── */

const REPORT_TYPES = [
  { value: "data_governance", label: "Data Governance", icon: ShieldCheck },
  { value: "pii_audit", label: "PII Audit", icon: Shield },
  { value: "permission_audit", label: "Permission Audit", icon: Lock },
  { value: "tag_coverage", label: "Tag Coverage", icon: Tag },
  { value: "ownership_audit", label: "Ownership Audit", icon: Users },
  { value: "full_report", label: "Full Report", icon: FileText },
];

const DATE_REPORT_TYPES = ["data_governance", "full_report"];

/* ── page ─────────────────────────────────────────────────── */

export default function CompliancePage() {
  const { job, run, isRunning } = usePageJob("compliance");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [reportType, setReportType] = useState(job?.params?.reportType || "data_governance");
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [expandedSections, setExpandedSections] = useState<Record<number, boolean>>({ 0: true });

  const results = job?.data as any;
  const summary = results?.summary;
  const sections = results?.sections || [];
  const score = results?.score ?? 0;

  const toggleSection = (i: number) => {
    setExpandedSections((prev) => ({ ...prev, [i]: !prev[i] }));
  };

  const handleGenerate = () => {
    setExpandedSections({ 0: true });
    run({ catalog, reportType }, () =>
      api.post("/compliance", {
        catalog,
        report_type: reportType,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
      })
    );
  };

  function downloadReport() {
    if (!results) return;
    const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `compliance-${catalog}-${reportType}-${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Compliance"
        icon={ShieldCheck}
        breadcrumbs={["Analysis", "Compliance"]}
        description="Generate governance and compliance reports — permission audits, tag coverage, PII exposure, ownership mapping, and access control analysis across catalogs."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/"
        docsLabel="Unity Catalog governance"
      />

      {/* Controls */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema=""
              table=""
              onCatalogChange={setCatalog}
              onSchemaChange={() => {}}
              onTableChange={() => {}}
              showSchema={false}
              showTable={false}
            />
            <div className="flex-1">
              <label className="text-sm font-medium text-foreground">Report Type</label>
              <select
                value={reportType}
                onChange={(e) => setReportType(e.target.value)}
                className="flex h-10 w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground"
              >
                {REPORT_TYPES.map((rt) => (
                  <option key={rt.value} value={rt.value}>{rt.label}</option>
                ))}
              </select>
            </div>
            <Button onClick={handleGenerate} disabled={!catalog || isRunning} className="text-white">
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ShieldCheck className="h-4 w-4 mr-2" />}
              {isRunning ? "Generating..." : "Generate Report"}
            </Button>
            {results && !results.error && (
              <Button variant="outline" onClick={downloadReport}>
                <Download className="h-4 w-4 mr-2" />Export
              </Button>
            )}
          </div>

          {/* Date range */}
          {DATE_REPORT_TYPES.includes(reportType) && (
            <div className="flex gap-4 items-end">
              <div>
                <label className="text-xs font-medium text-muted-foreground">From Date</label>
                <Input type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} className="w-40" />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">To Date</label>
                <Input type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} className="w-40" />
              </div>
              <span className="text-xs text-muted-foreground pb-2">Optional — filters audit trail operations</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Score banner */}
      {summary && !results.error && (
        <Card className={`border ${scoreColor(score)}`}>
          <CardContent className="pt-6 pb-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {statusIcon(results.status)}
                <div>
                  <p className="font-bold text-xl text-foreground">
                    {REPORT_TYPES.find((r) => r.value === reportType)?.label || reportType}
                  </p>
                  <p className="text-sm text-foreground/70">
                    Catalog: <span className="font-semibold text-foreground">{catalog}</span>
                    {summary.generated_at && <span className="ml-2 text-muted-foreground">· {new Date(summary.generated_at).toLocaleString()}</span>}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-4">
                <div className="text-center">
                  <p className="text-4xl font-extrabold">{score}%</p>
                  <Progress value={score} className="w-32 mt-1" />
                </div>
                {statusBadge(results.status)}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Metric cards */}
      {summary && !results.error && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="bg-card border-border">
            <CardContent className="pt-6 pb-5">
              <div className="flex items-center gap-4">
                <div className="p-2.5 rounded-xl bg-blue-100">
                  <ClipboardCheck className="h-5 w-5 text-blue-700" />
                </div>
                <div>
                  <p className="text-3xl font-extrabold text-blue-700">{summary.total_checks}</p>
                  <p className="text-sm font-medium text-foreground/60 mt-0.5">Total Checks</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 pb-5">
              <div className="flex items-center gap-4">
                <div className="p-2.5 rounded-xl bg-green-100">
                  <CheckCircle className="h-5 w-5 text-green-700" />
                </div>
                <div>
                  <p className="text-3xl font-extrabold text-green-700">{summary.passed}</p>
                  <p className="text-sm font-medium text-foreground/60 mt-0.5">Passed</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 pb-5">
              <div className="flex items-center gap-4">
                <div className="p-2.5 rounded-xl bg-yellow-100">
                  <AlertTriangle className="h-5 w-5 text-yellow-700" />
                </div>
                <div>
                  <p className="text-3xl font-extrabold text-yellow-700">{summary.warnings}</p>
                  <p className="text-sm font-medium text-foreground/60 mt-0.5">Warnings</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-6 pb-5">
              <div className="flex items-center gap-4">
                <div className="p-2.5 rounded-xl bg-red-100">
                  <XCircle className="h-5 w-5 text-red-700" />
                </div>
                <div>
                  <p className="text-3xl font-extrabold text-red-700">{summary.failures}</p>
                  <p className="text-sm font-medium text-foreground/60 mt-0.5">Failures</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Section cards */}
      {sections.map((section: any, i: number) => {
        const isExpanded = expandedSections[i] ?? false;
        const tableColumns: Column[] = (section.table_columns || []).map((c: any) => ({
          key: c.key, label: c.label, sortable: true,
        }));

        return (
          <Card key={i} className={`bg-card border-border ${sectionBorder(section.status)}`}>
            <CardHeader
              className="pb-3 cursor-pointer select-none"
              onClick={() => toggleSection(i)}
            >
              <CardTitle className="text-base flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {isExpanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
                  {section.title || `Check ${i + 1}`}
                </div>
                <div className="flex items-center gap-2">
                  {section.score != null && scoreBadge(section.score)}
                  {section.status && statusBadge(section.status)}
                </div>
              </CardTitle>
            </CardHeader>
            {isExpanded && (
              <CardContent className="pt-0">
                {section.description && (
                  <p className="text-sm text-muted-foreground mb-3">{section.description}</p>
                )}

                {/* Items / findings */}
                {(section.items || []).length > 0 && (
                  <div className="space-y-1.5 mb-4">
                    {section.items.map((item: any, j: number) => (
                      <div key={j} className="flex items-center justify-between px-3 py-2 rounded border border-border bg-background text-sm">
                        <span className="text-foreground">
                          {typeof item === "string" ? item : item.name || item.message || JSON.stringify(item)}
                        </span>
                        {item.status && statusBadge(item.status)}
                      </div>
                    ))}
                  </div>
                )}

                {/* DataTable */}
                {section.table_data && section.table_data.length > 0 && (
                  <DataTable
                    data={section.table_data}
                    columns={tableColumns}
                    searchable
                    searchKeys={tableColumns.map((c: Column) => c.key)}
                    pageSize={15}
                    compact
                    tableId={`compliance-${i}`}
                    emptyMessage="No data"
                  />
                )}
              </CardContent>
            )}
          </Card>
        );
      })}

      {/* Error */}
      {(job?.status === "error" || results?.error) && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5 shrink-0" />
            <span className="text-sm">{results?.error || job?.error}</span>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
