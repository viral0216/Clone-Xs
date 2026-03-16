// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { Loader2, XCircle, ShieldCheck, Download, FileText } from "lucide-react";

function statusBadge(status: string) {
  switch (status?.toUpperCase()) {
    case "COMPLIANT": return <Badge className="bg-green-600 text-white">{status}</Badge>;
    case "NON-COMPLIANT": case "NON_COMPLIANT": return <Badge variant="destructive">{status}</Badge>;
    case "WARNING": return <Badge className="bg-yellow-500 text-white">{status}</Badge>;
    default: return <Badge variant="outline">{status || "Unknown"}</Badge>;
  }
}

const REPORT_TYPES = [
  { value: "data_governance", label: "Data Governance" },
  { value: "pii_audit", label: "PII Audit" },
  { value: "permission_audit", label: "Permission Audit" },
];

export default function CompliancePage() {
  const [catalog, setCatalog] = useState("");
  const [reportType, setReportType] = useState("data_governance");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState<any>(null);

  async function generateReport() {
    setLoading(true);
    setError("");
    setResults(null);
    try {
      const data = await api.post("/compliance", { catalog, report_type: reportType });
      setResults(data);
    } catch (e: any) {
      setError(e.message || "Failed to generate report");
    } finally {
      setLoading(false);
    }
  }

  function downloadReport() {
    if (!results) return;
    const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `compliance-${catalog}-${reportType}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const sections = results?.sections || results?.findings || [];
  const score = results?.compliance_score ?? results?.score;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Compliance</h1>
        <p className="text-muted-foreground mt-1">Generate governance and compliance reports — permission audits, tag coverage, PII exposure, ownership mapping, and access control analysis across catalogs.</p>
        <p className="text-xs text-muted-foreground mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Unity Catalog governance</a> · <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-show-grants" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">SHOW GRANTS</a>
        </p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="pt-6">
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
            <Button onClick={generateReport} disabled={!catalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ShieldCheck className="h-4 w-4 mr-2" />}
              {loading ? "Generating..." : "Generate Report"}
            </Button>
            {results && (
              <Button variant="outline" onClick={downloadReport}>
                <Download className="h-4 w-4 mr-2" />Export JSON
              </Button>
            )}
          </div>
        </CardContent>
      </Card>

      {results && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <ShieldCheck className="h-6 w-6 text-muted-foreground" />
              <div>
                <p className="font-semibold text-foreground">
                  {REPORT_TYPES.find((r) => r.value === reportType)?.label || reportType}
                </p>
                <p className="text-sm text-muted-foreground">Catalog: {catalog}</p>
              </div>
            </div>
            <div className="flex items-center gap-3">
              {score != null && (
                <span className="text-2xl font-bold text-foreground">{score}%</span>
              )}
              {results.status && statusBadge(results.status)}
            </div>
          </CardContent>
        </Card>
      )}

      {sections.length > 0 && sections.map((section: any, i: number) => (
        <Card key={i} className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center justify-between">
              <div className="flex items-center gap-2">
                <FileText className="h-4 w-4 text-muted-foreground" />
                {section.title || section.name || `Section ${i + 1}`}
              </div>
              {section.status && statusBadge(section.status)}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {section.description && <p className="text-sm text-muted-foreground mb-3">{section.description}</p>}
            {(section.items || section.findings || []).length > 0 && (
              <div className="space-y-2">
                {(section.items || section.findings || []).map((item: any, j: number) => (
                  <div key={j} className="flex items-center justify-between px-3 py-2 rounded border border-border bg-background text-sm">
                    <span className="text-foreground">{typeof item === "string" ? item : item.name || item.message || JSON.stringify(item)}</span>
                    {item.status && statusBadge(item.status)}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      ))}

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
