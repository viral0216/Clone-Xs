// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import {
  Shield, Loader2, XCircle, AlertTriangle, CheckCircle,
} from "lucide-react";

function riskColor(risk: string) {
  switch (risk?.toUpperCase()) {
    case "HIGH": return "text-red-700 bg-red-50 border-red-200";
    case "MEDIUM": return "text-yellow-700 bg-yellow-50 border-yellow-200";
    case "LOW": return "text-green-700 bg-green-50 border-green-200";
    default: return "text-gray-700 bg-gray-50 border-gray-200";
  }
}

function riskBadgeVariant(risk: string) {
  switch (risk?.toUpperCase()) {
    case "HIGH": return "destructive";
    case "MEDIUM": return "warning";
    default: return "outline";
  }
}

function confidenceBadge(confidence: number | string) {
  const val = typeof confidence === "number" ? confidence : parseFloat(confidence);
  if (val >= 0.9) return <Badge variant="destructive" className="text-xs">{(val * 100).toFixed(0)}%</Badge>;
  if (val >= 0.7) return <Badge className="bg-yellow-500 text-xs">{(val * 100).toFixed(0)}%</Badge>;
  return <Badge variant="outline" className="text-xs">{(val * 100).toFixed(0)}%</Badge>;
}

export default function PiiPage() {
  const { job, run, isRunning } = usePageJob("pii");
  const [sourceCatalog, setSourceCatalog] = useState(job?.params?.sourceCatalog || "");

  const data = job?.data as any;
  const summary = data?.summary;
  const columns = data?.columns || data?.results || [];

  const handleScan = () => {
    run({ sourceCatalog }, () => api.post("/pii-scan", { source_catalog: sourceCatalog, no_exit_code: true }));
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">PII Scanner</h1>
        <p className="text-gray-500 mt-1">Scan column names and sample data for PII patterns — emails, phone numbers, SSNs, credit cards, IP addresses, and more. Supports regex-based and heuristic detection.</p>
        <p className="text-xs text-gray-400 mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/column-masking" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Column masking</a> · <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/row-filters" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Row filters</a>
        </p>
      </div>

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={sourceCatalog}
              onCatalogChange={setSourceCatalog}
              showSchema={false}
              showTable={false}
            />
            <Button
              onClick={handleScan}
              disabled={!sourceCatalog || isRunning}
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Shield className="h-4 w-4 mr-2" />}
              {isRunning ? "Scanning..." : "Scan for PII"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary */}
      {summary && (
        <Card className={`border-${summary.risk_level === "HIGH" ? "red" : summary.risk_level === "MEDIUM" ? "yellow" : "green"}-200`}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {summary.risk_level === "HIGH" ? (
                  <AlertTriangle className="h-6 w-6 text-red-600" />
                ) : summary.risk_level === "MEDIUM" ? (
                  <AlertTriangle className="h-6 w-6 text-yellow-600" />
                ) : (
                  <CheckCircle className="h-6 w-6 text-green-600" />
                )}
                <div>
                  <p className="font-semibold text-lg">
                    {summary.pii_columns_found || 0} PII columns detected
                  </p>
                  <p className="text-sm text-gray-600">
                    {summary.total_columns_scanned || 0} total columns scanned
                  </p>
                </div>
              </div>
              <Badge className={`text-sm px-3 py-1 ${riskColor(summary.risk_level)}`}>
                {summary.risk_level} RISK
              </Badge>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-3 gap-4">
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-blue-700">{summary.total_columns_scanned || 0}</p>
              <p className="text-xs text-gray-500 mt-1">Columns Scanned</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-red-700">{summary.pii_columns_found || 0}</p>
              <p className="text-xs text-gray-500 mt-1">PII Columns Found</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className={`text-2xl font-bold ${summary.risk_level === "HIGH" ? "text-red-700" : summary.risk_level === "MEDIUM" ? "text-yellow-700" : "text-green-700"}`}>
                {summary.risk_level || "N/A"}
              </p>
              <p className="text-xs text-gray-500 mt-1">Risk Level</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Results Table */}
      {columns.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">PII Detection Results</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto max-h-96 overflow-y-auto border rounded">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-white">
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3 font-medium">Schema</th>
                    <th className="text-left py-2 px-3 font-medium">Table</th>
                    <th className="text-left py-2 px-3 font-medium">Column</th>
                    <th className="text-left py-2 px-3 font-medium">PII Type</th>
                    <th className="text-center py-2 px-3 font-medium">Confidence</th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((row: any, i: number) => (
                    <tr key={i} className="border-b hover:bg-gray-50">
                      <td className="py-2 px-3 text-gray-600">{row.schema}</td>
                      <td className="py-2 px-3 font-medium">{row.table}</td>
                      <td className="py-2 px-3">{row.column}</td>
                      <td className="py-2 px-3">
                        <Badge variant="outline" className={`text-xs ${riskColor(row.risk || summary?.risk_level)}`}>
                          {row.pii_type || row.type}
                        </Badge>
                      </td>
                      <td className="py-2 px-3 text-center">
                        {row.confidence != null ? confidenceBadge(row.confidence) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error */}
      {job?.status === "error" && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
