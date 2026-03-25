// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import PageHeader from "@/components/PageHeader";
import {
  ClipboardCheck, Loader2, XCircle, CheckCircle, AlertTriangle, ArrowRight,
  Shield, Copy,
} from "lucide-react";

function statusIcon(status: string) {
  switch (status?.toUpperCase()) {
    case "PASS": case "OK": return <CheckCircle className="h-4 w-4 text-foreground" />;
    case "WARN": return <AlertTriangle className="h-4 w-4 text-muted-foreground" />;
    case "FAIL": return <XCircle className="h-4 w-4 text-red-500" />;
    default: return null;
  }
}

function statusBadge(status: string) {
  switch (status?.toUpperCase()) {
    case "PASS": case "OK": return <Badge className="bg-muted/40 text-foreground text-xs">{status}</Badge>;
    case "WARN": return <Badge className="bg-muted/40 text-foreground text-xs">WARN</Badge>;
    case "FAIL": return <Badge variant="destructive" className="text-xs">FAIL</Badge>;
    default: return <Badge variant="outline" className="text-xs">{status}</Badge>;
  }
}

export default function PreflightPage() {
  const { job, run, isRunning } = usePageJob("preflight");
  const [source, setSource] = useState(job?.params?.source || "");
  const [dest, setDest] = useState(job?.params?.dest || "");
  const data = job?.data as any;

  const checks = data?.checks || data?.results || [];

  const passed = checks.filter((c: any) => ["PASS", "OK"].includes(c.status?.toUpperCase())).length;
  const warnings = checks.filter((c: any) => ["WARN", "WARNING"].includes(c.status?.toUpperCase())).length;
  const failed = checks.filter((c: any) => ["FAIL", "FAILED", "ERROR"].includes(c.status?.toUpperCase())).length;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Preflight Checks"
        icon={ClipboardCheck}
        breadcrumbs={["Management", "Preflight"]}
        description="Pre-clone validation — verifies connectivity, warehouse, catalog permissions, UC grants (MANAGE, CREATE CATALOG, CREATE TABLE), and destination writability."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/"
        docsLabel="Unity Catalog privileges"
      />

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={source}
              onCatalogChange={setSource}
              showSchema={false}
              showTable={false}
            />
            <div className="flex items-center text-gray-400 pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <CatalogPicker
              catalog={dest}
              onCatalogChange={setDest}
              showSchema={false}
              showTable={false}
            />
            <Button
              onClick={() => run({ source, dest }, () => api.post("/preflight", { source_catalog: source, destination_catalog: dest }))}
              disabled={!source || !dest || isRunning}
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ClipboardCheck className="h-4 w-4 mr-2" />}
              {isRunning ? "Running..." : "Run Preflight"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Banner */}
      {checks.length > 0 && (
        <Card className={failed > 0 ? "border-red-200 bg-red-50" : warnings > 0 ? "border-border bg-muted/20" : "border-border bg-muted/20"}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {failed > 0 ? (
                  <XCircle className="h-6 w-6 text-red-600" />
                ) : warnings > 0 ? (
                  <AlertTriangle className="h-6 w-6 text-muted-foreground" />
                ) : (
                  <CheckCircle className="h-6 w-6 text-foreground" />
                )}
                <div>
                  <p className="font-semibold text-lg">
                    {failed > 0 ? "Preflight checks failed" : warnings > 0 ? "Preflight passed with warnings" : "All preflight checks passed"}
                  </p>
                  <p className="text-sm text-gray-600">
                    {source} to {dest}
                  </p>
                </div>
              </div>
              <div className="flex gap-3">
                <div className="text-center p-2 bg-muted/40 rounded min-w-[60px]">
                  <p className="text-lg font-bold text-foreground">{passed}</p>
                  <p className="text-xs text-gray-500">Passed</p>
                </div>
                <div className="text-center p-2 bg-muted/40 rounded min-w-[60px]">
                  <p className="text-lg font-bold text-muted-foreground">{warnings}</p>
                  <p className="text-xs text-gray-500">Warnings</p>
                </div>
                <div className="text-center p-2 bg-red-100 rounded min-w-[60px]">
                  <p className="text-lg font-bold text-red-700">{failed}</p>
                  <p className="text-xs text-gray-500">Failed</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Checks Table */}
      {checks.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              <Shield className="h-5 w-5" />
              Check Results
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto border rounded">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left py-2.5 px-3 font-medium w-8"></th>
                    <th className="text-left py-2.5 px-3 font-medium">Check</th>
                    <th className="text-center py-2.5 px-3 font-medium">Status</th>
                    <th className="text-left py-2.5 px-3 font-medium">Details</th>
                  </tr>
                </thead>
                <tbody>
                  {checks.map((check: any, i: number) => {
                    const detail = check.detail || check.message || "—";
                    // Extract GRANT command if present
                    const grantMatch = detail.match(/Grant with:\s*(GRANT\s+.+)/i);
                    const mainMsg = grantMatch ? detail.replace(grantMatch[0], "").trim().replace(/[.—]\s*$/, "") : detail;

                    return (
                      <tr
                        key={i}
                        className={`border-b ${
                          check.status?.toUpperCase() === "FAIL" ? "bg-red-500/5" :
                          check.status?.toUpperCase() === "WARN" ? "bg-muted/200/5" : ""
                        }`}
                      >
                        <td className="py-2.5 px-3">{statusIcon(check.status)}</td>
                        <td className="py-2.5 px-3 font-medium whitespace-nowrap">{check.name || check.check}</td>
                        <td className="py-2.5 px-3 text-center">{statusBadge(check.status)}</td>
                        <td className="py-2.5 px-3 text-muted-foreground">
                          <span>{mainMsg}</span>
                          {grantMatch && (
                            <div className="mt-1.5">
                              <code
                                className="inline-flex items-center gap-2 text-xs bg-muted px-2.5 py-1.5 rounded font-mono cursor-pointer hover:bg-muted/80 transition-colors"
                                onClick={() => {
                                  navigator.clipboard.writeText(grantMatch[1]);
                                }}
                                title="Click to copy"
                              >
                                {grantMatch[1]}
                                <Copy className="h-3 w-3 text-muted-foreground shrink-0" />
                              </code>
                            </div>
                          )}
                        </td>
                      </tr>
                    );
                  })}
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
