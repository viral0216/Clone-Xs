// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { usePreflight } from "@/hooks/useApi";
import {
  ClipboardCheck, Loader2, XCircle, CheckCircle, AlertTriangle, ArrowRight,
} from "lucide-react";

function statusIcon(status: string) {
  switch (status?.toUpperCase()) {
    case "PASS": case "OK": return <CheckCircle className="h-4 w-4 text-green-500" />;
    case "WARN": return <AlertTriangle className="h-4 w-4 text-yellow-500" />;
    case "FAIL": return <XCircle className="h-4 w-4 text-red-500" />;
    default: return null;
  }
}

function statusBadge(status: string) {
  switch (status?.toUpperCase()) {
    case "PASS": case "OK": return <Badge className="bg-green-100 text-green-800 text-xs">{status}</Badge>;
    case "WARN": return <Badge className="bg-yellow-100 text-yellow-800 text-xs">WARN</Badge>;
    case "FAIL": return <Badge variant="destructive" className="text-xs">FAIL</Badge>;
    default: return <Badge variant="outline" className="text-xs">{status}</Badge>;
  }
}

export default function PreflightPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const preflight = usePreflight();
  const data = preflight.data as any;

  const checks = data?.checks || data?.results || [];

  const passed = checks.filter((c: any) => ["PASS", "OK"].includes(c.status?.toUpperCase())).length;
  const warnings = checks.filter((c: any) => ["WARN", "WARNING"].includes(c.status?.toUpperCase())).length;
  const failed = checks.filter((c: any) => ["FAIL", "FAILED", "ERROR"].includes(c.status?.toUpperCase())).length;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Preflight Checks</h1>
        <p className="text-gray-500 mt-1">Validate prerequisites before cloning</p>
      </div>

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
              onClick={() => preflight.mutate({ source_catalog: source, destination_catalog: dest })}
              disabled={!source || !dest || preflight.isPending}
            >
              {preflight.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ClipboardCheck className="h-4 w-4 mr-2" />}
              {preflight.isPending ? "Running..." : "Run Preflight"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Banner */}
      {checks.length > 0 && (
        <Card className={failed > 0 ? "border-red-200 bg-red-50" : warnings > 0 ? "border-yellow-200 bg-yellow-50" : "border-green-200 bg-green-50"}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {failed > 0 ? (
                  <XCircle className="h-6 w-6 text-red-600" />
                ) : warnings > 0 ? (
                  <AlertTriangle className="h-6 w-6 text-yellow-600" />
                ) : (
                  <CheckCircle className="h-6 w-6 text-green-600" />
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
                <div className="text-center p-2 bg-green-100 rounded min-w-[60px]">
                  <p className="text-lg font-bold text-green-700">{passed}</p>
                  <p className="text-xs text-gray-500">Passed</p>
                </div>
                <div className="text-center p-2 bg-yellow-100 rounded min-w-[60px]">
                  <p className="text-lg font-bold text-yellow-700">{warnings}</p>
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
            <CardTitle className="text-lg">Check Results</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto border rounded">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3 font-medium w-8"></th>
                    <th className="text-left py-2 px-3 font-medium">Check</th>
                    <th className="text-center py-2 px-3 font-medium">Status</th>
                    <th className="text-left py-2 px-3 font-medium">Message</th>
                  </tr>
                </thead>
                <tbody>
                  {checks.map((check: any, i: number) => (
                    <tr
                      key={i}
                      className={`border-b ${
                        check.status?.toUpperCase() === "FAIL" ? "bg-red-50/50" :
                        check.status?.toUpperCase() === "WARN" ? "bg-yellow-50/50" : ""
                      }`}
                    >
                      <td className="py-2 px-3">{statusIcon(check.status)}</td>
                      <td className="py-2 px-3 font-medium">{check.name || check.check}</td>
                      <td className="py-2 px-3 text-center">{statusBadge(check.status)}</td>
                      <td className="py-2 px-3 text-gray-600">{check.message || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error */}
      {preflight.isError && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {(preflight.error as Error).message}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
