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
  GitCompare, CheckCircle, XCircle, Loader2, AlertCircle,
  Plus, Minus, Equal, ArrowRight,
} from "lucide-react";
import PageHeader from "@/components/PageHeader";

function DiffSection({ title, data }: { title: string; data: any }) {
  if (!data) return null;
  const onlySource = data.only_in_source || [];
  const onlyDest = data.only_in_dest || [];
  const inBoth = data.in_both || [];
  const total = onlySource.length + onlyDest.length + inBoth.length;

  if (total === 0 && !data.source_count) return null;

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-lg flex items-center justify-between">
          <span>{title}</span>
          <div className="flex gap-2">
            <Badge variant="outline" className="text-xs">
              Source: {data.source_count ?? onlySource.length + inBoth.length}
            </Badge>
            <Badge variant="outline" className="text-xs">
              Dest: {data.dest_count ?? onlyDest.length + inBoth.length}
            </Badge>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Only in Source */}
        {onlySource.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Minus className="h-4 w-4 text-red-500" />
              <span className="text-sm font-medium text-red-700">
                Only in Source ({onlySource.length})
              </span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {onlySource.map((item: string) => (
                <span
                  key={item}
                  className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-red-50 text-red-700 border border-red-200"
                >
                  {item}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Only in Dest */}
        {onlyDest.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Plus className="h-4 w-4 text-foreground" />
              <span className="text-sm font-medium text-foreground">
                Only in Destination ({onlyDest.length})
              </span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {onlyDest.map((item: string) => (
                <span
                  key={item}
                  className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-muted/20 text-foreground border border-border"
                >
                  {item}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* In Both */}
        {inBoth.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Equal className="h-4 w-4 text-gray-400" />
              <span className="text-sm font-medium text-gray-600">
                In Both ({inBoth.length})
              </span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {inBoth.map((item: string) => (
                <span
                  key={item}
                  className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-gray-50 text-gray-600 border border-gray-200"
                >
                  {item}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* All match */}
        {onlySource.length === 0 && onlyDest.length === 0 && inBoth.length > 0 && (
          <div className="flex items-center gap-2 text-foreground">
            <CheckCircle className="h-4 w-4" />
            <span className="text-sm">All {title.toLowerCase()} match</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export default function DiffPage() {
  const { job: diffJob, run: runDiff, isRunning: isDiffRunning } = usePageJob("diff");
  const { job: valJob, run: runValidate, isRunning: isValRunning } = usePageJob("validate");
  const [source, setSource] = useState(diffJob?.params?.source || valJob?.params?.source || "");
  const [dest, setDest] = useState(diffJob?.params?.dest || valJob?.params?.dest || "");

  const diffData = diffJob?.data as any;
  const valData = valJob?.data as any;

  // Summary counts
  const summaryItems = diffData
    ? Object.entries(diffData).map(([key, val]: [string, any]) => ({
        label: key.charAt(0).toUpperCase() + key.slice(1),
        onlySource: val?.only_in_source?.length || 0,
        onlyDest: val?.only_in_dest?.length || 0,
        inBoth: val?.in_both?.length || 0,
      }))
    : [];

  const totalDiffs = summaryItems.reduce((s, i) => s + i.onlySource + i.onlyDest, 0);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Diff & Compare"
        icon={GitCompare}
        breadcrumbs={["Discovery", "Diff & Compare"]}
        description="Object-level diff between two catalogs — identifies missing, extra, and modified schemas, tables, views, and columns. Validates that clones match their source."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/information-schema"
        docsLabel="INFORMATION_SCHEMA"
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
              onClick={() => runDiff({ source, dest }, () => api.post("/diff", { source_catalog: source, destination_catalog: dest }))}
              disabled={!source || !dest || isDiffRunning}
            >
              {isDiffRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <GitCompare className="h-4 w-4 mr-2" />}
              {isDiffRunning ? "Comparing..." : "Diff"}
            </Button>
            <Button
              variant="outline"
              onClick={() => runValidate({ source, dest }, () => api.post("/validate", { source_catalog: source, destination_catalog: dest }))}
              disabled={!source || !dest || isValRunning}
            >
              {isValRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <CheckCircle className="h-4 w-4 mr-2" />}
              {isValRunning ? "Validating..." : "Validate"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Diff Summary */}
      {diffData && (
        <Card className={totalDiffs === 0 ? "border-border bg-muted/20" : "border-border bg-muted/20"}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {totalDiffs === 0 ? (
                  <CheckCircle className="h-6 w-6 text-foreground" />
                ) : (
                  <GitCompare className="h-6 w-6 text-muted-foreground" />
                )}
                <div>
                  <p className="font-semibold text-lg">
                    {totalDiffs === 0 ? "Catalogs are in sync" : `${totalDiffs} differences found`}
                  </p>
                  <p className="text-sm text-gray-600">
                    {source} vs {dest}
                  </p>
                </div>
              </div>
              <div className="flex gap-3">
                {summaryItems.map((item) => (
                  <div key={item.label} className="text-center">
                    <p className="text-xs text-gray-500">{item.label}</p>
                    <div className="flex gap-1 mt-1">
                      {item.onlySource > 0 && (
                        <Badge variant="destructive" className="text-xs px-1.5">{item.onlySource}</Badge>
                      )}
                      {item.onlyDest > 0 && (
                        <Badge className="bg-foreground text-xs px-1.5">{item.onlyDest}</Badge>
                      )}
                      {item.onlySource === 0 && item.onlyDest === 0 && (
                        <Badge variant="outline" className="text-xs px-1.5 text-foreground">✓</Badge>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Diff Detail Sections */}
      {diffData && (
        <div className="space-y-4">
          {Object.entries(diffData).map(([key, val]) => (
            <DiffSection
              key={key}
              title={key.charAt(0).toUpperCase() + key.slice(1)}
              data={val}
            />
          ))}
        </div>
      )}

      {/* Validate Results */}
      {valData && (
        <Card className={valData.mismatched === 0 && valData.errors === 0 ? "border-border" : "border-border"}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              {valData.mismatched === 0 && valData.errors === 0 ? (
                <CheckCircle className="h-5 w-5 text-foreground" />
              ) : (
                <XCircle className="h-5 w-5 text-muted-foreground" />
              )}
              Validation Results
              <Badge className="ml-auto bg-muted/40 text-foreground">
                {valData.matched}/{valData.total_tables} matched
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Summary cards */}
            <div className="grid grid-cols-4 gap-3">
              <div className="text-center p-3 bg-muted/20 rounded">
                <p className="text-xl font-bold text-foreground">{valData.matched}</p>
                <p className="text-xs text-gray-500">Matched</p>
              </div>
              <div className="text-center p-3 bg-red-50 rounded">
                <p className="text-xl font-bold text-red-700">{valData.mismatched}</p>
                <p className="text-xs text-gray-500">Mismatched</p>
              </div>
              <div className="text-center p-3 bg-muted/20 rounded">
                <p className="text-xl font-bold text-muted-foreground">{valData.errors}</p>
                <p className="text-xs text-gray-500">Errors</p>
              </div>
              <div className="text-center p-3 bg-muted/30 rounded">
                <p className="text-xl font-bold text-[#E8453C]">{valData.total_tables}</p>
                <p className="text-xs text-gray-500">Total</p>
              </div>
            </div>

            {/* Mismatched tables */}
            {valData.mismatched_tables && valData.mismatched_tables.length > 0 && (
              <div>
                <p className="text-sm font-medium text-red-700 mb-2">Mismatched Tables</p>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b bg-red-50">
                      <th className="text-left py-2 px-3 font-medium">Schema</th>
                      <th className="text-left py-2 px-3 font-medium">Table</th>
                      <th className="text-right py-2 px-3 font-medium">Source Rows</th>
                      <th className="text-right py-2 px-3 font-medium">Dest Rows</th>
                      <th className="text-right py-2 px-3 font-medium">Diff</th>
                    </tr>
                  </thead>
                  <tbody>
                    {valData.mismatched_tables.map((m: any, i: number) => (
                      <tr key={i} className="border-b bg-red-50/50">
                        <td className="py-2 px-3 text-gray-600">{m.schema}</td>
                        <td className="py-2 px-3 font-medium">{m.table}</td>
                        <td className="py-2 px-3 text-right">{m.source_count?.toLocaleString() ?? "—"}</td>
                        <td className="py-2 px-3 text-right">{m.dest_count?.toLocaleString() ?? "—"}</td>
                        <td className="py-2 px-3 text-right">
                          <Badge variant="destructive">
                            {m.source_count != null && m.dest_count != null
                              ? (m.source_count - m.dest_count).toLocaleString()
                              : "—"}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* All tables detail */}
            {valData.details && valData.details.length > 0 && (
              <div>
                <p className="text-sm font-medium text-gray-600 mb-2">All Tables ({valData.details.length})</p>
                <div className="overflow-x-auto max-h-80 overflow-y-auto border rounded">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-white">
                      <tr className="border-b bg-gray-50">
                        <th className="text-left py-2 px-3 font-medium w-8"></th>
                        <th className="text-left py-2 px-3 font-medium">Schema</th>
                        <th className="text-left py-2 px-3 font-medium">Table</th>
                        <th className="text-right py-2 px-3 font-medium">Source Rows</th>
                        <th className="text-right py-2 px-3 font-medium">Dest Rows</th>
                      </tr>
                    </thead>
                    <tbody>
                      {valData.details.map((row: any, i: number) => (
                        <tr key={i} className={`border-b ${row.match ? "" : row.error ? "bg-muted/20" : "bg-red-50"}`}>
                          <td className="py-1.5 px-3">
                            {row.match ? (
                              <CheckCircle className="h-4 w-4 text-foreground" />
                            ) : row.error ? (
                              <AlertCircle className="h-4 w-4 text-muted-foreground" />
                            ) : (
                              <XCircle className="h-4 w-4 text-red-500" />
                            )}
                          </td>
                          <td className="py-1.5 px-3 text-gray-600">{row.schema}</td>
                          <td className="py-1.5 px-3 font-medium">{row.table}</td>
                          <td className="py-1.5 px-3 text-right">{row.source_count?.toLocaleString() ?? "—"}</td>
                          <td className="py-1.5 px-3 text-right">{row.dest_count?.toLocaleString() ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Errors */}
      {diffJob?.status === "error" && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {diffJob.error}
          </CardContent>
        </Card>
      )}
      {valJob?.status === "error" && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {valJob.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
