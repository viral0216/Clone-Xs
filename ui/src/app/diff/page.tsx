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
import DataTable, { Column } from "@/components/DataTable";

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

  // The presence/absence diff sections live under the canonical
  // object-type keys. The /diff-detail response also carries top-level
  // fields like `drift`, `summary`, `source_catalog`, `drift_errors` —
  // exclude those from the iteration so DiffSection only sees the
  // shape it expects.
  const OBJECT_TYPE_KEYS = ["schemas", "tables", "views", "functions", "volumes"];
  const summaryItems = diffData
    ? OBJECT_TYPE_KEYS
        .filter((k) => diffData[k])
        .map((k) => ({
          label: k.charAt(0).toUpperCase() + k.slice(1),
          onlySource: diffData[k]?.only_in_source?.length || 0,
          onlyDest: diffData[k]?.only_in_dest?.length || 0,
          inBoth: diffData[k]?.in_both?.length || 0,
        }))
    : [];

  const totalDiffs = summaryItems.reduce((s, i) => s + i.onlySource + i.onlyDest, 0);
  const drift: any[] = diffData?.drift ?? [];
  const driftSummary: any = diffData?.summary ?? {};
  const driftErrors: any[] = diffData?.drift_errors ?? [];

  // Format a signed byte delta for the size column. Negative deltas
  // (dest smaller than source) read as "-1.2 GB"; positive as "+1.2 GB".
  const formatBytesSigned = (n: number): string => {
    if (!n) return "0 B";
    const abs = Math.abs(n);
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.min(units.length - 1, Math.floor(Math.log(abs) / Math.log(1024)));
    const v = (abs / Math.pow(1024, i)).toFixed(i > 1 ? 2 : 0);
    return `${n < 0 ? "-" : "+"}${v} ${units[i]}`;
  };

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
              onClick={() => runDiff({ source, dest }, () => api.post("/diff-detail", { source_catalog: source, destination_catalog: dest }))}
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

      {/* Diff Detail Sections — presence/absence per object type */}
      {diffData && (
        <div className="space-y-4">
          {OBJECT_TYPE_KEYS.filter((k) => diffData[k]).map((key) => (
            <DiffSection
              key={key}
              title={key.charAt(0).toUpperCase() + key.slice(1)}
              data={diffData[key]}
            />
          ))}
        </div>
      )}

      {/* Drifted Tables — common tables that differ in column shape or
          size. Powered by /diff-detail's `drift` block. Renders only
          when there's signal; the presence/absence sections above cover
          the in-source-only / in-dest-only cases separately. */}
      {diffData && drift.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center justify-between">
              <span className="flex items-center gap-2">
                <GitCompare className="h-5 w-5 text-[#E8453C]" />
                Drifted Tables ({drift.length})
              </span>
              <div className="flex gap-2 text-xs">
                <Badge variant="outline">+{driftSummary.columns_added || 0} cols</Badge>
                <Badge variant="outline">-{driftSummary.columns_removed || 0} cols</Badge>
                <Badge variant="outline">{driftSummary.type_changes || 0} type changes</Badge>
                <Badge variant="outline" className={driftSummary.total_size_delta_bytes > 0 ? "border-amber-500/30 text-amber-600" : driftSummary.total_size_delta_bytes < 0 ? "border-[#E8453C]/30 text-[#E8453C]" : ""}>
                  {formatBytesSigned(driftSummary.total_size_delta_bytes || 0)} total
                </Badge>
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable
              data={drift}
              columns={[
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v}</span> },
                { key: "table", label: "Table", sortable: true, render: (v: string) => <span className="text-sm font-medium">{v}</span> },
                {
                  key: "columns_only_in_source", label: "Removed cols", sortable: true, align: "right" as const,
                  render: (cols: string[]) => cols?.length
                    ? <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-600">-{cols.length}</Badge>
                    : <span className="text-xs text-muted-foreground">—</span>,
                },
                {
                  key: "columns_only_in_dest", label: "Added cols", sortable: true, align: "right" as const,
                  render: (cols: string[]) => cols?.length
                    ? <Badge variant="outline" className="text-[10px] border-foreground/30 text-foreground">+{cols.length}</Badge>
                    : <span className="text-xs text-muted-foreground">—</span>,
                },
                {
                  key: "column_type_changes", label: "Type changes", sortable: true, align: "right" as const,
                  render: (changes: any[]) => changes?.length
                    ? <Badge variant="outline" className="text-[10px] border-amber-500/30 text-amber-600">{changes.length}</Badge>
                    : <span className="text-xs text-muted-foreground">—</span>,
                },
                {
                  key: "size_delta_bytes", label: "Size Δ", sortable: true, align: "right" as const,
                  render: (v: number) => <span className={`text-xs font-mono ${v > 0 ? "text-amber-600" : v < 0 ? "text-[#E8453C]" : "text-muted-foreground"}`}>{formatBytesSigned(v || 0)}</span>,
                },
                {
                  key: "row_delta", label: "Row Δ", sortable: true, align: "right" as const,
                  render: (v: number) => <span className={`text-xs font-mono ${v > 0 ? "text-amber-600" : v < 0 ? "text-[#E8453C]" : "text-muted-foreground"}`}>{(v ?? 0) >= 0 ? "+" : ""}{(v ?? 0).toLocaleString()}</span>,
                },
                {
                  key: "_detail", label: "Detail", width: "300px",
                  render: (_: any, row: any) => {
                    // Inline expandable detail — show the actual column
                    // names that drifted instead of just counts. Keeps
                    // the Drifted Tables view actionable without a modal.
                    const removed = row.columns_only_in_source || [];
                    const added = row.columns_only_in_dest || [];
                    const changes = row.column_type_changes || [];
                    return (
                      <div className="text-[10px] space-y-0.5 max-w-[300px]">
                        {removed.length > 0 && (
                          <div className="text-red-600 truncate" title={removed.join(", ")}>
                            <Minus className="h-2.5 w-2.5 inline mr-0.5" />{removed.slice(0, 3).join(", ")}{removed.length > 3 ? `… +${removed.length - 3}` : ""}
                          </div>
                        )}
                        {added.length > 0 && (
                          <div className="text-foreground truncate" title={added.join(", ")}>
                            <Plus className="h-2.5 w-2.5 inline mr-0.5" />{added.slice(0, 3).join(", ")}{added.length > 3 ? `… +${added.length - 3}` : ""}
                          </div>
                        )}
                        {changes.length > 0 && (
                          <div className="text-amber-600 truncate" title={changes.map((c: any) => `${c.column}: ${c.source_type} → ${c.dest_type}`).join("; ")}>
                            <AlertCircle className="h-2.5 w-2.5 inline mr-0.5" />{changes.slice(0, 2).map((c: any) => c.column).join(", ")}{changes.length > 2 ? `… +${changes.length - 2}` : ""}
                          </div>
                        )}
                      </div>
                    );
                  },
                },
              ] as Column[]}
              searchable
              searchPlaceholder="Filter drifted tables..."
              pageSize={25}
              emptyMessage="No drifted tables"
            />
          </CardContent>
        </Card>
      )}

      {/* Drift errors — only rendered if either bulk metadata query
          failed. Presence/absence diff still surfaces above. */}
      {diffData && driftErrors.length > 0 && (
        <Card className="border-red-500/30">
          <CardContent className="pt-4 text-xs space-y-1">
            <p className="font-medium text-red-600">{driftErrors.length} drift query failed (presence/absence still shown above):</p>
            {driftErrors.map((e: any) => (
              <div key={`${e.side}.${e.catalog}`} className="text-red-600">
                <span className="font-mono">{e.side} ({e.catalog})</span>: {e.error}
              </div>
            ))}
          </CardContent>
        </Card>
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
                <DataTable
                  data={valData.mismatched_tables}
                  columns={[
                    { key: "schema", label: "Schema", sortable: true, className: "text-gray-600" },
                    { key: "table", label: "Table", sortable: true, className: "font-medium" },
                    {
                      key: "source_count", label: "Source Rows", sortable: true, align: "right",
                      render: (v) => v?.toLocaleString() ?? "\u2014",
                    },
                    {
                      key: "dest_count", label: "Dest Rows", sortable: true, align: "right",
                      render: (v) => v?.toLocaleString() ?? "\u2014",
                    },
                    {
                      key: "_diff", label: "Diff", align: "right",
                      render: (_, row) => (
                        <Badge variant="destructive">
                          {row.source_count != null && row.dest_count != null
                            ? (row.source_count - row.dest_count).toLocaleString()
                            : "\u2014"}
                        </Badge>
                      ),
                    },
                  ] as Column[]}
                  searchable
                  searchKeys={["schema", "table"]}
                  pageSize={25}
                  compact
                  tableId="diff-mismatched-tables"
                  rowClassName={() => "bg-red-50/50"}
                  emptyMessage="No mismatched tables."
                />
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
