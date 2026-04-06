// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import DataTable, { Column } from "@/components/DataTable";
import {
  Network, Loader2, ArrowRight, Search,
} from "lucide-react";

interface TableEntry {
  source: string;
  destination: string;
  clone_type: string;
  timestamp: string;
  direction: "upstream" | "downstream";
  data_source: string;
  hop: number;
}

interface ColumnEntry {
  source: string;
  target: string;
  data_source: string;
}

interface LineageResult {
  table_entries: TableEntry[];
  column_entries: ColumnEntry[];
  sources_used: string[];
}

function directionColor(direction: string) {
  if (direction === "upstream") return "text-blue-500 border-blue-500/30 bg-blue-500/5";
  if (direction === "downstream") return "text-green-500 border-green-500/30 bg-green-500/5";
  return "text-muted-foreground border-border";
}

function sourceColor(source: string) {
  const colors: Record<string, string> = {
    system_table: "text-purple-500 border-purple-500/30 bg-purple-500/5",
    clone_audit: "text-blue-500 border-blue-500/30 bg-blue-500/5",
    dlt: "text-amber-500 border-amber-500/30 bg-amber-500/5",
    spark: "text-orange-500 border-orange-500/30 bg-orange-500/5",
  };
  return colors[source] || "text-muted-foreground border-border";
}

export default function DataLineagePage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [depth, setDepth] = useState(2);
  const [includeColumns, setIncludeColumns] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<LineageResult | null>(null);
  const [hasRun, setHasRun] = useState(false);

  async function traceLineage() {
    if (!catalog) {
      toast.error("Please select a catalog first.");
      return;
    }
    setLoading(true);
    try {
      const body: Record<string, any> = { catalog, depth };
      if (table) body.table = table;
      if (includeColumns) body.include_columns = true;
      const data = await api.post("/lineage", body);
      setResult({
        table_entries: Array.isArray(data?.table_entries) ? data.table_entries : [],
        column_entries: Array.isArray(data?.column_entries) ? data.column_entries : [],
        sources_used: Array.isArray(data?.sources_used) ? data.sources_used : [],
      });
      setHasRun(true);
    } catch (err: any) {
      toast.error(err?.message || "Failed to trace lineage.");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Lineage"
        description="Visualize table and column dependencies across your data platform."
        icon={Network}
        breadcrumbs={["Data Quality", "Observability", "Lineage"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[320px]">
              <CatalogPicker
                catalog={catalog}
                schema={schema}
                table={table}
                onCatalogChange={setCatalog}
                onSchemaChange={setSchema}
                onTableChange={setTable}
                showSchema
                showTable
                idPrefix="lineage"
              />
            </div>
            <div className="w-28">
              <label className="text-xs text-muted-foreground mb-1 block">Depth</label>
              <Input
                type="number"
                min={1}
                max={5}
                value={depth}
                onChange={(e) => setDepth(Math.min(5, Math.max(1, Number(e.target.value))))}
              />
            </div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none pb-2">
              <input
                type="checkbox"
                checked={includeColumns}
                onChange={(e) => setIncludeColumns(e.target.checked)}
                className="rounded border-border"
              />
              Include column lineage
            </label>
            <Button onClick={traceLineage} disabled={loading || !catalog}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}
              Trace Lineage
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Sources Used */}
      {hasRun && result && result.sources_used.length > 0 && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground">Sources used:</span>
          {result.sources_used.map((s) => (
            <Badge key={s} variant="outline" className={`text-[10px] ${sourceColor(s)}`}>
              {s}
            </Badge>
          ))}
        </div>
      )}

      {/* Empty State */}
      {!hasRun && (
        <Card>
          <CardContent className="py-12 text-center">
            <Network className="h-10 w-10 mx-auto text-muted-foreground/30 mb-3" />
            <p className="text-sm font-medium">Select a catalog or table and click Trace Lineage</p>
            <p className="text-xs text-muted-foreground mt-1">
              Lineage is resolved from system tables, clone audit logs, DLT pipelines, and Spark query history.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Table Lineage */}
      {hasRun && result && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">
              Table Lineage ({result.table_entries.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" /> Tracing lineage...
              </div>
            ) : result.table_entries.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4">No table lineage found.</p>
            ) : (
              <DataTable
                data={result.table_entries}
                columns={[
                  {
                    key: "source",
                    label: "Source",
                    sortable: true,
                    render: (v) => <span className="font-mono text-xs">{v}</span>,
                  },
                  {
                    key: "_arrow",
                    label: "",
                    render: () => <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />,
                  },
                  {
                    key: "destination",
                    label: "Destination",
                    sortable: true,
                    render: (v) => <span className="font-mono text-xs">{v}</span>,
                  },
                  {
                    key: "direction",
                    label: "Direction",
                    sortable: true,
                    align: "center",
                    render: (v) => (
                      <Badge variant="outline" className={`text-[10px] ${directionColor(v)}`}>
                        {v}
                      </Badge>
                    ),
                  },
                  {
                    key: "data_source",
                    label: "Data Source",
                    sortable: true,
                    align: "center",
                    render: (v) => (
                      <Badge variant="outline" className={`text-[10px] ${sourceColor(v)}`}>
                        {v}
                      </Badge>
                    ),
                  },
                  {
                    key: "hop",
                    label: "Hop",
                    sortable: true,
                    align: "right",
                    render: (v) => <span className="tabular-nums">{v}</span>,
                  },
                  {
                    key: "clone_type",
                    label: "Clone Type",
                    sortable: true,
                    render: (v) => <span className="text-xs">{v || "\u2014"}</span>,
                  },
                  {
                    key: "timestamp",
                    label: "Timestamp",
                    sortable: true,
                    render: (v) => <span className="text-xs">{v ? String(v).slice(0, 19) : "\u2014"}</span>,
                  },
                ] as Column[]}
                searchable
                searchKeys={["source", "destination", "direction", "data_source", "clone_type"]}
                pageSize={25}
                compact
                tableId="table-lineage"
                emptyMessage="No table lineage found."
              />
            )}
          </CardContent>
        </Card>
      )}

      {/* Column Lineage */}
      {hasRun && result && includeColumns && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">
              Column Lineage ({result.column_entries.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {result.column_entries.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4">No column lineage found.</p>
            ) : (
              <DataTable
                data={result.column_entries}
                columns={[
                  {
                    key: "source",
                    label: "Source Column",
                    sortable: true,
                    render: (v) => <span className="font-mono text-xs">{v}</span>,
                  },
                  {
                    key: "_arrow",
                    label: "",
                    render: () => <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />,
                  },
                  {
                    key: "target",
                    label: "Target Column",
                    sortable: true,
                    render: (v) => <span className="font-mono text-xs">{v}</span>,
                  },
                  {
                    key: "data_source",
                    label: "Data Source",
                    sortable: true,
                    align: "center",
                    render: (v) => (
                      <Badge variant="outline" className={`text-[10px] ${sourceColor(v)}`}>
                        {v}
                      </Badge>
                    ),
                  },
                ] as Column[]}
                searchable
                searchKeys={["source", "target", "data_source"]}
                pageSize={25}
                compact
                tableId="column-lineage"
                emptyMessage="No column lineage found."
              />
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
