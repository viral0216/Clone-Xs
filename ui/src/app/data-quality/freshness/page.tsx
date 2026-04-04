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
import {
  Clock, Loader2, CheckCircle, XCircle, AlertTriangle, HelpCircle, RefreshCw,
} from "lucide-react";
import DataTable, { Column } from "@/components/DataTable";

interface FreshnessRow {
  table_fqn: string;
  last_modified: string | null;
  hours_since_update: number | null;
  status: "fresh" | "stale" | "unknown";
}

function statusColor(status: string) {
  if (status === "fresh") return "text-green-500 border-green-500/30";
  if (status === "stale") return "text-red-500 border-red-500/30";
  return "text-muted-foreground border-border";
}

function hoursColor(hours: number | null) {
  if (hours == null) return "text-muted-foreground";
  if (hours < 6) return "text-green-500";
  if (hours <= 24) return "text-amber-500";
  return "text-red-500";
}

function StatusIcon({ status }: { status: string }) {
  if (status === "fresh") return <CheckCircle className="h-3.5 w-3.5 text-green-500" />;
  if (status === "stale") return <XCircle className="h-3.5 w-3.5 text-red-500" />;
  return <HelpCircle className="h-3.5 w-3.5 text-muted-foreground" />;
}

export default function DataFreshnessPage() {
  const [catalog, setCatalog] = useState("");
  const [maxStaleHours, setMaxStaleHours] = useState(24);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<FreshnessRow[]>([]);
  const [hasRun, setHasRun] = useState(false);

  async function checkFreshness() {
    if (!catalog) {
      toast.error("Please select a catalog first.");
      return;
    }
    setLoading(true);
    try {
      const data = await api.get(`/data-quality/freshness/${encodeURIComponent(catalog)}?max_stale_hours=${maxStaleHours}`);
      const tables = Array.isArray(data) ? data : (data?.tables ?? []);
      setResults(tables);
      setHasRun(true);
      if (data?.error) {
        toast.error(data.error);
      }
    } catch (err: any) {
      toast.error(err?.message || "Failed to check freshness.");
      setResults([]);
    } finally {
      setLoading(false);
    }
  }

  const fresh = results.filter((r) => r.status === "fresh").length;
  const stale = results.filter((r) => r.status === "stale").length;
  const unknown = results.filter((r) => r.status === "unknown").length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Freshness"
        description="Monitor how recently tables have been updated across your catalog."
        icon={Clock}
        breadcrumbs={["Data Quality", "Monitoring", "Freshness"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="min-w-[240px]">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog</label>
              <CatalogPicker value={catalog} onChange={setCatalog} placeholder="Select catalog..." />
            </div>
            <div className="w-36">
              <label className="text-xs text-muted-foreground mb-1 block">Max Stale Hours</label>
              <Input
                type="number"
                min={1}
                max={720}
                value={maxStaleHours}
                onChange={(e) => setMaxStaleHours(Number(e.target.value))}
              />
            </div>
            <Button onClick={checkFreshness} disabled={loading || !catalog}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Check Freshness
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {hasRun && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Tables</p>
              <p className="text-2xl font-bold mt-1">{results.length}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Fresh</p>
              <p className="text-2xl font-bold mt-1 text-green-500">{fresh}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Stale</p>
              <p className="text-2xl font-bold mt-1 text-red-500">{stale}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Unknown</p>
              <p className="text-2xl font-bold mt-1 text-muted-foreground">{unknown}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Results Table */}
      {hasRun && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Freshness Results ({results.length})</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" /> Checking freshness...
              </div>
            ) : results.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4">No tables found in this catalog.</p>
            ) : (
              <DataTable
                data={results}
                columns={[
                  { key: "table_fqn", label: "Table Name", sortable: true, className: "font-mono text-xs" },
                  { key: "last_modified", label: "Last Modified", sortable: true, render: (v) => v ? String(v).slice(0, 19) : "—" },
                  { key: "hours_since_update", label: "Hours Since Update", sortable: true, align: "right",
                    render: (v) => v != null ? <span className={`tabular-nums ${hoursColor(v)}`}>{v.toFixed(1)}h</span> : "—" },
                  { key: "status", label: "Status", sortable: true, align: "center",
                    render: (v) => (
                      <div className="flex items-center justify-center gap-1.5">
                        <StatusIcon status={v} />
                        <Badge variant="outline" className={`text-[10px] ${statusColor(v)}`}>{v}</Badge>
                      </div>
                    ) },
                ] as Column[]}
                searchable
                searchKeys={["table_fqn", "status"]}
                pageSize={25}
                compact
                tableId="freshness-results"
                emptyMessage="No tables found."
              />
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
