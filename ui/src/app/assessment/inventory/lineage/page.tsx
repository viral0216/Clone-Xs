// @ts-nocheck
"use client";

import { useState } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Share2, ArrowRight, Loader2, Search, ExternalLink, Info } from "lucide-react";

function getStoredCreds() {
  try {
    return {
      host:  localStorage.getItem("dbx_host")  || "",
      token: localStorage.getItem("dbx_token") || "",
    };
  } catch { return { host: "", token: "" }; }
}

function NodeCard({ table, direction }) {
  const dirColor = direction === "upstream"
    ? "border-blue-200 bg-blue-50/50 dark:bg-blue-950/20 dark:border-blue-900"
    : "border-green-200 bg-green-50/50 dark:bg-green-950/20 dark:border-green-900";
  const name = table.table_name || table.name || JSON.stringify(table);

  return (
    <div className={`rounded-md border px-3 py-2.5 text-xs ${dirColor}`}>
      <p className="font-mono font-medium truncate max-w-[200px]" title={name}>{name}</p>
      {table.table_type && (
        <p className="text-muted-foreground mt-0.5 capitalize">{table.table_type.toLowerCase()}</p>
      )}
    </div>
  );
}

export default function ColumnLineagePage() {
  const [tableName, setTableName] = useState("");
  const [lineage, setLineage]   = useState(null);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState("");
  const [history, setHistory]   = useState([]);

  async function lookupLineage(name) {
    const target = (name || tableName).trim();
    if (!target) return;
    const creds = getStoredCreds();
    setLoading(true);
    setError("");
    setLineage(null);
    try {
      const result = await api.get(
        `/assessment/lineage/table?table_name=${encodeURIComponent(target)}`,
        { headers: { "X-Databricks-Host": creds.host, "X-Databricks-Token": creds.token } },
      );
      setLineage(result);
      setTableName(target);
      setHistory(h => [target, ...h.filter(x => x !== target)].slice(0, 10));
    } catch (e) {
      setError(e?.message ?? "Failed to fetch lineage. Make sure lineage tracking is enabled for this workspace.");
    } finally {
      setLoading(false);
    }
  }

  const upstream   = lineage?.upstream_tables   || [];
  const downstream = lineage?.downstream_tables || [];
  const hasData    = upstream.length > 0 || downstream.length > 0;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Column Lineage Explorer"
        icon={Share2}
        breadcrumbs={["Assessment", "UC Inventory", "Column Lineage"]}
        description="Explore upstream sources and downstream consumers for any Unity Catalog table using Databricks lineage tracking."
      />

      {/* Search */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Search Table Lineage</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex gap-2">
            <input
              type="text"
              value={tableName}
              onChange={e => setTableName(e.target.value)}
              onKeyDown={e => e.key === "Enter" && lookupLineage()}
              placeholder="catalog.schema.table"
              className="flex-1 px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring font-mono"
            />
            <Button onClick={() => lookupLineage()} disabled={loading || !tableName.trim()}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            </Button>
          </div>
          {history.length > 1 && (
            <div className="flex gap-1.5 flex-wrap">
              <span className="text-xs text-muted-foreground mt-0.5">Recent:</span>
              {history.slice(1).map(h => (
                <button
                  key={h}
                  onClick={() => lookupLineage(h)}
                  className="text-xs font-mono text-muted-foreground hover:text-foreground underline underline-offset-2"
                >
                  {h}
                </button>
              ))}
            </div>
          )}
          <div className="flex items-start gap-2 text-xs text-muted-foreground bg-muted/30 rounded-md px-3 py-2">
            <Info className="h-3.5 w-3.5 shrink-0 mt-0.5" />
            <span>
              Lineage uses your stored workspace credentials (set via Run Scan).
              Databricks Lineage Tracking must be enabled for the workspace.
            </span>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-destructive/20">
          <CardContent className="pt-4 pb-3">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {/* Lineage Graph */}
      {lineage && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              Lineage for
              <span className="font-mono text-primary">{lineage.table_name}</span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {!hasData ? (
              <div className="py-8 text-center text-muted-foreground">
                <Share2 className="h-10 w-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm font-medium">No lineage tracked for this table</p>
                <p className="text-xs mt-1 opacity-70">
                  Lineage is captured only when the table is read or written by a Databricks workload.
                  Try running a query against this table first.
                </p>
              </div>
            ) : (
              <div className="flex items-start gap-4 overflow-x-auto pb-2">
                {/* Upstream column */}
                <div className="flex flex-col gap-2 min-w-[200px]">
                  <p className="text-xs font-semibold text-blue-600 dark:text-blue-400 uppercase tracking-wide mb-1">
                    Upstream ({upstream.length})
                  </p>
                  {upstream.length === 0
                    ? <p className="text-xs text-muted-foreground italic">No upstream sources</p>
                    : upstream.map((t, i) => (
                        <button
                          key={i}
                          onClick={() => lookupLineage(t.table_name || t.name)}
                          className="text-left hover:opacity-80 transition-opacity"
                          title="Click to explore this table's lineage"
                        >
                          <NodeCard table={t} direction="upstream" />
                        </button>
                      ))
                  }
                </div>

                {/* Arrows */}
                <div className="flex flex-col items-center justify-center min-h-[80px] gap-1 pt-6">
                  <ArrowRight className="h-5 w-5 text-muted-foreground" />
                </div>

                {/* Current table */}
                <div className="flex flex-col gap-2 min-w-[220px]">
                  <p className="text-xs font-semibold text-foreground uppercase tracking-wide mb-1">Current Table</p>
                  <div className="rounded-md border-2 border-primary/40 bg-primary/5 px-3 py-2.5">
                    <p className="font-mono text-sm font-bold truncate" title={lineage.table_name}>{lineage.table_name}</p>
                    <Badge variant="outline" className="mt-1 text-[10px]">Target</Badge>
                  </div>
                  <div className="flex flex-col gap-1 mt-2">
                    <p className="text-[10px] text-muted-foreground">
                      {upstream.length} upstream · {downstream.length} downstream
                    </p>
                  </div>
                </div>

                {/* Arrows */}
                <div className="flex flex-col items-center justify-center min-h-[80px] gap-1 pt-6">
                  <ArrowRight className="h-5 w-5 text-muted-foreground" />
                </div>

                {/* Downstream column */}
                <div className="flex flex-col gap-2 min-w-[200px]">
                  <p className="text-xs font-semibold text-green-600 dark:text-green-400 uppercase tracking-wide mb-1">
                    Downstream ({downstream.length})
                  </p>
                  {downstream.length === 0
                    ? <p className="text-xs text-muted-foreground italic">No downstream consumers</p>
                    : downstream.map((t, i) => (
                        <button
                          key={i}
                          onClick={() => lookupLineage(t.table_name || t.name)}
                          className="text-left hover:opacity-80 transition-opacity"
                          title="Click to explore this table's lineage"
                        >
                          <NodeCard table={t} direction="downstream" />
                        </button>
                      ))
                  }
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {!lineage && !loading && !error && (
        <div className="text-center py-16 text-muted-foreground">
          <Share2 className="h-12 w-12 mx-auto mb-3 opacity-20" />
          <p className="text-sm">Enter a full table name and press Search.</p>
          <p className="text-xs mt-1 opacity-70">Format: <span className="font-mono">catalog.schema.table</span></p>
        </div>
      )}
    </div>
  );
}
