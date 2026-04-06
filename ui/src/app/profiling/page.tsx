// @ts-nocheck
import { useState, useMemo } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import ProfileSummaryHeader from "@/components/sql/ProfileSummaryHeader";
import ColumnProfileCard from "@/components/sql/ColumnProfileCard";
import {
  Loader2, XCircle, BarChart3, ScanSearch,
  ChevronDown, ChevronRight, ChevronsUpDown, ChevronsDownUp,
  TableProperties, Rows3, Columns3, ShieldAlert, ShieldCheck,
} from "lucide-react";
import PageHeader from "@/components/PageHeader";

export default function ProfilingPage() {
  const { job, run, isRunning } = usePageJob("profiling");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [schema, setSchema] = useState(job?.params?.schema || "");
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const results = job?.data as any;
  const profiles = results?.profiles || [];

  // Compute catalog-level aggregates
  const { totalCols, totalRows, totalTables, nullRate, completeness } = useMemo(() => {
    if (!profiles.length) return { totalCols: 0, totalRows: 0, totalTables: 0, nullRate: 0, completeness: 1 };
    const totalTables = profiles.length;
    const totalRows = results?.total_rows || profiles.reduce((s: number, p: any) => s + (p.row_count || 0), 0);
    const allCols = profiles.flatMap((p: any) => p.columns || []);
    const totalCols = allCols.length;
    const totalNulls = allCols.reduce((s: number, c: any) => s + (c.null_count || 0), 0);
    const totalCells = profiles.reduce((s: number, p: any) => (p.row_count || 0) * (p.columns?.length || 0) + s, 0);
    const nullRate = totalCells > 0 ? totalNulls / totalCells : 0;
    return { totalCols, totalRows, totalTables, nullRate, completeness: 1 - nullRate };
  }, [profiles, results]);

  // Auto-expand first 5 tables when results change
  useMemo(() => {
    if (profiles.length > 0) {
      const keys = profiles.slice(0, 5).map((p: any) => `${p.schema}.${p.table}`);
      setExpandedTables(new Set(keys));
    }
  }, [results]);

  const toggleTable = (key: string) => {
    setExpandedTables(prev => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  };

  const expandAll = () => setExpandedTables(new Set(profiles.map((p: any) => `${p.schema}.${p.table}`)));
  const collapseAll = () => setExpandedTables(new Set());

  const compColor = (v: number) => v >= 0.95 ? "text-green-500" : v >= 0.8 ? "text-amber-500" : "text-red-500";
  const compBg = (v: number) => v >= 0.95 ? "bg-green-500" : v >= 0.8 ? "bg-amber-500" : "bg-red-500";

  return (
    <div className="space-y-4">
      <PageHeader
        title="Data Profiling"
        icon={ScanSearch}
        breadcrumbs={["Analysis", "Profiling"]}
        description="Per-column data quality profiling — null rates, distinct counts, min/max values, and string length distributions. Helps assess data completeness before and after cloning."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-analyze-table"
        docsLabel="ANALYZE TABLE"
      />

      {/* Catalog / Schema picker */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table=""
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
              onTableChange={() => {}}
              showTable={false}
            />
            <Button
              onClick={() => run({ catalog, schema }, () => api.post("/profile", { source_catalog: catalog, schema: schema || undefined }))}
              disabled={!catalog || isRunning}
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <BarChart3 className="h-4 w-4 mr-2" />}
              {isRunning ? "Profiling..." : "Run Profile"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Loading state */}
      {isRunning && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Running data profile...</p>
          </CardContent>
        </Card>
      )}

      {/* Catalog-level KPI cards */}
      {results && profiles.length > 0 && (
        <div className="grid grid-cols-5 gap-3">
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <TableProperties className="h-4 w-4 text-muted-foreground" />
                <p className="text-2xl font-bold text-foreground">{totalTables}</p>
              </div>
              <p className="text-xs text-muted-foreground">Tables Profiled</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Rows3 className="h-4 w-4 text-muted-foreground" />
                <p className="text-2xl font-bold text-foreground">{totalRows.toLocaleString()}</p>
              </div>
              <p className="text-xs text-muted-foreground">Total Rows</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Columns3 className="h-4 w-4 text-muted-foreground" />
                <p className="text-2xl font-bold text-foreground">{totalCols}</p>
              </div>
              <p className="text-xs text-muted-foreground">Total Columns</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <ShieldAlert className="h-4 w-4 text-muted-foreground" />
                <p className={`text-2xl font-bold ${compColor(1 - nullRate)}`}>{(nullRate * 100).toFixed(1)}%</p>
              </div>
              <p className="text-xs text-muted-foreground">Null Rate</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-5 pb-4 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <ShieldCheck className="h-4 w-4 text-muted-foreground" />
                <p className={`text-2xl font-bold ${compColor(completeness)}`}>{(completeness * 100).toFixed(1)}%</p>
              </div>
              <div className="flex items-center justify-center gap-2 mt-1">
                <div className="w-20 h-1.5 bg-muted rounded-full overflow-hidden">
                  <div className={`h-full rounded-full ${compBg(completeness)}`} style={{ width: `${(completeness * 100).toFixed(1)}%` }} />
                </div>
              </div>
              <p className="text-xs text-muted-foreground mt-1">Completeness</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Toolbar + Table groups */}
      {profiles.length > 0 && (
        <Card className="bg-card border-border overflow-hidden">
          {/* Toolbar */}
          <div className="flex items-center justify-between px-4 py-2.5 border-b border-border bg-muted/20">
            <span className="text-sm text-muted-foreground">
              {totalTables} table{totalTables !== 1 ? "s" : ""} profiled
              {results?.profile_time && (
                <span className="ml-2 text-xs">— {new Date(results.profile_time).toLocaleTimeString()}</span>
              )}
            </span>
            <div className="flex gap-1.5">
              <Button variant="ghost" size="sm" onClick={expandAll} className="h-7 text-xs gap-1">
                <ChevronsUpDown className="h-3.5 w-3.5" /> Expand All
              </Button>
              <Button variant="ghost" size="sm" onClick={collapseAll} className="h-7 text-xs gap-1">
                <ChevronsDownUp className="h-3.5 w-3.5" /> Collapse All
              </Button>
            </div>
          </div>

          {/* Table groups */}
          {profiles.map((profile: any) => {
            const key = `${profile.schema}.${profile.table}`;
            const isExpanded = expandedTables.has(key);
            const colCount = profile.columns?.length || 0;
            const tableCompleteness = profile.row_count > 0 && colCount > 0
              ? 1 - profile.columns.reduce((s: number, c: any) => s + (c.null_count || 0), 0) / (profile.row_count * colCount)
              : 1;

            // Enrich columns with distinct_pct for ColumnProfileCard
            const enrichedColumns = (profile.columns || []).map((col: any) => ({
              ...col,
              null_pct: col.null_pct ?? 0,
              null_count: col.null_count ?? 0,
              distinct_count: col.distinct_count ?? 0,
              distinct_pct: profile.row_count > 0
                ? parseFloat(((col.distinct_count / profile.row_count) * 100).toFixed(1))
                : 0,
            }));

            return (
              <div key={key} className="border-b border-border last:border-b-0">
                {/* Table header */}
                <button
                  className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-accent/20 transition-colors"
                  onClick={() => toggleTable(key)}
                >
                  {isExpanded
                    ? <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
                    : <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />
                  }
                  <span className="text-sm font-semibold text-foreground">{key}</span>
                  <Badge variant="outline" className="text-[10px] shrink-0">
                    {profile.row_count?.toLocaleString() ?? "?"} rows
                  </Badge>
                  <Badge variant="outline" className="text-[10px] shrink-0">
                    {colCount} col{colCount !== 1 ? "s" : ""}
                  </Badge>
                  <div className="flex items-center gap-1.5 ml-auto">
                    <div className="w-14 h-1.5 bg-muted rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${compBg(tableCompleteness)}`}
                        style={{ width: `${(tableCompleteness * 100).toFixed(1)}%` }}
                      />
                    </div>
                    <span className={`text-[10px] font-medium ${compColor(tableCompleteness)}`}>
                      {(tableCompleteness * 100).toFixed(1)}%
                    </span>
                  </div>
                  {profile.error && <XCircle className="h-4 w-4 text-red-500 shrink-0" />}
                </button>

                {/* Expanded content */}
                {isExpanded && (
                  <div>
                    {profile.error ? (
                      <div className="px-6 py-3 text-sm text-red-500 flex items-center gap-2 bg-red-50/50 dark:bg-red-950/20">
                        <XCircle className="h-4 w-4 shrink-0" />
                        {profile.error}
                      </div>
                    ) : (
                      <>
                        {/* Per-table summary header with pie chart */}
                        <ProfileSummaryHeader
                          rowCount={profile.row_count || 0}
                          columns={enrichedColumns}
                          profiledAt={results.profile_time}
                        />
                        {/* Column profile cards */}
                        <div className="max-h-[500px] overflow-y-auto">
                          {enrichedColumns.map((col: any, i: number) => (
                            <ColumnProfileCard
                              key={col.column_name}
                              profile={col}
                              rowCount={profile.row_count || 0}
                              index={i}
                            />
                          ))}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </Card>
      )}

      {/* Error state */}
      {job?.status === "error" && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
