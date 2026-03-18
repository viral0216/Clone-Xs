// @ts-nocheck
import { useState, useMemo, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Loader2, XCircle, Columns, ArrowRight, Search, AlertTriangle,
  CheckCircle2, ArrowUpDown, ChevronUp, ChevronDown, Eye,
  BarChart3, GitCompare, LayoutGrid,
} from "lucide-react";

export default function PreviewPage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState<any>(null);

  // Search & sort state
  const [searchQuery, setSearchQuery] = useState("");
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  // View mode and stats toggle
  const [viewMode, setViewMode] = useState<"side-by-side" | "unified">("side-by-side");
  const [showStats, setShowStats] = useState(false);

  // Auto-compare from URL params (e.g., from clone completion)
  const [searchParams] = useSearchParams();
  const [autoTriggered, setAutoTriggered] = useState(false);

  useEffect(() => {
    const src = searchParams.get("source");
    const dst = searchParams.get("dest");
    const sch = searchParams.get("schema");
    const tbl = searchParams.get("table");
    if (src) setSourceCatalog(src);
    if (dst) setDestCatalog(dst);
    if (sch) setSchema(sch);
    if (tbl) setTable(tbl);
    if (src && sch && tbl && !autoTriggered) {
      setAutoTriggered(true);
    }
  }, [searchParams]);

  // Auto-trigger preview when params are loaded
  useEffect(() => {
    if (autoTriggered && sourceCatalog && schema && table && !loading && !results) {
      preview();
    }
  }, [autoTriggered, sourceCatalog, schema, table]);

  async function preview() {
    setLoading(true);
    setError("");
    setResults(null);
    setSearchQuery("");
    setSortCol(null);
    try {
      let data;
      if (destCatalog) {
        const [compare, srcSample, dstSample] = await Promise.all([
          api.post("/sample/compare", {
            source_catalog: sourceCatalog, destination_catalog: destCatalog,
            schema_name: schema, table_name: table, limit: 50,
          }),
          api.post("/sample", { catalog: sourceCatalog, schema_name: schema, table_name: table, limit: 50 }),
          api.post("/sample", { catalog: destCatalog, schema_name: schema, table_name: table, limit: 50 }),
        ]);
        data = {
          source_rows: srcSample.rows || [],
          dest_rows: dstSample.rows || [],
          source_count: compare.source_rows,
          dest_count: compare.dest_rows,
          source_columns: srcSample.columns || [],
          dest_columns: dstSample.columns || [],
          differences: compare.sample_diffs || [],
          match: compare.match,
        };
      } else {
        const sample = await api.post("/sample", {
          catalog: sourceCatalog, schema_name: schema, table_name: table, limit: 50,
        });
        data = {
          source_rows: sample.rows || [],
          dest_rows: [],
          source_count: sample.rows?.length || 0,
          dest_count: 0,
          source_columns: sample.columns || [],
          dest_columns: [],
          differences: [],
        };
      }
      setResults(data);
    } catch (e: any) {
      setError(e.message || "Failed to load preview");
    } finally {
      setLoading(false);
    }
  }

  const sourceRows = results?.source_rows || [];
  const destRows = results?.dest_rows || [];
  const columns = results?.columns || (sourceRows.length > 0 ? Object.keys(sourceRows[0]) : []);
  const diffs = results?.differences || [];
  const srcColInfo = results?.source_columns || [];
  const destColInfo = results?.dest_columns || [];

  // Build column type maps
  const srcTypeMap = useMemo(() => {
    const m: Record<string, string> = {};
    if (Array.isArray(srcColInfo)) srcColInfo.forEach((c: any) => { m[c.name || c.column_name || ""] = c.type || c.data_type || ""; });
    return m;
  }, [srcColInfo]);

  const destTypeMap = useMemo(() => {
    const m: Record<string, string> = {};
    if (Array.isArray(destColInfo)) destColInfo.forEach((c: any) => { m[c.name || c.column_name || ""] = c.type || c.data_type || ""; });
    return m;
  }, [destColInfo]);

  // Build diff lookup: Set of "rowIndex-colName" for quick cell-level highlighting
  const diffCells = useMemo(() => {
    const s = new Set<string>();
    diffs.forEach((d: any) => {
      s.add(`${d.row_index}-${d.column}`);
    });
    return s;
  }, [diffs]);

  // Filter & sort rows
  const filterAndSort = (rows: any[]) => {
    let filtered = rows;
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      filtered = rows.filter((row: any) =>
        Object.values(row).some((v) => String(v ?? "").toLowerCase().includes(q))
      );
    }
    if (sortCol) {
      filtered = [...filtered].sort((a, b) => {
        const va = a[sortCol] ?? "";
        const vb = b[sortCol] ?? "";
        const na = Number(va);
        const nb = Number(vb);
        if (!isNaN(na) && !isNaN(nb)) return sortDir === "asc" ? na - nb : nb - na;
        return sortDir === "asc" ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
      });
    }
    return filtered;
  };

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(col);
      setSortDir("asc");
    }
  };

  // Row count mismatch detection
  const srcCount = results?.source_count ?? sourceRows.length;
  const destCount = results?.dest_count ?? destRows.length;
  const hasCountMismatch = results && destCatalog && srcCount !== destCount;
  const countDiffPct = srcCount > 0 ? Math.abs(((destCount - srcCount) / srcCount) * 100).toFixed(1) : "0";

  // Type mismatches
  const typeMismatches = useMemo(() => {
    if (!destCatalog || Object.keys(srcTypeMap).length === 0 || Object.keys(destTypeMap).length === 0) return [];
    return columns.filter((col: string) => {
      const st = srcTypeMap[col];
      const dt = destTypeMap[col];
      return st && dt && st.toLowerCase() !== dt.toLowerCase();
    }).map((col: string) => ({ column: col, source_type: srcTypeMap[col], dest_type: destTypeMap[col] }));
  }, [columns, srcTypeMap, destTypeMap, destCatalog]);

  // Columns only in source or only in dest
  const srcOnlyCols = useMemo(() => columns.filter((c: string) => destTypeMap[c] === undefined && Object.keys(destTypeMap).length > 0), [columns, destTypeMap]);
  const destOnlyCols = useMemo(() => Object.keys(destTypeMap).filter((c) => !srcTypeMap[c]), [srcTypeMap, destTypeMap]);

  // Column statistics (computed client-side from preview data)
  const colStats = useMemo(() => {
    if (sourceRows.length === 0) return [];
    return columns.map((col: string) => {
      const srcVals = sourceRows.map((r: any) => r[col]);
      const dstVals = destRows.map((r: any) => r[col]);
      const nonNull = srcVals.filter((v: any) => v != null && v !== "" && v !== "null");
      const nums = nonNull.map(Number).filter((n: number) => !isNaN(n));
      const dstNonNull = dstVals.filter((v: any) => v != null && v !== "" && v !== "null");
      return {
        column: col,
        type: srcTypeMap[col] || "—",
        srcNulls: srcVals.length - nonNull.length,
        dstNulls: destRows.length > 0 ? dstVals.length - dstNonNull.length : null,
        distinct: new Set(nonNull.map(String)).size,
        min: nums.length ? Math.min(...nums) : null,
        max: nums.length ? Math.max(...nums) : null,
        avg: nums.length ? (nums.reduce((a: number, b: number) => a + b, 0) / nums.length) : null,
        isNumeric: nums.length > nonNull.length * 0.5,
      };
    });
  }, [sourceRows, destRows, columns, srcTypeMap]);

  // Unified diff rows: interleave source and dest, showing only differences
  const unifiedDiffRows = useMemo(() => {
    if (diffs.length === 0 && sourceRows.length === destRows.length) return [];
    const rows: any[] = [];
    const maxRows = Math.max(sourceRows.length, destRows.length);
    let matchCount = 0;

    for (let i = 0; i < maxRows; i++) {
      const rowDiffs = diffs.filter((d: any) => d.row_index === i);
      const srcRow = sourceRows[i];
      const dstRow = destRows[i];

      if (rowDiffs.length === 0 && srcRow && dstRow) {
        matchCount++;
        continue;
      }

      // Flush match summary
      if (matchCount > 0) {
        rows.push({ type: "match_summary", count: matchCount });
        matchCount = 0;
      }

      if (rowDiffs.length > 0) {
        rowDiffs.forEach((d: any) => {
          rows.push({
            type: "diff",
            rowIndex: i,
            column: d.column,
            sourceValue: d.source_value,
            destValue: d.dest_value,
          });
        });
      } else if (srcRow && !dstRow) {
        rows.push({ type: "source_only", rowIndex: i, data: srcRow });
      } else if (!srcRow && dstRow) {
        rows.push({ type: "dest_only", rowIndex: i, data: dstRow });
      }
    }

    if (matchCount > 0) {
      rows.push({ type: "match_summary", count: matchCount });
    }
    return rows;
  }, [sourceRows, destRows, diffs]);

  const renderTable = (title: string, rows: any[], isSource: boolean) => {
    const filtered = filterAndSort(rows);
    const typeMap = isSource ? srcTypeMap : destTypeMap;

    return (
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-lg flex items-center justify-between">
            {title}
            <Badge variant="outline">{filtered.length} rows (max 50)</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto max-h-96 overflow-y-auto border border-border rounded">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-background z-10">
                <tr className="border-b border-border">
                  {columns.map((col: string) => {
                    const colType = typeMap[col] || "";
                    const hasTypeMismatch = typeMismatches.some((m: any) => m.column === col);
                    return (
                      <th
                        key={col}
                        className="text-left py-2 px-3 font-medium whitespace-nowrap cursor-pointer hover:bg-accent/50 select-none"
                        onClick={() => handleSort(col)}
                      >
                        <div className="flex items-center gap-1">
                          <span className={hasTypeMismatch ? "text-amber-500" : "text-foreground"}>{col}</span>
                          {sortCol === col && (sortDir === "asc" ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                          {sortCol !== col && <ArrowUpDown className="h-3 w-3 text-muted-foreground opacity-30" />}
                        </div>
                        {colType && (
                          <span className={`text-[10px] font-normal ${hasTypeMismatch ? "text-amber-500" : "text-muted-foreground"}`}>
                            {colType}
                          </span>
                        )}
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 && (
                  <tr><td colSpan={columns.length} className="text-center py-8 text-muted-foreground">No rows</td></tr>
                )}
                {filtered.map((row: any, i: number) => (
                  <tr key={i} className="border-b border-border hover:bg-accent/30">
                    {columns.map((col: string) => {
                      const cellKey = `${i}-${col}`;
                      const isDiff = isSource && diffCells.has(cellKey);
                      const isMatch = !isSource && destRows.length > 0 && i < destRows.length && !diffCells.has(cellKey);
                      return (
                        <td
                          key={col}
                          className={`py-1.5 px-3 whitespace-nowrap ${
                            isDiff
                              ? "bg-red-100 dark:bg-red-950 text-red-700 dark:text-red-400 font-medium"
                              : ""
                          }`}
                        >
                          {String(row[col] ?? "")}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Preview"
        icon={Eye}
        breadcrumbs={["Discovery", "Data Preview"]}
        description="Sample and compare data between source and destination tables side by side — verify row-level accuracy after clone or sync operations."
      />

      {/* Picker Card */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex flex-wrap gap-4 items-end">
            <CatalogPicker
              catalog={sourceCatalog} schema={schema} table={table}
              onCatalogChange={setSourceCatalog} onSchemaChange={setSchema} onTableChange={setTable}
            />
            <div className="flex items-center text-muted-foreground pb-2"><ArrowRight className="h-5 w-5" /></div>
            <CatalogPicker
              catalog={destCatalog} schema="" table=""
              onCatalogChange={setDestCatalog} onSchemaChange={() => {}} onTableChange={() => {}}
              showSchema={false} showTable={false}
            />
            <Button onClick={preview} disabled={!sourceCatalog || !schema || !table || loading} className="bg-red-600 hover:bg-red-700 text-white">
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Eye className="h-4 w-4 mr-2" />}
              {loading ? "Loading..." : "Preview"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Row Count Cards + Mismatch Alert */}
      {results && (
        <>
          <div className="grid grid-cols-2 gap-4">
            <Card className="bg-card border-border">
              <CardContent className="pt-4 text-center">
                <p className="text-3xl font-bold text-foreground">{srcCount.toLocaleString()}</p>
                <p className="text-xs text-muted-foreground">Source Rows</p>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-4 text-center">
                <p className={`text-3xl font-bold ${hasCountMismatch ? "text-amber-500" : "text-foreground"}`}>
                  {destCount.toLocaleString()}
                </p>
                <p className="text-xs text-muted-foreground">Dest Rows</p>
              </CardContent>
            </Card>
          </div>

          {/* Row Count Mismatch Alert */}
          {hasCountMismatch && (
            <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800">
              <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0" />
              <div>
                <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                  Row count mismatch: {srcCount.toLocaleString()} source vs {destCount.toLocaleString()} destination ({countDiffPct}% difference)
                </p>
                <p className="text-xs text-amber-600 dark:text-amber-400 mt-0.5">
                  {destCount > srcCount
                    ? `Destination has ${(destCount - srcCount).toLocaleString()} more rows than source`
                    : `Destination is missing ${(srcCount - destCount).toLocaleString()} rows from source`}
                </p>
              </div>
            </div>
          )}

          {/* Type Mismatches */}
          {typeMismatches.length > 0 && (
            <div className="flex items-start gap-3 px-4 py-3 rounded-lg bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800">
              <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                  {typeMismatches.length} column type mismatch{typeMismatches.length > 1 ? "es" : ""} detected
                </p>
                <div className="mt-2 space-y-1">
                  {typeMismatches.map((m: any) => (
                    <div key={m.column} className="flex items-center gap-2 text-xs">
                      <Badge variant="outline" className="font-mono">{m.column}</Badge>
                      <span className="text-red-600 font-mono">{m.source_type}</span>
                      <ArrowRight className="h-3 w-3 text-muted-foreground" />
                      <span className="text-green-600 font-mono">{m.dest_type}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Column presence mismatches */}
          {(srcOnlyCols.length > 0 || destOnlyCols.length > 0) && (
            <div className="flex items-start gap-3 px-4 py-3 rounded-lg bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800">
              <Columns className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-blue-800 dark:text-blue-300">Schema drift detected</p>
                {srcOnlyCols.length > 0 && (
                  <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                    Source only: {srcOnlyCols.map((c: string) => <Badge key={c} variant="outline" className="ml-1 font-mono text-[10px]">{c}</Badge>)}
                  </p>
                )}
                {destOnlyCols.length > 0 && (
                  <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                    Dest only: {destOnlyCols.map((c) => <Badge key={c} variant="outline" className="ml-1 font-mono text-[10px]">{c}</Badge>)}
                  </p>
                )}
              </div>
            </div>
          )}

          {/* Match badge */}
          {results.match !== undefined && diffs.length === 0 && !hasCountMismatch && typeMismatches.length === 0 && (
            <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-green-50 dark:bg-green-950/30 border border-green-200 dark:border-green-800">
              <CheckCircle2 className="h-5 w-5 text-green-600" />
              <p className="text-sm font-medium text-green-800 dark:text-green-300">Source and destination match — no differences found</p>
            </div>
          )}
        </>
      )}

      {/* Column Statistics Panel */}
      {results && sourceRows.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <button onClick={() => setShowStats(!showStats)} className="flex items-center justify-between w-full text-left">
              <CardTitle className="text-base flex items-center gap-2">
                <BarChart3 className="h-4 w-4 text-blue-600" />
                Column Statistics ({columns.length} columns)
              </CardTitle>
              <Badge variant="outline">{showStats ? "Hide" : "Show"}</Badge>
            </button>
          </CardHeader>
          {showStats && (
            <CardContent className="pt-0">
              <div className="overflow-x-auto border border-border rounded max-h-64 overflow-y-auto">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-background">
                    <tr className="border-b border-border">
                      <th className="text-left py-2 px-3 font-medium">Column</th>
                      <th className="text-left py-2 px-3 font-medium">Type</th>
                      <th className="text-right py-2 px-3 font-medium">Nulls (src)</th>
                      {destRows.length > 0 && <th className="text-right py-2 px-3 font-medium">Nulls (dest)</th>}
                      <th className="text-right py-2 px-3 font-medium">Distinct</th>
                      <th className="text-right py-2 px-3 font-medium">Min</th>
                      <th className="text-right py-2 px-3 font-medium">Max</th>
                      <th className="text-right py-2 px-3 font-medium">Avg</th>
                    </tr>
                  </thead>
                  <tbody>
                    {colStats.map((s: any) => (
                      <tr key={s.column} className="border-b border-border hover:bg-accent/30">
                        <td className="py-1.5 px-3 font-mono font-medium">{s.column}</td>
                        <td className="py-1.5 px-3 text-muted-foreground">{s.type}</td>
                        <td className={`py-1.5 px-3 text-right ${s.srcNulls > 0 ? "text-amber-600 font-medium" : "text-muted-foreground"}`}>{s.srcNulls}</td>
                        {destRows.length > 0 && (
                          <td className={`py-1.5 px-3 text-right ${s.dstNulls > 0 ? "text-amber-600 font-medium" : "text-muted-foreground"}`}>{s.dstNulls ?? "—"}</td>
                        )}
                        <td className="py-1.5 px-3 text-right">{s.distinct}</td>
                        <td className="py-1.5 px-3 text-right font-mono">{s.isNumeric && s.min != null ? s.min.toLocaleString() : "—"}</td>
                        <td className="py-1.5 px-3 text-right font-mono">{s.isNumeric && s.max != null ? s.max.toLocaleString() : "—"}</td>
                        <td className="py-1.5 px-3 text-right font-mono">{s.isNumeric && s.avg != null ? s.avg.toFixed(2) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          )}
        </Card>
      )}

      {/* Search bar + View mode toggle */}
      {results && sourceRows.length > 0 && (
        <div className="flex items-center gap-3 flex-wrap">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search rows by any column value..."
              className="pl-10"
            />
          </div>
          {searchQuery && (
            <Button variant="ghost" size="sm" onClick={() => setSearchQuery("")}>Clear</Button>
          )}
          {sortCol && (
            <Badge variant="outline" className="gap-1">
              Sorted by {sortCol} {sortDir === "asc" ? "↑" : "↓"}
              <button onClick={() => { setSortCol(null); setSortDir("asc"); }} className="ml-1 hover:text-red-500">×</button>
            </Badge>
          )}
          {/* View mode toggle */}
          {destRows.length > 0 && (
            <div className="flex items-center gap-1 ml-auto bg-background border border-border rounded-lg p-0.5">
              <button
                onClick={() => setViewMode("side-by-side")}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-colors ${viewMode === "side-by-side" ? "bg-blue-600 text-white" : "text-muted-foreground hover:text-foreground"}`}
              >
                <LayoutGrid className="h-3.5 w-3.5" /> Side-by-Side
              </button>
              <button
                onClick={() => setViewMode("unified")}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-colors ${viewMode === "unified" ? "bg-blue-600 text-white" : "text-muted-foreground hover:text-foreground"}`}
              >
                <GitCompare className="h-3.5 w-3.5" /> Unified Diff
              </button>
            </div>
          )}
        </div>
      )}

      {/* Data Tables — Side by Side */}
      {results && viewMode === "side-by-side" && (
        <div className="grid grid-cols-2 gap-4">
          {renderTable("Source", sourceRows, true)}
          {renderTable("Destination", destRows, false)}
        </div>

      )}

      {/* Data Tables — Unified Diff View */}
      {results && viewMode === "unified" && destRows.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg flex items-center gap-2">
              <GitCompare className="h-5 w-5 text-blue-600" />
              Unified Diff
              <Badge variant="outline" className="ml-2">
                {unifiedDiffRows.filter((r: any) => r.type === "diff").length} differences,{" "}
                {unifiedDiffRows.filter((r: any) => r.type === "match_summary").reduce((s: number, r: any) => s + r.count, 0)} matching rows
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {unifiedDiffRows.length === 0 ? (
              <div className="flex items-center gap-2 py-8 justify-center text-green-600">
                <CheckCircle2 className="h-5 w-5" />
                <span className="text-sm font-medium">All rows match — no differences</span>
              </div>
            ) : (
              <div className="overflow-x-auto max-h-96 overflow-y-auto border border-border rounded">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-background z-10">
                    <tr className="border-b border-border">
                      <th className="text-left py-2 px-3 font-medium w-16">Row</th>
                      <th className="text-left py-2 px-3 font-medium">Column</th>
                      <th className="text-left py-2 px-3 font-medium">Source Value</th>
                      <th className="text-left py-2 px-3 font-medium">Dest Value</th>
                      <th className="text-center py-2 px-3 font-medium w-20">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {unifiedDiffRows.map((row: any, idx: number) => {
                      if (row.type === "match_summary") {
                        return (
                          <tr key={`match-${idx}`} className="border-b border-border bg-green-50/50 dark:bg-green-950/20">
                            <td colSpan={5} className="py-2 px-3 text-center text-xs text-green-600">
                              <CheckCircle2 className="h-3 w-3 inline mr-1" />
                              {row.count} matching row{row.count > 1 ? "s" : ""} (collapsed)
                            </td>
                          </tr>
                        );
                      }
                      if (row.type === "diff") {
                        return (
                          <tr key={`diff-${idx}`} className="border-b border-border bg-red-50/50 dark:bg-red-950/20">
                            <td className="py-1.5 px-3"><Badge variant="outline" className="text-xs">{row.rowIndex}</Badge></td>
                            <td className="py-1.5 px-3 font-mono text-xs">{row.column}</td>
                            <td className="py-1.5 px-3 text-red-600 dark:text-red-400 font-mono text-xs">{String(row.sourceValue ?? "")}</td>
                            <td className="py-1.5 px-3 text-green-600 dark:text-green-400 font-mono text-xs">{String(row.destValue ?? "")}</td>
                            <td className="py-1.5 px-3 text-center"><XCircle className="h-4 w-4 text-red-500 inline" /></td>
                          </tr>
                        );
                      }
                      if (row.type === "source_only") {
                        return (
                          <tr key={`src-${idx}`} className="border-b border-border bg-amber-50/50 dark:bg-amber-950/20">
                            <td className="py-1.5 px-3"><Badge variant="outline" className="text-xs">{row.rowIndex}</Badge></td>
                            <td className="py-1.5 px-3 text-xs text-muted-foreground" colSpan={2}>Row exists in source only</td>
                            <td className="py-1.5 px-3 text-xs text-muted-foreground">—</td>
                            <td className="py-1.5 px-3 text-center"><AlertTriangle className="h-4 w-4 text-amber-500 inline" /></td>
                          </tr>
                        );
                      }
                      if (row.type === "dest_only") {
                        return (
                          <tr key={`dst-${idx}`} className="border-b border-border bg-blue-50/50 dark:bg-blue-950/20">
                            <td className="py-1.5 px-3"><Badge variant="outline" className="text-xs">{row.rowIndex}</Badge></td>
                            <td className="py-1.5 px-3 text-xs text-muted-foreground">—</td>
                            <td className="py-1.5 px-3 text-xs text-muted-foreground" colSpan={2}>Row exists in destination only</td>
                            <td className="py-1.5 px-3 text-center"><AlertTriangle className="h-4 w-4 text-blue-500 inline" /></td>
                          </tr>
                        );
                      }
                      return null;
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Differences Detail */}
      {diffs.length > 0 && (
        <Card className="bg-card border-amber-300 dark:border-amber-700">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-600" />
              Cell-Level Differences ({diffs.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto max-h-64 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-background">
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 font-medium">Row</th>
                    <th className="text-left py-2 px-3 font-medium">Column</th>
                    <th className="text-left py-2 px-3 font-medium">Source Value</th>
                    <th className="text-left py-2 px-3 font-medium">Dest Value</th>
                  </tr>
                </thead>
                <tbody>
                  {diffs.map((d: any, i: number) => (
                    <tr key={i} className="border-b border-border">
                      <td className="py-1.5 px-3"><Badge variant="outline">{d.row_index ?? i}</Badge></td>
                      <td className="py-1.5 px-3 font-mono text-xs">{d.column}</td>
                      <td className="py-1.5 px-3 text-red-600 dark:text-red-400 font-mono text-xs">{String(d.source_value ?? "")}</td>
                      <td className="py-1.5 px-3 text-green-600 dark:text-green-400 font-mono text-xs">{String(d.dest_value ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

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
