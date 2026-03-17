// @ts-nocheck
import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import CatalogPicker from "@/components/CatalogPicker";
import { Badge } from "@/components/ui/badge";
import { useSearch, useStats, useColumnUsage } from "@/hooks/useApi";
import {
  Search, BarChart3, Database, Table2, HardDrive, Rows3,
  ArrowUpDown, Loader2, FolderTree, Columns, Users,
} from "lucide-react";
import PageHeader from "@/components/PageHeader";

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function sizeBadgeColor(bytes: number): string {
  if (bytes >= 10_000_000_000) return "bg-red-100 text-red-800";
  if (bytes >= 1_000_000_000) return "bg-yellow-100 text-yellow-800";
  return "bg-green-100 text-green-800";
}

export default function ExplorePage() {
  const [catalog, setCatalog] = useState("");
  const [pattern, setPattern] = useState("");
  const [searchColumns, setSearchColumns] = useState(false);
  const [sortField, setSortField] = useState<string>("schema");
  const [sortAsc, setSortAsc] = useState(true);

  const search = useSearch();
  const stats = useStats();
  const columnUsage = useColumnUsage();

  // Auto-fetch column usage when stats load (once per catalog)
  const colUsageCatalogRef = useRef("");
  useEffect(() => {
    if (stats.data && catalog && colUsageCatalogRef.current !== catalog) {
      colUsageCatalogRef.current = catalog;
      columnUsage.mutate({ catalog });
    }
  }, [stats.data, catalog]);

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(true);
    }
  };

  const sortedSchemas = stats.data?.schema_summaries
    ? [...stats.data.schema_summaries].sort((a, b) => {
        let cmp = 0;
        if (sortField === "schema") cmp = a.schema.localeCompare(b.schema);
        else if (sortField === "tables") cmp = a.num_tables - b.num_tables;
        else if (sortField === "size") cmp = a.total_size_bytes - b.total_size_bytes;
        else if (sortField === "rows") cmp = a.total_rows - b.total_rows;
        return sortAsc ? cmp : -cmp;
      })
    : [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Catalog Explorer"
        icon={FolderTree}
        breadcrumbs={["Discovery", "Explorer"]}
        description="Browse Unity Catalog hierarchy — catalogs, schemas, tables, views, and columns with row counts, sizes, and metadata. Search across objects by name or pattern."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/"
        docsLabel="Unity Catalog objects"
      />

      {/* Catalog Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={catalog}
              onCatalogChange={setCatalog}
              showSchema={false}
              showTable={false}
            />
            <Button
              onClick={() => stats.mutate({ source_catalog: catalog })}
              disabled={!catalog || stats.isPending}
            >
              {stats.isPending ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <BarChart3 className="h-4 w-4 mr-2" />
              )}
              {stats.isPending ? "Loading..." : "Get Stats"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Stats Error */}
      {stats.isError && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="pt-6">
            <p className="text-red-700 text-sm">{stats.error?.message || "Failed to load stats"}</p>
          </CardContent>
        </Card>
      )}

      {/* Stats Results */}
      {stats.data && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <Database className="h-5 w-5 text-blue-600" />
                  </div>
                  <div>
                    <p className="text-2xl font-bold">{stats.data.num_schemas}</p>
                    <p className="text-xs text-gray-500">Schemas</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-purple-100 rounded-lg">
                    <Table2 className="h-5 w-5 text-purple-600" />
                  </div>
                  <div>
                    <p className="text-2xl font-bold">{stats.data.num_tables}</p>
                    <p className="text-xs text-gray-500">Tables</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-orange-100 rounded-lg">
                    <HardDrive className="h-5 w-5 text-orange-600" />
                  </div>
                  <div>
                    <p className="text-2xl font-bold">{stats.data.total_size_display}</p>
                    <p className="text-xs text-gray-500">Total Size</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-green-100 rounded-lg">
                    <Rows3 className="h-5 w-5 text-green-600" />
                  </div>
                  <div>
                    <p className="text-2xl font-bold">{formatNumber(stats.data.total_rows)}</p>
                    <p className="text-xs text-gray-500">Total Rows</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Schema Table */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span>Schema Breakdown</span>
                <Badge variant="outline">{stats.data.catalog}</Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b bg-gray-50">
                      {[
                        { key: "schema", label: "Schema" },
                        { key: "tables", label: "Tables" },
                        { key: "size", label: "Size" },
                        { key: "rows", label: "Rows" },
                      ].map(({ key, label }) => (
                        <th
                          key={key}
                          className="text-left py-3 px-4 font-medium text-gray-600 cursor-pointer hover:text-gray-900 select-none"
                          onClick={() => handleSort(key)}
                        >
                          <div className="flex items-center gap-1">
                            {label}
                            <ArrowUpDown className={`h-3 w-3 ${sortField === key ? "text-blue-600" : "text-gray-400"}`} />
                          </div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSchemas.map((s) => (
                      <tr key={s.schema} className="border-b hover:bg-gray-50 transition-colors">
                        <td className="py-3 px-4 font-medium">{s.schema}</td>
                        <td className="py-3 px-4">{s.num_tables}</td>
                        <td className="py-3 px-4">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${sizeBadgeColor(s.total_size_bytes)}`}>
                            {s.total_size_display}
                          </span>
                        </td>
                        <td className="py-3 px-4">{formatNumber(s.total_rows)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
          {/* Column Usage */}
          {columnUsage.isPending && (
            <div className="flex items-center justify-center py-6 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin mr-2" />Loading column usage...
            </div>
          )}
          {columnUsage.data && (columnUsage.data.top_columns?.length > 0 || columnUsage.data.top_users?.length > 0) && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Most Used Columns */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Columns className="h-4 w-4 text-cyan-600" />
                    Most Used Columns
                    {columnUsage.data.period_days && (
                      <Badge variant="outline" className="text-[10px] font-normal">last {columnUsage.data.period_days}d</Badge>
                    )}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!columnUsage.data.top_columns?.length ? (
                    <p className="text-sm text-muted-foreground">No column usage data. Ensure system tables are enabled.</p>
                  ) : (
                    <div className="space-y-2">
                      {columnUsage.data.top_columns.slice(0, 10).map((col: any, i: number) => {
                        const maxCount = columnUsage.data.top_columns[0]?.lineage_count + columnUsage.data.top_columns[0]?.query_count || 1;
                        const total = (col.lineage_count || 0) + (col.query_count || 0);
                        return (
                          <div key={`${col.table}-${col.column}-${i}`} className="flex items-center gap-2">
                            <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-1">
                                <div className="flex items-center gap-1.5 min-w-0">
                                  <span className="text-xs font-mono font-semibold text-foreground">{col.column}</span>
                                  <span className="text-[10px] text-muted-foreground truncate">{col.table?.split(".").slice(1).join(".")}</span>
                                </div>
                                <div className="flex items-center gap-2 ml-2 shrink-0">
                                  {col.user_count > 0 && (
                                    <span className="text-[10px] text-muted-foreground">{col.user_count} user{col.user_count > 1 ? "s" : ""}</span>
                                  )}
                                  <span className="text-xs font-semibold text-foreground">{total}</span>
                                </div>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                                <div className="h-full bg-cyan-600 rounded-full" style={{ width: `${(total / maxCount) * 100}%` }} />
                              </div>
                              {col.users?.length > 0 && (
                                <div className="flex flex-wrap gap-1 mt-1">
                                  {col.users.slice(0, 3).map((u: any) => (
                                    <span key={u.user} className="text-[9px] text-muted-foreground bg-muted/50 px-1.5 py-0.5 rounded">
                                      {u.user?.split("@")[0]} ({u.count})
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Active Users */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Users className="h-4 w-4 text-purple-600" />
                    Active Users
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!columnUsage.data.top_users?.length ? (
                    <p className="text-sm text-muted-foreground">No user data available.</p>
                  ) : (
                    <div className="space-y-2">
                      {columnUsage.data.top_users.slice(0, 10).map((u: any) => {
                        const maxQ = columnUsage.data.top_users[0]?.query_count || 1;
                        return (
                          <div key={u.user} className="flex items-center gap-2">
                            <div className="w-6 h-6 rounded-full bg-muted flex items-center justify-center text-[10px] font-semibold text-foreground shrink-0">
                              {u.user?.charAt(0)?.toUpperCase() || "?"}
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-1">
                                <span className="text-xs text-foreground truncate">{u.user?.split("@")[0] || u.user}</span>
                                <span className="text-xs font-semibold text-foreground ml-2">{u.query_count} queries</span>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                                <div className="h-full bg-purple-600 rounded-full" style={{ width: `${(u.query_count / maxQ) * 100}%` }} />
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}
        </>
      )}

      {/* Search */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            Search
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Pattern (regex)</label>
              <Input
                placeholder="e.g. email|phone|customer"
                value={pattern}
                onChange={(e) => setPattern(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && catalog && pattern) {
                    search.mutate({ source_catalog: catalog, pattern, search_columns: searchColumns });
                  }
                }}
              />
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={searchColumns}
                onChange={(e) => setSearchColumns(e.target.checked)}
              />
              Search columns
            </label>
            <Button
              onClick={() => search.mutate({ source_catalog: catalog, pattern, search_columns: searchColumns })}
              disabled={!catalog || !pattern || search.isPending}
            >
              {search.isPending ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Search className="h-4 w-4 mr-2" />
              )}
              Search
            </Button>
          </div>

          {/* Search Error */}
          {search.isError && (
            <div className="p-3 bg-red-50 border border-red-200 rounded text-red-700 text-sm">
              {search.error?.message || "Search failed"}
            </div>
          )}

          {/* Search Results */}
          {search.data && (
            <div className="space-y-3">
              <div className="flex items-center gap-2">
                <Badge variant="default" className="bg-blue-600">
                  {Array.isArray(search.data) ? search.data.length : 0} matches
                </Badge>
                <span className="text-sm text-gray-500">
                  for &ldquo;{pattern}&rdquo; in {catalog}
                </span>
              </div>

              {Array.isArray(search.data) && search.data.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-gray-50">
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Schema</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Table</th>
                        {searchColumns && (
                          <th className="text-left py-3 px-4 font-medium text-gray-600">Column</th>
                        )}
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {search.data.map((row, i) => (
                        <tr key={i} className="border-b hover:bg-gray-50 transition-colors">
                          <td className="py-3 px-4 text-gray-600">{row.schema || row.table_schema || "—"}</td>
                          <td className="py-3 px-4 font-medium">{row.table || row.table_name || "—"}</td>
                          {searchColumns && (
                            <td className="py-3 px-4">{row.column || row.column_name || "—"}</td>
                          )}
                          <td className="py-3 px-4">
                            <Badge variant="outline">{row.table_type || row.type || row.data_type || "TABLE"}</Badge>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
