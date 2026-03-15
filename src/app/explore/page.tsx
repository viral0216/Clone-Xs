// @ts-nocheck
"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useSearch, useStats } from "@/hooks/useApi";
import {
  Search, BarChart3, Database, Table2, HardDrive, Rows3,
  ArrowUpDown, Loader2,
} from "lucide-react";

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
      <div>
        <h1 className="text-3xl font-bold">Catalog Explorer</h1>
        <p className="text-gray-500 mt-1">Browse, search, and analyze catalog contents</p>
      </div>

      {/* Catalog Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Catalog Name</label>
              <Input
                placeholder="e.g. production"
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
              />
            </div>
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
