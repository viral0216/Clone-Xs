// @ts-nocheck
"use client";

import { useState, useEffect, useMemo } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ShieldCheck, Loader2, Users, Search, Download, AlertTriangle, GitBranch, Lock } from "lucide-react";

function StatCard({ label, value, icon: Icon, color }) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className={`text-2xl font-bold mt-0.5 ${color}`}>{value}</p>
          </div>
          <Icon className={`h-8 w-8 opacity-20 ${color}`} />
        </div>
      </CardContent>
    </Card>
  );
}

function PrivilegeBadge({ privilege }) {
  const p = (privilege ?? "").toUpperCase();
  if (p === "ALL PRIVILEGES") {
    return <Badge className="text-[10px] bg-red-600 text-white font-bold">ALL PRIVILEGES</Badge>;
  }
  const DANGEROUS = ["MANAGE", "CREATE CATALOG", "MANAGE GRANTS", "CREATE SCHEMA", "CREATE TABLE", "CREATE FUNCTION"];
  if (DANGEROUS.includes(p)) {
    return <Badge className="text-[10px] bg-amber-500 text-white">{p}</Badge>;
  }
  return <Badge variant="outline" className="text-[10px]">{p}</Badge>;
}

function getColIdx(columns, name) {
  return columns.indexOf(name);
}

function exportCsv(columns, rows) {
  const header = columns.join(",");
  const body = rows.map(r => r.map(v => `"${(v ?? "").toString().replace(/"/g, '""')}"`).join(",")).join("\n");
  const csv = `${header}\n${body}`;
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "permission_audit.csv";
  a.click();
  URL.revokeObjectURL(url);
}

export default function PermissionsAuditPage() {
  const [catalogs, setCatalogs] = useState([]);
  const [selectedCatalog, setSelectedCatalog] = useState("");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => {
        const cats = (d?.catalogs ?? []).map(c => c.name).filter(Boolean);
        setCatalogs(cats);
      })
      .catch(() => {});
  }, []);

  async function loadGrants() {
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const params = new URLSearchParams();
      if (selectedCatalog) params.set("catalog", selectedCatalog);
      const qs = params.toString();
      const data = await api.get(`/assessment/permissions/grants${qs ? `?${qs}` : ""}`);
      setResult(data);
    } catch (e) {
      setError(e?.message ?? "Failed to load permission grants. Make sure your SQL Warehouse ID is configured in Settings.");
    } finally {
      setLoading(false);
    }
  }

  const columns = result?.columns ?? [];
  const rows = result?.rows ?? [];

  const grantorIdx   = getColIdx(columns, "grantor");
  const granteeIdx   = getColIdx(columns, "grantee");
  const privIdx      = getColIdx(columns, "privilege_type");
  const objTypeIdx   = getColIdx(columns, "object_type");
  const objNameIdx   = getColIdx(columns, "object_name");
  const inheritedIdx = getColIdx(columns, "inherited_from");

  const filteredRows = useMemo(() => {
    if (!search) return rows;
    const q = search.toLowerCase();
    return rows.filter(r =>
      (r[granteeIdx] ?? "").toLowerCase().includes(q) ||
      (r[objNameIdx] ?? "").toLowerCase().includes(q) ||
      (r[privIdx] ?? "").toLowerCase().includes(q)
    );
  }, [rows, search, granteeIdx, objNameIdx, privIdx]);

  const uniqueGrantees  = useMemo(() => new Set(rows.map(r => r[granteeIdx])).size, [rows, granteeIdx]);
  const allPrivsCount   = useMemo(() => rows.filter(r => (r[privIdx] ?? "").toUpperCase() === "ALL PRIVILEGES").length, [rows, privIdx]);
  const inheritedCount  = useMemo(() => rows.filter(r => r[inheritedIdx] && r[inheritedIdx] !== "").length, [rows, inheritedIdx]);
  const totalGrants     = rows.length;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Permission Audit Matrix"
        icon={ShieldCheck}
        breadcrumbs={["Assessment", "UC Inventory", "Permissions"]}
        description="Audit Unity Catalog permission grants — identify ALL PRIVILEGES, inherited grants, and over-privileged principals."
      />

      {/* Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Audit Configuration</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-3 items-end">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">Catalog</label>
            <select
              value={selectedCatalog}
              onChange={e => setSelectedCatalog(e.target.value)}
              className="px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring min-w-[180px]"
            >
              <option value="">All catalogs</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <Button onClick={loadGrants} disabled={loading} className="flex items-center gap-2">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Lock className="h-4 w-4" />}
            {loading ? "Loading…" : "Load Grants"}
          </Button>
          {result && (
            <Button
              variant="outline"
              onClick={() => exportCsv(columns, rows)}
              className="flex items-center gap-2"
            >
              <Download className="h-4 w-4" />
              Export CSV
            </Button>
          )}
        </CardContent>
      </Card>

      {error && (
        <Card className="border-destructive/20">
          <CardContent className="pt-4 pb-3">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {loading && (
        <div className="text-center py-16 text-muted-foreground">
          <Loader2 className="h-10 w-10 mx-auto animate-spin opacity-30 mb-3" />
          <p className="text-sm">Loading permission grants…</p>
        </div>
      )}

      {/* Summary cards */}
      {result && !loading && (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatCard label="Total Grants" value={totalGrants} icon={ShieldCheck} color="text-foreground" />
            <StatCard label="Unique Grantees" value={uniqueGrantees} icon={Users} color="text-blue-600" />
            <StatCard label="ALL PRIVILEGES" value={allPrivsCount} icon={AlertTriangle} color="text-red-600" />
            <StatCard label="Inherited Grants" value={inheritedCount} icon={GitBranch} color="text-amber-600" />
          </div>

          {/* Grants table */}
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between flex-wrap gap-2">
                <CardTitle className="text-sm font-medium">
                  Permission Grants ({rows.length})
                </CardTitle>
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                  <input
                    type="text"
                    placeholder="Search grantee or object…"
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    className="pl-8 pr-3 py-1.5 text-xs border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring w-52"
                  />
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {filteredRows.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <ShieldCheck className="h-10 w-10 mx-auto mb-2 opacity-20" />
                  <p className="text-sm">No grants found matching your criteria.</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border bg-muted/30">
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Grantee</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Privilege</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Object Type</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Object Name</th>
                        <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Inherited</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((row, i) => {
                        const grantee    = row[granteeIdx] ?? "—";
                        const priv       = row[privIdx] ?? "—";
                        const objType    = row[objTypeIdx] ?? "—";
                        const objName    = row[objNameIdx] ?? "—";
                        const inherited  = row[inheritedIdx];
                        const isAllPrivs = priv.toUpperCase() === "ALL PRIVILEGES";

                        return (
                          <tr
                            key={i}
                            className={`border-b border-border last:border-0 hover:bg-muted/20 ${
                              isAllPrivs
                                ? "bg-red-50/40 dark:bg-red-950/15"
                                : ""
                            }`}
                          >
                            <td className="px-4 py-2.5 font-mono text-[11px] font-medium">{grantee}</td>
                            <td className="px-4 py-2.5">
                              <PrivilegeBadge privilege={priv} />
                            </td>
                            <td className="px-4 py-2.5">
                              <Badge variant="outline" className="text-[10px]">{objType}</Badge>
                            </td>
                            <td className="px-4 py-2.5 font-mono text-[11px] text-muted-foreground max-w-[200px] truncate" title={objName}>
                              {objName}
                            </td>
                            <td className="px-4 py-2.5">
                              {inherited && inherited !== ""
                                ? <Badge variant="secondary" className="text-[10px]">Inherited</Badge>
                                : <span className="text-muted-foreground/40">—</span>
                              }
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {!result && !loading && !error && (
        <div className="text-center py-20 text-muted-foreground">
          <Lock className="h-14 w-14 mx-auto mb-3 opacity-15" />
          <p className="text-sm font-medium">No grants loaded</p>
          <p className="text-xs mt-1 opacity-70">
            Select a catalog filter (or leave blank for all) and click "Load Grants" to audit permissions.
          </p>
        </div>
      )}
    </div>
  );
}
