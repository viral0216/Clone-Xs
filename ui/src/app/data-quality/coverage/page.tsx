// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  Map, Loader2, RefreshCw, CheckCircle2, XCircle, AlertTriangle,
  Database, Eye, EyeOff, ShieldAlert,
} from "lucide-react";
import CatalogPicker from "@/components/CatalogPicker";

function coverageBarColor(pct: number) {
  if (pct >= 80) return "bg-green-500";
  if (pct >= 60) return "bg-amber-500";
  return "bg-red-500";
}

function CoverageBar({ pct }: { pct: number }) {
  return (
    <div className="flex items-center gap-2">
      <div className="w-24 h-2 bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${coverageBarColor(pct)}`}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
      <span className="text-xs text-muted-foreground w-10 text-right">{pct.toFixed(0)}%</span>
    </div>
  );
}

function CheckIcon({ value }: { value: boolean }) {
  return value ? (
    <CheckCircle2 className="h-4 w-4 text-green-400 mx-auto" />
  ) : (
    <XCircle className="h-4 w-4 text-red-400/50 mx-auto" />
  );
}

export default function CoveragePage() {
  const [catalog, setCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [coverage, setCoverage] = useState<any[]>([]);
  const [summary, setSummary] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  async function loadCoverage() {
    if (!catalog.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const [covData, sumData] = await Promise.all([
        api.get(`/coverage/${encodeURIComponent(catalog.trim())}`),
        api.get(`/coverage/${encodeURIComponent(catalog.trim())}/summary`),
      ]);
      setCoverage(Array.isArray(covData) ? covData : covData?.tables || []);
      setSummary(sumData);
    } catch (e: any) {
      setError(e.message || "Failed to load coverage data.");
      setCoverage([]);
      setSummary(null);
    }
    setLoading(false);
  }

  async function scanCoverage() {
    if (!catalog.trim()) return;
    setScanning(true);
    setError(null);
    try {
      await api.post(`/coverage/${encodeURIComponent(catalog.trim())}/compute`, {});
      await loadCoverage();
    } catch (e: any) {
      setError(e.message || "Failed to scan coverage.");
    }
    setScanning(false);
  }

  useEffect(() => {
    if (catalog) loadCoverage();
  }, [catalog]);

  const totalTables = summary?.total_tables ?? coverage.length;
  const avgCoverage = summary?.avg_coverage ?? (
    coverage.length
      ? coverage.reduce((a, c) => a + (c.coverage_pct ?? 0), 0) / coverage.length
      : 0
  );
  const fullyCovered = summary?.fully_covered ?? coverage.filter((c) => (c.coverage_pct ?? 0) >= 100).length;
  const blindSpots = summary?.blind_spots ?? coverage.filter((c) => (c.coverage_pct ?? 0) === 0).length;
  const gaps = coverage.filter((c) => (c.coverage_pct ?? 0) < 60).sort((a, b) => (a.coverage_pct ?? 0) - (b.coverage_pct ?? 0));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Coverage Map"
        icon={Map}
        description="DQ check coverage across your catalog"
        breadcrumbs={["Data Quality", "Coverage Map"]}
      />

      {/* Catalog Picker */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <div className="flex-1 max-w-md">
              <CatalogPicker
                catalog={catalog}
                onCatalogChange={(c) => setCatalog(c)}
                showSchema={false}
                showTable={false}
              />
            </div>
            <Button onClick={loadCoverage} disabled={loading || !catalog}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Load Coverage
            </Button>
            <Button onClick={scanCoverage} disabled={scanning || !catalog} variant="secondary">
              {scanning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Eye className="h-4 w-4 mr-2" />}
              Scan Coverage
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <div className="text-red-400 text-sm bg-red-500/10 border border-red-500/30 rounded-md p-3">
          {error}
        </div>
      )}

      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground flex items-center gap-1">
              <Database className="h-4 w-4" /> Total Tables
            </p>
            <p className="text-3xl font-bold mt-1">{totalTables}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Avg Coverage</p>
            <p className={`text-3xl font-bold mt-1 ${avgCoverage >= 80 ? "text-green-400" : avgCoverage >= 60 ? "text-amber-400" : "text-red-400"}`}>
              {avgCoverage.toFixed(1)}%
            </p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground flex items-center gap-1">
              <CheckCircle2 className="h-4 w-4 text-green-400" /> Fully Covered
            </p>
            <p className="text-3xl font-bold mt-1 text-green-400">{fullyCovered}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground flex items-center gap-1">
              <EyeOff className="h-4 w-4 text-red-400" /> Blind Spots
            </p>
            <p className="text-3xl font-bold mt-1 text-red-400">{blindSpots}</p>
          </CardContent>
        </Card>
      </div>

      {/* Coverage Heatmap Table */}
      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : coverage.length > 0 ? (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-lg">Coverage Heatmap</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="text-left py-2 px-3">Table</th>
                    <th className="text-center py-2 px-2">DQ Rules</th>
                    <th className="text-center py-2 px-2">SLA</th>
                    <th className="text-center py-2 px-2">PII Scan</th>
                    <th className="text-center py-2 px-2">Monitoring</th>
                    <th className="text-center py-2 px-2">Contract</th>
                    <th className="text-center py-2 px-2">Coverage</th>
                  </tr>
                </thead>
                <tbody>
                  {coverage.map((row, i) => (
                    <tr key={i} className="border-b border-border/50 hover:bg-muted/30 transition-colors">
                      <td className="py-2 px-3 font-medium flex items-center gap-2">
                        <Database className="h-4 w-4 text-muted-foreground" />
                        {row.table_name}
                      </td>
                      <td className="text-center py-2 px-2"><CheckIcon value={row.has_dq_rules} /></td>
                      <td className="text-center py-2 px-2"><CheckIcon value={row.has_sla} /></td>
                      <td className="text-center py-2 px-2"><CheckIcon value={row.has_pii_scan} /></td>
                      <td className="text-center py-2 px-2"><CheckIcon value={row.has_monitoring} /></td>
                      <td className="text-center py-2 px-2"><CheckIcon value={row.has_contract} /></td>
                      <td className="py-2 px-2">
                        <CoverageBar pct={row.coverage_pct ?? 0} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      ) : (
        !error && (
          <div className="text-center text-muted-foreground py-12">
            No coverage data found. Enter a catalog and click <strong>Load Coverage</strong> or <strong>Scan Coverage</strong>.
          </div>
        )
      )}

      {/* Gaps / Priority Section */}
      {gaps.length > 0 && (
        <Card className="bg-card border-border border-amber-500/30">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <ShieldAlert className="h-5 w-5 text-amber-400" />
              Coverage Gaps (below 60%)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {gaps.map((g, i) => (
                <div key={i} className="flex items-center justify-between py-2 px-3 rounded-md bg-muted/30">
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="text-xs">#{i + 1}</Badge>
                    <span className="font-medium text-sm">{g.table_name}</span>
                  </div>
                  <CoverageBar pct={g.coverage_pct ?? 0} />
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
