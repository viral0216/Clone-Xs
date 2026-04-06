// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  Shield, Loader2, RefreshCw, TrendingUp, AlertTriangle,
  CheckCircle2, XCircle, Database, ArrowRight,
} from "lucide-react";
import CatalogPicker from "@/components/CatalogPicker";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

function scoreColor(score: number) {
  if (score >= 90) return "text-green-400";
  if (score >= 70) return "text-amber-400";
  return "text-red-400";
}

function scoreBg(score: number) {
  if (score >= 90) return "bg-green-500/20 text-green-400";
  if (score >= 70) return "bg-amber-500/20 text-amber-400";
  return "bg-red-500/20 text-red-400";
}

function scoreBadge(score: number) {
  if (score == null) return <Badge variant="outline">N/A</Badge>;
  return (
    <Badge className={scoreBg(score)}>{score.toFixed(1)}</Badge>
  );
}

export default function TrustScoresPage() {
  const [catalog, setCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [computing, setComputing] = useState(false);
  const [scores, setScores] = useState<any[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [trend, setTrend] = useState<any[]>([]);
  const [trendLoading, setTrendLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadScores() {
    if (!catalog.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const data = await api.get(`/trust-scores/scores/${encodeURIComponent(catalog.trim())}`);
      setScores(Array.isArray(data) ? data : data?.scores || []);
    } catch (e: any) {
      setError(e.message || "Failed to load trust scores.");
      setScores([]);
    }
    setLoading(false);
  }

  async function computeScores() {
    if (!catalog.trim()) return;
    setComputing(true);
    setError(null);
    try {
      await api.post(`/trust-scores/compute/${encodeURIComponent(catalog.trim())}`, {});
      await loadScores();
    } catch (e: any) {
      setError(e.message || "Failed to compute trust scores.");
    }
    setComputing(false);
  }

  async function loadTrend(tableName: string) {
    setSelectedTable(tableName);
    setTrendLoading(true);
    try {
      const data = await api.get(`/trust-scores/trend/${encodeURIComponent(tableName)}`);
      setTrend(Array.isArray(data) ? data : data?.trend || []);
    } catch {
      setTrend([]);
    }
    setTrendLoading(false);
  }

  useEffect(() => {
    if (catalog) loadScores();
  }, [catalog]);

  const avgScore = scores.length
    ? scores.reduce((a, s) => a + (s.overall ?? 0), 0) / scores.length
    : 0;
  const highTrust = scores.filter((s) => (s.overall ?? 0) >= 90).length;
  const lowTrust = scores.filter((s) => (s.overall ?? 0) < 50).length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Trust Scores"
        icon={Shield}
        description="Per-table composite trust scores (0-100)"
        breadcrumbs={["Data Quality", "Trust Scores"]}
      />

      {/* Catalog Picker */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <div className="flex-1 max-w-md">
              <CatalogPicker
                catalog={catalog}
                onCatalogChange={(c) => { setCatalog(c); }}
                showSchema={false}
                showTable={false}
              />
            </div>
            <Button onClick={loadScores} disabled={loading || !catalog}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Load Scores
            </Button>
            <Button onClick={computeScores} disabled={computing || !catalog} variant="secondary">
              {computing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <TrendingUp className="h-4 w-4 mr-2" />}
              Compute Scores
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
            <p className="text-sm text-muted-foreground">Avg Trust Score</p>
            <p className={`text-3xl font-bold mt-1 ${scoreColor(avgScore)}`}>
              {scores.length ? avgScore.toFixed(1) : "—"}
            </p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Tables Scored</p>
            <p className="text-3xl font-bold mt-1 text-blue-400">{scores.length}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground flex items-center gap-1">
              <CheckCircle2 className="h-4 w-4 text-green-400" /> High Trust (&ge;90)
            </p>
            <p className="text-3xl font-bold mt-1 text-green-400">{highTrust}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground flex items-center gap-1">
              <AlertTriangle className="h-4 w-4 text-red-400" /> Low Trust (&lt;50)
            </p>
            <p className="text-3xl font-bold mt-1 text-red-400">{lowTrust}</p>
          </CardContent>
        </Card>
      </div>

      {/* Score Table */}
      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : scores.length > 0 ? (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-lg">Trust Score Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="text-left py-2 px-3">Table</th>
                    <th className="text-center py-2 px-2">Overall</th>
                    <th className="text-center py-2 px-2">DQ</th>
                    <th className="text-center py-2 px-2">Freshness</th>
                    <th className="text-center py-2 px-2">Anomaly</th>
                    <th className="text-center py-2 px-2">Schema</th>
                    <th className="text-center py-2 px-2">PII</th>
                    <th className="text-center py-2 px-2">Lineage</th>
                    <th className="text-center py-2 px-2">Computed</th>
                  </tr>
                </thead>
                <tbody>
                  {scores.map((row, i) => (
                    <tr
                      key={i}
                      className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer transition-colors ${
                        selectedTable === row.table_name ? "bg-muted/50" : ""
                      }`}
                      onClick={() => loadTrend(row.table_name)}
                    >
                      <td className="py-2 px-3 font-medium flex items-center gap-2">
                        <Database className="h-4 w-4 text-muted-foreground" />
                        {row.table_name}
                      </td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.overall)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.dq_score)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.freshness_score)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.anomaly_score)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.schema_score)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.pii_score)}</td>
                      <td className="text-center py-2 px-2">{scoreBadge(row.lineage_score)}</td>
                      <td className="text-center py-2 px-2 text-xs text-muted-foreground">
                        {row.computed_at ? new Date(row.computed_at).toLocaleString() : "—"}
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
            No trust scores found. Enter a catalog and click <strong>Load Scores</strong> or <strong>Compute Scores</strong>.
          </div>
        )
      )}

      {/* Trend Chart */}
      {selectedTable && (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <TrendingUp className="h-5 w-5" />
              Trend: {selectedTable}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {trendLoading ? (
              <div className="flex justify-center py-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : trend.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={trend}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="date" tick={{ fontSize: 12 }} stroke="#888" />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} stroke="#888" />
                  <Tooltip
                    contentStyle={{ backgroundColor: "#1c1c1c", border: "1px solid #333" }}
                  />
                  <Legend />
                  <Line type="monotone" dataKey="overall" stroke="#3b82f6" strokeWidth={2} name="Overall" />
                  <Line type="monotone" dataKey="dq_score" stroke="#10b981" strokeWidth={1.5} name="DQ" />
                  <Line type="monotone" dataKey="freshness_score" stroke="#f59e0b" strokeWidth={1.5} name="Freshness" />
                  <Line type="monotone" dataKey="anomaly_score" stroke="#ef4444" strokeWidth={1.5} name="Anomaly" />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-muted-foreground text-sm text-center py-8">
                No trend data available for this table.
              </p>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
