// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  Target, Loader2, Clock, ShieldCheck, Activity, GitBranch,
  CheckCircle2, AlertTriangle, XCircle, Sparkles,
} from "lucide-react";
import { Link } from "react-router-dom";

const DIMENSION_META: Record<string, { label: string; icon: any; link: string; description: string }> = {
  completeness: { label: "Completeness", icon: CheckCircle2, link: "/data-quality/anomalies", description: "How complete is the data (low null rates)" },
  freshness: { label: "Freshness", icon: Clock, link: "/data-quality/freshness", description: "How recently was the table updated" },
  sla_compliance: { label: "SLA Compliance", icon: ShieldCheck, link: "/data-quality/sla", description: "Are SLA thresholds being met" },
  anomaly_free: { label: "Anomaly Free", icon: Activity, link: "/data-quality/anomalies", description: "Low anomaly count in recent metrics" },
  schema_stability: { label: "Schema Stability", icon: GitBranch, link: "/data-quality/schema-drift", description: "No unexpected schema changes" },
};

function scoreColor(score: number) {
  if (score >= 90) return "text-green-500";
  if (score >= 70) return "text-amber-500";
  return "text-red-500";
}

function scoreBg(score: number) {
  if (score >= 90) return "bg-green-500";
  if (score >= 70) return "bg-amber-500";
  return "bg-red-500";
}

function scoreIcon(score: number) {
  if (score >= 90) return <CheckCircle2 className="h-5 w-5 text-green-500" />;
  if (score >= 70) return <AlertTriangle className="h-5 w-5 text-amber-500" />;
  return <XCircle className="h-5 w-5 text-red-500" />;
}

export default function ScorecardPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [scorecard, setScorecard] = useState<any>(null);

  async function runScorecard() {
    if (!catalog || !schema || !table) { toast.error("Select a table."); return; }
    const fqn = `${catalog}.${schema}.${table}`;
    setLoading(true);
    setScorecard(null);
    try {
      const data = await api.get(`/data-quality/scorecard/${encodeURIComponent(fqn)}`);
      setScorecard(data);
    } catch (e: any) {
      toast.error(e.message || "Scorecard failed.");
    }
    setLoading(false);
  }

  const dims = scorecard?.dimensions || {};
  const dimKeys = Object.keys(dims);

  return (
    <div className="space-y-4">
      <PageHeader
        title="DQ Scorecard"
        icon={Target}
        breadcrumbs={["Data Quality", "Rules & Checks", "Scorecard"]}
        description="Per-table quality scorecard combining completeness, freshness, SLA compliance, anomaly detection, and schema stability into a single score."
      />

      {/* Table selector */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <CatalogPicker
              catalog={catalog}
              schema={schema}
              table={table}
              onCatalogChange={(v) => { setCatalog(v); setSchema(""); setTable(""); }}
              onSchemaChange={(v) => { setSchema(v); setTable(""); }}
              onTableChange={setTable}
            />
            <Button onClick={runScorecard} disabled={loading || !table}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Sparkles className="h-4 w-4 mr-2" />}
              {loading ? "Scoring..." : "Run Scorecard"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Loading */}
      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Computing quality scorecard...</p>
          </CardContent>
        </Card>
      )}

      {/* Overall Score */}
      {scorecard && (
        <>
          <Card className="bg-card border-border">
            <CardContent className="py-8 text-center">
              <p className="text-xs text-muted-foreground uppercase tracking-wider mb-2">Overall Quality Score</p>
              <p className={`text-6xl font-bold ${scoreColor(scorecard.overall_score)}`}>
                {scorecard.overall_score}
              </p>
              <div className="w-32 h-2 bg-muted rounded-full overflow-hidden mx-auto mt-3">
                <div className={`h-full rounded-full transition-all ${scoreBg(scorecard.overall_score)}`}
                  style={{ width: `${scorecard.overall_score}%` }} />
              </div>
              <p className="text-xs text-muted-foreground mt-2 font-mono">{scorecard.table_fqn}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {scorecard.dimension_count} dimension{scorecard.dimension_count !== 1 ? "s" : ""} evaluated
              </p>
            </CardContent>
          </Card>

          {/* Dimension Cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {dimKeys.map((key) => {
              const dim = dims[key];
              const meta = DIMENSION_META[key] || { label: key, icon: Target, link: "/data-quality", description: "" };
              const Icon = meta.icon;
              const score = dim.score ?? 0;

              return (
                <Card key={key} className="bg-card border-border hover:border-[#E8453C]/30 transition-colors">
                  <CardContent className="pt-5 pb-4">
                    <div className="flex items-start justify-between mb-3">
                      <div className="flex items-center gap-2">
                        <Icon className={`h-5 w-5 ${scoreColor(score)}`} />
                        <div>
                          <p className="text-sm font-medium text-foreground">{meta.label}</p>
                          <p className="text-[10px] text-muted-foreground">{meta.description}</p>
                        </div>
                      </div>
                      {scoreIcon(score)}
                    </div>

                    {/* Score bar */}
                    <div className="flex items-center gap-3 mb-2">
                      <p className={`text-2xl font-bold ${scoreColor(score)}`}>{score}</p>
                      <div className="flex-1 h-1.5 bg-muted rounded-full overflow-hidden">
                        <div className={`h-full rounded-full ${scoreBg(score)}`} style={{ width: `${score}%` }} />
                      </div>
                    </div>

                    {/* Dimension details */}
                    <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-[10px] text-muted-foreground">
                      {dim.hours_since_update != null && <span>Updated {dim.hours_since_update}h ago</span>}
                      {dim.null_rate_pct != null && <span>Null rate: {dim.null_rate_pct}%</span>}
                      {dim.rules != null && <span>{dim.passing}/{dim.rules} SLAs passing</span>}
                      {dim.recent_anomalies != null && <span>{dim.recent_anomalies} anomalies (7d)</span>}
                      {dim.recent_changes != null && <span>{dim.recent_changes} schema changes</span>}
                    </div>

                    {/* Drill-down link */}
                    <Link to={meta.link} className="text-[10px] text-[#E8453C] hover:underline mt-2 inline-block">
                      View details →
                    </Link>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          {/* Empty dimensions message */}
          {dimKeys.length === 0 && (
            <Card className="bg-card border-border">
              <CardContent className="py-8 text-center">
                <p className="text-sm text-muted-foreground">
                  No quality dimensions could be evaluated. Run monitoring and SLA checks first to build up data.
                </p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
