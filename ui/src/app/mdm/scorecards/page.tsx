// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { BarChart3, TrendingUp, AlertTriangle } from "lucide-react";
import { useMdmEntities } from "@/hooks/useMdm";

export default function ScorecardsPage() {
  const { data: entities, isLoading } = useMdmEntities();
  const records = Array.isArray(entities) ? entities : [];

  // Compute per-entity-type scorecards
  const types = [...new Set(records.map(r => r.entity_type))];
  const scorecards = types.map(type => {
    const typeRecords = records.filter(r => r.entity_type === type);
    const total = typeRecords.length;
    const avgConfidence = total > 0 ? typeRecords.reduce((s, r) => s + (r.confidence_score || 0), 0) / total : 0;
    const active = typeRecords.filter(r => r.status === "active").length;
    const withSources = typeRecords.filter(r => (r.source_count || 0) > 0).length;
    const completeness = total > 0 ? Math.round((withSources / total) * 100) : 0;
    const accuracy = Math.round(avgConfidence * 100);
    const overall = Math.round((completeness + accuracy) / 2);

    return { type, total, active, accuracy, completeness, overall, avgConfidence };
  });

  const overallScore = scorecards.length > 0 ? Math.round(scorecards.reduce((s, c) => s + c.overall, 0) / scorecards.length) : 0;

  return (
    <div className="space-y-4">
      <PageHeader title="Data Quality Scorecards" icon={BarChart3} breadcrumbs={["MDM", "Scorecards"]}
        description="Per-entity quality metrics — completeness, accuracy, and timeliness scores with trends." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4">
          <p className={`text-3xl font-bold ${overallScore >= 80 ? "text-foreground" : overallScore >= 60 ? "text-muted-foreground" : "text-red-500"}`}>{isLoading ? "—" : overallScore}</p>
          <p className="text-xs text-muted-foreground">Overall DQ Score</p>
        </CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : records.length}</p><p className="text-xs text-muted-foreground">Total Entities</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : types.length}</p><p className="text-xs text-muted-foreground">Entity Types</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : scorecards.filter(s => s.overall < 70).length}</p><p className="text-xs text-muted-foreground">Below Threshold</p></CardContent></Card>
      </div>

      {scorecards.length === 0 ? (
        <Card><CardContent className="py-8 text-center text-sm text-muted-foreground">No entities to score yet</CardContent></Card>
      ) : (
        <div className="space-y-3">
          {scorecards.map(card => (
            <Card key={card.type}>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm flex items-center gap-2">
                    {card.type}
                    <Badge variant="outline" className="text-[10px]">{card.total} entities</Badge>
                    {card.overall < 70 && <AlertTriangle className="h-3.5 w-3.5 text-red-500" />}
                  </CardTitle>
                  <span className={`text-2xl font-bold ${card.overall >= 80 ? "text-foreground" : card.overall >= 60 ? "text-muted-foreground" : "text-red-500"}`}>{card.overall}</span>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-3 gap-4">
                  {[
                    { label: "Accuracy", value: card.accuracy, desc: "Avg confidence score" },
                    { label: "Completeness", value: card.completeness, desc: "Records with source data" },
                    { label: "Active Rate", value: card.total > 0 ? Math.round((card.active / card.total) * 100) : 0, desc: "Non-deleted records" },
                  ].map(metric => (
                    <div key={metric.label}>
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-muted-foreground">{metric.label}</span>
                        <span className="text-xs font-mono">{metric.value}%</span>
                      </div>
                      <div className="h-2 bg-muted/30 rounded-full overflow-hidden">
                        <div className={`h-full rounded-full transition-all ${metric.value >= 80 ? "bg-foreground/60" : metric.value >= 60 ? "bg-muted-foreground" : "bg-red-500/60"}`}
                          style={{ width: `${metric.value}%` }} />
                      </div>
                      <p className="text-[10px] text-muted-foreground mt-0.5">{metric.desc}</p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
