// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import {
  Shield,
  Database,
  Settings2,
  Zap,
  TrendingDown,
  Activity,
  Cpu,
  Loader2,
  LayoutGrid,
} from "lucide-react";

const PILLAR_ICONS: Record<string, React.ComponentType<any>> = {
  shield:         Shield,
  database:       Database,
  settings:       Settings2,
  zap:            Zap,
  "trending-down": TrendingDown,
  activity:       Activity,
  cpu:            Cpu,
};

function scoreColor(score: number) {
  if (score >= 90) return { bg: "bg-green-500",  text: "text-green-600 dark:text-green-400",  badge: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400" };
  if (score >= 75) return { bg: "bg-lime-500",   text: "text-lime-600 dark:text-lime-400",    badge: "bg-lime-100 text-lime-700 dark:bg-lime-900/30 dark:text-lime-400" };
  if (score >= 60) return { bg: "bg-yellow-500", text: "text-yellow-600 dark:text-yellow-400", badge: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400" };
  if (score >= 45) return { bg: "bg-orange-500", text: "text-orange-600 dark:text-orange-400", badge: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400" };
  return           { bg: "bg-red-500",    text: "text-red-600 dark:text-red-400",     badge: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400" };
}

export default function PillarsPage() {
  const navigate = useNavigate();
  const [pillars, setPillars] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/assessment/waf-pillars")
      .then(d => setPillars(Array.isArray(d) ? d : []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  function handleClick(pillar: any) {
    navigate(`/assessment/findings?category=${encodeURIComponent(pillar.pillar)}`);
  }

  const overallScore = pillars.length
    ? Math.round(pillars.reduce((sum, p) => sum + (p.score || 0), 0) / pillars.length)
    : 0;

  return (
    <div className="space-y-6">
      <PageHeader
        title="WAF Pillars"
        icon={LayoutGrid}
        breadcrumbs={["Assessment", "WAF Pillars"]}
        description="Security posture across the 7 Databricks Well-Architected Framework pillars. Click any pillar to see its findings."
      />

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <span className="ml-2 text-sm text-muted-foreground">Loading…</span>
        </div>
      )}

      {!loading && pillars.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground text-sm">
            No assessment results. Run a scan first.
          </CardContent>
        </Card>
      )}

      {!loading && pillars.length > 0 && (
        <>
          {/* Overall WAF score banner */}
          <div className="flex items-center gap-4 px-4 py-3 rounded-lg bg-muted/50 border">
            <div className="text-3xl font-bold tabular-nums" style={{ color: overallScore >= 75 ? "#22c55e" : overallScore >= 60 ? "#eab308" : "#ef4444" }}>
              {overallScore}
            </div>
            <div>
              <p className="text-sm font-medium">Overall WAF Score</p>
              <p className="text-xs text-muted-foreground">Average across all 7 pillars</p>
            </div>
          </div>

          {/* Pillar cards — 1 col mobile, 2 tablet, 3 desktop, 4 wide */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {pillars.map(pillar => {
              const { bg, text, badge } = scoreColor(pillar.score);
              const Icon = PILLAR_ICONS[pillar.icon] ?? Shield;
              const total = (pillar.passed || 0) + (pillar.failed || 0) + (pillar.warnings || 0) + (pillar.not_applicable || 0);
              return (
                <Card
                  key={pillar.pillar}
                  className="cursor-pointer hover:bg-accent/30 transition-colors group"
                  onClick={() => handleClick(pillar)}
                >
                  <CardContent className="pt-5 pb-4 space-y-3">
                    {/* Header row: icon + pillar name + grade badge */}
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex items-center gap-2">
                        <div className="p-1.5 rounded-md bg-muted group-hover:bg-background transition-colors">
                          <Icon className="h-4 w-4 text-muted-foreground" />
                        </div>
                        <p className="text-sm font-semibold leading-tight">{pillar.pillar}</p>
                      </div>
                      <span className={`text-xs font-bold px-2 py-0.5 rounded-full shrink-0 ${badge}`}>
                        {pillar.grade}
                      </span>
                    </div>

                    {/* Description */}
                    <p className="text-xs text-muted-foreground line-clamp-2 leading-relaxed">
                      {pillar.description}
                    </p>

                    {/* Score bar */}
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <span className="text-xs text-muted-foreground">Score</span>
                        <span className={`text-base font-bold tabular-nums ${text}`}>
                          {pillar.score}
                        </span>
                      </div>
                      <div className="w-full h-2 bg-muted rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all ${bg}`}
                          style={{ width: `${Math.min(100, pillar.score || 0)}%` }}
                        />
                      </div>
                    </div>

                    {/* Counts row */}
                    <div className="flex items-center gap-3 text-xs text-muted-foreground pt-0.5">
                      {pillar.failed > 0 && (
                        <span className="text-red-500 font-medium">{pillar.failed} failed</span>
                      )}
                      {pillar.warnings > 0 && (
                        <span className="text-yellow-500 font-medium">{pillar.warnings} warn</span>
                      )}
                      {pillar.passed > 0 && (
                        <span className="text-green-500">{pillar.passed} passed</span>
                      )}
                      {total > 0 && (
                        <span className="ml-auto">{pillar.check_count} checks</span>
                      )}
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}
