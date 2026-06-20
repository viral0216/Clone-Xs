// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { BarChart2, Loader2 } from "lucide-react";

function scoreColor(score: number) {
  if (score >= 90) return { bg: "bg-green-500", text: "text-green-600 dark:text-green-400" };
  if (score >= 75) return { bg: "bg-lime-500", text: "text-lime-600 dark:text-lime-400" };
  if (score >= 60) return { bg: "bg-yellow-500", text: "text-yellow-600 dark:text-yellow-400" };
  if (score >= 45) return { bg: "bg-orange-500", text: "text-orange-600 dark:text-orange-400" };
  return { bg: "bg-red-500", text: "text-red-600 dark:text-red-400" };
}

export default function CategoriesPage() {
  const navigate = useNavigate();
  const [categories, setCategories] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/assessment/categories")
      .then(d => setCategories(Array.isArray(d) ? d : []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  function handleClick(cat: any) {
    navigate(`/assessment/findings?status=FAIL,WARN`);
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Category Scores"
        icon={BarChart2}
        breadcrumbs={["Assessment", "Category Scores"]}
        description="Security posture broken down by category. Click any card to filter findings."
      />

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <span className="ml-2 text-sm text-muted-foreground">Loading…</span>
        </div>
      )}

      {!loading && categories.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground text-sm">
            No assessment results. Run a scan first.
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {categories.map(cat => {
          const { bg, text } = scoreColor(cat.score);
          const total = (cat.passed || 0) + (cat.failed || 0) + (cat.warnings || 0) + (cat.not_applicable || 0);
          return (
            <Card
              key={cat.category}
              className="cursor-pointer hover:bg-accent/30 transition-colors"
              onClick={() => handleClick(cat)}
            >
              <CardContent className="pt-4 pb-3 space-y-2">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-sm font-medium leading-tight">{cat.category}</p>
                  <span className={`text-lg font-bold shrink-0 ${text}`}>{cat.score}</span>
                </div>
                {/* Score bar */}
                <div className="w-full h-1.5 bg-muted rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${bg}`}
                    style={{ width: `${Math.min(100, cat.score)}%` }}
                  />
                </div>
                {/* Counts */}
                <div className="flex items-center gap-3 text-xs text-muted-foreground">
                  {cat.failed > 0 && (
                    <span className="text-red-500 font-medium">{cat.failed} failed</span>
                  )}
                  {cat.warnings > 0 && (
                    <span className="text-yellow-500 font-medium">{cat.warnings} warn</span>
                  )}
                  {cat.passed > 0 && (
                    <span className="text-green-500">{cat.passed} passed</span>
                  )}
                  {total > 0 && (
                    <span className="ml-auto">{total} checks</span>
                  )}
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
