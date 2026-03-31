/**
 * AiInsightCard — Reusable AI-powered insight panel.
 *
 * Shows a collapsible card with an AI-generated narrative summary.
 * Gracefully degrades when AI is not available (no API key).
 */

import { useState, useEffect } from "react";
import { Sparkles, RefreshCw, ChevronDown, ChevronUp, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useAiStatus, useAiSummary } from "@/hooks/useAi";

interface AiInsightCardProps {
  /** The type of summary to generate (dashboard, audit, report, profiling, pii). */
  contextType: string;
  /** The raw data to send to the AI for analysis. */
  data: Record<string, unknown>;
  /** Optional title override. */
  title?: string;
  /** Auto-generate on mount? Defaults to true. */
  autoGenerate?: boolean;
}

export default function AiInsightCard({
  contextType,
  data,
  title = "AI Insights",
  autoGenerate = true,
}: AiInsightCardProps) {
  const aiStatus = useAiStatus();
  const summary = useAiSummary();
  const [collapsed, setCollapsed] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);

  // Auto-generate on first render when data is available
  useEffect(() => {
    if (
      autoGenerate &&
      !hasGenerated &&
      aiStatus.data?.available &&
      data &&
      Object.keys(data).length > 0
    ) {
      setHasGenerated(true);
      summary.mutate({ contextType, data });
    }
  }, [aiStatus.data?.available, data, autoGenerate, hasGenerated]);

  // Don't render at all if AI is not available
  if (aiStatus.isLoading) return null;
  if (!aiStatus.data?.available) return null;

  const handleRegenerate = () => {
    summary.mutate({ contextType, data });
  };

  return (
    <div className="rounded-lg border border-primary/20 bg-primary/5 overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-primary/10 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <span className="text-sm font-medium">{title}</span>
          {summary.isPending && (
            <span className="text-xs text-muted-foreground">Generating...</span>
          )}
        </div>
        {collapsed ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" />
        )}
      </button>

      {/* Content */}
      {!collapsed && (
        <div className="px-4 pb-3">
          {summary.isPending ? (
            <div className="space-y-2">
              <Skeleton className="h-3 w-full" />
              <Skeleton className="h-3 w-4/5" />
              <Skeleton className="h-3 w-3/5" />
            </div>
          ) : summary.isError ? (
            <div className="flex items-center gap-2 text-xs text-destructive">
              <AlertCircle className="h-3.5 w-3.5" />
              <span>Failed to generate insights. {summary.error?.message}</span>
            </div>
          ) : summary.data?.summary ? (
            <p className="text-sm text-foreground/80 leading-relaxed">
              {summary.data.summary}
            </p>
          ) : !hasGenerated ? (
            <p className="text-xs text-muted-foreground">
              Click regenerate to generate AI insights.
            </p>
          ) : (
            <p className="text-xs text-muted-foreground">
              No insights available for the current data.
            </p>
          )}

          {/* Regenerate button */}
          <div className="flex items-center gap-2 mt-2.5">
            <Button
              variant="ghost"
              size="xs"
              onClick={handleRegenerate}
              disabled={summary.isPending}
              className="text-xs text-muted-foreground hover:text-foreground"
            >
              <RefreshCw className={`h-3 w-3 mr-1 ${summary.isPending ? "animate-spin" : ""}`} />
              Regenerate
            </Button>
            <span className="text-[10px] text-muted-foreground/50">
              Powered by Claude
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
