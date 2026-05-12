// @ts-nocheck
//
// Reusable "AI-draft content" toggle used by all 4 unstructured tabs
// that support narrative content (Documents, Media, Knowledge, Code).
//
// Wraps the existing checkbox pattern with two extras:
//   1. Inline backend status — pulls from GET /api/ai/status to show
//      whether the toggle will actually do anything ("→ databricks:..."
//      or "→ anthropic" or "→ no backend configured (open Settings)").
//      The endpoint is selected via Settings; this UI only displays the
//      effective backend so users don't toggle the box and wonder why
//      content didn't change.
//   2. Token-budget input (visible only when checked). Posts as
//      `ai_token_budget` in the submit body — capped per-job spend.

import { useEffect, useState } from "react";
import { Sparkles } from "lucide-react";
import { api } from "@/lib/api-client";
import { Input } from "@/components/ui/input";

interface AIStatus {
  available: boolean;
  model?: string;
  backend?: string;
  reason?: string;
}

interface Props {
  enabled: boolean;
  onEnabledChange: (v: boolean) => void;
  tokenBudget: number;
  onTokenBudgetChange: (v: number) => void;
  /** Per-tab label (e.g. "AI-draft document content"). */
  label: string;
  /** Per-tab note (e.g. "spreadsheets ignore this flag"). */
  note?: string;
}

export default function AIModeToggle({
  enabled,
  onEnabledChange,
  tokenBudget,
  onTokenBudgetChange,
  label,
  note,
}: Props) {
  const [status, setStatus] = useState<AIStatus | null>(null);

  useEffect(() => {
    api
      .get<AIStatus>("/ai/status")
      .then(setStatus)
      .catch(() => setStatus({ available: false, reason: "Status check failed" }));
  }, []);

  const backendLine = (() => {
    if (status === null) return "checking backend…";
    if (!status.available) {
      return status.reason || "no AI backend configured (open Settings)";
    }
    if (status.backend === "databricks" && status.model) {
      return `→ databricks: ${status.model}`;
    }
    return `→ ${status.backend ?? status.model ?? "ai"}`;
  })();

  return (
    <div className="space-y-2">
      <label className="flex items-center gap-2 text-sm cursor-pointer">
        <input
          type="checkbox"
          checked={enabled}
          onChange={(e) => onEnabledChange(e.target.checked)}
        />
        <span className="flex items-center gap-1.5">
          <Sparkles className="h-3.5 w-3.5 text-purple-500" />
          <span className="font-medium">{label}</span>
          <span className="text-muted-foreground">— slower, calls the LLM</span>
        </span>
      </label>

      <div className="text-xs text-muted-foreground pl-6">
        Backend{" "}
        <code className="px-1 bg-muted rounded text-[11px] font-mono">{backendLine}</code>
        {note ? <> · {note}</> : null}
      </div>

      {enabled && (
        <div className="pl-6 flex items-center gap-2">
          <label className="text-xs font-medium" htmlFor="ai-token-budget">
            Token budget per run
          </label>
          <Input
            id="ai-token-budget"
            type="number"
            value={tokenBudget}
            min={0}
            max={10_000_000}
            step={1000}
            onChange={(e) =>
              onTokenBudgetChange(
                Math.max(0, Math.min(10_000_000, parseInt(e.target.value) || 0)),
              )
            }
            className="h-7 w-32 text-xs"
          />
          <span className="text-[10px] text-muted-foreground">
            ~$0.01/1K tokens on Sonnet — the adapter falls back to templates after this is hit.
          </span>
        </div>
      )}
    </div>
  );
}
