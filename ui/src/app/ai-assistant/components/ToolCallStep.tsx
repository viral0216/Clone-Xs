"use client";

import { CheckCircle2, Database, Info, Loader2, Search, TerminalSquare } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ToolCallStep } from "../hooks/useChatStream";

const TOOL_META: Record<string, { label: string; Icon: React.ElementType }> = {
  describe_table:    { label: "Checking table schema",    Icon: Database },
  run_sql:           { label: "Running SQL query",         Icon: TerminalSquare },
  list_tables:       { label: "Listing tables",            Icon: Search },
  list_schemas:      { label: "Listing schemas",           Icon: Search },
  list_catalogs:     { label: "Listing catalogs",          Icon: Search },
  get_workspace_info:{ label: "Getting workspace info",    Icon: Info },
};

function tableArg(args: Record<string, unknown>): string | null {
  if (args.table)   return [args.catalog, args.schema, args.table].filter(Boolean).join(".");
  if (args.schema)  return [args.catalog, args.schema].filter(Boolean).join(".");
  if (args.catalog) return String(args.catalog);
  if (args.query)   return `"${String(args.query).slice(0, 50)}${String(args.query).length > 50 ? "…" : ""}"`;
  return null;
}

export function ToolCallStepView({ step }: { step: ToolCallStep }) {
  const meta = TOOL_META[step.tool] ?? { label: step.tool, Icon: Database };
  const { Icon } = meta;
  const arg = tableArg(step.args);
  const done = step.status === "done";

  return (
    <div className={cn(
      "flex items-center gap-2 px-3 py-1.5 my-1 rounded-lg text-xs border select-none",
      done
        ? "bg-muted/40 border-border/40 text-muted-foreground"
        : "bg-blue-50 border-blue-200 text-blue-700 dark:bg-blue-950/30 dark:border-blue-800 dark:text-blue-300",
    )}>
      {done
        ? <CheckCircle2 className="h-3 w-3 shrink-0 text-emerald-500" />
        : <Loader2 className="h-3 w-3 shrink-0 animate-spin" />
      }
      <Icon className="h-3 w-3 shrink-0" />
      <span className="font-medium">{meta.label}</span>
      {arg && (
        <code className="text-[10px] bg-muted/70 px-1 py-px rounded font-mono truncate max-w-[200px]">
          {arg}
        </code>
      )}
    </div>
  );
}
