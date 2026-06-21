"use client";

import { useState } from "react";
import {
  CheckCircle2, ChevronDown, Database, GitBranch, Info, Loader2, Search,
  ShieldAlert, TerminalSquare, BarChart3, FileSearch,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ToolCallStep } from "../hooks/useChatStream";

const TOOL_META: Record<string, { label: string; Icon: React.ElementType }> = {
  describe_table:         { label: "Checking table schema",   Icon: Database },
  run_sql:                { label: "Running SQL query",        Icon: TerminalSquare },
  list_tables:            { label: "Listing tables",           Icon: Search },
  list_schemas:           { label: "Listing schemas",          Icon: Search },
  list_catalogs:          { label: "Listing catalogs",         Icon: Search },
  get_workspace_info:     { label: "Getting workspace info",   Icon: Info },
  search_tables:          { label: "Searching catalog",        Icon: FileSearch },
  get_table_lineage:      { label: "Tracing lineage",          Icon: GitBranch },
  profile_column:         { label: "Profiling column",         Icon: BarChart3 },
  explain_query:          { label: "Explaining query plan",    Icon: TerminalSquare },
  get_assessment_findings:{ label: "Reading security findings",Icon: ShieldAlert },
  list_pii_columns:       { label: "Checking PII columns",     Icon: ShieldAlert },
};

function tableArg(args: Record<string, unknown>): string | null {
  if (args.column)  return [args.catalog, args.schema, args.table, args.column].filter(Boolean).join(".");
  if (args.table)   return [args.catalog, args.schema, args.table].filter(Boolean).join(".");
  if (args.schema)  return [args.catalog, args.schema].filter(Boolean).join(".");
  if (args.term)    return String(args.term);
  if (args.severity) return String(args.severity);
  if (args.catalog) return String(args.catalog);
  if (args.query)   return `"${String(args.query).slice(0, 50)}${String(args.query).length > 50 ? "…" : ""}"`;
  return null;
}

export function ToolCallStepView({ step }: { step: ToolCallStep }) {
  const [open, setOpen] = useState(false);
  const meta = TOOL_META[step.tool] ?? { label: step.tool, Icon: Database };
  const { Icon } = meta;
  const arg = tableArg(step.args);
  const done = step.status === "done";
  const expandable = done && !!step.result_preview;

  return (
    <div className="my-1">
      <button
        type="button"
        disabled={!expandable}
        onClick={() => expandable && setOpen((o) => !o)}
        className={cn(
          "flex items-center gap-2 px-3 py-1.5 w-full rounded-lg text-xs border text-left",
          done
            ? "bg-muted/40 border-border/40 text-muted-foreground"
            : "bg-blue-50 border-blue-200 text-blue-700 dark:bg-blue-950/30 dark:border-blue-800 dark:text-blue-300",
          expandable && "hover:bg-muted/60 cursor-pointer",
        )}
      >
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
        {expandable && (
          <ChevronDown className={cn("h-3 w-3 shrink-0 ml-auto transition-transform", open && "rotate-180")} />
        )}
      </button>
      {open && step.result_preview && (
        <pre className="mt-1 mx-1 px-3 py-2 rounded-lg bg-muted/60 border border-border/40 text-[10px] font-mono whitespace-pre-wrap overflow-x-auto max-h-56 overflow-y-auto leading-relaxed text-muted-foreground">
          {step.result_preview}
        </pre>
      )}
    </div>
  );
}
