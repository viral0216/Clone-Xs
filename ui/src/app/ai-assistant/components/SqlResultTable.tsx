"use client";

import { useState } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Loader2, Play, Sparkles } from "lucide-react";
import { api } from "@/lib/api-client";

interface SqlResultTableProps {
  sql: string;
  catalog?: string;
  schemaName?: string;
  onExplainResults?: (prompt: string) => void;
}

interface QueryResult {
  results: Record<string, unknown>[];
  row_count: number;
  sql: string;
  explanation?: string;
  error?: string;
}

// Build a compact markdown table (max 20 rows) to feed query results back to the agent.
function resultToMarkdown(cols: string[], rows: Record<string, unknown>[], total: number): string {
  const head = `| ${cols.join(" | ")} |`;
  const sep  = `| ${cols.map(() => "---").join(" | ")} |`;
  const body = rows.slice(0, 20).map(
    (r) => `| ${cols.map((c) => (r[c] == null ? "" : String(r[c]))).join(" | ")} |`,
  ).join("\n");
  const more = total > 20 ? `\n\n(${total} rows total; first 20 shown)` : "";
  return `${head}\n${sep}\n${body}${more}`;
}

export function SqlResultTable({ sql, catalog, schemaName, onExplainResults }: SqlResultTableProps) {
  const [result, setResult]   = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [ran, setRan]         = useState(false);

  const run = async () => {
    setLoading(true);
    setRan(true);
    try {
      const data = await api.post<QueryResult>("/ai-assistant/execute-nl", {
        question:    sql,
        catalog:     catalog     || "",
        schema_name: schemaName  || "",
      });
      setResult(data);
    } catch (err: any) {
      setResult({ results: [], row_count: 0, sql, error: err.message });
    } finally {
      setLoading(false);
    }
  };

  if (!ran) {
    return (
      <Button size="sm" variant="outline" className="mt-1 h-7 gap-1.5 text-xs" onClick={run}>
        <Play className="h-3 w-3" />
        Run Query
      </Button>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
        <Loader2 className="h-3 w-3 animate-spin" />
        Executing…
      </div>
    );
  }

  if (!result || result.error) {
    return (
      <div className="mt-1 text-xs text-destructive">
        {result?.error || "Query failed"}
      </div>
    );
  }

  if (!result.results.length) {
    return <div className="mt-1 text-xs text-muted-foreground">No rows returned.</div>;
  }

  const cols = Object.keys(result.results[0]);

  return (
    <div className="mt-2">
      <div className="rounded border border-border overflow-auto max-h-64">
        <Table className="text-xs">
          <TableHeader>
            <TableRow>
              {cols.map((c) => (
                <TableHead key={c} className="h-7 px-2 whitespace-nowrap">{c}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.results.slice(0, 100).map((row, i) => (
              <TableRow key={i}>
                {cols.map((c) => (
                  <TableCell key={c} className="px-2 py-1 max-w-xs truncate">
                    {row[c] == null ? "" : String(row[c])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {result.row_count > 100 && (
          <p className="px-2 py-1 text-[10px] text-muted-foreground">
            Showing 100 of {result.row_count} rows
          </p>
        )}
      </div>
      {onExplainResults && (
        <Button
          size="sm"
          variant="outline"
          className="mt-1.5 h-7 gap-1.5 text-xs"
          onClick={() =>
            onExplainResults(
              `Here are the results of the query I ran:\n\n` +
              `\`\`\`sql\n${result.sql || sql}\n\`\`\`\n\n` +
              resultToMarkdown(cols, result.results, result.row_count) +
              `\n\nExplain what this shows, highlight anything notable, and suggest a follow-up.`,
            )
          }
        >
          <Sparkles className="h-3 w-3" />
          Explain results
        </Button>
      )}
    </div>
  );
}
