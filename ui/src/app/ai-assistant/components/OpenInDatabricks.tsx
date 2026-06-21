"use client";

import { useState } from "react";
import { Check, ExternalLink, FileCode, Loader2, NotebookPen } from "lucide-react";
import { api } from "@/lib/api-client";

interface OpenInDatabricksProps {
  sql: string;
}

type Kind = "notebook" | "query";

export function OpenInDatabricks({ sql }: OpenInDatabricksProps) {
  const [busy, setBusy] = useState<Kind | null>(null);
  const [done, setDone] = useState<Kind | null>(null);

  const open = async (kind: Kind) => {
    setBusy(kind);
    try {
      const path = kind === "notebook" ? "/ai-assistant/open/notebook" : "/ai-assistant/open/query";
      const res = await api.post<{ url: string }>(path, {
        sql,
        title: sql.slice(0, 50).replace(/\s+/g, " ").trim(),
      });
      if (res.url) window.open(res.url, "_blank", "noopener");
      setDone(kind);
      setTimeout(() => setDone(null), 2000);
    } catch {
      /* surfaced by api client; keep UI quiet */
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="flex items-center gap-2 mt-1.5">
      <button
        onClick={() => open("notebook")}
        disabled={!!busy}
        className="inline-flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
        title="Create a Databricks notebook with this SQL"
      >
        {busy === "notebook" ? <Loader2 className="h-3 w-3 animate-spin" />
          : done === "notebook" ? <Check className="h-3 w-3 text-emerald-500" />
          : <NotebookPen className="h-3 w-3" />}
        Open as notebook
      </button>
      <button
        onClick={() => open("query")}
        disabled={!!busy}
        className="inline-flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
        title="Save as a Databricks SQL query"
      >
        {busy === "query" ? <Loader2 className="h-3 w-3 animate-spin" />
          : done === "query" ? <Check className="h-3 w-3 text-emerald-500" />
          : <FileCode className="h-3 w-3" />}
        Save as query
        <ExternalLink className="h-2.5 w-2.5 opacity-50" />
      </button>
    </div>
  );
}
