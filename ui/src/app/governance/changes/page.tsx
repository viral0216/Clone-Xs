// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { History, Clock, Plus, Pencil, Trash2, CheckCircle2, XCircle } from "lucide-react";
import DataTable, { Column } from "@/components/DataTable";

const CHANGE_ICONS: Record<string, { icon: any; color: string }> = {
  created: { icon: Plus, color: "text-foreground bg-muted/40 dark:bg-white/5" },
  updated: { icon: Pencil, color: "text-[#E8453C] bg-muted/50 dark:bg-white/5" },
  deleted: { icon: Trash2, color: "text-red-600 bg-red-100 dark:bg-red-950" },
  approved: { icon: CheckCircle2, color: "text-foreground bg-muted/40 dark:bg-white/5" },
  rejected: { icon: XCircle, color: "text-red-600 bg-red-100 dark:bg-red-950" },
};

function timeAgo(dateStr: string) {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  const now = new Date();
  const secs = Math.floor((now.getTime() - d.getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

export default function ChangesPage() {
  const [changes, setChanges] = useState<any[]>([]);
  const [entityFilter, setEntityFilter] = useState("");

  useEffect(() => { load(); }, [entityFilter]);
  async function load() {
    try {
      const url = entityFilter ? `/governance/changes?entity_type=${entityFilter}&limit=100` : "/governance/changes?limit=100";
      const d = await api.get(url);
      setChanges(Array.isArray(d) ? d : []);
    } catch {}
  }

  const entities = ["", "glossary", "certification", "dq_rule", "sla_rule", "contract"];

  return (
    <div className="space-y-4">
      <PageHeader title="Change History" icon={History} breadcrumbs={["Governance", "Change History"]} description="Track who changed what metadata — glossary terms, certifications, DQ rules, SLA rules, and data contracts." />

      <select value={entityFilter} onChange={e => setEntityFilter(e.target.value)} className="border rounded px-3 py-2 text-sm bg-background">
        <option value="">All Entity Types</option>
        {entities.filter(Boolean).map(e => <option key={e} value={e}>{e.replace("_", " ")}</option>)}
      </select>

      <DataTable
        data={changes}
        columns={[
          {
            key: "change_type",
            label: "Change",
            sortable: true,
            render: (v: any) => {
              const cfg = CHANGE_ICONS[v] || CHANGE_ICONS.updated;
              const Icon = cfg.icon;
              return (
                <div className="flex items-center gap-2">
                  <div className={`w-5 h-5 rounded-full flex items-center justify-center ${cfg.color}`}><Icon className="h-3 w-3" /></div>
                  <Badge className={v === "created" ? "bg-muted/40 text-foreground" : v === "deleted" ? "bg-red-100 text-red-800" : v === "approved" ? "bg-muted/40 text-foreground" : v === "rejected" ? "bg-red-100 text-red-800" : "bg-muted/50 text-foreground"}>{v}</Badge>
                </div>
              );
            },
          },
          { key: "entity_type", label: "Entity Type", sortable: true, render: (v: any) => <Badge variant="outline" className="text-xs">{v}</Badge> },
          { key: "entity_id", label: "Entity ID", sortable: true, render: (v: any) => <span className="font-mono text-xs text-muted-foreground">{v}</span> },
          { key: "changed_by", label: "Changed By", sortable: true, render: (v: any) => v ? <span className="text-xs text-muted-foreground">{v}</span> : null },
          {
            key: "changed_at",
            label: "When",
            sortable: true,
            render: (v: any) => <span className="text-xs text-muted-foreground flex items-center gap-1"><Clock className="h-3 w-3" />{timeAgo(v)}</span>,
          },
          {
            key: "details",
            label: "Details",
            render: (v: any) => {
              let details: Record<string, any> = {};
              try { details = typeof v === "string" ? JSON.parse(v) : (v || {}); } catch {}
              if (Object.keys(details).length === 0) return null;
              return (
                <div className="flex flex-wrap gap-2">
                  {Object.entries(details).map(([k, val]) => (
                    <span key={k} className="text-xs bg-accent/50 rounded px-2 py-0.5"><span className="text-muted-foreground">{k}:</span> {typeof val === "object" ? JSON.stringify(val) : String(val)}</span>
                  ))}
                </div>
              );
            },
          },
        ] as Column[]}
        searchable
        searchKeys={["change_type", "entity_type", "entity_id", "changed_by"]}
        pageSize={25}
        compact
        tableId="change-history"
        emptyMessage="No changes recorded yet."
      />
    </div>
  );
}
