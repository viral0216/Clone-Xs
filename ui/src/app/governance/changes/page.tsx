// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { History, Clock, Plus, Pencil, Trash2, CheckCircle2, XCircle } from "lucide-react";

const CHANGE_ICONS: Record<string, { icon: any; color: string }> = {
  created: { icon: Plus, color: "text-green-600 bg-green-100 dark:bg-green-950" },
  updated: { icon: Pencil, color: "text-blue-600 bg-blue-100 dark:bg-blue-950" },
  deleted: { icon: Trash2, color: "text-red-600 bg-red-100 dark:bg-red-950" },
  approved: { icon: CheckCircle2, color: "text-green-600 bg-green-100 dark:bg-green-950" },
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
    <div className="space-y-6">
      <PageHeader title="Change History" icon={History} breadcrumbs={["Governance", "Change History"]} description="Track who changed what metadata — glossary terms, certifications, DQ rules, SLA rules, and data contracts." />

      <select value={entityFilter} onChange={e => setEntityFilter(e.target.value)} className="border rounded px-3 py-2 text-sm bg-background">
        <option value="">All Entity Types</option>
        {entities.filter(Boolean).map(e => <option key={e} value={e}>{e.replace("_", " ")}</option>)}
      </select>

      {changes.length === 0 ? (
        <div className="text-center py-16"><Clock className="h-12 w-12 text-muted-foreground/30 mx-auto mb-4" /><p className="text-muted-foreground">No changes recorded yet</p></div>
      ) : (
        <div className="relative">
          <div className="absolute left-6 top-0 bottom-0 w-px bg-border" />
          <div className="space-y-4">
            {changes.map((ch, i) => {
              const cfg = CHANGE_ICONS[ch.change_type] || CHANGE_ICONS.updated;
              const Icon = cfg.icon;
              let details: Record<string, any> = {};
              try { details = typeof ch.details === "string" ? JSON.parse(ch.details) : (ch.details || {}); } catch {}

              return (
                <div key={i} className="relative flex items-start gap-4 pl-12">
                  <div className={`absolute left-4 w-5 h-5 rounded-full flex items-center justify-center ${cfg.color}`}>
                    <Icon className="h-3 w-3" />
                  </div>
                  <Card className="flex-1">
                    <CardContent className="pt-3 pb-3">
                      <div className="flex items-center gap-2 flex-wrap">
                        <Badge className={ch.change_type === "created" ? "bg-green-100 text-green-800" : ch.change_type === "deleted" ? "bg-red-100 text-red-800" : ch.change_type === "approved" ? "bg-green-100 text-green-800" : ch.change_type === "rejected" ? "bg-red-100 text-red-800" : "bg-blue-100 text-blue-800"}>{ch.change_type}</Badge>
                        <Badge variant="outline" className="text-xs">{ch.entity_type}</Badge>
                        <span className="font-mono text-xs text-muted-foreground">{ch.entity_id}</span>
                        <span className="text-xs text-muted-foreground ml-auto flex items-center gap-1"><Clock className="h-3 w-3" />{timeAgo(ch.changed_at)}</span>
                      </div>
                      {ch.changed_by && <p className="text-xs text-muted-foreground mt-1">by {ch.changed_by}</p>}
                      {Object.keys(details).length > 0 && (
                        <div className="mt-2 flex flex-wrap gap-2">
                          {Object.entries(details).map(([k, v]) => (
                            <span key={k} className="text-xs bg-accent/50 rounded px-2 py-0.5"><span className="text-muted-foreground">{k}:</span> {typeof v === "object" ? JSON.stringify(v) : String(v)}</span>
                          ))}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
