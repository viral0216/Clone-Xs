// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { FileText, Search, Filter, Download } from "lucide-react";
import { useMdmPairs, useMdmStewardship, useMdmEntities } from "@/hooks/useMdm";

export default function AuditLogPage() {
  const { data: pairs } = useMdmPairs();
  const { data: tasks } = useMdmStewardship();
  const { data: entities } = useMdmEntities();
  const [search, setSearch] = useState("");
  const [filterAction, setFilterAction] = useState("all");

  // Combine all actions into a unified audit log
  const log = [];

  // Entities created
  (Array.isArray(entities) ? entities : []).forEach(e => {
    log.push({ timestamp: e.created_at, action: "entity_created", user: e.created_by || "system", detail: `Created ${e.entity_type} entity: ${e.display_name}`, entity_id: e.entity_id });
    if (e.updated_at && e.updated_at !== e.created_at) {
      log.push({ timestamp: e.updated_at, action: "entity_updated", user: "system", detail: `Updated ${e.entity_type} entity: ${e.display_name}`, entity_id: e.entity_id });
    }
  });

  // Merge decisions
  (Array.isArray(pairs) ? pairs : []).forEach(p => {
    if (p.status === "merged" || p.status === "auto_merged") {
      log.push({ timestamp: p.reviewed_at || p.created_at, action: "merge", user: p.reviewed_by || "auto", detail: `Merged: ${p.record_a_name || "?"} ↔ ${p.record_b_name || "?"} (${Math.round(p.match_score || 0)}%)`, entity_id: "" });
    }
    if (p.status === "dismissed") {
      log.push({ timestamp: p.reviewed_at || p.created_at, action: "dismiss", user: p.reviewed_by || "auto", detail: `Dismissed match: ${p.record_a_name || "?"} ↔ ${p.record_b_name || "?"}`, entity_id: "" });
    }
  });

  // Stewardship actions
  (Array.isArray(tasks) ? tasks : []).forEach(t => {
    log.push({ timestamp: t.created_at, action: "task_created", user: "system", detail: `Stewardship task: ${t.description}`, entity_id: t.related_entity_id });
    if (t.resolved_at) {
      log.push({ timestamp: t.resolved_at, action: "task_resolved", user: t.resolved_by || "system", detail: `Resolved: ${t.resolution}`, entity_id: t.related_entity_id });
    }
  });

  // Sort by timestamp descending
  log.sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""));

  const actions = [...new Set(log.map(l => l.action))];
  const filtered = log.filter(l => {
    if (filterAction !== "all" && l.action !== filterAction) return false;
    if (search && !l.detail.toLowerCase().includes(search.toLowerCase()) && !l.user.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const exportLog = () => {
    const csv = ["Timestamp,Action,User,Detail", ...filtered.map(l => `"${l.timestamp}","${l.action}","${l.user}","${l.detail}"`)].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "mdm_audit_log.csv"; a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      <PageHeader title="MDM Audit Log" icon={FileText} breadcrumbs={["MDM", "Audit Log"]}
        description="Complete audit trail — every create, merge, split, approve, and reject with who, what, and when." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{log.length}</p><p className="text-xs text-muted-foreground">Total Events</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{log.filter(l => l.action === "merge").length}</p><p className="text-xs text-muted-foreground">Merges</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{log.filter(l => l.action === "entity_created").length}</p><p className="text-xs text-muted-foreground">Entities Created</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{log.filter(l => l.action === "task_resolved").length}</p><p className="text-xs text-muted-foreground">Tasks Resolved</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Event Log ({filtered.length} events)</CardTitle>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <input className="pl-8 pr-3 py-1.5 text-sm bg-muted border border-border rounded-md w-48" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)} />
              </div>
              <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md" value={filterAction} onChange={e => setFilterAction(e.target.value)}>
                <option value="all">All Actions</option>
                {actions.map(a => <option key={a} value={a}>{a}</option>)}
              </select>
              <Button size="sm" variant="outline" className="h-8 text-xs" onClick={exportLog}><Download className="h-3 w-3 mr-1" /> Export CSV</Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {filtered.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">No audit events yet</div>
          ) : (
            <div className="space-y-1">
              {filtered.slice(0, 100).map((event, i) => (
                <div key={i} className="flex items-center justify-between px-3 py-2 rounded-lg hover:bg-muted/20 transition-colors">
                  <div className="flex items-center gap-3 min-w-0">
                    <Badge variant="outline" className={`text-[10px] min-w-[90px] justify-center ${
                      event.action.includes("merge") ? "text-[#E8453C] border-[#E8453C]/30" :
                      event.action.includes("created") ? "text-foreground" :
                      event.action.includes("resolved") ? "text-foreground" :
                      "text-muted-foreground"
                    }`}>{event.action}</Badge>
                    <span className="text-sm truncate">{event.detail}</span>
                  </div>
                  <div className="flex items-center gap-3 shrink-0 ml-3">
                    <span className="text-xs text-muted-foreground">{event.user}</span>
                    <span className="text-xs text-muted-foreground">{event.timestamp?.slice(0, 19) || ""}</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
