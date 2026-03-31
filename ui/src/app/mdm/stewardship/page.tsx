// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { UserCheck, CheckCircle, XCircle, Clock, MessageSquare, User, CheckCheck, AlertTriangle } from "lucide-react";
import { useMdmStewardship, useApproveTask, useRejectTask } from "@/hooks/useMdm";

function timeAgo(ts: string) {
  if (!ts) return "";
  const diff = Date.now() - new Date(ts).getTime();
  const hrs = Math.floor(diff / 3600000);
  if (hrs < 1) return `${Math.floor(diff / 60000)}m open`;
  if (hrs < 24) return `${hrs}h open`;
  const days = Math.floor(hrs / 24);
  return `${days}d open`;
}

function slaStatus(ts: string, slaDays: number = 3) {
  if (!ts) return { label: "", color: "" };
  const diff = Date.now() - new Date(ts).getTime();
  const days = diff / 86400000;
  if (days >= slaDays) return { label: "OVERDUE", color: "text-red-500 border-red-500/30" };
  if (days >= slaDays * 0.7) return { label: "AT RISK", color: "text-yellow-600 border-yellow-500/30" };
  return { label: "ON TRACK", color: "text-foreground border-border" };
}

export default function StewardshipPage() {
  const [filter, setFilter] = useState("all");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [comments, setComments] = useState<Record<string, string[]>>({});
  const [newComment, setNewComment] = useState("");
  const [assignTo, setAssignTo] = useState("");
  const priorityFilter = filter === "all" ? undefined : filter;
  const { data, isLoading, refetch } = useMdmStewardship("open", priorityFilter);
  const approveTask = useApproveTask();
  const rejectTask = useRejectTask();

  const queue = Array.isArray(data) ? data : [];
  const high = queue.filter(q => q.priority === "high").length;
  const medium = queue.filter(q => q.priority === "medium").length;
  const low = queue.filter(q => q.priority === "low").length;
  const overdue = queue.filter(q => slaStatus(q.created_at).label === "OVERDUE").length;

  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const toggleSelect = (id: string) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };
  const selectAll = () => setSelectedIds(new Set(queue.map(q => q.task_id)));
  const selectNone = () => setSelectedIds(new Set());

  const bulkApprove = async () => {
    for (const id of selectedIds) {
      await approveTask.mutateAsync(id);
    }
    setSelectedIds(new Set());
  };
  const bulkReject = async () => {
    for (const id of selectedIds) {
      await rejectTask.mutateAsync({ taskId: id });
    }
    setSelectedIds(new Set());
  };

  const addComment = (taskId: string) => {
    if (!newComment.trim()) return;
    setComments(prev => ({ ...prev, [taskId]: [...(prev[taskId] || []), `${new Date().toLocaleTimeString()} — ${newComment.trim()}`] }));
    setNewComment("");
  };

  // Mock side-by-side comparison data (in real app, fetched from source records)
  const mockCompare = {
    fields: [
      { field: "name", valueA: "Acme Corp", valueB: "ACME Corporation", match: false },
      { field: "email", valueA: "info@acme.com", valueB: "info@acme.com", match: true },
      { field: "address", valueA: "123 Main St, NYC", valueB: "123 Main Street, New York", match: false },
      { field: "phone", valueA: "+1-555-0100", valueB: "+15550100", match: true },
      { field: "industry", valueA: "Technology", valueB: "Tech", match: false },
    ],
  };

  return (
    <div className="space-y-4">
      <PageHeader title="Data Stewardship" icon={UserCheck} breadcrumbs={["MDM", "Stewardship"]}
        description="Manual review queue for records that need human judgment — approve, reject, or escalate." />

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : queue.length}</p><p className="text-xs text-muted-foreground">Queue Size</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold text-red-500">{isLoading ? "—" : high}</p><p className="text-xs text-muted-foreground">High Priority</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : medium}</p><p className="text-xs text-muted-foreground">Medium</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : low}</p><p className="text-xs text-muted-foreground">Low</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className={`text-2xl font-bold ${overdue > 0 ? "text-red-500" : ""}`}>{isLoading ? "—" : overdue}</p><p className="text-xs text-muted-foreground">Overdue SLA</p></CardContent></Card>
      </div>

      {/* Queue */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Review Queue</CardTitle>
            <div className="flex items-center gap-2">
              {/* Bulk actions */}
              {selectedIds.size > 0 && (
                <div className="flex items-center gap-1.5 mr-2">
                  <span className="text-xs text-muted-foreground">{selectedIds.size} selected</span>
                  <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" onClick={bulkApprove} disabled={approveTask.isPending}>
                    <CheckCheck className="h-3 w-3 mr-1" /> Approve All
                  </Button>
                  <Button size="sm" variant="ghost" className="h-6 text-[10px] px-2" onClick={bulkReject} disabled={rejectTask.isPending}>
                    <XCircle className="h-3 w-3 mr-1" /> Reject All
                  </Button>
                  <button className="text-xs text-muted-foreground hover:text-foreground" onClick={selectNone}>Clear</button>
                </div>
              )}
              {queue.length > 0 && selectedIds.size === 0 && (
                <button className="text-xs text-muted-foreground hover:text-foreground mr-2" onClick={selectAll}>Select all</button>
              )}
              {/* Priority filter */}
              <div className="flex gap-1">
                {["all", "high", "medium", "low"].map(f => (
                  <Button key={f} size="sm" variant={filter === f ? "default" : "outline"} className={`h-6 text-[10px] px-2 ${filter === f ? "bg-[#E8453C]" : ""}`} onClick={() => setFilter(f)}>
                    {f.charAt(0).toUpperCase() + f.slice(1)}
                  </Button>
                ))}
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">{[1, 2, 3].map(i => <Skeleton key={i} className="h-16 w-full rounded-lg" />)}</div>
          ) : queue.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">Queue empty — all caught up</div>
          ) : (
            <div className="space-y-2">
              {queue.map(item => {
                const sla = slaStatus(item.created_at);
                const isExpanded = expandedId === item.task_id;
                const itemComments = comments[item.task_id] || [];
                return (
                  <div key={item.task_id} className="rounded-lg border border-border hover:bg-muted/10 transition-colors">
                    {/* Main row */}
                    <div className="px-3 py-3">
                      <div className="flex items-center justify-between mb-1.5">
                        <div className="flex items-center gap-2">
                          <input type="checkbox" checked={selectedIds.has(item.task_id)} onChange={() => toggleSelect(item.task_id)} className="h-3.5 w-3.5 rounded border-border" />
                          <Badge variant="outline" className={`text-[10px] ${item.priority === "high" ? "border-red-500/30 text-red-500" : item.priority === "medium" ? "border-border" : "border-border text-muted-foreground"}`}>{item.priority}</Badge>
                          <Badge variant="outline" className="text-[10px]">{item.entity_type}</Badge>
                          <Badge variant="outline" className="text-[10px]">{item.task_type}</Badge>
                          <Badge variant="outline" className={`text-[10px] ${sla.color}`}>{sla.label}</Badge>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-muted-foreground">{timeAgo(item.created_at)}</span>
                          <button className="text-xs text-[#E8453C] hover:underline" onClick={() => setExpandedId(isExpanded ? null : item.task_id)}>
                            {isExpanded ? "Collapse" : "Details"}
                          </button>
                        </div>
                      </div>
                      <p className="text-sm mb-2 ml-6">{item.description}</p>
                      <div className="flex items-center justify-between ml-6">
                        <div className="flex items-center gap-2">
                          <User className="h-3 w-3 text-muted-foreground" />
                          <span className="text-xs text-muted-foreground">{item.assignee || "Unassigned"}</span>
                          {itemComments.length > 0 && (
                            <span className="flex items-center gap-1 text-xs text-muted-foreground">
                              <MessageSquare className="h-3 w-3" /> {itemComments.length}
                            </span>
                          )}
                        </div>
                        <div className="flex gap-1.5">
                          <Button size="sm" variant="outline" className="h-7 text-xs" disabled={approveTask.isPending}
                            onClick={() => approveTask.mutate(item.task_id)}>
                            <CheckCircle className="h-3 w-3 mr-1" /> Approve
                          </Button>
                          <Button size="sm" variant="ghost" className="h-7 text-xs" disabled={rejectTask.isPending}
                            onClick={() => rejectTask.mutate({ taskId: item.task_id })}>
                            <XCircle className="h-3 w-3 mr-1" /> Reject
                          </Button>
                        </div>
                      </div>
                    </div>

                    {/* Expanded detail panel */}
                    {isExpanded && (
                      <div className="border-t border-border px-3 py-3 bg-muted/10 space-y-3">
                        {/* Side-by-side comparison */}
                        <div>
                          <p className="text-xs font-semibold text-muted-foreground uppercase mb-2">Record Comparison</p>
                          <div className="rounded-md border border-border overflow-hidden">
                            <div className="grid grid-cols-3 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                              <span>Field</span><span>Record A</span><span>Record B</span>
                            </div>
                            {mockCompare.fields.map(f => (
                              <div key={f.field} className={`grid grid-cols-3 px-3 py-2 text-sm border-t border-border ${!f.match ? "bg-red-500/5" : ""}`}>
                                <span className="text-xs font-medium text-muted-foreground">{f.field}</span>
                                <span className="text-xs">{f.valueA}</span>
                                <span className="text-xs">{f.valueB}</span>
                              </div>
                            ))}
                          </div>
                        </div>

                        {/* Assign */}
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-muted-foreground">Assign to:</span>
                          <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md w-48" placeholder="user@company.com" value={assignTo} onChange={e => setAssignTo(e.target.value)} />
                          <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" disabled={!assignTo.trim()}>Assign</Button>
                        </div>

                        {/* Comments */}
                        <div>
                          <p className="text-xs font-semibold text-muted-foreground uppercase mb-1.5">Notes & Comments</p>
                          {itemComments.length > 0 && (
                            <div className="space-y-1 mb-2">
                              {itemComments.map((c, i) => (
                                <p key={i} className="text-xs text-muted-foreground bg-muted/30 px-2 py-1 rounded">{c}</p>
                              ))}
                            </div>
                          )}
                          <div className="flex gap-2">
                            <input className="flex-1 px-2 py-1 text-xs bg-muted border border-border rounded-md" placeholder="Add a note..." value={newComment} onChange={e => setNewComment(e.target.value)}
                              onKeyDown={e => e.key === "Enter" && addComment(item.task_id)} />
                            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" onClick={() => addComment(item.task_id)}>
                              <MessageSquare className="h-3 w-3 mr-1" /> Add
                            </Button>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
