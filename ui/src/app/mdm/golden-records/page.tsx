// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { Crown, Search, CheckCircle, Clock, Plus, X, History, Link2, BarChart3, ArrowRight } from "lucide-react";
import { useMdmEntities, useMdmEntity, useCreateEntity } from "@/hooks/useMdm";

function Entity360({ entityId, onClose }: { entityId: string; onClose: () => void }) {
  const { data, isLoading } = useMdmEntity(entityId);
  const entity = data?.entity;
  const sources = data?.source_records || [];
  const attributes = entity?.attributes || {};
  const attrEntries = typeof attributes === "object" && !Array.isArray(attributes) ? Object.entries(attributes) : [];

  return (
    <div className="fixed inset-0 z-50 flex">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="absolute right-0 top-0 bottom-0 w-full max-w-2xl bg-background border-l border-border shadow-xl overflow-y-auto">
        <div className="sticky top-0 bg-background z-10 px-6 py-4 border-b border-border flex items-center justify-between">
          <div>
            <p className="text-base font-semibold">{entity?.display_name || "Loading..."}</p>
            <p className="text-xs text-muted-foreground">{entity?.entity_type} — {entityId.slice(0, 12)}...</p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-md hover:bg-muted"><X className="h-4 w-4" /></button>
        </div>

        {isLoading ? (
          <div className="p-6 space-y-3">{[1, 2, 3].map(i => <Skeleton key={i} className="h-12 w-full" />)}</div>
        ) : (
          <div className="p-6 space-y-5">
            {/* Overview Stats */}
            <div className="grid grid-cols-3 gap-3">
              <div className="p-3 rounded-lg bg-muted/30 text-center">
                <p className="text-xl font-bold">{Math.round((entity?.confidence_score || 0) * 100)}%</p>
                <p className="text-[10px] text-muted-foreground">Confidence</p>
              </div>
              <div className="p-3 rounded-lg bg-muted/30 text-center">
                <p className="text-xl font-bold">{entity?.source_count || 0}</p>
                <p className="text-[10px] text-muted-foreground">Sources</p>
              </div>
              <div className="p-3 rounded-lg bg-muted/30 text-center">
                <p className="text-xl font-bold">{entity?.status}</p>
                <p className="text-[10px] text-muted-foreground">Status</p>
              </div>
            </div>

            {/* Attributes */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-2 flex items-center gap-1.5"><BarChart3 className="h-3 w-3" /> Golden Record Attributes</p>
              {attrEntries.length === 0 ? (
                <p className="text-xs text-muted-foreground">No attributes stored</p>
              ) : (
                <div className="rounded-md border border-border overflow-hidden">
                  {attrEntries.map(([k, v]) => (
                    <div key={k} className="grid grid-cols-3 px-3 py-2 text-sm border-b border-border last:border-0">
                      <span className="text-xs font-medium text-muted-foreground">{k}</span>
                      <span className="text-xs col-span-2">{String(v)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Source Records */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-2 flex items-center gap-1.5"><Link2 className="h-3 w-3" /> Source Records ({sources.length})</p>
              {sources.length === 0 ? (
                <p className="text-xs text-muted-foreground">No linked source records</p>
              ) : (
                <div className="space-y-2">
                  {sources.map((src, i) => {
                    const srcAttrs = typeof src.attributes === "object" ? Object.entries(src.attributes || {}) : [];
                    return (
                      <Card key={src.source_record_id || i} className="bg-muted/10">
                        <CardContent className="pt-3 pb-3">
                          <div className="flex items-center justify-between mb-2">
                            <div className="flex items-center gap-2">
                              <Badge variant="outline" className="text-[10px]">{src.source_system}</Badge>
                              <span className="text-xs text-muted-foreground">{src.source_table}</span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="text-xs font-mono">trust: {src.trust_score}</span>
                              <span className="text-xs text-muted-foreground">{src.ingested_at?.slice(0, 10)}</span>
                            </div>
                          </div>
                          {srcAttrs.length > 0 && (
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                              {srcAttrs.slice(0, 8).map(([k, v]) => (
                                <div key={k} className="text-[11px]">
                                  <span className="text-muted-foreground">{k}: </span>
                                  <span>{String(v)}</span>
                                </div>
                              ))}
                              {srcAttrs.length > 8 && <p className="text-[10px] text-muted-foreground col-span-2">+{srcAttrs.length - 8} more fields</p>}
                            </div>
                          )}
                        </CardContent>
                      </Card>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Timeline */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-2 flex items-center gap-1.5"><History className="h-3 w-3" /> Timeline</p>
              <div className="space-y-2 pl-3 border-l-2 border-border">
                {entity?.updated_at && (
                  <div className="relative pl-4">
                    <div className="absolute -left-[9px] top-1 h-3 w-3 rounded-full bg-foreground border-2 border-background" />
                    <p className="text-xs"><span className="font-medium">Updated</span> — {new Date(entity.updated_at).toLocaleString()}</p>
                  </div>
                )}
                {entity?.created_at && (
                  <div className="relative pl-4">
                    <div className="absolute -left-[9px] top-1 h-3 w-3 rounded-full bg-muted-foreground border-2 border-background" />
                    <p className="text-xs"><span className="font-medium">Created</span> by {entity.created_by || "system"} — {new Date(entity.created_at).toLocaleString()}</p>
                  </div>
                )}
                {sources.map((src, i) => (
                  <div key={i} className="relative pl-4">
                    <div className="absolute -left-[9px] top-1 h-3 w-3 rounded-full bg-muted border-2 border-background" />
                    <p className="text-xs"><span className="font-medium">Source linked</span> from {src.source_system} — {src.ingested_at?.slice(0, 19) || ""}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function GoldenRecordsPage() {
  const [search, setSearch] = useState("");
  const [selectedEntity, setSelectedEntity] = useState<string | null>(null);
  const { data: entities, isLoading } = useMdmEntities();
  const createEntity = useCreateEntity();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newType, setNewType] = useState("Customer");

  const records = Array.isArray(entities) ? entities : [];
  const filtered = search
    ? records.filter(r =>
        (r.display_name || "").toLowerCase().includes(search.toLowerCase()) ||
        (r.entity_type || "").toLowerCase().includes(search.toLowerCase()) ||
        (r.entity_id || "").toLowerCase().includes(search.toLowerCase())
      )
    : records;

  const active = records.filter(r => r.status === "active");
  const avgConf = records.length > 0 ? Math.round(records.reduce((s, r) => s + (r.confidence_score || 0) * 100, 0) / records.length) : 0;

  return (
    <div className="space-y-4">
      <PageHeader title="Golden Records" icon={Crown} breadcrumbs={["MDM", "Golden Records"]}
        description="Master entities — the single source of truth for each record across all source systems." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : records.length}</p><p className="text-xs text-muted-foreground">Total Records</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : active.length}</p><p className="text-xs text-muted-foreground">Active</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : records.length - active.length}</p><p className="text-xs text-muted-foreground">Deleted</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : `${avgConf}%`}</p><p className="text-xs text-muted-foreground">Avg Confidence</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Master Entities</CardTitle>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <input className="pl-8 pr-3 py-1.5 text-sm bg-muted border border-border rounded-md w-56" placeholder="Search records..." value={search} onChange={e => setSearch(e.target.value)} />
              </div>
              <Button size="sm" variant="outline" className="h-8 text-xs" onClick={() => setShowCreate(!showCreate)}>
                <Plus className="h-3 w-3 mr-1" /> New
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {showCreate && (
            <div className="flex items-center gap-2 mb-3 pb-3 border-b border-border">
              <select className="px-2 py-1.5 text-sm bg-muted border border-border rounded-md" value={newType} onChange={e => setNewType(e.target.value)}>
                {["Customer", "Product", "Supplier", "Employee", "Location"].map(t => <option key={t}>{t}</option>)}
              </select>
              <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Display name..." value={newName} onChange={e => setNewName(e.target.value)} />
              <Button size="sm" className="h-8" disabled={!newName.trim() || createEntity.isPending}
                onClick={() => { createEntity.mutate({ entity_type: newType, display_name: newName.trim() }); setNewName(""); setShowCreate(false); }}>
                Create
              </Button>
            </div>
          )}

          {isLoading ? (
            <div className="space-y-2">{[1, 2, 3].map(i => <Skeleton key={i} className="h-10 w-full rounded-lg" />)}</div>
          ) : filtered.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">
              {records.length === 0 ? "No golden records yet — ingest source records and run duplicate detection from Match & Merge" : "No records match your search"}
            </div>
          ) : (
            <div className="space-y-1">
              {filtered.map(r => (
                <div key={r.entity_id} className="flex items-center justify-between px-3 py-2.5 rounded-lg hover:bg-muted/30 transition-colors cursor-pointer"
                  onClick={() => setSelectedEntity(r.entity_id)}>
                  <div className="flex items-center gap-3">
                    <Badge variant="outline" className="text-[10px] min-w-[70px] justify-center">{r.entity_type}</Badge>
                    <span className="text-sm font-medium">{r.display_name || r.entity_id?.slice(0, 8)}</span>
                    <span className="text-xs text-muted-foreground">{r.entity_id?.slice(0, 8)}...</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-muted-foreground">{r.source_count || 0} sources</span>
                    <Badge variant="outline" className="text-[10px]">{Math.round((r.confidence_score || 0) * 100)}%</Badge>
                    {r.status === "active" ? <CheckCircle className="h-3.5 w-3.5 text-foreground" /> : <Clock className="h-3.5 w-3.5 text-muted-foreground" />}
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Entity 360 Drawer */}
      {selectedEntity && <Entity360 entityId={selectedEntity} onClose={() => setSelectedEntity(null)} />}
    </div>
  );
}
