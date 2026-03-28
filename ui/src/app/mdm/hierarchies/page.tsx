// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { Network, ChevronRight, Plus, Loader2 } from "lucide-react";
import { useMdmHierarchies, useCreateHierarchy } from "@/hooks/useMdm";

export default function HierarchiesPage() {
  const { data, isLoading } = useMdmHierarchies();
  const createHierarchy = useCreateHierarchy();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newType, setNewType] = useState("Customer");

  const hierarchies = Array.isArray(data) ? data : [];
  const totalNodes = hierarchies.reduce((s, h) => s + (h.node_count || 0), 0);
  const maxDepth = hierarchies.length > 0 ? Math.max(...hierarchies.map(h => h.max_depth || 0)) + 1 : 0;

  return (
    <div className="space-y-4">
      <PageHeader title="Hierarchy Management" icon={Network} breadcrumbs={["MDM", "Hierarchies"]}
        description="Manage parent-child entity relationships — corporate structures, product categories, and organizational trees." />

      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => setShowCreate(!showCreate)}>
          <Plus className="h-3 w-3 mr-1" /> New Hierarchy
        </Button>
      </div>

      {showCreate && (
        <Card>
          <CardContent className="pt-4">
            <div className="flex items-center gap-2">
              <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Hierarchy name..." value={newName} onChange={e => setNewName(e.target.value)} />
              <select className="px-2 py-1.5 text-sm bg-muted border border-border rounded-md" value={newType} onChange={e => setNewType(e.target.value)}>
                {["Customer", "Product", "Supplier", "Employee", "Location"].map(t => <option key={t}>{t}</option>)}
              </select>
              <Button size="sm" disabled={!newName.trim() || createHierarchy.isPending}
                onClick={() => { createHierarchy.mutate({ name: newName.trim(), entity_type: newType }); setNewName(""); setShowCreate(false); }}>
                {createHierarchy.isPending ? <Loader2 className="h-3 w-3 animate-spin" /> : "Create"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : hierarchies.length}</p><p className="text-xs text-muted-foreground">Hierarchies</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : totalNodes}</p><p className="text-xs text-muted-foreground">Total Nodes</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : maxDepth}</p><p className="text-xs text-muted-foreground">Max Depth</p></CardContent></Card>
      </div>

      {isLoading ? (
        <div className="space-y-2">{[1, 2].map(i => <Skeleton key={i} className="h-24 w-full rounded-lg" />)}</div>
      ) : hierarchies.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground text-sm">
            No hierarchies yet — create one to organize entity relationships
          </CardContent>
        </Card>
      ) : (
        hierarchies.map(h => (
          <Card key={h.hierarchy_id}>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <Network className="h-4 w-4" />
                {h.name}
                <Badge variant="outline" className="text-[10px] ml-1">{h.entity_type}</Badge>
                <span className="text-xs text-muted-foreground ml-auto">{h.node_count} nodes, depth {(h.max_depth || 0) + 1}</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-xs text-muted-foreground">ID: {h.hierarchy_id}</p>
            </CardContent>
          </Card>
        ))
      )}
    </div>
  );
}
