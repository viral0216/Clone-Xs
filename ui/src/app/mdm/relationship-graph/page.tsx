// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { Network, ZoomIn, ZoomOut, Maximize2, Filter } from "lucide-react";
import { useMdmEntities } from "@/hooks/useMdm";

function EntityNode({ entity, x, y, selected, onClick }: any) {
  const colors = { Customer: "#E8453C", Product: "#3B82F6", Supplier: "#10B981", Employee: "#F59E0B" };
  const color = colors[entity.entity_type] || "#6B7280";
  return (
    <g onClick={() => onClick(entity)} className="cursor-pointer">
      <circle cx={x} cy={y} r={selected ? 28 : 24} fill={color} opacity={selected ? 0.9 : 0.15} stroke={color} strokeWidth={selected ? 2 : 1} />
      <text x={x} y={y - 6} textAnchor="middle" className="text-[9px] font-medium fill-foreground">{(entity.display_name || "").slice(0, 12)}</text>
      <text x={x} y={y + 6} textAnchor="middle" className="text-[8px] fill-muted-foreground">{entity.entity_type}</text>
      <text x={x} y={y + 16} textAnchor="middle" className="text-[7px] fill-muted-foreground">{Math.round((entity.confidence_score || 0) * 100)}%</text>
    </g>
  );
}

export default function RelationshipGraphPage() {
  const { data: entities, isLoading } = useMdmEntities();
  const [selectedEntity, setSelectedEntity] = useState<any>(null);
  const [zoom, setZoom] = useState(1);
  const [filterType, setFilterType] = useState("all");

  const allEntities = Array.isArray(entities) ? entities : [];
  const filtered = filterType === "all" ? allEntities : allEntities.filter(e => e.entity_type === filterType);
  const types = [...new Set(allEntities.map(e => e.entity_type))];

  // Layout entities in a circle
  const cx = 400, cy = 300, radius = Math.min(200, 50 + filtered.length * 15);
  const positions = filtered.map((e, i) => {
    const angle = (2 * Math.PI * i) / Math.max(filtered.length, 1) - Math.PI / 2;
    return { x: cx + radius * Math.cos(angle), y: cy + radius * Math.sin(angle) };
  });

  // Generate edges between entities of same type (simple heuristic)
  const edges = [];
  for (let i = 0; i < filtered.length; i++) {
    for (let j = i + 1; j < filtered.length; j++) {
      if (filtered[i].entity_type === filtered[j].entity_type) {
        edges.push({ from: i, to: j });
      }
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader title="Entity Relationship Graph" icon={Network} breadcrumbs={["MDM", "Relationships"]}
        description="Visual network of entity connections — explore how golden records relate across sources and hierarchies." />

      <div className="flex items-center gap-2">
        <Filter className="h-3.5 w-3.5 text-muted-foreground" />
        <Button size="sm" variant={filterType === "all" ? "default" : "outline"} className={`h-6 text-[10px] px-2 ${filterType === "all" ? "bg-[#E8453C]" : ""}`} onClick={() => setFilterType("all")}>All</Button>
        {types.map(t => (
          <Button key={t} size="sm" variant={filterType === t ? "default" : "outline"} className={`h-6 text-[10px] px-2 ${filterType === t ? "bg-[#E8453C]" : ""}`} onClick={() => setFilterType(t)}>{t}</Button>
        ))}
        <div className="ml-auto flex items-center gap-1">
          <button onClick={() => setZoom(z => Math.max(0.5, z - 0.1))} className="p-1 rounded hover:bg-muted"><ZoomOut className="h-3.5 w-3.5" /></button>
          <span className="text-xs text-muted-foreground w-10 text-center">{Math.round(zoom * 100)}%</span>
          <button onClick={() => setZoom(z => Math.min(2, z + 0.1))} className="p-1 rounded hover:bg-muted"><ZoomIn className="h-3.5 w-3.5" /></button>
          <button onClick={() => setZoom(1)} className="p-1 rounded hover:bg-muted"><Maximize2 className="h-3.5 w-3.5" /></button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="md:col-span-2">
          <CardContent className="pt-4">
            {isLoading ? <Skeleton className="h-[500px] w-full" /> : filtered.length === 0 ? (
              <div className="h-[500px] flex items-center justify-center text-sm text-muted-foreground">No entities to display — create golden records first</div>
            ) : (
              <svg viewBox="0 0 800 600" className="w-full h-[500px] bg-muted/10 rounded-lg" style={{ transform: `scale(${zoom})`, transformOrigin: "center" }}>
                {/* Edges */}
                {edges.map((edge, i) => (
                  <line key={i} x1={positions[edge.from].x} y1={positions[edge.from].y} x2={positions[edge.to].x} y2={positions[edge.to].y}
                    stroke="currentColor" strokeOpacity={0.1} strokeWidth={1} />
                ))}
                {/* Nodes */}
                {filtered.map((entity, i) => (
                  <EntityNode key={entity.entity_id} entity={entity} x={positions[i].x} y={positions[i].y}
                    selected={selectedEntity?.entity_id === entity.entity_id} onClick={setSelectedEntity} />
                ))}
              </svg>
            )}
          </CardContent>
        </Card>

        {/* Detail Panel */}
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Entity Detail</CardTitle></CardHeader>
          <CardContent>
            {selectedEntity ? (
              <div className="space-y-3">
                <div>
                  <p className="text-sm font-medium">{selectedEntity.display_name}</p>
                  <Badge variant="outline" className="text-[10px] mt-1">{selectedEntity.entity_type}</Badge>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="p-2 rounded bg-muted/30 text-center"><p className="text-lg font-bold">{Math.round((selectedEntity.confidence_score || 0) * 100)}%</p><p className="text-[10px] text-muted-foreground">Confidence</p></div>
                  <div className="p-2 rounded bg-muted/30 text-center"><p className="text-lg font-bold">{selectedEntity.source_count || 0}</p><p className="text-[10px] text-muted-foreground">Sources</p></div>
                </div>
                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase mb-1">Connections</p>
                  {allEntities.filter(e => e.entity_type === selectedEntity.entity_type && e.entity_id !== selectedEntity.entity_id).slice(0, 5).map(e => (
                    <div key={e.entity_id} className="flex items-center gap-2 py-1 text-xs cursor-pointer hover:text-[#E8453C]" onClick={() => setSelectedEntity(e)}>
                      <span>{e.display_name}</span>
                      <Badge variant="outline" className="text-[9px]">{Math.round((e.confidence_score || 0) * 100)}%</Badge>
                    </div>
                  ))}
                </div>
                <p className="text-[10px] text-muted-foreground">ID: {selectedEntity.entity_id}</p>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground text-center py-8">Click an entity node to see details</p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
