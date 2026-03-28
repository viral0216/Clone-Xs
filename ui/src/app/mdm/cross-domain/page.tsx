// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { Shuffle, Play, Loader2 } from "lucide-react";
import { useMdmEntities } from "@/hooks/useMdm";
import { useState } from "react";

export default function CrossDomainPage() {
  const { data: entities } = useMdmEntities();
  const records = Array.isArray(entities) ? entities : [];
  const types = [...new Set(records.map(r => r.entity_type))];
  const [sourceType, setSourceType] = useState(types[0] || "Customer");
  const [targetType, setTargetType] = useState(types[1] || "Supplier");
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState<any[]>([]);

  const runCrossDomain = () => {
    setRunning(true);
    // Simulate cross-domain matching
    setTimeout(() => {
      const sourceRecords = records.filter(r => r.entity_type === sourceType);
      const targetRecords = records.filter(r => r.entity_type === targetType);
      const matches = [];
      for (const s of sourceRecords.slice(0, 5)) {
        for (const t of targetRecords.slice(0, 5)) {
          const nameA = (s.display_name || "").toLowerCase();
          const nameB = (t.display_name || "").toLowerCase();
          if (nameA && nameB && (nameA.includes(nameB.slice(0, 4)) || nameB.includes(nameA.slice(0, 4)))) {
            matches.push({ sourceEntity: s, targetEntity: t, score: 70 + Math.floor(Math.random() * 25), reason: "Name similarity detected" });
          }
        }
      }
      setResults(matches);
      setRunning(false);
    }, 1500);
  };

  return (
    <div className="space-y-4">
      <PageHeader title="Cross-Domain Matching" icon={Shuffle} breadcrumbs={["MDM", "Cross-Domain"]}
        description="Match across entity types — find that a Customer and a Supplier are the same organization." />

      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Configure Cross-Domain Match</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <div className="flex items-center gap-3">
            <div>
              <label className="text-[10px] font-medium text-muted-foreground uppercase">Source Type</label>
              <select className="block w-full mt-1 px-2 py-1.5 text-sm bg-muted border border-border rounded-md" value={sourceType} onChange={e => setSourceType(e.target.value)}>
                {types.map(t => <option key={t}>{t}</option>)}
              </select>
            </div>
            <span className="text-muted-foreground mt-4">↔</span>
            <div>
              <label className="text-[10px] font-medium text-muted-foreground uppercase">Target Type</label>
              <select className="block w-full mt-1 px-2 py-1.5 text-sm bg-muted border border-border rounded-md" value={targetType} onChange={e => setTargetType(e.target.value)}>
                {types.map(t => <option key={t}>{t}</option>)}
              </select>
            </div>
            <Button className="mt-4" disabled={running || sourceType === targetType} onClick={runCrossDomain}>
              {running ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Play className="h-3 w-3 mr-1" />}
              Run Cross-Domain Match
            </Button>
          </div>
          {sourceType === targetType && <p className="text-xs text-red-500">Source and target types must be different</p>}
        </CardContent>
      </Card>

      {results.length > 0 && (
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Cross-Domain Matches ({results.length})</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-2">
              {results.map((r, i) => (
                <div key={i} className="px-3 py-3 rounded-lg border border-border hover:bg-muted/20">
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-[10px]">{r.sourceEntity.entity_type}</Badge>
                      <span className="text-sm font-medium">{r.sourceEntity.display_name}</span>
                      <span className="text-muted-foreground">↔</span>
                      <Badge variant="outline" className="text-[10px]">{r.targetEntity.entity_type}</Badge>
                      <span className="text-sm font-medium">{r.targetEntity.display_name}</span>
                    </div>
                    <Badge className="text-[10px] bg-muted/40 text-foreground border-border">{r.score}%</Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">{r.reason}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
