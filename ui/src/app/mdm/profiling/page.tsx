// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { Activity } from "lucide-react";
import { useMdmEntities } from "@/hooks/useMdm";

export default function MdmProfilingPage() {
  const { data: entities } = useMdmEntities();
  const records = Array.isArray(entities) ? entities : [];

  // Profile golden records — analyze attribute completeness
  const allKeys = new Set<string>();
  records.forEach(r => {
    const attrs = r.attributes || {};
    if (typeof attrs === "object") Object.keys(attrs).forEach(k => allKeys.add(k));
  });

  const profiles = [...allKeys].map(key => {
    const total = records.length;
    const filled = records.filter(r => {
      const attrs = r.attributes || {};
      return typeof attrs === "object" && attrs[key] && attrs[key] !== "" && attrs[key] !== "None";
    }).length;
    const distinct = new Set(records.map(r => (r.attributes || {})[key]).filter(Boolean)).size;
    return { field: key, total, filled, fillRate: total > 0 ? Math.round((filled / total) * 100) : 0, distinct };
  }).sort((a, b) => a.fillRate - b.fillRate);

  return (
    <div className="space-y-4">
      <PageHeader title="Data Profiling" icon={Activity} breadcrumbs={["MDM", "Profiling"]}
        description="Profile golden record attributes — completeness, distinct values, and data patterns." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{records.length}</p><p className="text-xs text-muted-foreground">Records Profiled</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{allKeys.size}</p><p className="text-xs text-muted-foreground">Attributes Found</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{profiles.filter(p => p.fillRate < 50).length}</p><p className="text-xs text-muted-foreground">Low Completeness</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{profiles.length > 0 ? Math.round(profiles.reduce((s, p) => s + p.fillRate, 0) / profiles.length) : 0}%</p><p className="text-xs text-muted-foreground">Avg Fill Rate</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Attribute Profiles</CardTitle></CardHeader>
        <CardContent>
          {profiles.length === 0 ? (
            <div className="text-center py-8 text-sm text-muted-foreground">No attributes to profile yet</div>
          ) : (
            <div className="space-y-2">
              {profiles.map(p => (
                <div key={p.field} className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-muted/20">
                  <span className="text-sm font-mono w-32 truncate">{p.field}</span>
                  <div className="flex-1 h-3 bg-muted/30 rounded-full overflow-hidden">
                    <div className={`h-full rounded-full ${p.fillRate >= 80 ? "bg-foreground/60" : p.fillRate >= 50 ? "bg-muted-foreground" : "bg-red-500/60"}`}
                      style={{ width: `${p.fillRate}%` }} />
                  </div>
                  <span className="text-xs font-mono w-12 text-right">{p.fillRate}%</span>
                  <span className="text-xs text-muted-foreground w-16 text-right">{p.filled}/{p.total}</span>
                  <Badge variant="outline" className="text-[10px] w-20 justify-center">{p.distinct} distinct</Badge>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
