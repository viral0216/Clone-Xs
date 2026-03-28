// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { FileText, Download } from "lucide-react";
import { useMdmDashboard, useMdmEntities, useMdmPairs, useMdmStewardship } from "@/hooks/useMdm";

export default function MdmReportsPage() {
  const { data: dashboard } = useMdmDashboard();
  const { data: entities } = useMdmEntities();
  const { data: pairs } = useMdmPairs();
  const { data: tasks } = useMdmStewardship();

  const records = Array.isArray(entities) ? entities : [];
  const pairList = Array.isArray(pairs) ? pairs : [];
  const taskList = Array.isArray(tasks) ? tasks : [];

  const exportReport = (format: string) => {
    const report = {
      generated: new Date().toISOString(),
      summary: {
        total_entities: records.length,
        entity_types: [...new Set(records.map(r => r.entity_type))],
        avg_confidence: records.length > 0 ? (records.reduce((s, r) => s + (r.confidence_score || 0), 0) / records.length).toFixed(2) : 0,
        total_pairs: pairList.length,
        pending_pairs: pairList.filter(p => p.status === "pending").length,
        merged_pairs: pairList.filter(p => p.status === "merged" || p.status === "auto_merged").length,
        stewardship_queue: taskList.length,
        stewardship_open: taskList.filter(t => t.status === "open").length,
      },
    };

    if (format === "json") {
      const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob); const a = document.createElement("a"); a.href = url; a.download = "mdm_report.json"; a.click(); URL.revokeObjectURL(url);
    } else {
      const lines = [`# MDM Compliance Report`, `Generated: ${report.generated}`, ``, `## Summary`, `- Total Golden Records: ${report.summary.total_entities}`, `- Entity Types: ${report.summary.entity_types.join(", ")}`, `- Avg Confidence: ${report.summary.avg_confidence}`, `- Total Match Pairs: ${report.summary.total_pairs}`, `- Pending Review: ${report.summary.pending_pairs}`, `- Merged: ${report.summary.merged_pairs}`, `- Stewardship Queue: ${report.summary.stewardship_queue}`, `- Open Tasks: ${report.summary.stewardship_open}`];
      const blob = new Blob([lines.join("\n")], { type: "text/markdown" });
      const url = URL.createObjectURL(blob); const a = document.createElement("a"); a.href = url; a.download = "mdm_report.md"; a.click(); URL.revokeObjectURL(url);
    }
  };

  return (
    <div className="space-y-4">
      <PageHeader title="MDM Reports" icon={FileText} breadcrumbs={["MDM", "Reports"]}
        description="Compliance reports — entity counts, merge rates, SLA compliance, and stewardship productivity." />

      <div className="flex gap-2">
        <Button size="sm" variant="outline" onClick={() => exportReport("json")}><Download className="h-3 w-3 mr-1" /> Export JSON</Button>
        <Button size="sm" variant="outline" onClick={() => exportReport("md")}><Download className="h-3 w-3 mr-1" /> Export Markdown</Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{records.length}</p><p className="text-xs text-muted-foreground">Golden Records</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{pairList.filter(p => p.status === "merged" || p.status === "auto_merged").length}</p><p className="text-xs text-muted-foreground">Total Merges</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{records.length > 0 ? Math.round(records.reduce((s, r) => s + (r.confidence_score || 0) * 100, 0) / records.length) : 0}%</p><p className="text-xs text-muted-foreground">Avg Confidence</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{taskList.filter(t => t.status === "open").length}</p><p className="text-xs text-muted-foreground">Open Tasks</p></CardContent></Card>
      </div>

      {/* Entity Type Summary */}
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Entity Summary by Type</CardTitle></CardHeader>
        <CardContent>
          <div className="rounded-md border border-border overflow-hidden">
            <div className="grid grid-cols-5 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
              <span>Type</span><span>Count</span><span>Avg Confidence</span><span>Active</span><span>Sources/Entity</span>
            </div>
            {[...new Set(records.map(r => r.entity_type))].map(type => {
              const typeRecs = records.filter(r => r.entity_type === type);
              return (
                <div key={type} className="grid grid-cols-5 px-3 py-2 border-t border-border text-sm">
                  <span className="font-medium">{type}</span>
                  <span>{typeRecs.length}</span>
                  <span>{Math.round(typeRecs.reduce((s, r) => s + (r.confidence_score || 0) * 100, 0) / typeRecs.length)}%</span>
                  <span>{typeRecs.filter(r => r.status === "active").length}</span>
                  <span>{(typeRecs.reduce((s, r) => s + (r.source_count || 0), 0) / typeRecs.length).toFixed(1)}</span>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>

      {/* Merge Activity */}
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Merge Activity</CardTitle></CardHeader>
        <CardContent>
          <div className="grid grid-cols-3 gap-4">
            <div className="p-3 rounded-lg bg-muted/30 text-center">
              <p className="text-xl font-bold">{pairList.filter(p => p.status === "auto_merged").length}</p>
              <p className="text-[10px] text-muted-foreground">Auto-Merged</p>
            </div>
            <div className="p-3 rounded-lg bg-muted/30 text-center">
              <p className="text-xl font-bold">{pairList.filter(p => p.status === "merged").length}</p>
              <p className="text-[10px] text-muted-foreground">Manual Merges</p>
            </div>
            <div className="p-3 rounded-lg bg-muted/30 text-center">
              <p className="text-xl font-bold">{pairList.filter(p => p.status === "dismissed").length}</p>
              <p className="text-[10px] text-muted-foreground">Dismissed</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
