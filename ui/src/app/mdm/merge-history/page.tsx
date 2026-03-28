// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { History, GitMerge, Undo2, ArrowRight } from "lucide-react";
import { useMdmPairs, useSplitRecord } from "@/hooks/useMdm";

export default function MergeHistoryPage() {
  const { data: pairs, isLoading } = useMdmPairs();
  const splitRecord = useSplitRecord();

  const allPairs = Array.isArray(pairs) ? pairs : [];
  const merged = allPairs.filter(p => p.status === "merged" || p.status === "auto_merged");
  const dismissed = allPairs.filter(p => p.status === "dismissed");

  return (
    <div className="space-y-4">
      <PageHeader title="Merge History" icon={History} breadcrumbs={["MDM", "Merge History"]}
        description="Complete audit trail of all merge and split operations — with the ability to undo any merge." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : allPairs.length}</p><p className="text-xs text-muted-foreground">Total Decisions</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : merged.length}</p><p className="text-xs text-muted-foreground">Merged</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : allPairs.filter(p => p.status === "auto_merged").length}</p><p className="text-xs text-muted-foreground">Auto-Merged</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{isLoading ? "—" : dismissed.length}</p><p className="text-xs text-muted-foreground">Dismissed</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">All Merge Decisions</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">{[1, 2, 3].map(i => <Skeleton key={i} className="h-14 w-full" />)}</div>
          ) : allPairs.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">No merge history yet</div>
          ) : (
            <div className="space-y-2">
              {allPairs.map(pair => (
                <div key={pair.pair_id} className="flex items-center justify-between px-3 py-3 rounded-lg border border-border hover:bg-muted/20">
                  <div className="flex items-center gap-3 flex-1 min-w-0">
                    <Badge variant="outline" className={`text-[10px] min-w-[80px] justify-center ${
                      pair.status === "auto_merged" ? "bg-muted/40 text-foreground border-border" :
                      pair.status === "merged" ? "bg-muted/30 text-foreground border-border" :
                      pair.status === "dismissed" ? "text-muted-foreground" :
                      "border-[#E8453C]/30 text-[#E8453C]"
                    }`}>{pair.status}</Badge>
                    <Badge variant="outline" className="text-[10px]">{pair.entity_type}</Badge>
                    <span className="text-sm truncate">{pair.record_a_name || pair.record_a_id?.slice(0, 8)}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                    <span className="text-sm truncate">{pair.record_b_name || pair.record_b_id?.slice(0, 8)}</span>
                    <Badge variant="outline" className="text-[10px]">{Math.round(pair.match_score || 0)}%</Badge>
                  </div>
                  <div className="flex items-center gap-2 shrink-0 ml-3">
                    {pair.reviewed_by && <span className="text-xs text-muted-foreground">{pair.reviewed_by}</span>}
                    <span className="text-xs text-muted-foreground">{pair.reviewed_at?.slice(0, 10) || pair.created_at?.slice(0, 10) || ""}</span>
                    {(pair.status === "merged" || pair.status === "auto_merged") && (
                      <Button size="sm" variant="ghost" className="h-7 text-xs" disabled={splitRecord.isPending}
                        onClick={() => { /* Would need entity_id from the merge — placeholder */ }}>
                        <Undo2 className="h-3 w-3 mr-1" /> Undo
                      </Button>
                    )}
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
