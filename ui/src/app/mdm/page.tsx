// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { Link } from "react-router-dom";
import { Crown, GitMerge, UserCheck, Network, ArrowRight, Database, Loader2, Shield, Search } from "lucide-react";
import { useMdmDashboard, useMdmEntities, useInitMdm } from "@/hooks/useMdm";
import { useState } from "react";

export default function MdmOverview() {
  const { data, isLoading } = useMdmDashboard();
  const { data: allEntities } = useMdmEntities();
  const initMdm = useInitMdm();
  const [globalSearch, setGlobalSearch] = useState("");

  const entityRows = data?.entities || [];
  const pairRows = data?.pairs || [];
  const stewardship = data?.stewardship || {};

  const totalEntities = entityRows.reduce((s, r) => s + (r.cnt || 0), 0);
  const pendingPairs = pairRows.filter(p => p.status === "pending").reduce((s, r) => s + (r.cnt || 0), 0);
  const openTasks = stewardship.open || 0;
  const entities = Array.isArray(allEntities) ? allEntities : [];

  // Entity type breakdown for chart
  const typeBreakdown = {};
  entityRows.forEach(r => {
    const t = r.entity_type || "Unknown";
    typeBreakdown[t] = (typeBreakdown[t] || 0) + (r.cnt || 0);
  });
  const typeEntries = Object.entries(typeBreakdown).sort((a, b) => b[1] - a[1]);
  const maxTypeCount = typeEntries.length > 0 ? Math.max(...typeEntries.map(e => e[1])) : 1;

  // Confidence distribution
  const confBuckets = { "90-100%": 0, "80-89%": 0, "70-79%": 0, "<70%": 0 };
  entities.forEach(e => {
    const c = (e.confidence_score || 0) * 100;
    if (c >= 90) confBuckets["90-100%"]++;
    else if (c >= 80) confBuckets["80-89%"]++;
    else if (c >= 70) confBuckets["70-79%"]++;
    else confBuckets["<70%"]++;
  });
  const confEntries = Object.entries(confBuckets);
  const maxConf = Math.max(...confEntries.map(e => e[1]), 1);

  // Global search results
  const searchResults = globalSearch.trim()
    ? entities.filter(e =>
        (e.display_name || "").toLowerCase().includes(globalSearch.toLowerCase()) ||
        (e.entity_type || "").toLowerCase().includes(globalSearch.toLowerCase()) ||
        (e.entity_id || "").includes(globalSearch.toLowerCase())
      ).slice(0, 8)
    : [];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Master Data Management"
        icon={Database}
        breadcrumbs={["MDM"]}
        description="Establish golden records, resolve duplicates, and manage master entity hierarchies across Unity Catalog."
      />

      <div className="flex items-center gap-3">
        <Button variant="outline" size="sm" onClick={() => initMdm.mutate()} disabled={initMdm.isPending}>
          {initMdm.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Shield className="h-4 w-4 mr-2" />}
          Initialize MDM Tables
        </Button>
        {initMdm.isSuccess && <Badge className="bg-muted/40 text-foreground">Tables initialized</Badge>}
      </div>

      {/* Global Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <input className="w-full pl-10 pr-4 py-2.5 text-sm bg-muted border border-border rounded-lg" placeholder="Search across all entities, source records, and hierarchies..."
          value={globalSearch} onChange={e => setGlobalSearch(e.target.value)} />
        {searchResults.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 bg-card border border-border rounded-lg shadow-lg z-50 overflow-hidden">
            {searchResults.map(r => (
              <Link key={r.entity_id} to="/mdm/golden-records" className="flex items-center gap-3 px-4 py-2.5 hover:bg-muted/30 transition-colors">
                <Badge variant="outline" className="text-[10px]">{r.entity_type}</Badge>
                <span className="text-sm font-medium">{r.display_name}</span>
                <span className="text-xs text-muted-foreground ml-auto">{Math.round((r.confidence_score || 0) * 100)}%</span>
              </Link>
            ))}
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4">
          <p className="text-2xl font-bold">{isLoading ? "—" : totalEntities}</p>
          <p className="text-xs text-muted-foreground">Golden Records</p>
        </CardContent></Card>
        <Card><CardContent className="pt-4">
          <p className="text-2xl font-bold">{isLoading ? "—" : pendingPairs}</p>
          <p className="text-xs text-muted-foreground">Pending Matches</p>
        </CardContent></Card>
        <Card><CardContent className="pt-4">
          <p className="text-2xl font-bold">{isLoading ? "—" : openTasks}</p>
          <p className="text-xs text-muted-foreground">Open Tasks</p>
        </CardContent></Card>
        <Card><CardContent className="pt-4">
          <p className="text-2xl font-bold">{isLoading ? "—" : stewardship.high_priority || 0}</p>
          <p className="text-xs text-muted-foreground">High Priority</p>
        </CardContent></Card>
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Entity Type Breakdown */}
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Entity Type Breakdown</CardTitle></CardHeader>
          <CardContent>
            {typeEntries.length === 0 ? (
              <p className="text-xs text-muted-foreground text-center py-4">No entities yet</p>
            ) : (
              <div className="space-y-2">
                {typeEntries.map(([type, count]) => (
                  <div key={type} className="flex items-center gap-3">
                    <span className="text-xs w-20 truncate text-muted-foreground">{type}</span>
                    <div className="flex-1 h-5 bg-muted/30 rounded-full overflow-hidden">
                      <div className="h-full bg-[#E8453C]/70 rounded-full transition-all" style={{ width: `${(count / maxTypeCount) * 100}%` }} />
                    </div>
                    <span className="text-xs font-mono w-8 text-right">{count}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Confidence Distribution */}
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Confidence Distribution</CardTitle></CardHeader>
          <CardContent>
            {entities.length === 0 ? (
              <p className="text-xs text-muted-foreground text-center py-4">No entities yet</p>
            ) : (
              <div className="space-y-2">
                {confEntries.map(([bucket, count]) => (
                  <div key={bucket} className="flex items-center gap-3">
                    <span className="text-xs w-16 text-muted-foreground">{bucket}</span>
                    <div className="flex-1 h-5 bg-muted/30 rounded-full overflow-hidden">
                      <div className={`h-full rounded-full transition-all ${bucket === "90-100%" ? "bg-foreground/60" : bucket === "80-89%" ? "bg-foreground/40" : bucket === "70-79%" ? "bg-muted-foreground/60" : "bg-red-500/50"}`} style={{ width: `${(count / maxConf) * 100}%` }} />
                    </div>
                    <span className="text-xs font-mono w-8 text-right">{count}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Quick Links */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {[
          { href: "/mdm/golden-records", label: "Golden Records", desc: "View and search master entities with merge history", icon: Crown },
          { href: "/mdm/match-merge", label: "Match & Merge", desc: "Configure matching rules, review duplicates, merge or split", icon: GitMerge },
          { href: "/mdm/stewardship", label: "Data Stewardship", desc: "Review queue for manual resolution — approve or reject", icon: UserCheck },
          { href: "/mdm/hierarchies", label: "Hierarchy Management", desc: "Manage parent-child entity relationships", icon: Network },
        ].map((link) => (
          <Link key={link.href} to={link.href}>
            <Card className="hover:border-[#E8453C]/30 transition-colors cursor-pointer h-full">
              <CardContent className="pt-4 flex items-start gap-3">
                <link.icon className="h-5 w-5 mt-0.5 text-muted-foreground" />
                <div>
                  <p className="text-sm font-medium">{link.label}</p>
                  <p className="text-xs text-muted-foreground">{link.desc}</p>
                </div>
                <ArrowRight className="h-4 w-4 text-muted-foreground ml-auto mt-1" />
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
