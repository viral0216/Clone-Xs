// @ts-nocheck
"use client";
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Search, Table2, Columns, BookOpen, Tag, Loader2 } from "lucide-react";

export default function MetadataSearchPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState("all");

  async function doSearch() {
    if (!query.trim()) return;
    setLoading(true);
    try {
      const data = await api.post("/governance/search", { query, search_type: tab === "all" ? "all" : tab, limit: 50 });
      setResults(data);
    } catch {}
    setLoading(false);
  }

  const tabs = [
    { key: "all", label: "All", count: results?.total },
    { key: "tables", label: "Tables", icon: Table2, count: results?.tables?.length },
    { key: "columns", label: "Columns", icon: Columns, count: results?.columns?.length },
    { key: "terms", label: "Terms", icon: BookOpen, count: results?.terms?.length },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Metadata Search" icon={Search} breadcrumbs={["Governance", "Search"]} description="Search across all catalogs — find tables, columns, business terms, and tags by keyword." />

      <div className="flex gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
          <Input value={query} onChange={e => setQuery(e.target.value)} onKeyDown={e => e.key === "Enter" && doSearch()} placeholder="Search tables, columns, terms, tags..." className="pl-12 h-12 text-lg" />
        </div>
        <Button onClick={doSearch} disabled={loading || !query.trim()} className="h-12 px-6">
          {loading ? <Loader2 className="h-5 w-5 animate-spin" /> : "Search"}
        </Button>
      </div>

      {results && (
        <>
          <div className="flex gap-1 border-b border-border">
            {tabs.map(t => (
              <button key={t.key} onClick={() => setTab(t.key)} className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px ${tab === t.key ? "border-blue-600 text-blue-600" : "border-transparent text-muted-foreground hover:text-foreground"}`}>
                {t.label} {t.count != null && <span className="ml-1 text-xs">({t.count})</span>}
              </button>
            ))}
          </div>

          {(tab === "all" || tab === "tables") && results.tables?.length > 0 && (
            <Card><CardHeader className="pb-2"><CardTitle className="text-base flex items-center gap-2"><Table2 className="h-4 w-4" />Tables ({results.tables.length})</CardTitle></CardHeader>
              <CardContent><div className="space-y-2">
                {results.tables.map((t: any, i: number) => (
                  <div key={i} className="flex items-center gap-3 py-2 px-3 rounded border border-border hover:bg-accent/30">
                    <Table2 className="h-4 w-4 text-blue-500" />
                    <span className="font-mono text-sm font-medium">{t.fqn}</span>
                    <Badge variant="outline" className="text-xs">{t.type}</Badge>
                    {t.comment && <span className="text-xs text-muted-foreground truncate max-w-xs">{t.comment}</span>}
                  </div>
                ))}
              </div></CardContent>
            </Card>
          )}

          {(tab === "all" || tab === "columns") && results.columns?.length > 0 && (
            <Card><CardHeader className="pb-2"><CardTitle className="text-base flex items-center gap-2"><Columns className="h-4 w-4" />Columns ({results.columns.length})</CardTitle></CardHeader>
              <CardContent><div className="space-y-2">
                {results.columns.map((c: any, i: number) => (
                  <div key={i} className="flex items-center gap-3 py-2 px-3 rounded border border-border hover:bg-accent/30">
                    <Columns className="h-4 w-4 text-green-500" />
                    <span className="font-mono text-sm">{c.fqn}</span>
                    <Badge variant="outline" className="text-xs font-mono">{c.type}</Badge>
                    {c.comment && <span className="text-xs text-muted-foreground truncate max-w-xs">{c.comment}</span>}
                  </div>
                ))}
              </div></CardContent>
            </Card>
          )}

          {(tab === "all" || tab === "terms") && results.terms?.length > 0 && (
            <Card><CardHeader className="pb-2"><CardTitle className="text-base flex items-center gap-2"><BookOpen className="h-4 w-4" />Business Terms ({results.terms.length})</CardTitle></CardHeader>
              <CardContent><div className="space-y-2">
                {results.terms.map((t: any, i: number) => (
                  <div key={i} className="flex items-center gap-3 py-2 px-3 rounded border border-border hover:bg-accent/30">
                    <BookOpen className="h-4 w-4 text-purple-500" />
                    <span className="font-medium">{t.name}</span>
                    {t.abbreviation && <Badge variant="outline" className="text-xs">{t.abbreviation}</Badge>}
                    <Badge className={t.status === "approved" ? "bg-green-100 text-green-800" : "bg-gray-100 text-gray-800"}>{t.status}</Badge>
                    <span className="text-xs text-muted-foreground truncate max-w-sm">{t.definition}</span>
                  </div>
                ))}
              </div></CardContent>
            </Card>
          )}

          {results.total === 0 && <div className="text-center py-12 text-muted-foreground">No results found for "{query}"</div>}
        </>
      )}
    </div>
  );
}
