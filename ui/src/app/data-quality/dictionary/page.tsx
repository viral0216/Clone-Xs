// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  BookOpen, Plus, Loader2, Trash2, Search,
  Table2, Columns3, Tags, FileText,
} from "lucide-react";

/* ── Status helpers ────────────────────────────────────── */

function statusColor(s: string) {
  return s === "active"
    ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400"
    : s === "draft"
    ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
    : "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400";
}

const STATUSES = ["active", "draft", "deprecated"] as const;

/* ── Page ──────────────────────────────────────────────── */

export default function DataDictionaryPage() {
  const [terms, setTerms] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);

  // Filters
  const [domainFilter, setDomainFilter] = useState("All");
  const [statusFilter, setStatusFilter] = useState("All");
  const [searchText, setSearchText] = useState("");

  // Form state
  const [form, setForm] = useState({
    name: "",
    abbreviation: "",
    definition: "",
    domain: "",
    owner: "",
    tags: "",
    status: "draft" as string,
  });

  // Global metadata search
  const [metaQuery, setMetaQuery] = useState("");
  const [metaResults, setMetaResults] = useState<any>(null);
  const [metaSearching, setMetaSearching] = useState(false);
  const [metaTab, setMetaTab] = useState<"tables" | "columns" | "terms" | "tags">("tables");

  useEffect(() => {
    load();
  }, []);

  async function load() {
    setLoading(true);
    try {
      const data = await api.get("/governance/glossary");
      setTerms(Array.isArray(data) ? data : []);
    } catch {
      /* silent */
    }
    setLoading(false);
  }

  async function createTerm() {
    if (!form.name.trim()) {
      toast.error("Name is required.");
      return;
    }
    const body = {
      name: form.name.trim(),
      abbreviation: form.abbreviation.trim(),
      definition: form.definition.trim(),
      domain: form.domain.trim(),
      owner: form.owner.trim(),
      tags: form.tags
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean),
      status: form.status,
    };
    try {
      await api.post("/governance/glossary", body);
      toast.success("Term created.");
      setShowForm(false);
      setForm({ name: "", abbreviation: "", definition: "", domain: "", owner: "", tags: "", status: "draft" });
      load();
    } catch (e: any) {
      toast.error(e.message || "Failed to create term.");
    }
  }

  async function deleteTerm(termId: string) {
    try {
      await api.delete(`/governance/glossary/${termId}`);
      toast.success("Term deleted.");
      setTerms((prev) => prev.filter((t) => t.term_id !== termId && t.id !== termId));
    } catch (e: any) {
      toast.error(e.message || "Failed to delete term.");
    }
  }

  async function runMetaSearch() {
    if (!metaQuery.trim()) return;
    setMetaSearching(true);
    try {
      const res = await api.post(`/governance/search?q=${encodeURIComponent(metaQuery.trim())}`, {});
      setMetaResults(res || {});
    } catch (e: any) {
      toast.error(e.message || "Search failed.");
    }
    setMetaSearching(false);
  }

  /* ── Derived data ───────────────────────────────────── */

  const domains = ["All", ...Array.from(new Set(terms.map((t) => t.domain).filter(Boolean)))];

  const filtered = terms.filter((t) => {
    if (domainFilter !== "All" && t.domain !== domainFilter) return false;
    if (statusFilter !== "All" && t.status !== statusFilter.toLowerCase()) return false;
    if (searchText) {
      const q = searchText.toLowerCase();
      const hay = `${t.name} ${t.abbreviation} ${t.definition} ${t.domain} ${t.owner} ${(t.tags || []).join(" ")}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });

  const totalTerms = terms.length;
  const activeCount = terms.filter((t) => t.status === "active").length;
  const draftCount = terms.filter((t) => t.status === "draft").length;
  const deprecatedCount = terms.filter((t) => t.status === "deprecated").length;

  /* ── Table columns ──────────────────────────────────── */

  const termColumns: Column[] = [
    {
      key: "name",
      label: "Name",
      sortable: true,
      render: (v) => <span className="font-mono text-xs font-medium">{v}</span>,
    },
    {
      key: "abbreviation",
      label: "Abbreviation",
      sortable: true,
      render: (v) => <span className="font-mono text-xs">{v || "—"}</span>,
    },
    {
      key: "domain",
      label: "Domain",
      sortable: true,
      render: (v) => <span className="text-xs">{v || "—"}</span>,
    },
    {
      key: "owner",
      label: "Owner",
      sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{v || "—"}</span>,
    },
    {
      key: "status",
      label: "Status",
      sortable: true,
      render: (v) => (
        <Badge variant="outline" className={`text-[10px] ${statusColor(v)}`}>
          {v}
        </Badge>
      ),
    },
    {
      key: "tags",
      label: "Tags",
      render: (v) =>
        Array.isArray(v) && v.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {v.map((tag: string) => (
              <Badge key={tag} variant="secondary" className="text-[10px]">
                {tag}
              </Badge>
            ))}
          </div>
        ) : (
          <span className="text-xs text-muted-foreground">—</span>
        ),
    },
    {
      key: "linked_columns",
      label: "Linked Cols",
      sortable: true,
      render: (v) => (
        <span className="text-xs font-mono">
          {Array.isArray(v) ? v.length : typeof v === "number" ? v : 0}
        </span>
      ),
    },
    {
      key: "term_id",
      label: "",
      render: (_, row) => (
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={() => deleteTerm(row.term_id || row.id)}
        >
          <Trash2 className="h-3.5 w-3.5 text-destructive" />
        </Button>
      ),
    },
  ];

  /* ── Metadata search result helpers ─────────────────── */

  const metaTabs = [
    { key: "tables", label: "Tables", icon: Table2, count: metaResults?.tables?.length || 0 },
    { key: "columns", label: "Columns", icon: Columns3, count: metaResults?.columns?.length || 0 },
    { key: "terms", label: "Terms", icon: FileText, count: metaResults?.terms?.length || 0 },
    { key: "tags", label: "Tags", icon: Tags, count: metaResults?.tags?.length || 0 },
  ] as const;

  /* ── Render ─────────────────────────────────────────── */

  return (
    <div className="space-y-4">
      <PageHeader
        title="Data Dictionary"
        icon={BookOpen}
        breadcrumbs={["Data Quality", "Governance", "Data Dictionary"]}
        description="Centralized glossary of business terms. Define, categorize, and link terms to physical columns for consistent understanding across teams."
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{totalTerms}</p>
            <p className="text-xs text-muted-foreground mt-1">Total Terms</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-green-500">{activeCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Active</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold text-amber-500`}>{draftCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Draft</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold ${deprecatedCount > 0 ? "text-red-500" : "text-foreground"}`}>
              {deprecatedCount}
            </p>
            <p className="text-xs text-muted-foreground mt-1">Deprecated</p>
          </CardContent>
        </Card>
      </div>

      {/* Filters: Domain pills + Status pills + Search */}
      <div className="space-y-2">
        {/* Domain filter */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground font-medium mr-1">Domain:</span>
          {domains.map((d) => (
            <Button
              key={d}
              variant={domainFilter === d ? "default" : "outline"}
              size="sm"
              className="h-7 text-xs"
              onClick={() => setDomainFilter(d)}
            >
              {d}
            </Button>
          ))}
        </div>
        {/* Status filter */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground font-medium mr-1">Status:</span>
          {["All", "Active", "Draft", "Deprecated"].map((s) => (
            <Button
              key={s}
              variant={statusFilter === s ? "default" : "outline"}
              size="sm"
              className="h-7 text-xs"
              onClick={() => setStatusFilter(s)}
            >
              {s}
            </Button>
          ))}
        </div>
        {/* Search + Add Term */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <Input
              className="pl-8 h-9 text-sm"
              placeholder="Search terms..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
            />
          </div>
          <Button onClick={() => setShowForm(!showForm)}>
            <Plus className="h-4 w-4 mr-2" />
            {showForm ? "Cancel" : "Add Term"}
          </Button>
        </div>
      </div>

      {/* Create Term Form */}
      {showForm && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">New Term</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Name *</label>
                <Input
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  placeholder="e.g. Customer LTV"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Abbreviation</label>
                <Input
                  value={form.abbreviation}
                  onChange={(e) => setForm({ ...form, abbreviation: e.target.value })}
                  placeholder="e.g. CLTV"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Domain</label>
                <Input
                  value={form.domain}
                  onChange={(e) => setForm({ ...form, domain: e.target.value })}
                  placeholder="e.g. Finance"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Owner</label>
                <Input
                  value={form.owner}
                  onChange={(e) => setForm({ ...form, owner: e.target.value })}
                  placeholder="e.g. data-eng"
                />
              </div>
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Definition</label>
              <textarea
                value={form.definition}
                onChange={(e) => setForm({ ...form, definition: e.target.value })}
                placeholder="Describe the business meaning of this term..."
                className="w-full min-h-[80px] rounded-md border border-input bg-background px-3 py-2 text-sm resize-y"
              />
            </div>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3 items-end">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Tags (comma-separated)</label>
                <Input
                  value={form.tags}
                  onChange={(e) => setForm({ ...form, tags: e.target.value })}
                  placeholder="e.g. revenue, kpi, finance"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Status</label>
                <select
                  value={form.status}
                  onChange={(e) => setForm({ ...form, status: e.target.value })}
                  className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
                >
                  {STATUSES.map((s) => (
                    <option key={s} value={s}>
                      {s.charAt(0).toUpperCase() + s.slice(1)}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex items-end">
                <Button onClick={createTerm} disabled={!form.name.trim()}>
                  Create Term
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Terms DataTable */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <BookOpen className="h-4 w-4" /> Glossary Terms ({filtered.length})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : filtered.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">
              {terms.length === 0
                ? 'No terms found. Click "Add Term" to create your first glossary entry.'
                : "No terms match the current filters."}
            </p>
          ) : (
            <DataTable
              data={filtered}
              columns={termColumns}
              searchable={false}
              pageSize={15}
              compact
              tableId="dictionary-terms"
            />
          )}
        </CardContent>
      </Card>

      {/* Global Metadata Search */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Search className="h-4 w-4" /> Global Metadata Search
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex items-center gap-3">
            <div className="relative flex-1 max-w-md">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                className="pl-8 h-9 text-sm"
                placeholder="Search tables, columns, terms, tags..."
                value={metaQuery}
                onChange={(e) => setMetaQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && runMetaSearch()}
              />
            </div>
            <Button variant="outline" onClick={runMetaSearch} disabled={metaSearching || !metaQuery.trim()}>
              {metaSearching ? <Loader2 className="h-4 w-4 animate-spin" /> : "Search"}
            </Button>
          </div>

          {metaResults && (
            <div className="space-y-2">
              {/* Tabs */}
              <div className="flex items-center gap-1 border-b border-border">
                {metaTabs.map((tab) => {
                  const Icon = tab.icon;
                  return (
                    <button
                      key={tab.key}
                      onClick={() => setMetaTab(tab.key as any)}
                      className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium border-b-2 transition-colors ${
                        metaTab === tab.key
                          ? "border-foreground text-foreground"
                          : "border-transparent text-muted-foreground hover:text-foreground"
                      }`}
                    >
                      <Icon className="h-3.5 w-3.5" />
                      {tab.label}
                      <Badge variant="secondary" className="text-[10px] ml-1">
                        {tab.count}
                      </Badge>
                    </button>
                  );
                })}
              </div>

              {/* Tab content */}
              <div className="max-h-64 overflow-y-auto">
                {metaTab === "tables" && (
                  <div className="space-y-1">
                    {(metaResults.tables || []).length === 0 ? (
                      <p className="text-xs text-muted-foreground py-4 text-center">No tables found.</p>
                    ) : (
                      (metaResults.tables || []).map((item: any, i: number) => (
                        <div key={i} className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-muted/50">
                          <Table2 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                          <span className="font-mono text-xs">{item.table_fqn || item.name || item}</span>
                          {item.catalog && (
                            <span className="text-[10px] text-muted-foreground ml-auto">{item.catalog}</span>
                          )}
                        </div>
                      ))
                    )}
                  </div>
                )}

                {metaTab === "columns" && (
                  <div className="space-y-1">
                    {(metaResults.columns || []).length === 0 ? (
                      <p className="text-xs text-muted-foreground py-4 text-center">No columns found.</p>
                    ) : (
                      (metaResults.columns || []).map((item: any, i: number) => (
                        <div key={i} className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-muted/50">
                          <Columns3 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                          <span className="font-mono text-xs">{item.column_fqn || item.name || item}</span>
                          {item.data_type && (
                            <Badge variant="outline" className="text-[10px] ml-auto">{item.data_type}</Badge>
                          )}
                        </div>
                      ))
                    )}
                  </div>
                )}

                {metaTab === "terms" && (
                  <div className="space-y-1">
                    {(metaResults.terms || []).length === 0 ? (
                      <p className="text-xs text-muted-foreground py-4 text-center">No terms found.</p>
                    ) : (
                      (metaResults.terms || []).map((item: any, i: number) => (
                        <div key={i} className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-muted/50">
                          <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                          <span className="font-mono text-xs font-medium">{item.name || item}</span>
                          {item.domain && (
                            <Badge variant="secondary" className="text-[10px]">{item.domain}</Badge>
                          )}
                          {item.status && (
                            <Badge variant="outline" className={`text-[10px] ${statusColor(item.status)}`}>
                              {item.status}
                            </Badge>
                          )}
                        </div>
                      ))
                    )}
                  </div>
                )}

                {metaTab === "tags" && (
                  <div className="space-y-1">
                    {(metaResults.tags || []).length === 0 ? (
                      <p className="text-xs text-muted-foreground py-4 text-center">No tags found.</p>
                    ) : (
                      <div className="flex flex-wrap gap-2 p-2">
                        {(metaResults.tags || []).map((item: any, i: number) => (
                          <Badge key={i} variant="secondary" className="text-xs">
                            <Tags className="h-3 w-3 mr-1" />
                            {item.name || item}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {metaResults.total != null && (
                <p className="text-[10px] text-muted-foreground text-right">
                  {metaResults.total} total result{metaResults.total !== 1 ? "s" : ""}
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
