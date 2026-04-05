// @ts-nocheck
import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { Link } from "react-router-dom";
import {
  FolderTree, ChevronRight, ChevronDown, Loader2, Database,
  Table2, FolderOpen, Folder, Search, Award, BookOpen,
  ExternalLink, RefreshCw,
} from "lucide-react";

/* ── Types ────────────────────────────────────────────── */

interface TableEntry {
  table_name: string;
  table_type?: string;
}

interface TreeNode {
  catalog: string;
  expanded: boolean;
  loading: boolean;
  schemas: SchemaNode[];
}

interface SchemaNode {
  schema_name: string;
  expanded: boolean;
  loading: boolean;
  tables: TableEntry[];
}

/* ── Component ────────────────────────────────────────── */

export default function CatalogBrowserPage() {
  const [catalogs, setCatalogs] = useState<TreeNode[]>([]);
  const [loadingCatalogs, setLoadingCatalogs] = useState(true);
  const [searchFilter, setSearchFilter] = useState("");

  // Selected table info
  const [selected, setSelected] = useState<{
    catalog: string;
    schema: string;
    table: string;
    table_type?: string;
  } | null>(null);

  // Certification & glossary data for detail panel
  const [certifications, setCertifications] = useState<any[]>([]);
  const [glossary, setGlossary] = useState<any[]>([]);

  /* ── Load catalogs ─────────────────────────────────── */

  useEffect(() => {
    loadCatalogs();
    loadGovernanceData();
  }, []);

  async function loadCatalogs() {
    setLoadingCatalogs(true);
    try {
      const data = await api.get<any>("/catalogs");
      const names: string[] = Array.isArray(data)
        ? data.map((d: any) => (typeof d === "string" ? d : d.catalog_name))
        : [];
      setCatalogs(
        names.map((name) => ({
          catalog: name,
          expanded: false,
          loading: false,
          schemas: [],
        }))
      );
    } catch (e: any) {
      toast.error(e.message || "Failed to load catalogs.");
    }
    setLoadingCatalogs(false);
  }

  async function loadGovernanceData() {
    try {
      const [certs, terms] = await Promise.all([
        api.get<any>("/governance/certifications").catch(() => []),
        api.get<any>("/governance/glossary").catch(() => []),
      ]);
      setCertifications(Array.isArray(certs) ? certs : []);
      setGlossary(Array.isArray(terms) ? terms : []);
    } catch {}
  }

  /* ── Expand / collapse catalog ─────────────────────── */

  async function toggleCatalog(catalogName: string) {
    setCatalogs((prev) =>
      prev.map((c) => {
        if (c.catalog !== catalogName) return c;
        if (c.expanded) return { ...c, expanded: false };
        if (c.schemas.length > 0) return { ...c, expanded: true };
        return { ...c, expanded: true, loading: true };
      })
    );

    const node = catalogs.find((c) => c.catalog === catalogName);
    if (node && node.schemas.length === 0) {
      try {
        const data = await api.get<any>(`/catalogs/${encodeURIComponent(catalogName)}/schemas`);
        const names: string[] = Array.isArray(data)
          ? data.map((d: any) => (typeof d === "string" ? d : d.schema_name))
          : [];
        setCatalogs((prev) =>
          prev.map((c) =>
            c.catalog === catalogName
              ? {
                  ...c,
                  loading: false,
                  schemas: names.map((s) => ({
                    schema_name: s,
                    expanded: false,
                    loading: false,
                    tables: [],
                  })),
                }
              : c
          )
        );
      } catch (e: any) {
        toast.error(e.message || "Failed to load schemas.");
        setCatalogs((prev) =>
          prev.map((c) =>
            c.catalog === catalogName ? { ...c, loading: false } : c
          )
        );
      }
    }
  }

  /* ── Expand / collapse schema ──────────────────────── */

  async function toggleSchema(catalogName: string, schemaName: string) {
    setCatalogs((prev) =>
      prev.map((c) => {
        if (c.catalog !== catalogName) return c;
        return {
          ...c,
          schemas: c.schemas.map((s) => {
            if (s.schema_name !== schemaName) return s;
            if (s.expanded) return { ...s, expanded: false };
            if (s.tables.length > 0) return { ...s, expanded: true };
            return { ...s, expanded: true, loading: true };
          }),
        };
      })
    );

    const catNode = catalogs.find((c) => c.catalog === catalogName);
    const schNode = catNode?.schemas.find((s) => s.schema_name === schemaName);
    if (schNode && schNode.tables.length === 0) {
      try {
        const data = await api.get<any>(
          `/catalogs/${encodeURIComponent(catalogName)}/${encodeURIComponent(schemaName)}/tables`
        );
        const tables: TableEntry[] = Array.isArray(data)
          ? data.map((d: any) =>
              typeof d === "string"
                ? { table_name: d }
                : { table_name: d.table_name, table_type: d.table_type }
            )
          : [];
        setCatalogs((prev) =>
          prev.map((c) => {
            if (c.catalog !== catalogName) return c;
            return {
              ...c,
              schemas: c.schemas.map((s) =>
                s.schema_name === schemaName
                  ? { ...s, loading: false, tables }
                  : s
              ),
            };
          })
        );
      } catch (e: any) {
        toast.error(e.message || "Failed to load tables.");
        setCatalogs((prev) =>
          prev.map((c) => {
            if (c.catalog !== catalogName) return c;
            return {
              ...c,
              schemas: c.schemas.map((s) =>
                s.schema_name === schemaName ? { ...s, loading: false } : s
              ),
            };
          })
        );
      }
    }
  }

  /* ── Select a table ────────────────────────────────── */

  function selectTable(
    catalog: string,
    schema: string,
    table: string,
    table_type?: string
  ) {
    setSelected({ catalog, schema, table, table_type });
  }

  /* ── Certification status for the selected table ──── */

  const selectedFqn = selected
    ? `${selected.catalog}.${selected.schema}.${selected.table}`
    : "";

  const tableCert = useMemo(
    () => certifications.find((c) => c.table_fqn === selectedFqn),
    [certifications, selectedFqn]
  );

  const tableTerms = useMemo(
    () => glossary.filter((t) => t.table_fqn === selectedFqn || t.related_table === selectedFqn),
    [glossary, selectedFqn]
  );

  /* ── Filter tree by search ─────────────────────────── */

  const filteredCatalogs = useMemo(() => {
    if (!searchFilter.trim()) return catalogs;
    const q = searchFilter.toLowerCase();
    return catalogs.filter((c) => {
      if (c.catalog.toLowerCase().includes(q)) return true;
      return c.schemas.some(
        (s) =>
          s.schema_name.toLowerCase().includes(q) ||
          s.tables.some((t) => t.table_name.toLowerCase().includes(q))
      );
    });
  }, [catalogs, searchFilter]);

  /* ── Render ────────────────────────────────────────── */

  return (
    <div className="space-y-4">
      <PageHeader
        title="Catalog Browser"
        icon={FolderTree}
        breadcrumbs={["Data Quality", "Discovery", "Catalog Browser"]}
        description="Browse catalogs, schemas, and tables. View metadata, certifications, and linked glossary terms."
      />

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        {/* ── Left panel: tree navigator ─────────────── */}
        <div className="lg:col-span-4 xl:col-span-3">
          <Card className="bg-card border-border h-full">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm flex items-center gap-2">
                  <Database className="h-4 w-4" /> Catalogs
                </CardTitle>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7"
                  onClick={loadCatalogs}
                  disabled={loadingCatalogs}
                >
                  <RefreshCw
                    className={`h-3.5 w-3.5 ${loadingCatalogs ? "animate-spin" : ""}`}
                  />
                </Button>
              </div>
              <div className="relative mt-2">
                <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder="Filter catalogs, schemas, tables..."
                  value={searchFilter}
                  onChange={(e) => setSearchFilter(e.target.value)}
                  className="pl-8 h-8 text-xs"
                />
              </div>
            </CardHeader>
            <CardContent className="pt-0 pb-3 max-h-[600px] overflow-y-auto">
              {loadingCatalogs ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
                  <Loader2 className="h-4 w-4 animate-spin" /> Loading catalogs...
                </div>
              ) : filteredCatalogs.length === 0 ? (
                <p className="text-xs text-muted-foreground text-center py-8">
                  {searchFilter ? "No matches found." : "No catalogs found."}
                </p>
              ) : (
                <div className="space-y-0.5">
                  {filteredCatalogs.map((cat) => (
                    <div key={cat.catalog}>
                      {/* Catalog row */}
                      <button
                        onClick={() => toggleCatalog(cat.catalog)}
                        className="flex items-center gap-1.5 w-full text-left px-2 py-1.5 rounded hover:bg-muted/50 transition-colors group"
                      >
                        {cat.expanded ? (
                          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                        )}
                        <Database className="h-3.5 w-3.5 text-[#E8453C] shrink-0" />
                        <span className="text-xs font-medium text-foreground truncate">
                          {cat.catalog}
                        </span>
                        {cat.loading && (
                          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground ml-auto" />
                        )}
                      </button>

                      {/* Schemas */}
                      {cat.expanded && (
                        <div className="ml-4">
                          {cat.schemas.map((sch) => (
                            <div key={sch.schema_name}>
                              {/* Schema row */}
                              <button
                                onClick={() =>
                                  toggleSchema(cat.catalog, sch.schema_name)
                                }
                                className="flex items-center gap-1.5 w-full text-left px-2 py-1 rounded hover:bg-muted/50 transition-colors"
                              >
                                {sch.expanded ? (
                                  <ChevronDown className="h-3 w-3 text-muted-foreground shrink-0" />
                                ) : (
                                  <ChevronRight className="h-3 w-3 text-muted-foreground shrink-0" />
                                )}
                                {sch.expanded ? (
                                  <FolderOpen className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                                ) : (
                                  <Folder className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                                )}
                                <span className="text-xs text-foreground truncate">
                                  {sch.schema_name}
                                </span>
                                {sch.loading && (
                                  <Loader2 className="h-3 w-3 animate-spin text-muted-foreground ml-auto" />
                                )}
                              </button>

                              {/* Tables */}
                              {sch.expanded && (
                                <div className="ml-5">
                                  {sch.tables.length === 0 && !sch.loading ? (
                                    <p className="text-[10px] text-muted-foreground px-2 py-1">
                                      No tables found.
                                    </p>
                                  ) : (
                                    sch.tables.map((tbl) => {
                                      const isActive =
                                        selected?.catalog === cat.catalog &&
                                        selected?.schema === sch.schema_name &&
                                        selected?.table === tbl.table_name;
                                      return (
                                        <button
                                          key={tbl.table_name}
                                          onClick={() =>
                                            selectTable(
                                              cat.catalog,
                                              sch.schema_name,
                                              tbl.table_name,
                                              tbl.table_type
                                            )
                                          }
                                          className={`flex items-center gap-1.5 w-full text-left px-2 py-1 rounded transition-colors ${
                                            isActive
                                              ? "bg-[#E8453C]/10 text-[#E8453C]"
                                              : "hover:bg-muted/50 text-foreground"
                                          }`}
                                        >
                                          <Table2 className="h-3 w-3 shrink-0" />
                                          <span className="text-xs truncate">
                                            {tbl.table_name}
                                          </span>
                                        </button>
                                      );
                                    })
                                  )}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* ── Right panel: table details ─────────────── */}
        <div className="lg:col-span-8 xl:col-span-9">
          {!selected ? (
            <Card className="bg-card border-border h-full">
              <CardContent className="py-20 text-center">
                <FolderTree className="h-10 w-10 mx-auto text-muted-foreground mb-3" />
                <p className="text-sm text-muted-foreground">
                  Select a table from the catalog tree to view its details.
                </p>
              </CardContent>
            </Card>
          ) : (
            <div className="space-y-4">
              {/* Table header */}
              <Card className="bg-card border-border">
                <CardContent className="pt-6 pb-5">
                  <div className="flex items-start justify-between flex-wrap gap-3">
                    <div>
                      <p className="text-xs text-muted-foreground font-mono mb-1">
                        {selected.catalog}.{selected.schema}
                      </p>
                      <h2 className="text-xl font-bold text-foreground flex items-center gap-2">
                        <Table2 className="h-5 w-5 text-[#E8453C]" />
                        {selected.table}
                      </h2>
                      <div className="flex items-center gap-2 mt-2">
                        {selected.table_type && (
                          <Badge variant="outline" className="text-[10px]">
                            {selected.table_type}
                          </Badge>
                        )}
                        {tableCert && (
                          <Badge
                            variant="outline"
                            className={`text-[10px] gap-1 ${
                              tableCert.status === "certified"
                                ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400"
                                : tableCert.status === "proposed"
                                ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
                                : "text-gray-500 bg-gray-50 border-gray-200 dark:bg-gray-800/30 dark:text-gray-400"
                            }`}
                          >
                            <Award className="h-3 w-3" />
                            {tableCert.status}
                          </Badge>
                        )}
                        {tableTerms.length > 0 && (
                          <Badge
                            variant="outline"
                            className="text-[10px] gap-1 text-purple-600 bg-purple-50 border-purple-200 dark:bg-purple-950/30 dark:text-purple-400"
                          >
                            <BookOpen className="h-3 w-3" />
                            {tableTerms.length} glossary term{tableTerms.length !== 1 ? "s" : ""}
                          </Badge>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Link to="/data-quality/profiling">
                        <Button variant="outline" size="sm" className="text-xs gap-1.5">
                          <ExternalLink className="h-3 w-3" /> Run Profile
                        </Button>
                      </Link>
                      <Link to="/data-quality/scorecard">
                        <Button variant="outline" size="sm" className="text-xs gap-1.5">
                          <ExternalLink className="h-3 w-3" /> View Scorecard
                        </Button>
                      </Link>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Metadata card */}
              <Card className="bg-card border-border">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">Metadata</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                        Catalog
                      </p>
                      <p className="text-sm font-mono text-foreground">
                        {selected.catalog}
                      </p>
                    </div>
                    <div>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                        Schema
                      </p>
                      <p className="text-sm font-mono text-foreground">
                        {selected.schema}
                      </p>
                    </div>
                    <div>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                        Table
                      </p>
                      <p className="text-sm font-mono text-foreground">
                        {selected.table}
                      </p>
                    </div>
                    <div>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                        Type
                      </p>
                      <p className="text-sm font-mono text-foreground">
                        {selected.table_type || "Unknown"}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Certification details */}
              {tableCert && (
                <Card className="bg-card border-border">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Award className="h-4 w-4" /> Certification
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div>
                        <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                          Status
                        </p>
                        <Badge
                          variant="outline"
                          className={`text-[10px] capitalize ${
                            tableCert.status === "certified"
                              ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400"
                              : "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
                          }`}
                        >
                          {tableCert.status}
                        </Badge>
                      </div>
                      <div>
                        <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                          Certified By
                        </p>
                        <p className="text-sm text-foreground">
                          {tableCert.certified_by || "Pending"}
                        </p>
                      </div>
                      <div>
                        <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                          Review Frequency
                        </p>
                        <p className="text-sm text-foreground capitalize">
                          {tableCert.review_frequency || "N/A"}
                        </p>
                      </div>
                      <div>
                        <p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                          Notes
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {tableCert.notes || "None"}
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Linked glossary terms */}
              {tableTerms.length > 0 && (
                <Card className="bg-card border-border">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <BookOpen className="h-4 w-4" /> Linked Glossary Terms
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {tableTerms.map((term: any, i: number) => (
                        <div
                          key={term.term_id || i}
                          className="flex items-center justify-between p-2 rounded bg-muted/30"
                        >
                          <div>
                            <p className="text-sm font-medium text-foreground">
                              {term.term || term.name}
                            </p>
                            <p className="text-xs text-muted-foreground">
                              {term.definition || term.description || "No definition"}
                            </p>
                          </div>
                          {term.owner && (
                            <Badge variant="outline" className="text-[10px]">
                              {term.owner}
                            </Badge>
                          )}
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
