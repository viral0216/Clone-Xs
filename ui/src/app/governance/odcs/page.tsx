// @ts-nocheck
"use client";
import { useState, useEffect, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { Link, useNavigate } from "react-router-dom";
import {
  FileText, Plus, Play, Trash2, Download, Upload, Search,
  CheckCircle2, XCircle, Loader2, Filter, ArrowRight, FileCode,
  Database, Wand2, ChevronRight, ChevronLeft, Settings,
} from "lucide-react";
import CatalogPicker from "@/components/CatalogPicker";

const STATUS_COLORS: Record<string, string> = {
  active: "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-300",
  draft: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300",
  proposed: "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-300",
  deprecated: "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300",
  retired: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
};

export default function ODCSContractsPage() {
  const navigate = useNavigate();
  const [contracts, setContracts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterDomain, setFilterDomain] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showImport, setShowImport] = useState(false);
  const [yamlContent, setYamlContent] = useState("");
  const [importing, setImporting] = useState(false);
  const [validating, setValidating] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  // Generate from UC wizard state
  const [showGenerate, setShowGenerate] = useState(false);
  const [genStep, setGenStep] = useState(1);
  const [genScope, setGenScope] = useState<"table" | "schema" | "catalog">("table");
  const [genCatalog, setGenCatalog] = useState("");
  const [genSchema, setGenSchema] = useState("");
  const [genTable, setGenTable] = useState("");
  const [genOpts, setGenOpts] = useState({
    include_quality_rules: true, include_dqx_profiling: false,
    include_lineage: true, include_sla: true, include_tags: true,
    include_properties: true, include_masks: true, include_row_filters: true,
    include_history: true,
  });
  const [generating, setGenerating] = useState(false);
  const [genResult, setGenResult] = useState<any>(null);

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (filterDomain) params.set("domain", filterDomain);
      if (filterStatus) params.set("status", filterStatus);
      const qs = params.toString();
      const d = await api.get(`/governance/odcs/contracts${qs ? "?" + qs : ""}`);
      setContracts(Array.isArray(d) ? d : []);
    } catch { setContracts([]); }
    setLoading(false);
  }

  async function importYaml() {
    if (!yamlContent.trim()) return;
    setImporting(true);
    try {
      const result = await api.post("/governance/odcs/import", { yaml_content: yamlContent });
      toast.success(`Contract imported: ${result.name || result.contract_id}`);
      setShowImport(false);
      setYamlContent("");
      load();
    } catch (e: any) { toast.error(e.message); }
    setImporting(false);
  }

  async function handleFileUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    const text = await file.text();
    setYamlContent(text);
    setShowImport(true);
  }

  async function exportYaml(contractId: string) {
    try {
      const headers: Record<string, string> = {};
      const h = sessionStorage.getItem("dbx_host"); if (h) headers["X-Databricks-Host"] = h;
      const tk = sessionStorage.getItem("dbx_token"); if (tk) headers["X-Databricks-Token"] = tk;
      const wh = localStorage.getItem("dbx_warehouse_id"); if (wh) headers["X-Databricks-Warehouse"] = wh;
      const res = await fetch(`/api/governance/odcs/contracts/${contractId}/export`, { headers });
      const text = await res.text();
      const blob = new Blob([text], { type: "text/yaml" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${contractId}.odcs.yaml`;
      a.click();
      URL.revokeObjectURL(url);
      toast.success("YAML exported");
    } catch (e: any) { toast.error(e.message); }
  }

  async function validate(contractId: string) {
    setValidating(contractId);
    try {
      const result = await api.post(`/governance/odcs/contracts/${contractId}/validate`, {});
      if (result.compliant) {
        toast.success("Contract is compliant!");
      } else {
        toast.error(`${result.total_violations} violation(s) found`);
      }
      navigate(`/governance/odcs/validate/${contractId}`);
    } catch (e: any) { toast.error(e.message); }
    setValidating(null);
  }

  async function deleteContract(contractId: string, name: string) {
    if (!confirm(`Delete contract "${name || contractId}"? This cannot be undone.`)) return;
    try {
      const res = await api.delete(`/governance/odcs/contracts/${contractId}`);
      if (res?.error) {
        toast.error(res.error);
      } else {
        toast.success(`Contract "${name || contractId}" deleted`);
        load();
      }
    } catch (e: any) {
      toast.error(`Delete failed: ${e.message || "Unknown error"}`);
    }
  }

  async function runGenerate() {
    setGenerating(true);
    setGenResult(null);
    try {
      let result: any;
      if (genScope === "table") {
        const fqn = genTable ? `${genCatalog}.${genSchema}.${genTable}` : "";
        if (!fqn || fqn.split(".").length !== 3) { toast.error("Select catalog, schema, and table"); setGenerating(false); return; }
        result = await api.post("/governance/odcs/generate", { table_fqn: fqn, ...genOpts });
      } else if (genScope === "schema") {
        if (!genCatalog || !genSchema) { toast.error("Select catalog and schema"); setGenerating(false); return; }
        result = await api.post("/governance/odcs/generate-schema", { catalog: genCatalog, schema_name: genSchema, ...genOpts });
      } else {
        if (!genCatalog) { toast.error("Select a catalog"); setGenerating(false); return; }
        result = await api.post("/governance/odcs/generate-catalog", { catalog: genCatalog, ...genOpts });
      }
      setGenResult(result);
      setGenStep(3);
      toast.success(genScope === "table" ? "Contract generated!" : `${result.count || 0} contract(s) generated`);
    } catch (e: any) { toast.error(e.message); }
    setGenerating(false);
  }

  async function saveGenerated() {
    if (!genResult) return;
    setGenerating(true);
    try {
      if (genScope === "table" && genResult && !genResult.contracts) {
        const saved = await api.post("/governance/odcs/contracts", genResult);
        toast.success(`Saved: ${saved.name || saved.contract_id}`);
        setShowGenerate(false);
        setGenStep(1);
        setGenResult(null);
        load();
        navigate(`/governance/odcs/${saved.contract_id}`);
      } else if (genResult.contracts) {
        let savedCount = 0;
        for (const doc of genResult.contracts) {
          if (doc && !doc.error) {
            try { await api.post("/governance/odcs/contracts", doc); savedCount++; } catch {}
          }
        }
        toast.success(`${savedCount} contract(s) saved`);
        setShowGenerate(false);
        setGenStep(1);
        setGenResult(null);
        load();
      }
    } catch (e: any) { toast.error(e.message); }
    setGenerating(false);
  }

  // Client-side search filter
  const filtered = contracts.filter((c) => {
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      return (
        c.name?.toLowerCase().includes(q) ||
        c.domain?.toLowerCase().includes(q) ||
        c.data_product?.toLowerCase().includes(q) ||
        JSON.stringify(c.table_fqns || []).toLowerCase().includes(q)
      );
    }
    return true;
  });

  // Unique domains for filter
  const domains = [...new Set(contracts.map((c) => c.domain).filter(Boolean))];

  return (
    <div className="space-y-6">
      <PageHeader
        title="ODCS Data Contracts"
        icon={FileCode}
        breadcrumbs={["Governance", "ODCS Contracts"]}
        description="Open Data Contract Standard v3.1.0 — define, validate, and manage data contracts with full YAML import/export."
      />

      {/* Actions bar */}
      <div className="flex items-center gap-3 flex-wrap">
        <Link to="/governance/odcs/new">
          <Button><Plus className="h-4 w-4 mr-2" />New Contract</Button>
        </Link>
        <Button variant="outline" onClick={() => setShowImport(!showImport)}>
          <Upload className="h-4 w-4 mr-2" />Import YAML
        </Button>
        <input ref={fileRef} type="file" accept=".yaml,.yml" className="hidden" onChange={handleFileUpload} />
        <Button variant="outline" onClick={() => fileRef.current?.click()}>
          <FileText className="h-4 w-4 mr-2" />Upload File
        </Button>
        <Button variant="outline" onClick={() => { setShowGenerate(!showGenerate); setGenStep(1); setGenResult(null); }} className="border-purple-300 text-purple-700 hover:bg-purple-50 dark:border-purple-700 dark:text-purple-400 dark:hover:bg-purple-950">
          <Wand2 className="h-4 w-4 mr-2" />Generate from UC
        </Button>

        <div className="ml-auto flex items-center gap-2">
          <div className="relative">
            <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
            <Input placeholder="Search contracts..." className="pl-9 w-60" value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} />
          </div>
          <select value={filterDomain} onChange={(e) => { setFilterDomain(e.target.value); setTimeout(load, 0); }}
            className="border rounded px-3 py-2 text-sm bg-background">
            <option value="">All Domains</option>
            {domains.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
          <select value={filterStatus} onChange={(e) => { setFilterStatus(e.target.value); setTimeout(load, 0); }}
            className="border rounded px-3 py-2 text-sm bg-background">
            <option value="">All Statuses</option>
            {["proposed", "draft", "active", "deprecated", "retired"].map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
      </div>

      {/* Import YAML panel */}
      {showImport && (
        <Card>
          <CardContent className="pt-4 space-y-3">
            <p className="text-sm font-medium">Import ODCS YAML</p>
            <textarea
              className="w-full h-64 border rounded p-3 font-mono text-xs bg-muted/50"
              placeholder="Paste your .odcs.yaml content here..."
              value={yamlContent}
              onChange={(e) => setYamlContent(e.target.value)}
            />
            <div className="flex gap-2">
              <Button onClick={importYaml} disabled={importing || !yamlContent.trim()}>
                {importing ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Upload className="h-4 w-4 mr-2" />}
                Import
              </Button>
              <Button variant="ghost" onClick={() => { setShowImport(false); setYamlContent(""); }}>Cancel</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Generate from UC wizard */}
      {showGenerate && (
        <Card className="border-purple-200 dark:border-purple-800">
          <CardContent className="pt-4 space-y-4">
            <div className="flex items-center gap-2 mb-2">
              <Wand2 className="h-5 w-5 text-purple-600" />
              <p className="text-sm font-medium">Generate Contract from Unity Catalog</p>
              <div className="ml-auto flex items-center gap-1 text-xs text-muted-foreground">
                <span className={genStep >= 1 ? "text-purple-600 font-medium" : ""}>1. Scope</span>
                <ChevronRight className="h-3 w-3" />
                <span className={genStep >= 2 ? "text-purple-600 font-medium" : ""}>2. Options</span>
                <ChevronRight className="h-3 w-3" />
                <span className={genStep >= 3 ? "text-purple-600 font-medium" : ""}>3. Preview</span>
              </div>
            </div>

            {/* Step 1: Scope */}
            {genStep === 1 && (
              <div className="space-y-4">
                <div className="flex gap-3">
                  {(["table", "schema", "catalog"] as const).map((s) => (
                    <button key={s} onClick={() => { setGenScope(s); setGenSchema(""); setGenTable(""); }}
                      className={`px-4 py-2 rounded border text-sm transition-colors ${genScope === s ? "bg-purple-100 border-purple-400 text-purple-800 dark:bg-purple-950 dark:border-purple-600 dark:text-purple-300" : "border-border hover:bg-muted"}`}>
                      <Database className="h-4 w-4 inline mr-1.5" />
                      {s === "table" ? "Single Table" : s === "schema" ? "Entire Schema" : "Entire Catalog"}
                    </button>
                  ))}
                </div>
                <CatalogPicker
                  catalog={genCatalog} schema={genSchema} table={genTable}
                  onCatalogChange={(v) => { setGenCatalog(v); setGenSchema(""); setGenTable(""); }}
                  onSchemaChange={(v) => { setGenSchema(v); setGenTable(""); }}
                  onTableChange={setGenTable}
                  showSchema={genScope !== "catalog"}
                  showTable={genScope === "table"}
                />
                <div className="flex gap-2">
                  <Button onClick={() => setGenStep(2)} disabled={!genCatalog || (genScope !== "catalog" && !genSchema) || (genScope === "table" && !genTable)}>
                    Next <ChevronRight className="h-4 w-4 ml-1" />
                  </Button>
                  <Button variant="ghost" onClick={() => setShowGenerate(false)}>Cancel</Button>
                </div>
              </div>
            )}

            {/* Step 2: Options */}
            {genStep === 2 && (
              <div className="space-y-4">
                <div className="grid grid-cols-3 gap-3">
                  {[
                    { key: "include_quality_rules", label: "Quality Rules", desc: "Auto-generate from column metadata" },
                    { key: "include_dqx_profiling", label: "DQX Profiling", desc: "Data-driven rules (slower, requires DQX)" },
                    { key: "include_lineage", label: "Lineage", desc: "system.access.table_lineage + column_lineage" },
                    { key: "include_sla", label: "SLA Properties", desc: "Freshness and update frequency" },
                    { key: "include_tags", label: "UC Tags", desc: "Table and column tags" },
                    { key: "include_properties", label: "Table Properties", desc: "TBLPROPERTIES" },
                    { key: "include_masks", label: "Column Masks", desc: "Encryption / masking policies" },
                    { key: "include_row_filters", label: "Row Filters", desc: "Security policies → roles" },
                    { key: "include_history", label: "Table History", desc: "Retention and frequency estimate" },
                  ].map((opt) => (
                    <label key={opt.key} className="flex items-start gap-2 p-2 border rounded hover:bg-muted/50 cursor-pointer">
                      <input type="checkbox" checked={genOpts[opt.key as keyof typeof genOpts]}
                        onChange={(e) => setGenOpts({ ...genOpts, [opt.key]: e.target.checked })}
                        className="mt-0.5" />
                      <div>
                        <span className="text-sm font-medium">{opt.label}</span>
                        <p className="text-xs text-muted-foreground">{opt.desc}</p>
                      </div>
                    </label>
                  ))}
                </div>
                {genOpts.include_dqx_profiling && (
                  <div className="p-2 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded text-xs text-blue-800 dark:text-blue-300">
                    DQX Profiling uses data sampling to generate quality rules. This may take longer for large tables.
                  </div>
                )}
                <div className="flex gap-2">
                  <Button variant="outline" onClick={() => setGenStep(1)}>
                    <ChevronLeft className="h-4 w-4 mr-1" /> Back
                  </Button>
                  <Button onClick={runGenerate} disabled={generating}>
                    {generating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Wand2 className="h-4 w-4 mr-2" />}
                    Generate
                  </Button>
                  <Button variant="ghost" onClick={() => setShowGenerate(false)}>Cancel</Button>
                </div>
              </div>
            )}

            {/* Step 3: Preview */}
            {genStep === 3 && genResult && (
              <div className="space-y-4">
                {genScope === "table" && !genResult.contracts ? (
                  <div className="space-y-3">
                    <div className="flex items-center gap-3">
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                      <span className="font-medium">{genResult.name || "Generated Contract"}</span>
                      <Badge variant="outline">{genResult.status}</Badge>
                    </div>
                    <div className="grid grid-cols-4 gap-3 text-sm">
                      <div className="p-2 bg-muted/50 rounded"><span className="text-muted-foreground">Schema objects:</span> <strong>{(genResult.schema || []).length}</strong></div>
                      <div className="p-2 bg-muted/50 rounded"><span className="text-muted-foreground">Columns:</span> <strong>{(genResult.schema?.[0]?.properties || []).length}</strong></div>
                      <div className="p-2 bg-muted/50 rounded"><span className="text-muted-foreground">Quality rules:</span> <strong>{(genResult.schema?.[0]?.quality || []).length + (genResult.schema?.[0]?.properties || []).reduce((s: number, p: any) => s + (p.quality?.length || 0), 0)}</strong></div>
                      <div className="p-2 bg-muted/50 rounded"><span className="text-muted-foreground">SLA props:</span> <strong>{(genResult.slaProperties || []).length}</strong></div>
                    </div>
                    <details className="text-xs" open>
                      <summary className="cursor-pointer text-muted-foreground hover:text-foreground font-medium py-1">ODCS Contract Preview</summary>
                      <pre className="mt-2 p-4 bg-muted/30 border rounded-lg max-h-[500px] overflow-auto font-mono text-[11px] leading-relaxed whitespace-pre-wrap break-words">{JSON.stringify(genResult, null, 2)}</pre>
                    </details>
                  </div>
                ) : genResult.contracts ? (
                  <div className="space-y-3">
                    <div className="flex items-center gap-3">
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                      <span className="font-medium">{genResult.count} contract(s) generated</span>
                    </div>
                    <div className="border rounded max-h-60 overflow-auto">
                      {genResult.contracts.map((c: any, i: number) => (
                        <div key={i} className="flex items-center gap-2 px-3 py-2 border-b last:border-0 text-sm">
                          {c.error ? <XCircle className="h-4 w-4 text-red-500" /> : <CheckCircle2 className="h-4 w-4 text-green-500" />}
                          <span>{c.name || c.table_fqn || `Contract ${i + 1}`}</span>
                          {c.error && <span className="text-xs text-red-500 ml-auto">{c.error}</span>}
                          {!c.error && <span className="text-xs text-muted-foreground ml-auto">{(c.schema?.[0]?.properties || []).length} cols</span>}
                        </div>
                      ))}
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-4 text-muted-foreground">No results</div>
                )}
                <div className="flex gap-2">
                  <Button variant="outline" onClick={() => setGenStep(2)}>
                    <ChevronLeft className="h-4 w-4 mr-1" /> Back
                  </Button>
                  <Button onClick={saveGenerated} disabled={generating}>
                    {generating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Database className="h-4 w-4 mr-2" />}
                    Save {genResult?.contracts ? `All (${genResult.contracts.filter((c: any) => !c.error).length})` : "Contract"}
                  </Button>
                  <Button variant="ghost" onClick={() => { setShowGenerate(false); setGenStep(1); setGenResult(null); }}>Close</Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Contracts table */}
      {loading ? (
        <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-16 space-y-3">
          <FileCode className="h-12 w-12 mx-auto text-muted-foreground/50" />
          <p className="text-muted-foreground">No ODCS contracts found.</p>
          <p className="text-xs text-muted-foreground">Create a new contract or import from YAML to get started.</p>
        </div>
      ) : (
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="text-left px-4 py-3 font-medium">Name</th>
                <th className="text-left px-4 py-3 font-medium">Version</th>
                <th className="text-left px-4 py-3 font-medium">Status</th>
                <th className="text-left px-4 py-3 font-medium">Domain</th>
                <th className="text-left px-4 py-3 font-medium">Tables</th>
                <th className="text-left px-4 py-3 font-medium">Updated</th>
                <th className="text-right px-4 py-3 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((c) => (
                <tr key={c.contract_id} className="border-t hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3">
                    <Link to={`/governance/odcs/${c.contract_id}`} className="font-medium hover:text-blue-600 transition-colors">
                      {c.name || "Untitled"}
                    </Link>
                    {c.data_product && <span className="text-xs text-muted-foreground ml-2">({c.data_product})</span>}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs">{c.version}</td>
                  <td className="px-4 py-3">
                    <Badge className={STATUS_COLORS[c.status] || STATUS_COLORS.draft}>{c.status}</Badge>
                  </td>
                  <td className="px-4 py-3 text-muted-foreground">{c.domain || "-"}</td>
                  <td className="px-4 py-3">
                    <div className="flex flex-wrap gap-1">
                      {(c.table_fqns || []).slice(0, 3).map((t: string, i: number) => (
                        <Badge key={i} variant="outline" className="font-mono text-xs">{t.split(".").pop()}</Badge>
                      ))}
                      {(c.table_fqns || []).length > 3 && <Badge variant="outline" className="text-xs">+{c.table_fqns.length - 3}</Badge>}
                    </div>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{c.updated_at?.slice(0, 16)}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1 justify-end">
                      <Button variant="ghost" size="sm" title="Validate" onClick={() => validate(c.contract_id)} disabled={validating === c.contract_id}>
                        {validating === c.contract_id ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                      </Button>
                      <Button variant="ghost" size="sm" title="Export YAML" onClick={() => exportYaml(c.contract_id)}>
                        <Download className="h-4 w-4" />
                      </Button>
                      <Button variant="ghost" size="sm" title="Delete" onClick={() => deleteContract(c.contract_id, c.name)}>
                        <Trash2 className="h-4 w-4 text-red-500" />
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
