// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { useParams, useNavigate } from "react-router-dom";
import {
  FileCode, Save, Play, Download, Plus, Trash2, Loader2,
  Database, Shield, Users, Lock, Clock, Server, DollarSign,
  MessageSquare, Link2, Settings, Code, ChevronDown, ChevronRight,
} from "lucide-react";

const TABS = [
  { key: "fundamentals", label: "Fundamentals", icon: FileCode },
  { key: "schema", label: "Schema", icon: Database },
  { key: "quality", label: "Quality", icon: Shield },
  { key: "sla", label: "SLA", icon: Clock },
  { key: "team", label: "Team", icon: Users },
  { key: "roles", label: "Roles", icon: Lock },
  { key: "support", label: "Support", icon: MessageSquare },
  { key: "servers", label: "Servers", icon: Server },
  { key: "pricing", label: "Pricing", icon: DollarSign },
  { key: "custom", label: "Custom Props", icon: Settings },
  { key: "yaml", label: "YAML", icon: Code },
];

const EMPTY_CONTRACT = {
  apiVersion: "v3.1.0", kind: "DataContract", name: "", version: "1.0.0", status: "draft",
  tenant: "", domain: "", dataProduct: "", tags: [],
  description: { purpose: "", limitations: "", usage: "" },
  schema: [], quality: [], slaProperties: [], servers: [], support: [],
  team: { name: "", members: [] }, roles: [], price: null,
  authoritativeDefinitions: [], customProperties: [],
};

export default function ODCSContractDetail() {
  const { contractId } = useParams();
  const navigate = useNavigate();
  const isNew = !contractId || contractId === "new";
  const [tab, setTab] = useState("fundamentals");
  const [doc, setDoc] = useState<any>({ ...EMPTY_CONTRACT });
  const [loading, setLoading] = useState(!isNew);
  const [saving, setSaving] = useState(false);
  const [yamlText, setYamlText] = useState("");

  useEffect(() => { if (!isNew) loadContract(); }, [contractId]);

  async function loadContract() {
    setLoading(true);
    try {
      const d = await api.get(`/governance/odcs/contracts/${contractId}`);
      if (d && !d.error) { setDoc(d); }
      else toast.error(d?.error || "Contract not found");
    } catch (e: any) { toast.error(e.message); }
    setLoading(false);
  }

  async function save() {
    if (!doc.name?.trim()) { toast.error("Contract name is required"); return; }
    setSaving(true);
    try {
      if (isNew) {
        const result = await api.post("/governance/odcs/contracts", doc);
        if (result.error) { toast.error(result.error); setSaving(false); return; }
        toast.success("Contract created");
        navigate(`/governance/odcs/${result.contract_id}`);
      } else {
        const result = await api.put(`/governance/odcs/contracts/${contractId}`, doc);
        if (result?.error) { toast.error(result.error); setSaving(false); return; }
        toast.success("Contract saved");
      }
    } catch (e: any) { toast.error(e.message); }
    setSaving(false);
  }

  async function loadYaml() {
    if (!contractId) return;
    try {
      const headers: Record<string, string> = {};
      const sid = localStorage.getItem("clxs_session_id"); if (sid) headers["X-Clone-Session"] = sid;
      const h = localStorage.getItem("dbx_host"); if (h) headers["X-Databricks-Host"] = h;
      const tk = localStorage.getItem("dbx_token"); if (tk) headers["X-Databricks-Token"] = tk;
      const wh = localStorage.getItem("dbx_warehouse_id"); if (wh) headers["X-Databricks-Warehouse"] = wh;
      const res = await fetch(`/api/governance/odcs/contracts/${contractId}/export`, { headers });
      setYamlText(await res.text());
    } catch { setYamlText("# Error loading YAML"); }
  }

  async function autoDetectSchema(objIndex: number) {
    const obj = doc.schema?.[objIndex];
    let fqn = obj?.physicalName || obj?.name || "";
    // Resolve FQN from server config if not fully qualified
    if (fqn && fqn.split(".").length < 3) {
      const srv = (doc.servers || []).find((s: any) => s.type === "databricks");
      if (srv?.catalog && srv?.schema) fqn = `${srv.catalog}.${srv.schema}.${fqn}`;
    }
    if (!fqn || fqn.split(".").length < 3) {
      toast.error("Enter a fully qualified table name (catalog.schema.table) or configure a Databricks server first");
      return;
    }
    toast.info(`Auto-detecting schema for ${fqn}...`);
    try {
      const generated = await api.post("/governance/odcs/generate", {
        table_fqn: fqn, include_quality_rules: true, include_lineage: true,
        include_sla: false, include_tags: true, include_masks: true,
      });
      if (generated.error) { toast.error(generated.error); return; }
      const genObj = generated.schema?.[0];
      if (genObj) {
        upd(`schema.${objIndex}.properties`, genObj.properties || []);
        upd(`schema.${objIndex}.physicalType`, genObj.physicalType || "table");
        upd(`schema.${objIndex}.businessName`, genObj.businessName || "");
        upd(`schema.${objIndex}.description`, genObj.description || "");
        if (genObj.relationships?.length) upd(`schema.${objIndex}.relationships`, genObj.relationships);
        if (genObj.quality?.length) upd(`schema.${objIndex}.quality`, genObj.quality);
        if (genObj.tags?.length) upd(`schema.${objIndex}.tags`, genObj.tags);
        toast.success(`Detected ${(genObj.properties || []).length} columns from ${fqn}`);
      } else { toast.error("No schema data returned"); }
    } catch (e: any) { toast.error(e.message); }
  }

  async function importDQ() {
    if (!contractId) return;
    try {
      const rules = await api.post(`/governance/odcs/contracts/${contractId}/map-dq`, {});
      if (Array.isArray(rules) && rules.length > 0) {
        setDoc((d: any) => ({ ...d, quality: [...(d.quality || []), ...rules] }));
        toast.success(`Imported ${rules.length} DQ rules`);
      } else toast.info("No matching DQ rules found");
    } catch (e: any) { toast.error(e.message); }
  }

  async function importSLA() {
    if (!contractId) return;
    try {
      const props = await api.post(`/governance/odcs/contracts/${contractId}/map-sla`, {});
      if (Array.isArray(props) && props.length > 0) {
        setDoc((d: any) => ({ ...d, slaProperties: [...(d.slaProperties || []), ...props] }));
        toast.success(`Imported ${props.length} SLA properties`);
      } else toast.info("No matching SLA rules found");
    } catch (e: any) { toast.error(e.message); }
  }

  async function prefillServer() {
    try {
      const srv = await api.get("/governance/odcs/prefill");
      setDoc((d: any) => ({ ...d, servers: [...(d.servers || []), srv] }));
      toast.success("Server config added from workspace");
    } catch (e: any) { toast.error(e.message); }
  }

  // Helper to update nested doc field
  function upd(path: string, value: any) {
    setDoc((d: any) => {
      const copy = JSON.parse(JSON.stringify(d));
      const keys = path.split(".");
      let obj = copy;
      for (let i = 0; i < keys.length - 1; i++) {
        if (obj[keys[i]] === undefined) obj[keys[i]] = isNaN(+keys[i + 1]) ? {} : [];
        obj = obj[keys[i]];
      }
      obj[keys[keys.length - 1]] = value;
      return copy;
    });
  }

  function addToArray(path: string, item: any) {
    setDoc((d: any) => {
      const copy = JSON.parse(JSON.stringify(d));
      const keys = path.split(".");
      let obj = copy;
      for (const k of keys) obj = obj[k];
      if (!Array.isArray(obj)) return copy;
      obj.push(item);
      return copy;
    });
  }

  function removeFromArray(path: string, index: number) {
    setDoc((d: any) => {
      const copy = JSON.parse(JSON.stringify(d));
      const keys = path.split(".");
      let obj = copy;
      for (const k of keys) obj = obj[k];
      if (Array.isArray(obj)) obj.splice(index, 1);
      return copy;
    });
  }

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="h-6 w-6 animate-spin" /></div>;

  return (
    <div className="space-y-4">
      <PageHeader
        title={isNew ? "New ODCS Contract" : doc.name || "Untitled Contract"}
        icon={FileCode}
        breadcrumbs={["Governance", "ODCS Contracts", isNew ? "New" : doc.name || contractId]}
        description={isNew ? "Create a new ODCS v3.1.0 data contract" : `v${doc.version} — ${doc.status}`}
      />

      {/* Top action bar */}
      <div className="flex items-center gap-2 flex-wrap">
        <Button onClick={save} disabled={saving || !doc.name?.trim()}>
          {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
          {isNew ? "Create" : "Save"}
        </Button>
        {!isNew && (
          <>
            <Button variant="outline" onClick={() => navigate(`/governance/odcs/validate/${contractId}`)}>
              <Play className="h-4 w-4 mr-2" />Validate
            </Button>
            <Button variant="outline" onClick={() => { setTab("yaml"); loadYaml(); }}>
              <Download className="h-4 w-4 mr-2" />YAML
            </Button>
          </>
        )}
        <Badge className="ml-auto font-mono text-xs">ODCS v3.1.0</Badge>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 flex-wrap border-b pb-1">
        {TABS.map((t) => (
          <button key={t.key} onClick={() => { setTab(t.key); if (t.key === "yaml" && !isNew) loadYaml(); }}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-t transition-colors ${tab === t.key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground hover:bg-muted"}`}>
            <t.icon className="h-3.5 w-3.5" />{t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="min-h-[400px]">
        {tab === "fundamentals" && <FundamentalsTab doc={doc} upd={upd} />}
        {tab === "schema" && <SchemaTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} onAutoDetect={autoDetectSchema} />}
        {tab === "quality" && <QualityTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} onImportDQ={importDQ} />}
        {tab === "sla" && <SLATab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} onImportSLA={importSLA} />}
        {tab === "team" && <TeamTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} />}
        {tab === "roles" && <RolesTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} />}
        {tab === "support" && <SupportTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} />}
        {tab === "servers" && <ServersTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} onPrefill={prefillServer} />}
        {tab === "pricing" && <PricingTab doc={doc} upd={upd} />}
        {tab === "custom" && <CustomPropsTab doc={doc} upd={upd} addToArray={addToArray} removeFromArray={removeFromArray} />}
        {tab === "yaml" && <YAMLTab yamlText={yamlText} setYamlText={setYamlText} contractId={contractId} />}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Fundamentals Tab
// ---------------------------------------------------------------------------
function FundamentalsTab({ doc, upd }: any) {
  return (
    <div className="space-y-4">
      <Card><CardContent className="pt-4 space-y-3">
        <h2 className="text-sm font-medium" style={{ fontSize: '16px' }}>Identity</h2>
        <div className="grid grid-cols-3 gap-3">
          <div><label className="text-xs text-muted-foreground">Name *</label>
            <Input value={doc.name || ""} onChange={(e) => upd("name", e.target.value)} placeholder="Contract name" /></div>
          <div><label className="text-xs text-muted-foreground">Version *</label>
            <Input value={doc.version || "1.0.0"} onChange={(e) => upd("version", e.target.value)} placeholder="1.0.0" /></div>
          <div><label className="text-xs text-muted-foreground">Status *</label>
            <select value={doc.status || "draft"} onChange={(e) => upd("status", e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
              {["proposed", "draft", "active", "deprecated", "retired"].map((s) => <option key={s} value={s}>{s}</option>)}
            </select></div>
        </div>
        <div className="grid grid-cols-3 gap-3">
          <div><label className="text-xs text-muted-foreground">Domain</label>
            <Input value={doc.domain || ""} onChange={(e) => upd("domain", e.target.value)} placeholder="e.g. sales, finance" /></div>
          <div><label className="text-xs text-muted-foreground">Data Product</label>
            <Input value={doc.dataProduct || ""} onChange={(e) => upd("dataProduct", e.target.value)} placeholder="Product name" /></div>
          <div><label className="text-xs text-muted-foreground">Tenant</label>
            <Input value={doc.tenant || ""} onChange={(e) => upd("tenant", e.target.value)} placeholder="Organization" /></div>
        </div>
        <div><label className="text-xs text-muted-foreground">Tags (comma-separated)</label>
          <Input value={(doc.tags || []).join(", ")} onChange={(e) => upd("tags", e.target.value.split(",").map((t: string) => t.trim()).filter(Boolean))} placeholder="finance, sensitive" /></div>
      </CardContent></Card>

      <Card><CardContent className="pt-4 space-y-3">
        <h2 className="text-sm font-medium" style={{ fontSize: '16px' }}>Description</h2>
        <div><label className="text-xs text-muted-foreground">Purpose</label>
          <textarea className="w-full border rounded p-2 text-sm bg-background min-h-[60px]"
            value={doc.description?.purpose || ""} onChange={(e) => upd("description.purpose", e.target.value)} placeholder="What is this data used for?" /></div>
        <div className="grid grid-cols-2 gap-3">
          <div><label className="text-xs text-muted-foreground">Limitations</label>
            <textarea className="w-full border rounded p-2 text-sm bg-background min-h-[60px]"
              value={doc.description?.limitations || ""} onChange={(e) => upd("description.limitations", e.target.value)} /></div>
          <div><label className="text-xs text-muted-foreground">Usage</label>
            <textarea className="w-full border rounded p-2 text-sm bg-background min-h-[60px]"
              value={doc.description?.usage || ""} onChange={(e) => upd("description.usage", e.target.value)} /></div>
        </div>
      </CardContent></Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Schema Tab
// ---------------------------------------------------------------------------
function SchemaTab({ doc, upd, addToArray, removeFromArray, onAutoDetect }: any) {
  const [expanded, setExpanded] = useState<number | null>(0);
  const schema = doc.schema || [];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => addToArray("schema", { name: "", physicalName: "", physicalType: "table", businessName: "", description: "", properties: [], quality: [], tags: [], relationships: [], customProperties: [] })}>
          <Plus className="h-4 w-4 mr-1" />Add Object
        </Button>
      </div>

      {schema.map((obj: any, i: number) => (
        <Card key={i}>
          <CardHeader className="py-3 cursor-pointer" onClick={() => setExpanded(expanded === i ? null : i)}>
            <div className="flex items-center gap-2">
              {expanded === i ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
              <Database className="h-4 w-4 text-[#E8453C]" />
              <span className="font-medium text-sm">{obj.name || `Object ${i + 1}`}</span>
              <Badge variant="outline" className="text-xs">{obj.physicalType || "table"}</Badge>
              <span className="text-xs text-muted-foreground ml-auto">{(obj.properties || []).length} columns</span>
              <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); removeFromArray("schema", i); }}><Trash2 className="h-3.5 w-3.5 text-red-500" /></Button>
            </div>
          </CardHeader>
          {expanded === i && (
            <CardContent className="space-y-4 pt-0">
              <div className="grid grid-cols-4 gap-3">
                <div><label className="text-xs text-muted-foreground">Name *</label>
                  <Input value={obj.name || ""} onChange={(e) => upd(`schema.${i}.name`, e.target.value)} /></div>
                <div><label className="text-xs text-muted-foreground">Physical Name</label>
                  <Input value={obj.physicalName || ""} onChange={(e) => upd(`schema.${i}.physicalName`, e.target.value)} placeholder="table_name" /></div>
                <div><label className="text-xs text-muted-foreground">Business Name</label>
                  <Input value={obj.businessName || ""} onChange={(e) => upd(`schema.${i}.businessName`, e.target.value)} /></div>
                <div><label className="text-xs text-muted-foreground">Type</label>
                  <select value={obj.physicalType || "table"} onChange={(e) => upd(`schema.${i}.physicalType`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                    {["table", "view", "topic", "file"].map((t) => <option key={t}>{t}</option>)}
                  </select></div>
              </div>
              <div><label className="text-xs text-muted-foreground">Description</label>
                <Input value={obj.description || ""} onChange={(e) => upd(`schema.${i}.description`, e.target.value)} /></div>

              {/* Properties (columns) */}
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <h3 className="text-xs font-medium" style={{ fontSize: '14px' }}>Properties (Columns)</h3>
                  <Button size="sm" variant="outline" className="h-7 text-xs" onClick={() => addToArray(`schema.${i}.properties`, { name: "", physicalName: "", logicalType: "string", physicalType: "", required: false, unique: false, primaryKey: false, description: "", classification: "", criticalDataElement: false })}>
                    <Plus className="h-3 w-3 mr-1" />Column
                  </Button>
                  <Button size="sm" variant="outline" className="h-7 text-xs" onClick={() => onAutoDetect(i)}>
                    <Database className="h-3 w-3 mr-1" />Auto-detect
                  </Button>
                </div>
                {(obj.properties || []).length > 0 && (
                  <div className="border rounded overflow-hidden">
                    <table className="w-full text-xs">
                      <thead className="bg-muted/50">
                        <tr>
                          <th className="text-left px-2 py-1.5">Name</th>
                          <th className="text-left px-2 py-1.5">Logical</th>
                          <th className="text-left px-2 py-1.5">Physical</th>
                          <th className="text-center px-2 py-1.5">PK</th>
                          <th className="text-center px-2 py-1.5">Req</th>
                          <th className="text-center px-2 py-1.5">Unique</th>
                          <th className="text-left px-2 py-1.5">Classification</th>
                          <th className="px-2 py-1.5"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {(obj.properties || []).map((prop: any, j: number) => (
                          <tr key={j} className="border-t">
                            <td className="px-2 py-1"><Input className="h-7 text-xs" value={prop.name || ""} onChange={(e) => upd(`schema.${i}.properties.${j}.name`, e.target.value)} /></td>
                            <td className="px-2 py-1">
                              <select value={prop.logicalType || "string"} onChange={(e) => upd(`schema.${i}.properties.${j}.logicalType`, e.target.value)} className="h-7 border rounded px-1 text-xs bg-background w-full">
                                {["string", "integer", "number", "boolean", "date", "timestamp", "time", "object", "array"].map((t) => <option key={t}>{t}</option>)}
                              </select>
                            </td>
                            <td className="px-2 py-1"><Input className="h-7 text-xs" value={prop.physicalType || ""} onChange={(e) => upd(`schema.${i}.properties.${j}.physicalType`, e.target.value)} placeholder="VARCHAR(255)" /></td>
                            <td className="px-2 py-1 text-center"><input type="checkbox" checked={prop.primaryKey || false} onChange={(e) => upd(`schema.${i}.properties.${j}.primaryKey`, e.target.checked)} /></td>
                            <td className="px-2 py-1 text-center"><input type="checkbox" checked={prop.required || false} onChange={(e) => upd(`schema.${i}.properties.${j}.required`, e.target.checked)} /></td>
                            <td className="px-2 py-1 text-center"><input type="checkbox" checked={prop.unique || false} onChange={(e) => upd(`schema.${i}.properties.${j}.unique`, e.target.checked)} /></td>
                            <td className="px-2 py-1">
                              <select value={prop.classification || ""} onChange={(e) => upd(`schema.${i}.properties.${j}.classification`, e.target.value)} className="h-7 border rounded px-1 text-xs bg-background w-full">
                                <option value="">-</option><option>public</option><option>internal</option><option>restricted</option><option>confidential</option>
                              </select>
                            </td>
                            <td className="px-2 py-1"><Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => removeFromArray(`schema.${i}.properties`, j)}><Trash2 className="h-3 w-3 text-red-500" /></Button></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </CardContent>
          )}
        </Card>
      ))}

      {schema.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No schema objects. Click "Add Object" to define tables.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Quality Tab
// ---------------------------------------------------------------------------
function QualityTab({ doc, upd, addToArray, removeFromArray, onImportDQ }: any) {
  const quality = doc.quality || [];
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => addToArray("quality", { metric: "", type: "library", dimension: "completeness", severity: "error", mustBe: null, mustBeGreaterThan: null, schedule: "" })}>
          <Plus className="h-4 w-4 mr-1" />Add Rule
        </Button>
        <Button size="sm" variant="outline" onClick={onImportDQ}>
          <Shield className="h-4 w-4 mr-1" />Import from DQ Engine
        </Button>
      </div>

      {quality.map((rule: any, i: number) => (
        <Card key={i}><CardContent className="pt-4 space-y-3">
          <div className="flex items-center gap-2 mb-2">
            <Badge variant="outline" className="text-xs">{rule.type || "library"}</Badge>
            <span className="text-sm font-medium">{rule.name || rule.metric || `Rule ${i + 1}`}</span>
            <Button variant="ghost" size="sm" className="ml-auto" onClick={() => removeFromArray("quality", i)}><Trash2 className="h-3.5 w-3.5 text-red-500" /></Button>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div><label className="text-xs text-muted-foreground">Type</label>
              <select value={rule.type || "library"} onChange={(e) => upd(`quality.${i}.type`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                {["library", "text", "sql", "custom"].map((t) => <option key={t}>{t}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Metric</label>
              <select value={rule.metric || ""} onChange={(e) => upd(`quality.${i}.metric`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">Select...</option>
                {["nullValues", "missingValues", "invalidValues", "duplicateValues", "rowCount"].map((m) => <option key={m}>{m}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Dimension</label>
              <select value={rule.dimension || ""} onChange={(e) => upd(`quality.${i}.dimension`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">Select...</option>
                {["accuracy", "completeness", "conformity", "consistency", "coverage", "timeliness", "uniqueness"].map((d) => <option key={d}>{d}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Severity</label>
              <select value={rule.severity || "error"} onChange={(e) => upd(`quality.${i}.severity`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                {["critical", "error", "warning", "info"].map((s) => <option key={s}>{s}</option>)}
              </select></div>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div><label className="text-xs text-muted-foreground">mustBe</label>
              <Input type="number" value={rule.mustBe ?? ""} onChange={(e) => upd(`quality.${i}.mustBe`, e.target.value ? Number(e.target.value) : null)} /></div>
            <div><label className="text-xs text-muted-foreground">mustBeGreaterThan</label>
              <Input type="number" value={rule.mustBeGreaterThan ?? ""} onChange={(e) => upd(`quality.${i}.mustBeGreaterThan`, e.target.value ? Number(e.target.value) : null)} /></div>
            <div><label className="text-xs text-muted-foreground">mustBeLessThan</label>
              <Input type="number" value={rule.mustBeLessThan ?? ""} onChange={(e) => upd(`quality.${i}.mustBeLessThan`, e.target.value ? Number(e.target.value) : null)} /></div>
            <div><label className="text-xs text-muted-foreground">Schedule</label>
              <Input value={rule.schedule || ""} onChange={(e) => upd(`quality.${i}.schedule`, e.target.value)} placeholder="0 20 * * *" /></div>
          </div>
          {rule.type === "sql" && (
            <div><label className="text-xs text-muted-foreground">SQL Query</label>
              <textarea className="w-full border rounded p-2 text-xs font-mono bg-muted/50 min-h-[60px]"
                value={rule.query || ""} onChange={(e) => upd(`quality.${i}.query`, e.target.value)} placeholder="SELECT count(*) FROM {object} WHERE..." /></div>
          )}
          {rule.type === "custom" && (
            <div className="grid grid-cols-2 gap-3">
              <div><label className="text-xs text-muted-foreground">Engine</label>
                <Input value={rule.engine || ""} onChange={(e) => upd(`quality.${i}.engine`, e.target.value)} placeholder="dqx, soda, great_expectations" /></div>
              <div><label className="text-xs text-muted-foreground">DQX Function</label>
                <Input value={rule.dqx_function || ""} onChange={(e) => upd(`quality.${i}.dqx_function`, e.target.value)} placeholder="is_not_null, is_in_range" /></div>
            </div>
          )}
        </CardContent></Card>
      ))}
      {quality.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No quality rules. Add rules or import from the DQ Engine.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// SLA Tab
// ---------------------------------------------------------------------------
function SLATab({ doc, upd, addToArray, removeFromArray, onImportSLA }: any) {
  const sla = doc.slaProperties || [];
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => addToArray("slaProperties", { property: "", value: "", unit: "d", element: "", driver: "operational" })}>
          <Plus className="h-4 w-4 mr-1" />Add SLA Property
        </Button>
        <Button size="sm" variant="outline" onClick={onImportSLA}>
          <Clock className="h-4 w-4 mr-1" />Import from SLA Monitor
        </Button>
      </div>
      {sla.map((s: any, i: number) => (
        <Card key={i}><CardContent className="pt-4">
          <div className="grid grid-cols-6 gap-3 items-end">
            <div><label className="text-xs text-muted-foreground">Property *</label>
              <select value={s.property || ""} onChange={(e) => upd(`slaProperties.${i}.property`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">Select...</option>
                {["latency", "availability", "throughput", "errorRate", "generalAvailability", "endOfSupport", "endOfLife", "retention", "frequency", "timeOfAvailability", "timeToDetect", "timeToNotify", "timeToRepair"].map((p) => <option key={p}>{p}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Value *</label>
              <Input value={s.value ?? ""} onChange={(e) => upd(`slaProperties.${i}.value`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Unit</label>
              <select value={s.unit || ""} onChange={(e) => upd(`slaProperties.${i}.unit`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">-</option>{["d", "h", "m", "y"].map((u) => <option key={u}>{u}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Element</label>
              <Input value={s.element || ""} onChange={(e) => upd(`slaProperties.${i}.element`, e.target.value)} placeholder="table.column" /></div>
            <div><label className="text-xs text-muted-foreground">Driver</label>
              <select value={s.driver || ""} onChange={(e) => upd(`slaProperties.${i}.driver`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">-</option>{["regulatory", "analytics", "operational"].map((d) => <option key={d}>{d}</option>)}
              </select></div>
            <Button variant="ghost" size="sm" onClick={() => removeFromArray("slaProperties", i)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
          </div>
        </CardContent></Card>
      ))}
      {sla.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No SLA properties defined.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Team Tab
// ---------------------------------------------------------------------------
function TeamTab({ doc, upd, addToArray, removeFromArray }: any) {
  const team = doc.team || { name: "", members: [] };
  return (
    <div className="space-y-4">
      <Card><CardContent className="pt-4 space-y-3">
        <div className="grid grid-cols-2 gap-3">
          <div><label className="text-xs text-muted-foreground">Team Name</label>
            <Input value={team.name || ""} onChange={(e) => upd("team.name", e.target.value)} /></div>
          <div><label className="text-xs text-muted-foreground">Description</label>
            <Input value={team.description || ""} onChange={(e) => upd("team.description", e.target.value)} /></div>
        </div>
      </CardContent></Card>

      <div className="flex items-center gap-2">
        <h3 className="text-sm font-medium" style={{ fontSize: '14px' }}>Members</h3>
        <Button size="sm" variant="outline" onClick={() => {
          const members = [...(team.members || []), { username: "", role: "", dateIn: "", dateOut: "" }];
          upd("team.members", members);
        }}><Plus className="h-4 w-4 mr-1" />Add Member</Button>
      </div>

      {(team.members || []).map((m: any, i: number) => (
        <Card key={i}><CardContent className="pt-4">
          <div className="grid grid-cols-5 gap-3 items-end">
            <div><label className="text-xs text-muted-foreground">Username *</label>
              <Input value={m.username || ""} onChange={(e) => upd(`team.members.${i}.username`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Role</label>
              <Input value={m.role || ""} onChange={(e) => upd(`team.members.${i}.role`, e.target.value)} placeholder="Data Scientist" /></div>
            <div><label className="text-xs text-muted-foreground">Date In</label>
              <Input type="date" value={m.dateIn || ""} onChange={(e) => upd(`team.members.${i}.dateIn`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Date Out</label>
              <Input type="date" value={m.dateOut || ""} onChange={(e) => upd(`team.members.${i}.dateOut`, e.target.value)} /></div>
            <Button variant="ghost" size="sm" onClick={() => {
              const members = [...(team.members || [])]; members.splice(i, 1); upd("team.members", members);
            }}><Trash2 className="h-4 w-4 text-red-500" /></Button>
          </div>
        </CardContent></Card>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Roles Tab
// ---------------------------------------------------------------------------
function RolesTab({ doc, upd, addToArray, removeFromArray }: any) {
  const roles = doc.roles || [];
  return (
    <div className="space-y-4">
      <Button size="sm" variant="outline" onClick={() => addToArray("roles", { role: "", access: "read", firstLevelApprovers: "", secondLevelApprovers: "" })}>
        <Plus className="h-4 w-4 mr-1" />Add Role
      </Button>
      {roles.map((r: any, i: number) => (
        <Card key={i}><CardContent className="pt-4">
          <div className="grid grid-cols-5 gap-3 items-end">
            <div><label className="text-xs text-muted-foreground">Role *</label>
              <Input value={r.role || ""} onChange={(e) => upd(`roles.${i}.role`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Access</label>
              <select value={r.access || "read"} onChange={(e) => upd(`roles.${i}.access`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option>read</option><option>write</option>
              </select></div>
            <div><label className="text-xs text-muted-foreground">1st Approvers</label>
              <Input value={r.firstLevelApprovers || ""} onChange={(e) => upd(`roles.${i}.firstLevelApprovers`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">2nd Approvers</label>
              <Input value={r.secondLevelApprovers || ""} onChange={(e) => upd(`roles.${i}.secondLevelApprovers`, e.target.value)} /></div>
            <Button variant="ghost" size="sm" onClick={() => removeFromArray("roles", i)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
          </div>
        </CardContent></Card>
      ))}
      {roles.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No roles defined.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Support Tab
// ---------------------------------------------------------------------------
function SupportTab({ doc, upd, addToArray, removeFromArray }: any) {
  const support = doc.support || [];
  return (
    <div className="space-y-4">
      <Button size="sm" variant="outline" onClick={() => addToArray("support", { channel: "", tool: "slack", url: "", scope: "", description: "" })}>
        <Plus className="h-4 w-4 mr-1" />Add Channel
      </Button>
      {support.map((s: any, i: number) => (
        <Card key={i}><CardContent className="pt-4">
          <div className="grid grid-cols-6 gap-3 items-end">
            <div><label className="text-xs text-muted-foreground">Channel *</label>
              <Input value={s.channel || ""} onChange={(e) => upd(`support.${i}.channel`, e.target.value)} placeholder="#data-help" /></div>
            <div><label className="text-xs text-muted-foreground">Tool</label>
              <select value={s.tool || ""} onChange={(e) => upd(`support.${i}.tool`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">-</option>{["slack", "email", "teams", "jira"].map((t) => <option key={t}>{t}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">URL</label>
              <Input value={s.url || ""} onChange={(e) => upd(`support.${i}.url`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Scope</label>
              <Input value={s.scope || ""} onChange={(e) => upd(`support.${i}.scope`, e.target.value)} placeholder="issues" /></div>
            <div><label className="text-xs text-muted-foreground">Description</label>
              <Input value={s.description || ""} onChange={(e) => upd(`support.${i}.description`, e.target.value)} /></div>
            <Button variant="ghost" size="sm" onClick={() => removeFromArray("support", i)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
          </div>
        </CardContent></Card>
      ))}
      {support.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No support channels defined.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Servers Tab
// ---------------------------------------------------------------------------
function ServersTab({ doc, upd, addToArray, removeFromArray, onPrefill }: any) {
  const servers = doc.servers || [];
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => addToArray("servers", { server: "", type: "databricks", catalog: "", schema: "", host: "", environment: "" })}>
          <Plus className="h-4 w-4 mr-1" />Add Server
        </Button>
        <Button size="sm" variant="outline" onClick={onPrefill}>
          <Server className="h-4 w-4 mr-1" />Auto-fill from Config
        </Button>
      </div>
      {servers.map((s: any, i: number) => (
        <Card key={i}><CardContent className="pt-4 space-y-3">
          <div className="flex items-center gap-2 mb-2">
            <Server className="h-4 w-4 text-muted-foreground" />
            <span className="font-medium text-sm">{s.server || `Server ${i + 1}`}</span>
            <Badge variant="outline" className="text-xs">{s.type}</Badge>
            <Button variant="ghost" size="sm" className="ml-auto" onClick={() => removeFromArray("servers", i)}><Trash2 className="h-3.5 w-3.5 text-red-500" /></Button>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div><label className="text-xs text-muted-foreground">Server ID *</label>
              <Input value={s.server || ""} onChange={(e) => upd(`servers.${i}.server`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Type *</label>
              <select value={s.type || "custom"} onChange={(e) => upd(`servers.${i}.type`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                {["databricks", "postgres", "snowflake", "bigquery", "kafka", "s3", "mysql", "redshift", "trino", "custom"].map((t) => <option key={t}>{t}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Environment</label>
              <select value={s.environment || ""} onChange={(e) => upd(`servers.${i}.environment`, e.target.value)} className="w-full border rounded px-3 py-2 text-sm bg-background">
                <option value="">-</option>{["prod", "staging", "dev", "uat"].map((e) => <option key={e}>{e}</option>)}
              </select></div>
            <div><label className="text-xs text-muted-foreground">Host</label>
              <Input value={s.host || ""} onChange={(e) => upd(`servers.${i}.host`, e.target.value)} /></div>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div><label className="text-xs text-muted-foreground">Catalog</label>
              <Input value={s.catalog || ""} onChange={(e) => upd(`servers.${i}.catalog`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Schema</label>
              <Input value={s.schema || ""} onChange={(e) => upd(`servers.${i}.schema`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Database</label>
              <Input value={s.database || ""} onChange={(e) => upd(`servers.${i}.database`, e.target.value)} /></div>
            <div><label className="text-xs text-muted-foreground">Port</label>
              <Input type="number" value={s.port ?? ""} onChange={(e) => upd(`servers.${i}.port`, e.target.value ? parseInt(e.target.value) : null)} /></div>
          </div>
        </CardContent></Card>
      ))}
      {servers.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No servers configured.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Pricing Tab
// ---------------------------------------------------------------------------
function PricingTab({ doc, upd }: any) {
  const price = doc.price || {};
  return (
    <Card><CardContent className="pt-4 space-y-3">
      <h2 className="text-sm font-medium" style={{ fontSize: '16px' }}>Pricing</h2>
      <div className="grid grid-cols-3 gap-3">
        <div><label className="text-xs text-muted-foreground">Amount</label>
          <Input type="number" step="0.01" value={price.priceAmount ?? ""} onChange={(e) => upd("price.priceAmount", parseFloat(e.target.value) || 0)} /></div>
        <div><label className="text-xs text-muted-foreground">Currency</label>
          <Input value={price.priceCurrency || "USD"} onChange={(e) => upd("price.priceCurrency", e.target.value)} /></div>
        <div><label className="text-xs text-muted-foreground">Unit</label>
          <Input value={price.priceUnit || ""} onChange={(e) => upd("price.priceUnit", e.target.value)} placeholder="megabyte, record, query" /></div>
      </div>
    </CardContent></Card>
  );
}

// ---------------------------------------------------------------------------
// Custom Properties Tab
// ---------------------------------------------------------------------------
function CustomPropsTab({ doc, upd, addToArray, removeFromArray }: any) {
  const props = doc.customProperties || [];
  return (
    <div className="space-y-4">
      <Button size="sm" variant="outline" onClick={() => addToArray("customProperties", { property: "", value: "" })}>
        <Plus className="h-4 w-4 mr-1" />Add Property
      </Button>
      {props.map((p: any, i: number) => (
        <div key={i} className="flex items-center gap-3">
          <Input value={p.property || ""} onChange={(e) => upd(`customProperties.${i}.property`, e.target.value)} placeholder="Property name" className="flex-1" />
          <Input value={typeof p.value === "string" ? p.value : JSON.stringify(p.value)} onChange={(e) => upd(`customProperties.${i}.value`, e.target.value)} placeholder="Value" className="flex-1" />
          <Button variant="ghost" size="sm" onClick={() => removeFromArray("customProperties", i)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
        </div>
      ))}
      {props.length === 0 && <p className="text-sm text-muted-foreground text-center py-8">No custom properties.</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// YAML Tab
// ---------------------------------------------------------------------------
function YAMLTab({ yamlText, setYamlText, contractId }: any) {
  function download() {
    const blob = new Blob([yamlText], { type: "text/yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${contractId || "contract"}.odcs.yaml`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={download} disabled={!yamlText}>
          <Download className="h-4 w-4 mr-1" />Download .odcs.yaml
        </Button>
        <span className="text-xs text-muted-foreground">Read-only view of the exported ODCS YAML</span>
      </div>
      <textarea
        className="w-full h-[500px] border rounded p-3 font-mono text-xs bg-muted/30"
        value={yamlText}
        readOnly
      />
    </div>
  );
}
