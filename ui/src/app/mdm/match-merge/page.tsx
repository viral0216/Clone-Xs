// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import PageHeader from "@/components/PageHeader";
import { GitMerge, XCircle, Loader2, Plus, Play, Trash2, Upload, Settings2, FlaskConical } from "lucide-react";
import { useMdmRules, useMdmPairs, useMergeRecords, useCreateRule, useDeleteRule, useDetectDuplicates, useIngestSource } from "@/hooks/useMdm";

export default function MatchMergePage() {
  const { data: rules, isLoading: rulesLoading } = useMdmRules();
  const { data: pairs, isLoading: pairsLoading } = useMdmPairs();
  const mergeRecords = useMergeRecords();
  const createRule = useCreateRule();
  const deleteRule = useDeleteRule();
  const detectDuplicates = useDetectDuplicates();
  const ingestSource = useIngestSource();

  const [activeTab, setActiveTab] = useState<"rules" | "pairs" | "ingest" | "survivorship" | "trust">("pairs");

  // Rule form
  const [showAddRule, setShowAddRule] = useState(false);
  const [ruleName, setRuleName] = useState("");
  const [ruleField, setRuleField] = useState("");
  const [ruleType, setRuleType] = useState("exact");
  const [ruleWeight, setRuleWeight] = useState("1.0");
  const [ruleEntityType, setRuleEntityType] = useState("Customer");

  // Ingest form
  const [ingestCatalog, setIngestCatalog] = useState("");
  const [ingestSchema, setIngestSchema] = useState("");
  const [ingestTable, setIngestTable] = useState("");
  const [ingestKey, setIngestKey] = useState("");
  const [ingestEntityType, setIngestEntityType] = useState("Customer");

  // Detect
  const [detectType, setDetectType] = useState("Customer");
  const [autoThreshold, setAutoThreshold] = useState("95");
  const [reviewThreshold, setReviewThreshold] = useState("80");

  // Survivorship rules (local state — would persist to API in full implementation)
  const [survivorshipRules, setSurvivorshipRules] = useState([
    { field: "name", strategy: "longest", source_priority: "" },
    { field: "email", strategy: "most_trusted", source_priority: "crm" },
    { field: "phone", strategy: "most_recent", source_priority: "" },
    { field: "address", strategy: "most_complete", source_priority: "" },
  ]);

  // Source trust scores (local state)
  const [trustScores, setTrustScores] = useState([
    { source: "crm", trust: 0.95, description: "Salesforce CRM" },
    { source: "erp", trust: 0.85, description: "SAP ERP" },
    { source: "web", trust: 0.60, description: "Web forms" },
    { source: "import", trust: 0.50, description: "CSV imports" },
  ]);

  // Match tuning
  const [tuneTestA, setTuneTestA] = useState("Acme Corporation");
  const [tuneTestB, setTuneTestB] = useState("ACME Corp LLC");
  const [tuneResult, setTuneResult] = useState<string | null>(null);

  const ruleList = Array.isArray(rules) ? rules : [];
  const pairList = Array.isArray(pairs) ? pairs : [];
  const pendingPairs = pairList.filter(p => p.status === "pending");

  const tabs = [
    { key: "pairs", label: `Duplicates (${pendingPairs.length})` },
    { key: "rules", label: `Rules (${ruleList.length})` },
    { key: "survivorship", label: "Survivorship" },
    { key: "trust", label: "Source Trust" },
    { key: "ingest", label: "Ingest" },
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="Match & Merge" icon={GitMerge} breadcrumbs={["MDM", "Match & Merge"]}
        description="Configure matching rules, ingest source records, detect duplicates, and merge or dismiss." />

      {/* Actions */}
      <div className="flex items-center gap-2 flex-wrap">
        <div className="flex items-center gap-1.5">
          <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md" value={detectType} onChange={e => setDetectType(e.target.value)}>
            {["Customer", "Product", "Supplier", "Employee"].map(t => <option key={t}>{t}</option>)}
          </select>
          <Button size="sm" onClick={() => detectDuplicates.mutate({ entity_type: detectType, auto_merge_threshold: parseFloat(autoThreshold), review_threshold: parseFloat(reviewThreshold) })} disabled={detectDuplicates.isPending}>
            {detectDuplicates.isPending ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Play className="h-3 w-3 mr-1" />}
            Detect Duplicates
          </Button>
        </div>
        <div className="flex items-center gap-1 text-xs text-muted-foreground">
          <span>Auto-merge ≥</span>
          <input className="w-12 px-1 py-0.5 text-xs bg-muted border border-border rounded text-center" value={autoThreshold} onChange={e => setAutoThreshold(e.target.value)} />%
          <span className="ml-1">Review ≥</span>
          <input className="w-12 px-1 py-0.5 text-xs bg-muted border border-border rounded text-center" value={reviewThreshold} onChange={e => setReviewThreshold(e.target.value)} />%
        </div>
        {detectDuplicates.isSuccess && <Badge className="bg-muted/40 text-foreground text-[10px]">{detectDuplicates.data?.pairs_found || 0} pairs, {detectDuplicates.data?.auto_merged || 0} auto-merged</Badge>}
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border">
        {tabs.map(tab => (
          <button key={tab.key} onClick={() => setActiveTab(tab.key as any)}
            className={`px-3 py-2 text-xs font-medium border-b-2 transition-colors ${activeTab === tab.key ? "border-[#E8453C] text-[#E8453C]" : "border-transparent text-muted-foreground hover:text-foreground"}`}>
            {tab.label}
          </button>
        ))}
      </div>

      {/* Duplicates Tab */}
      {activeTab === "pairs" && (
        <Card>
          <CardContent className="pt-4">
            {pairsLoading ? <Skeleton className="h-20 w-full" /> : pairList.length === 0 ? (
              <div className="text-center py-6 text-muted-foreground text-sm">No duplicate pairs yet — ingest source records and run detection</div>
            ) : (
              <div className="space-y-2">
                {pairList.map(d => (
                  <div key={d.pair_id} className={`px-3 py-3 rounded-lg border transition-colors ${d.status === "pending" ? "border-border" : "border-border bg-muted/20 opacity-60"}`}>
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Badge variant="outline" className="text-[10px]">{d.entity_type}</Badge>
                        <span className="text-xs text-muted-foreground">{d.pair_id?.slice(0, 8)}...</span>
                        {d.matched_rules && <span className="text-xs text-muted-foreground">rules: {d.matched_rules}</span>}
                      </div>
                      <Badge className={`text-[10px] ${d.match_score >= 95 ? "bg-muted/40 text-foreground border-border" : "bg-muted/20 text-muted-foreground border-border"}`}>{Math.round(d.match_score || 0)}%</Badge>
                    </div>
                    <div className="flex items-center justify-between">
                      <div className="text-sm">
                        <span className="font-medium">{d.record_a_name || d.record_a_id?.slice(0, 8)}</span>
                        <span className="text-muted-foreground mx-2">↔</span>
                        <span className="font-medium">{d.record_b_name || d.record_b_id?.slice(0, 8)}</span>
                      </div>
                      {d.status === "pending" ? (
                        <div className="flex gap-1.5">
                          <Button size="sm" variant="outline" className="h-7 text-xs" disabled={mergeRecords.isPending} onClick={() => mergeRecords.mutate({ pair_id: d.pair_id })}>
                            <GitMerge className="h-3 w-3 mr-1" /> Merge
                          </Button>
                          <Button size="sm" variant="ghost" className="h-7 text-xs"><XCircle className="h-3 w-3 mr-1" /> Dismiss</Button>
                        </div>
                      ) : (
                        <Badge variant="outline" className="text-[10px]">{d.status}</Badge>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Rules Tab */}
      {activeTab === "rules" && (
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm">Matching Rules</CardTitle>
              <Button size="sm" variant="outline" className="h-7 text-xs" onClick={() => setShowAddRule(!showAddRule)}><Plus className="h-3 w-3 mr-1" /> Add Rule</Button>
            </div>
          </CardHeader>
          <CardContent>
            {showAddRule && (
              <div className="flex items-center gap-2 mb-3 pb-3 border-b border-border flex-wrap">
                <select className="px-2 py-1 text-xs bg-muted border border-border rounded-md" value={ruleEntityType} onChange={e => setRuleEntityType(e.target.value)}>
                  {["Customer", "Product", "Supplier", "Employee"].map(t => <option key={t}>{t}</option>)}
                </select>
                <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md w-28" placeholder="Rule name" value={ruleName} onChange={e => setRuleName(e.target.value)} />
                <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md w-24" placeholder="Field" value={ruleField} onChange={e => setRuleField(e.target.value)} />
                <select className="px-2 py-1 text-xs bg-muted border border-border rounded-md" value={ruleType} onChange={e => setRuleType(e.target.value)}>
                  {["exact", "fuzzy_jaro_winkler", "fuzzy_levenshtein", "soundex", "normalized", "numeric"].map(t => <option key={t}>{t}</option>)}
                </select>
                <input className="px-2 py-1 text-xs bg-muted border border-border rounded-md w-16" placeholder="Weight" value={ruleWeight} onChange={e => setRuleWeight(e.target.value)} />
                <Button size="sm" className="h-6 text-xs px-2" disabled={!ruleName || !ruleField}
                  onClick={() => { createRule.mutate({ entity_type: ruleEntityType, name: ruleName, field: ruleField, match_type: ruleType, weight: parseFloat(ruleWeight) || 1.0 }); setRuleName(""); setRuleField(""); setShowAddRule(false); }}>
                  Save
                </Button>
              </div>
            )}
            {/* Match Tuning */}
            <div className="mb-3 pb-3 border-b border-border">
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-2 flex items-center gap-1"><FlaskConical className="h-3 w-3" /> Test Match Rules</p>
              <div className="flex items-center gap-2">
                <input className="flex-1 px-2 py-1 text-xs bg-muted border border-border rounded-md" placeholder="Value A" value={tuneTestA} onChange={e => setTuneTestA(e.target.value)} />
                <span className="text-xs text-muted-foreground">↔</span>
                <input className="flex-1 px-2 py-1 text-xs bg-muted border border-border rounded-md" placeholder="Value B" value={tuneTestB} onChange={e => setTuneTestB(e.target.value)} />
                <Button size="sm" variant="outline" className="h-6 text-xs px-2" onClick={() => {
                  // Client-side test using available match types
                  if (!tuneTestA || !tuneTestB) return;
                  const a = tuneTestA.toLowerCase(), b = tuneTestB.toLowerCase();
                  const exact = a === b ? 100 : 0;
                  // Simple Levenshtein ratio
                  const maxLen = Math.max(a.length, b.length);
                  let dist = 0;
                  const m = Array.from({ length: a.length + 1 }, (_, i) => Array.from({ length: b.length + 1 }, (_, j) => i === 0 ? j : j === 0 ? i : 0));
                  for (let i = 1; i <= a.length; i++) for (let j = 1; j <= b.length; j++) m[i][j] = Math.min(m[i-1][j]+1, m[i][j-1]+1, m[i-1][j-1]+(a[i-1]!==b[j-1]?1:0));
                  const levRatio = Math.round((1 - m[a.length][b.length] / maxLen) * 100);
                  setTuneResult(`exact: ${exact}% | levenshtein: ${levRatio}% | normalized: ${a.replace(/\s*(llc|ltd|inc|corp)\s*/g, '') === b.replace(/\s*(llc|ltd|inc|corp)\s*/g, '') ? 100 : levRatio}%`);
                }}>Test</Button>
              </div>
              {tuneResult && <p className="text-xs text-muted-foreground mt-1.5 font-mono">{tuneResult}</p>}
            </div>
            {rulesLoading ? <Skeleton className="h-20 w-full" /> : ruleList.length === 0 ? (
              <div className="text-center py-6 text-muted-foreground text-sm">No matching rules configured yet</div>
            ) : (
              <div className="space-y-1">
                {ruleList.map(rule => (
                  <div key={rule.rule_id || rule.name} className="flex items-center justify-between px-3 py-2 rounded-lg hover:bg-muted/30">
                    <div className="flex items-center gap-3">
                      <Badge variant={rule.enabled ? "default" : "outline"} className={`text-[10px] min-w-[55px] justify-center ${rule.enabled ? "bg-muted/40 text-foreground border-border" : ""}`}>{rule.enabled ? "On" : "Off"}</Badge>
                      <span className="text-sm font-medium font-mono">{rule.name}</span>
                      <span className="text-xs text-muted-foreground">field: {rule.field}</span>
                      <Badge variant="outline" className="text-[10px]">{rule.entity_type}</Badge>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-xs text-muted-foreground">{rule.match_type}</span>
                      <span className="text-xs font-mono">w={rule.weight}</span>
                      <button onClick={() => deleteRule.mutate(rule.rule_id)} className="text-muted-foreground hover:text-red-500"><Trash2 className="h-3 w-3" /></button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Survivorship Tab */}
      {activeTab === "survivorship" && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2"><Settings2 className="h-4 w-4" /> Survivorship Rules</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xs text-muted-foreground mb-3">When merging records, these rules determine which source value wins for each field.</p>
            <div className="rounded-md border border-border overflow-hidden">
              <div className="grid grid-cols-4 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                <span>Field</span><span>Strategy</span><span>Preferred Source</span><span></span>
              </div>
              {survivorshipRules.map((rule, i) => (
                <div key={i} className="grid grid-cols-4 items-center px-3 py-2 border-t border-border">
                  <input className="text-xs px-1.5 py-1 bg-muted border border-border rounded w-24" value={rule.field}
                    onChange={e => { const r = [...survivorshipRules]; r[i] = { ...r[i], field: e.target.value }; setSurvivorshipRules(r); }} />
                  <select className="text-xs px-1.5 py-1 bg-muted border border-border rounded" value={rule.strategy}
                    onChange={e => { const r = [...survivorshipRules]; r[i] = { ...r[i], strategy: e.target.value }; setSurvivorshipRules(r); }}>
                    {["most_trusted", "most_recent", "most_complete", "longest", "custom_sql"].map(s => <option key={s}>{s}</option>)}
                  </select>
                  <input className="text-xs px-1.5 py-1 bg-muted border border-border rounded w-24" placeholder="any" value={rule.source_priority}
                    onChange={e => { const r = [...survivorshipRules]; r[i] = { ...r[i], source_priority: e.target.value }; setSurvivorshipRules(r); }} />
                  <button className="text-muted-foreground hover:text-red-500 justify-self-end" onClick={() => setSurvivorshipRules(prev => prev.filter((_, j) => j !== i))}>
                    <Trash2 className="h-3 w-3" />
                  </button>
                </div>
              ))}
            </div>
            <Button size="sm" variant="outline" className="mt-2 h-7 text-xs" onClick={() => setSurvivorshipRules(prev => [...prev, { field: "", strategy: "most_trusted", source_priority: "" }])}>
              <Plus className="h-3 w-3 mr-1" /> Add Field Rule
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Source Trust Tab */}
      {activeTab === "trust" && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Source System Trust Scores</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xs text-muted-foreground mb-3">Higher trust scores mean the source's values are preferred during survivorship merges.</p>
            <div className="rounded-md border border-border overflow-hidden">
              <div className="grid grid-cols-4 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                <span>Source System</span><span>Description</span><span>Trust Score</span><span></span>
              </div>
              {trustScores.map((src, i) => (
                <div key={i} className="grid grid-cols-4 items-center px-3 py-2 border-t border-border">
                  <input className="text-xs px-1.5 py-1 bg-muted border border-border rounded w-24" value={src.source}
                    onChange={e => { const s = [...trustScores]; s[i] = { ...s[i], source: e.target.value }; setTrustScores(s); }} />
                  <input className="text-xs px-1.5 py-1 bg-muted border border-border rounded" value={src.description}
                    onChange={e => { const s = [...trustScores]; s[i] = { ...s[i], description: e.target.value }; setTrustScores(s); }} />
                  <div className="flex items-center gap-2">
                    <input type="range" min="0" max="1" step="0.05" value={src.trust} className="flex-1 h-1.5"
                      onChange={e => { const s = [...trustScores]; s[i] = { ...s[i], trust: parseFloat(e.target.value) }; setTrustScores(s); }} />
                    <span className="text-xs font-mono w-10">{src.trust.toFixed(2)}</span>
                  </div>
                  <button className="text-muted-foreground hover:text-red-500 justify-self-end" onClick={() => setTrustScores(prev => prev.filter((_, j) => j !== i))}>
                    <Trash2 className="h-3 w-3" />
                  </button>
                </div>
              ))}
            </div>
            <Button size="sm" variant="outline" className="mt-2 h-7 text-xs" onClick={() => setTrustScores(prev => [...prev, { source: "", trust: 0.5, description: "" }])}>
              <Plus className="h-3 w-3 mr-1" /> Add Source
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Ingest Tab */}
      {activeTab === "ingest" && (
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm flex items-center gap-2"><Upload className="h-4 w-4" /> Ingest Source Records</CardTitle></CardHeader>
          <CardContent>
            <p className="text-xs text-muted-foreground mb-3">Load records from any Unity Catalog table as source records for entity resolution.</p>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">Catalog</label>
                <input className="w-full px-2 py-1.5 text-sm bg-muted border border-border rounded-md mt-1" placeholder="e.g. production" value={ingestCatalog} onChange={e => setIngestCatalog(e.target.value)} />
              </div>
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">Schema</label>
                <input className="w-full px-2 py-1.5 text-sm bg-muted border border-border rounded-md mt-1" placeholder="e.g. crm" value={ingestSchema} onChange={e => setIngestSchema(e.target.value)} />
              </div>
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">Table</label>
                <input className="w-full px-2 py-1.5 text-sm bg-muted border border-border rounded-md mt-1" placeholder="e.g. customers" value={ingestTable} onChange={e => setIngestTable(e.target.value)} />
              </div>
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">Key Column</label>
                <input className="w-full px-2 py-1.5 text-sm bg-muted border border-border rounded-md mt-1" placeholder="e.g. customer_id" value={ingestKey} onChange={e => setIngestKey(e.target.value)} />
              </div>
              <div>
                <label className="text-[10px] font-medium text-muted-foreground uppercase">Entity Type</label>
                <select className="w-full px-2 py-1.5 text-sm bg-muted border border-border rounded-md mt-1" value={ingestEntityType} onChange={e => setIngestEntityType(e.target.value)}>
                  {["Customer", "Product", "Supplier", "Employee"].map(t => <option key={t}>{t}</option>)}
                </select>
              </div>
              <div className="flex items-end">
                <Button className="w-full" disabled={!ingestCatalog || !ingestSchema || !ingestTable || !ingestKey || ingestSource.isPending}
                  onClick={() => ingestSource.mutate({ catalog: ingestCatalog, schema_name: ingestSchema, table: ingestTable, entity_type: ingestEntityType, key_column: ingestKey })}>
                  {ingestSource.isPending ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Upload className="h-3 w-3 mr-1" />}
                  Ingest Records
                </Button>
              </div>
            </div>
            {ingestSource.isSuccess && (
              <div className="mt-3 p-2 rounded-md bg-muted/30 text-xs">
                Ingested <strong>{ingestSource.data?.records_ingested}</strong> records from <code>{ingestSource.data?.source_table}</code>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
