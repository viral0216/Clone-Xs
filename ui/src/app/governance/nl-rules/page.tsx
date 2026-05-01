// @ts-nocheck
import { useState } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  MessageSquare, Loader2, Send, Save, Sparkles, Copy,
  AlertTriangle, CheckCircle2, FileText, List, ArrowRight,
} from "lucide-react";

function confidenceBadge(confidence: number | string) {
  if (confidence == null) return null;
  const numericConf = typeof confidence === "string"
    ? confidence === "high" ? 0.9 : confidence === "medium" ? 0.7 : 0.4
    : confidence;
  if (numericConf >= 0.8) return <Badge className="bg-green-500/20 text-green-400">High Confidence</Badge>;
  if (numericConf >= 0.6) return <Badge className="bg-amber-500/20 text-amber-400">Medium Confidence</Badge>;
  return <Badge className="bg-red-500/20 text-red-400">Low Confidence</Badge>;
}

export default function NLRulesPage() {
  // Single rule mode
  const [nlInput, setNlInput] = usePersistedState<string>("nl-rules-input", "");
  const [tableFqn, setTableFqn] = usePersistedState<string>("nl-rules-table-fqn", "");
  const [parsing, setParsing] = useState(false);
  const [parsedRule, setParsedRule] = usePersistedState<any>("nl-rules-parsed", null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  // Batch mode
  const [batchMode, setBatchMode] = useState(false);
  const [batchInput, setBatchInput] = usePersistedState<string>("nl-rules-batch-input", "");
  const [batchParsing, setBatchParsing] = useState(false);
  const [batchResults, setBatchResults] = usePersistedState<any[]>("nl-rules-batch-results", []);

  // Explain mode
  const [explainMode, setExplainMode] = useState(false);
  const [ruleJson, setRuleJson] = useState("");
  const [explaining, setExplaining] = useState(false);
  const [explanation, setExplanation] = usePersistedState<string | null>("nl-rules-explanation", null);

  const [error, setError] = useState<string | null>(null);

  async function parseRule() {
    if (!nlInput.trim()) return;
    setParsing(true);
    setError(null);
    setParsedRule(null);
    setSaved(false);
    try {
      const data = await api.post("/nl-rules/from-natural-language", {
        text: nlInput.trim(),
        table_fqn: tableFqn.trim() || undefined,
      });
      setParsedRule(data);
    } catch (e: any) {
      setError(e.message || "Failed to parse rule.");
    }
    setParsing(false);
  }

  async function saveRule() {
    if (!parsedRule) return;
    setSaving(true);
    setError(null);
    try {
      await api.post("/governance/dq/rules", parsedRule.rule ?? parsedRule);
      setSaved(true);
    } catch (e: any) {
      setError(e.message || "Failed to save rule.");
    }
    setSaving(false);
  }

  async function parseBatch() {
    if (!batchInput.trim()) return;
    setBatchParsing(true);
    setError(null);
    setBatchResults([]);
    try {
      const lines = batchInput.trim().split("\n").filter((l) => l.trim());
      const results = [];
      for (const line of lines) {
        try {
          const data = await api.post("/nl-rules/from-natural-language", {
            text: line.trim(),
            table_fqn: tableFqn.trim() || undefined,
          });
          results.push({ input: line.trim(), result: data, error: null });
        } catch (e: any) {
          results.push({ input: line.trim(), result: null, error: e.message });
        }
      }
      setBatchResults(results);
    } catch (e: any) {
      setError(e.message || "Batch parsing failed.");
    }
    setBatchParsing(false);
  }

  async function explainRule() {
    if (!ruleJson.trim()) return;
    setExplaining(true);
    setError(null);
    setExplanation(null);
    try {
      let parsed;
      try {
        parsed = JSON.parse(ruleJson.trim());
      } catch {
        setError("Invalid JSON. Please paste a valid rule JSON.");
        setExplaining(false);
        return;
      }
      const data = await api.post("/nl-rules/explain", { rule: parsed });
      setExplanation(typeof data === "string" ? data : data?.explanation ?? JSON.stringify(data, null, 2));
    } catch (e: any) {
      setError(e.message || "Failed to explain rule.");
    }
    setExplaining(false);
  }

  const activeTab = explainMode ? "explain" : batchMode ? "batch" : "single";

  return (
    <div className="space-y-6">
      <PageHeader
        title="Natural Language Rules"
        icon={MessageSquare}
        description="Define DQ rules in plain English"
        breadcrumbs={["Governance", "NL Rule Builder"]}
      />

      {/* Mode Tabs */}
      <div className="flex gap-2">
        <Button
          variant={activeTab === "single" ? "default" : "outline"}
          size="sm"
          onClick={() => { setBatchMode(false); setExplainMode(false); }}
        >
          <MessageSquare className="h-4 w-4 mr-2" /> Single Rule
        </Button>
        <Button
          variant={activeTab === "batch" ? "default" : "outline"}
          size="sm"
          onClick={() => { setBatchMode(true); setExplainMode(false); }}
        >
          <List className="h-4 w-4 mr-2" /> Batch Mode
        </Button>
        <Button
          variant={activeTab === "explain" ? "default" : "outline"}
          size="sm"
          onClick={() => { setExplainMode(true); setBatchMode(false); }}
        >
          <FileText className="h-4 w-4 mr-2" /> Explain Rule
        </Button>
      </div>

      {error && (
        <div className="text-red-400 text-sm bg-red-500/10 border border-red-500/30 rounded-md p-3">
          {error}
        </div>
      )}

      {/* Single Rule Mode */}
      {activeTab === "single" && (
        <>
          <Card className="bg-card border-border">
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <Sparkles className="h-5 w-5 text-blue-400" /> Describe Your Rule
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex flex-col gap-1">
                <label className="text-sm text-muted-foreground">Table (optional)</label>
                <input
                  className="px-3 py-2 rounded-md border border-border bg-background text-sm w-full max-w-md"
                  value={tableFqn}
                  onChange={(e) => setTableFqn(e.target.value)}
                  placeholder="e.g. main.default.orders"
                />
              </div>
              <div className="flex flex-col gap-1">
                <label className="text-sm text-muted-foreground">Rule Description (plain English)</label>
                <textarea
                  className="px-3 py-2 rounded-md border border-border bg-background text-sm w-full min-h-[100px] resize-y"
                  value={nlInput}
                  onChange={(e) => setNlInput(e.target.value)}
                  placeholder="e.g. The email column should never be null and must contain an @ symbol"
                />
              </div>
              <Button onClick={parseRule} disabled={parsing || !nlInput.trim()}>
                {parsing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Send className="h-4 w-4 mr-2" />}
                Parse Rule
              </Button>
            </CardContent>
          </Card>

          {/* Parsed Rule Preview */}
          {parsedRule && (
            <Card className="bg-card border-border border-green-500/30">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <CheckCircle2 className="h-5 w-5 text-green-400" /> Parsed Rule
                  </CardTitle>
                  {confidenceBadge(parsedRule.confidence)}
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Rule Summary */}
                {parsedRule.rule_name && (
                  <div>
                    <p className="text-xs text-muted-foreground">Rule Name</p>
                    <p className="text-sm font-medium">{parsedRule.rule_name}</p>
                  </div>
                )}
                {parsedRule.rule_type && (
                  <div>
                    <p className="text-xs text-muted-foreground">Rule Type</p>
                    <Badge variant="secondary">{parsedRule.rule_type}</Badge>
                  </div>
                )}
                {parsedRule.column && (
                  <div>
                    <p className="text-xs text-muted-foreground">Column</p>
                    <p className="text-sm font-mono">{parsedRule.column}</p>
                  </div>
                )}
                {parsedRule.expectation && (
                  <div>
                    <p className="text-xs text-muted-foreground">Expectation</p>
                    <p className="text-sm font-mono">{parsedRule.expectation}</p>
                  </div>
                )}

                {/* Full JSON */}
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Full Configuration</p>
                  <pre className="bg-muted/30 rounded-md p-3 text-xs overflow-x-auto max-h-64 overflow-y-auto">
                    {JSON.stringify(parsedRule.rule ?? parsedRule, null, 2)}
                  </pre>
                </div>

                <div className="flex gap-3">
                  <Button onClick={saveRule} disabled={saving || saved}>
                    {saving ? (
                      <Loader2 className="h-4 w-4 animate-spin mr-2" />
                    ) : saved ? (
                      <CheckCircle2 className="h-4 w-4 mr-2 text-green-400" />
                    ) : (
                      <Save className="h-4 w-4 mr-2" />
                    )}
                    {saved ? "Saved" : "Create Rule"}
                  </Button>
                  <Button
                    variant="outline"
                    onClick={() => {
                      navigator.clipboard.writeText(JSON.stringify(parsedRule.rule ?? parsedRule, null, 2));
                    }}
                  >
                    <Copy className="h-4 w-4 mr-2" /> Copy JSON
                  </Button>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* Batch Mode */}
      {activeTab === "batch" && (
        <>
          <Card className="bg-card border-border">
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <List className="h-5 w-5 text-purple-400" /> Batch Rule Parsing
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex flex-col gap-1">
                <label className="text-sm text-muted-foreground">Table (optional, applies to all)</label>
                <input
                  className="px-3 py-2 rounded-md border border-border bg-background text-sm w-full max-w-md"
                  value={tableFqn}
                  onChange={(e) => setTableFqn(e.target.value)}
                  placeholder="e.g. main.default.orders"
                />
              </div>
              <div className="flex flex-col gap-1">
                <label className="text-sm text-muted-foreground">Rules (one per line)</label>
                <textarea
                  className="px-3 py-2 rounded-md border border-border bg-background text-sm w-full min-h-[160px] resize-y font-mono"
                  value={batchInput}
                  onChange={(e) => setBatchInput(e.target.value)}
                  placeholder={`email must not be null\norder_amount should be positive\nstatus must be one of: active, inactive, pending`}
                />
              </div>
              <Button onClick={parseBatch} disabled={batchParsing || !batchInput.trim()}>
                {batchParsing ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Send className="h-4 w-4 mr-2" />}
                Parse All
              </Button>
            </CardContent>
          </Card>

          {/* Batch Results */}
          {batchResults.length > 0 && (
            <Card className="bg-card border-border">
              <CardHeader>
                <CardTitle className="text-lg">
                  Batch Results ({batchResults.filter((r) => r.result).length}/{batchResults.length} parsed)
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                {batchResults.map((item, i) => (
                  <div
                    key={i}
                    className={`p-3 rounded-md border ${
                      item.error
                        ? "border-red-500/30 bg-red-500/5"
                        : "border-green-500/30 bg-green-500/5"
                    }`}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        {item.error ? (
                          <AlertTriangle className="h-4 w-4 text-red-400" />
                        ) : (
                          <CheckCircle2 className="h-4 w-4 text-green-400" />
                        )}
                        <span className="text-sm font-medium">{item.input}</span>
                      </div>
                      {item.result && confidenceBadge(item.result.confidence)}
                    </div>
                    {item.error && (
                      <p className="text-xs text-red-400">{item.error}</p>
                    )}
                    {item.result && (
                      <pre className="bg-muted/30 rounded p-2 text-xs overflow-x-auto mt-2">
                        {JSON.stringify(item.result.rule ?? item.result, null, 2)}
                      </pre>
                    )}
                  </div>
                ))}
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* Explain Mode */}
      {activeTab === "explain" && (
        <>
          <Card className="bg-card border-border">
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <FileText className="h-5 w-5 text-cyan-400" /> Explain a Rule
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex flex-col gap-1">
                <label className="text-sm text-muted-foreground">Rule JSON</label>
                <textarea
                  className="px-3 py-2 rounded-md border border-border bg-background text-sm w-full min-h-[160px] resize-y font-mono"
                  value={ruleJson}
                  onChange={(e) => setRuleJson(e.target.value)}
                  placeholder={`{\n  "rule_type": "not_null",\n  "column": "email",\n  "table_fqn": "main.default.users"\n}`}
                />
              </div>
              <Button onClick={explainRule} disabled={explaining || !ruleJson.trim()}>
                {explaining ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Sparkles className="h-4 w-4 mr-2" />}
                Explain
              </Button>
            </CardContent>
          </Card>

          {explanation && (
            <Card className="bg-card border-border border-cyan-500/30">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <MessageSquare className="h-5 w-5 text-cyan-400" /> Explanation
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="prose prose-invert max-w-none text-sm leading-relaxed whitespace-pre-wrap">
                  {explanation}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
