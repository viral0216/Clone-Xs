// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { Loader2, Plus, TestTube, Settings2 } from "lucide-react";

interface PiiPatternEditorProps {
  onConfigChange?: (config: any) => void;
}

export default function PiiPatternEditor({ onConfigChange }: PiiPatternEditorProps) {
  const [patterns, setPatterns] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [disabledTypes, setDisabledTypes] = useState<string[]>([]);

  // New pattern form
  const [newPattern, setNewPattern] = useState("");
  const [newType, setNewType] = useState("");
  const [newMasking, setNewMasking] = useState("hash");
  const [testInput, setTestInput] = useState("");
  const [testResult, setTestResult] = useState<boolean | null>(null);

  useEffect(() => {
    setLoading(true);
    api.get<any>("/pii-patterns")
      .then(setPatterns)
      .catch(() => setPatterns(null))
      .finally(() => setLoading(false));
  }, []);

  const toggleType = (type: string) => {
    const updated = disabledTypes.includes(type)
      ? disabledTypes.filter((t) => t !== type)
      : [...disabledTypes, type];
    setDisabledTypes(updated);
    onConfigChange?.({ disabled_patterns: updated });
  };

  const testPattern = () => {
    if (!newPattern || !testInput) return;
    try {
      const regex = new RegExp(newPattern);
      setTestResult(regex.test(testInput));
    } catch {
      setTestResult(false);
    }
  };

  const addCustomPattern = () => {
    if (!newPattern || !newType) return;
    onConfigChange?.({
      disabled_patterns: disabledTypes,
      custom_column_patterns: [{
        pattern: newPattern,
        pii_type: newType.toUpperCase().replace(/\s+/g, "_"),
        masking: newMasking,
      }],
    });
    setNewPattern("");
    setNewType("");
    setTestResult(null);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mr-2" /> Loading patterns...
      </div>
    );
  }

  const builtInTypes = patterns?.built_in_types || [];
  const maskingOptions = Object.keys(patterns?.masking || {}).filter((v, i, a) => a.indexOf(v) === i);

  return (
    <Card className="bg-card border-border">
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <Settings2 className="h-4 w-4" /> Detection Patterns
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Built-in patterns toggle */}
        <div>
          <p className="text-sm font-medium mb-2">Built-in PII Types</p>
          <div className="flex flex-wrap gap-2">
            {builtInTypes.map((type: string) => (
              <button
                key={type}
                onClick={() => toggleType(type)}
                className={`inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border transition-colors ${
                  disabledTypes.includes(type)
                    ? "bg-gray-100 text-gray-400 border-gray-200 line-through"
                    : "bg-muted/30 text-[#E8453C] border-border"
                }`}
              >
                {type.replace(/_/g, " ")}
              </button>
            ))}
          </div>
          {disabledTypes.length > 0 && (
            <p className="text-xs text-muted-foreground mt-2">
              {disabledTypes.length} pattern(s) disabled
            </p>
          )}
        </div>

        {/* Add custom pattern */}
        <div className="border-t pt-4">
          <p className="text-sm font-medium mb-2">Add Custom Pattern</p>
          <div className="grid grid-cols-4 gap-2">
            <Input
              placeholder="Regex pattern e.g. (?i)(cust.*id)"
              value={newPattern}
              onChange={(e) => { setNewPattern(e.target.value); setTestResult(null); }}
            />
            <Input
              placeholder="PII type name"
              value={newType}
              onChange={(e) => setNewType(e.target.value)}
            />
            <select
              className="border rounded-md px-3 py-2 text-sm bg-background"
              value={newMasking}
              onChange={(e) => setNewMasking(e.target.value)}
            >
              {["hash", "redact", "null", "partial", "email_mask"].map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
            <Button size="sm" onClick={addCustomPattern} disabled={!newPattern || !newType}>
              <Plus className="h-3.5 w-3.5 mr-1" /> Add
            </Button>
          </div>

          {/* Test pattern */}
          <div className="flex gap-2 mt-2">
            <Input
              placeholder="Test value..."
              value={testInput}
              onChange={(e) => { setTestInput(e.target.value); setTestResult(null); }}
              className="max-w-xs"
            />
            <Button size="sm" variant="outline" onClick={testPattern} disabled={!newPattern || !testInput}>
              <TestTube className="h-3.5 w-3.5 mr-1" /> Test
            </Button>
            {testResult !== null && (
              <Badge className={testResult ? "bg-muted/40 text-foreground" : "bg-red-100 text-red-700"}>
                {testResult ? "Match" : "No match"}
              </Badge>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
