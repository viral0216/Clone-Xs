// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { GitBranch, Search, ArrowRight, Loader2 } from "lucide-react";

interface LineageEntry {
  source: string;
  destination: string;
  clone_type: string;
  timestamp: string;
  depth?: number;
  children?: LineageEntry[];
}

export default function LineagePage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [entries, setEntries] = useState<LineageEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [searched, setSearched] = useState(false);

  const trace = async () => {
    setLoading(true);
    setError("");
    setSearched(true);
    try {
      const payload: Record<string, string> = { catalog };
      if (schema && table) payload.table = `${schema}.${table}`;
      else if (table) payload.table = table;
      const res = await api.post<LineageEntry[] | { entries?: LineageEntry[] }>("/lineage", payload);
      const data = Array.isArray(res) ? res : res.entries ?? [];
      setEntries(data);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  const renderEntry = (entry: LineageEntry, depth: number = 0) => (
    <div key={`${entry.source}-${entry.destination}-${depth}`}>
      <div
        className="flex items-center gap-3 py-2 px-3 rounded hover:bg-muted/50"
        style={{ paddingLeft: `${depth * 24 + 12}px` }}
      >
        {depth > 0 && <span className="text-muted-foreground text-xs">|--</span>}
        <span className="font-mono text-sm text-foreground">{entry.source}</span>
        <ArrowRight className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        <span className="font-mono text-sm text-foreground">{entry.destination}</span>
        <Badge variant="secondary" className="text-xs">{entry.clone_type}</Badge>
        <span className="text-xs text-muted-foreground ml-auto">{entry.timestamp}</span>
      </div>
      {entry.children?.map((child, i) => renderEntry(child, depth + 1))}
    </div>
  );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Lineage</h1>
        <p className="text-muted-foreground mt-1">Track data flow from source to cloned tables</p>
      </div>

      <Card>
        <CardContent className="pt-6 space-y-4">
          <CatalogPicker
            catalog={catalog}
            schema={schema}
            table={table}
            onCatalogChange={setCatalog}
            onSchemaChange={setSchema}
            onTableChange={setTable}
            schemaLabel="Schema (optional)"
            tableLabel="Table (optional)"
          />
          <Button onClick={trace} disabled={!catalog || loading}>
            {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}
            Trace Lineage
          </Button>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      {!searched && (
        <div className="text-center py-12 text-muted-foreground">
          <GitBranch className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>Enter a catalog to trace lineage</p>
        </div>
      )}

      {searched && entries.length === 0 && !loading && !error && (
        <div className="text-center py-12 text-muted-foreground">
          <GitBranch className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>No lineage data found</p>
        </div>
      )}

      {entries.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm flex items-center gap-2">
              <GitBranch className="h-4 w-4" />
              Lineage ({entries.length} entr{entries.length === 1 ? "y" : "ies"})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="divide-y divide-border">
              {entries.map((entry, i) => renderEntry(entry, 0))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
