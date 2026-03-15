// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import { Copy, Plus, Trash2, Play } from "lucide-react";

interface Destination {
  id: string;
  host: string;
  catalog: string;
  status: "idle" | "queued" | "running" | "completed" | "failed";
  message?: string;
}

let nextId = 1;

export default function MultiClonePage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [destinations, setDestinations] = useState<Destination[]>([
    { id: String(nextId++), host: "", catalog: "", status: "idle" },
  ]);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  const addDest = () => {
    setDestinations([...destinations, { id: String(nextId++), host: "", catalog: "", status: "idle" }]);
  };

  const removeDest = (id: string) => {
    setDestinations(destinations.filter((d) => d.id !== id));
  };

  const updateDest = (id: string, field: "host" | "catalog", value: string) => {
    setDestinations(destinations.map((d) => (d.id === id ? { ...d, [field]: value } : d)));
  };

  const statusVariant = (s: Destination["status"]) => {
    if (s === "completed") return "default";
    if (s === "failed") return "destructive";
    if (s === "running") return "secondary";
    if (s === "queued") return "outline";
    return "outline";
  };

  const startMultiClone = async () => {
    setRunning(true);
    setError("");
    setDestinations((prev) => prev.map((d) => ({ ...d, status: "queued" as const })));
    try {
      const payload = {
        source_catalog: sourceCatalog,
        destinations: destinations.map((d) => ({ host: d.host, catalog: d.catalog })),
      };
      const res = await api.post<{ results?: { catalog: string; status: string; message?: string }[] }>("/multi-clone", payload);
      if (res.results) {
        setDestinations((prev) =>
          prev.map((d) => {
            const match = res.results?.find((r) => r.catalog === d.catalog);
            if (match) return { ...d, status: match.status as Destination["status"], message: match.message };
            return { ...d, status: "completed" };
          })
        );
      } else {
        setDestinations((prev) => prev.map((d) => ({ ...d, status: "completed" })));
      }
    } catch (e) {
      setError((e as Error).message);
      setDestinations((prev) => prev.map((d) => ({ ...d, status: "failed" })));
    }
    setRunning(false);
  };

  const canStart = sourceCatalog && destinations.length > 0 && destinations.every((d) => d.host && d.catalog);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Multi-Clone</h1>
        <p className="text-muted-foreground mt-1">Clone to multiple workspaces simultaneously</p>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      <Card>
        <CardHeader><CardTitle className="text-sm">Source</CardTitle></CardHeader>
        <CardContent>
          <label className="text-sm font-medium text-foreground">Source Catalog</label>
          <CatalogPicker catalog={sourceCatalog} onCatalogChange={setSourceCatalog} showSchema={false} showTable={false} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Destinations</CardTitle>
            <Button size="sm" variant="outline" onClick={addDest}>
              <Plus className="h-4 w-4 mr-1" /> Add Destination
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {destinations.map((d) => (
            <div key={d.id} className="flex gap-3 items-center">
              <div className="flex-1">
                <Input value={d.host} onChange={(e) => updateDest(d.id, "host", e.target.value)} placeholder="https://workspace.cloud.databricks.com" />
              </div>
              <div className="flex-1">
                <Input value={d.catalog} onChange={(e) => updateDest(d.id, "catalog", e.target.value)} placeholder="staging_catalog" />
              </div>
              <Badge variant={statusVariant(d.status)}>{d.status}</Badge>
              <Button size="sm" variant="ghost" onClick={() => removeDest(d.id)} disabled={destinations.length <= 1}>
                <Trash2 className="h-4 w-4 text-muted-foreground" />
              </Button>
            </div>
          ))}
          {destinations.some((d) => d.message) && (
            <div className="mt-2 space-y-1">
              {destinations.filter((d) => d.message).map((d) => (
                <p key={d.id} className="text-xs text-muted-foreground">{d.catalog}: {d.message}</p>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Button onClick={startMultiClone} disabled={!canStart || running} className="w-full">
        <Play className="h-4 w-4 mr-2" />
        {running ? "Cloning..." : "Start Multi-Clone"}
      </Button>
    </div>
  );
}
