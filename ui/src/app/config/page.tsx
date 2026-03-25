import { useState, useEffect } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { Wrench, Save, Download, RefreshCw } from "lucide-react";

export default function ConfigPage() {
  const [yaml, setYaml] = useState("");
  const [profiles, setProfiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    loadConfig();
  }, []);

  const loadConfig = async () => {
    setLoading(true);
    try {
      const config = await api.get<Record<string, unknown>>("/config");
      setYaml(JSON.stringify(config, null, 2));
      const profResult = await api.get<{ profiles: string[] }>("/config/profiles");
      setProfiles(profResult.profiles);
    } catch {
      setYaml("# Could not load config\n# Check API connection");
    }
    setLoading(false);
  };

  const saveConfig = async () => {
    try {
      await api.put("/config", { yaml_content: yaml });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Configuration</h1>
          <p className="text-gray-500 mt-1">View and edit clone configuration — source/destination catalogs, warehouse selection, exclude schemas, copy options, parallelism, and audit settings.</p>
          <p className="text-xs text-gray-400 mt-1">
            <a href="https://learn.microsoft.com/en-us/azure/databricks/compute/configure" target="_blank" rel="noopener noreferrer" className="text-[#E8453C] hover:underline">Cluster configuration</a>
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={loadConfig} disabled={loading}>
            <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
            Reload
          </Button>
          <Button onClick={saveConfig}>
            <Save className="h-4 w-4 mr-2" />
            {saved ? "Saved!" : "Save"}
          </Button>
        </div>
      </div>

      {/* Profiles */}
      {profiles.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Config Profiles</CardTitle>
          </CardHeader>
          <CardContent className="flex gap-2">
            {profiles.map((p) => (
              <Badge key={p} variant="outline" className="cursor-pointer hover:bg-gray-100">
                {p}
              </Badge>
            ))}
          </CardContent>
        </Card>
      )}

      {/* YAML Editor */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Wrench className="h-5 w-5" />
            Config Editor
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Textarea
            className="font-mono text-sm min-h-[500px]"
            value={yaml}
            onChange={(e) => setYaml(e.target.value)}
            placeholder="Loading config..."
          />
        </CardContent>
      </Card>
    </div>
  );
}
