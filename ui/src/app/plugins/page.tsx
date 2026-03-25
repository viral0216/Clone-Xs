// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Loader2, XCircle, Puzzle, ToggleLeft, ToggleRight } from "lucide-react";

export default function PluginsPage() {
  const [plugins, setPlugins] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [toggling, setToggling] = useState<string | null>(null);

  async function loadPlugins() {
    try {
      const data = await api.get<any>("/plugins");
      setPlugins(data?.plugins || data || []);
      setError("");
    } catch {
      setPlugins([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadPlugins(); }, []);

  async function togglePlugin(plugin: any) {
    const id = plugin.id || plugin.name;
    setToggling(id);
    try {
      const enabled = plugin.enabled ?? plugin.status === "enabled";
      await api.post(`/plugins/${id}/${enabled ? "disable" : "enable"}`);
      await loadPlugins();
    } catch (e: any) {
      setError(e.message || "Failed to toggle plugin");
    } finally {
      setToggling(null);
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Plugins"
        icon={Puzzle}
        description="Plugin system for extending Clone-Xs — add custom pre/post-clone hooks, validation rules, notification channels, and metadata transformations."
        breadcrumbs={["Management", "Plugins"]}
      />

      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Loading plugins...</p>
          </CardContent>
        </Card>
      )}

      {!loading && plugins.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12 text-muted-foreground">
            <Puzzle className="h-10 w-10 mx-auto mb-3 opacity-40" />
            <p>No plugins installed</p>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {plugins.map((plugin: any) => {
          const id = plugin.id || plugin.name;
          const enabled = plugin.enabled ?? plugin.status === "enabled";
          const isToggling = toggling === id;
          return (
            <Card key={id} className="bg-card border-border">
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Puzzle className="h-4 w-4 text-muted-foreground" />
                    {plugin.name || "Unnamed"}
                  </div>
                  <Badge className={enabled ? "bg-foreground text-white" : ""} variant={enabled ? "default" : "outline"}>
                    {enabled ? "Enabled" : "Disabled"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                {plugin.description && (
                  <p className="text-sm text-muted-foreground">{plugin.description}</p>
                )}
                <div className="flex items-center justify-between">
                  {plugin.version && (
                    <Badge variant="outline" className="text-xs">v{plugin.version}</Badge>
                  )}
                  <Button
                    size="sm"
                    variant={enabled ? "outline" : "default"}
                    onClick={() => togglePlugin(plugin)}
                    disabled={isToggling}
                  >
                    {isToggling ? (
                      <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                    ) : enabled ? (
                      <ToggleRight className="h-3 w-3 mr-1" />
                    ) : (
                      <ToggleLeft className="h-3 w-3 mr-1" />
                    )}
                    {enabled ? "Disable" : "Enable"}
                  </Button>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
