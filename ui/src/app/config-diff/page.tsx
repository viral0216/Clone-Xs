// @ts-nocheck
import { useState } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import {
  GitCompareArrows, Loader2, Upload, CheckCircle, XCircle,
} from "lucide-react";

export default function ConfigDiffPage() {
  const [configA, setConfigA] = useState("");
  const [configB, setConfigB] = useState("");
  const [loading, setLoading] = useState(false);
  const [differences, setDifferences] = useState<any[] | null>(null);
  const [profilesLoading, setProfilesLoading] = useState(false);
  const [profiles, setProfiles] = useState<string[]>([]);

  const loadProfiles = async () => {
    setProfilesLoading(true);
    try {
      const res = await api.get<{ profiles: string[] }>("/config/profiles");
      setProfiles(res.profiles || []);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setProfilesLoading(false);
  };

  const loadProfileConfig = async (profile: string, target: "a" | "b") => {
    try {
      const res = await api.get<Record<string, unknown>>(`/config?profile=${profile}`);
      const yaml = JSON.stringify(res, null, 2);
      if (target === "a") setConfigA(yaml);
      else setConfigB(yaml);
      toast.success(`Loaded profile "${profile}" into Config ${target.toUpperCase()}`);
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const loadFromFile = (target: "a" | "b") => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".yaml,.yml,.json";
    input.onchange = (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        const text = ev.target?.result as string;
        if (target === "a") setConfigA(text);
        else setConfigB(text);
      };
      reader.readAsText(file);
    };
    input.click();
  };

  const compare = async () => {
    setLoading(true);
    setDifferences(null);
    try {
      let parsedA: any;
      let parsedB: any;
      try {
        parsedA = JSON.parse(configA);
      } catch {
        parsedA = configA;
      }
      try {
        parsedB = JSON.parse(configB);
      } catch {
        parsedB = configB;
      }
      const res = await api.post("/config/diff", {
        config_a: parsedA,
        config_b: parsedB,
      });
      setDifferences(res.differences ?? res.diffs ?? res);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Config Diff"
        icon={GitCompareArrows}
        description="Side-by-side comparison of two clone configurations — highlights differences in settings, permissions, and options between config files or profiles."
        breadcrumbs={["Discovery", "Config Diff"]}
      />

      {/* Profile Loader */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <Upload className="h-4 w-4" />
            Load from Profiles
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2 flex-wrap">
            <Button variant="outline" size="sm" onClick={loadProfiles} disabled={profilesLoading}>
              {profilesLoading ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : null}
              {profilesLoading ? "Loading..." : "Load Profiles"}
            </Button>
            {profiles.map((p) => (
              <div key={p} className="flex items-center gap-1">
                <Badge variant="outline" className="text-xs">{p}</Badge>
                <Button variant="ghost" size="sm" className="text-xs h-6 px-2" onClick={() => loadProfileConfig(p, "a")}>
                  &rarr; A
                </Button>
                <Button variant="ghost" size="sm" className="text-xs h-6 px-2" onClick={() => loadProfileConfig(p, "b")}>
                  &rarr; B
                </Button>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Side by Side Editors */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">Config A</CardTitle>
            <Button variant="outline" size="sm" onClick={() => loadFromFile("a")}>
              <Upload className="h-3 w-3 mr-1" />
              Load File
            </Button>
          </CardHeader>
          <CardContent>
            <Textarea
              className="font-mono text-sm min-h-[300px]"
              value={configA}
              onChange={(e) => setConfigA(e.target.value)}
              placeholder="Paste YAML or JSON config here..."
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">Config B</CardTitle>
            <Button variant="outline" size="sm" onClick={() => loadFromFile("b")}>
              <Upload className="h-3 w-3 mr-1" />
              Load File
            </Button>
          </CardHeader>
          <CardContent>
            <Textarea
              className="font-mono text-sm min-h-[300px]"
              value={configB}
              onChange={(e) => setConfigB(e.target.value)}
              placeholder="Paste YAML or JSON config here..."
            />
          </CardContent>
        </Card>
      </div>

      {/* Compare Button */}
      <div className="flex justify-center">
        <Button
          onClick={compare}
          disabled={!configA || !configB || loading}
          size="lg"
        >
          {loading ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <GitCompareArrows className="h-4 w-4 mr-2" />
          )}
          {loading ? "Comparing..." : "Compare"}
        </Button>
      </div>

      {/* Diff Results */}
      {differences && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <GitCompareArrows className="h-5 w-5" />
              Differences
            </CardTitle>
          </CardHeader>
          <CardContent>
            {Array.isArray(differences) && differences.length === 0 ? (
              <div className="flex items-center gap-2 text-foreground">
                <CheckCircle className="h-5 w-5" />
                <p className="font-medium">Configurations are identical</p>
              </div>
            ) : (
              <DataTable
                data={(Array.isArray(differences) ? differences : []).map((diff: any) => ({
                  ...diff,
                  _changed: diff.changed ?? (diff.value_a !== diff.value_b),
                }))}
                columns={[
                  { key: "key", label: "Key", sortable: true, render: (v: string) => <span className="font-mono text-xs font-medium">{v}</span> },
                  {
                    key: "value_a",
                    label: "Value A",
                    sortable: true,
                    render: (v: any) => (
                      <span className="font-mono text-xs">
                        {typeof v === "object" ? JSON.stringify(v) : String(v ?? "\u2014")}
                      </span>
                    ),
                  },
                  {
                    key: "value_b",
                    label: "Value B",
                    sortable: true,
                    render: (v: any) => (
                      <span className="font-mono text-xs">
                        {typeof v === "object" ? JSON.stringify(v) : String(v ?? "\u2014")}
                      </span>
                    ),
                  },
                  {
                    key: "_changed",
                    label: "Changed",
                    sortable: true,
                    render: (v: boolean) =>
                      v ? (
                        <Badge variant="destructive" className="text-xs">Changed</Badge>
                      ) : (
                        <Badge className="bg-foreground text-xs">Same</Badge>
                      ),
                  },
                ] as Column[]}
                searchable
                searchKeys={["key", "value_a", "value_b"]}
                pageSize={25}
                compact
                tableId="config-diff-differences"
                rowClassName={(row: any) =>
                  row._changed ? "bg-red-50 hover:bg-red-100" : "bg-muted/20 hover:bg-muted/40"
                }
                emptyMessage="No differences found."
              />
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
