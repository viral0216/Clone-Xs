// @ts-nocheck
import { useState } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
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
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Config Diff</h1>
        <p className="text-gray-500 mt-1">Compare two configurations side by side</p>
      </div>

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
              <div className="flex items-center gap-2 text-green-600">
                <CheckCircle className="h-5 w-5" />
                <p className="font-medium">Configurations are identical</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b bg-gray-50">
                      <th className="text-left py-2 px-3 font-medium">Key</th>
                      <th className="text-left py-2 px-3 font-medium">Value A</th>
                      <th className="text-left py-2 px-3 font-medium">Value B</th>
                      <th className="text-left py-2 px-3 font-medium">Changed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(Array.isArray(differences) ? differences : []).map((diff: any, i: number) => {
                      const changed = diff.changed ?? (diff.value_a !== diff.value_b);
                      return (
                        <tr
                          key={i}
                          className={`border-b ${
                            changed
                              ? "bg-red-50 hover:bg-red-100"
                              : "bg-green-50 hover:bg-green-100"
                          }`}
                        >
                          <td className="py-2 px-3 font-mono text-xs font-medium">{diff.key}</td>
                          <td className="py-2 px-3 font-mono text-xs">
                            {typeof diff.value_a === "object"
                              ? JSON.stringify(diff.value_a)
                              : String(diff.value_a ?? "\u2014")}
                          </td>
                          <td className="py-2 px-3 font-mono text-xs">
                            {typeof diff.value_b === "object"
                              ? JSON.stringify(diff.value_b)
                              : String(diff.value_b ?? "\u2014")}
                          </td>
                          <td className="py-2 px-3">
                            {changed ? (
                              <Badge variant="destructive" className="text-xs">Changed</Badge>
                            ) : (
                              <Badge className="bg-green-600 text-xs">Same</Badge>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
