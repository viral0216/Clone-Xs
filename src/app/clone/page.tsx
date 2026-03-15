// @ts-nocheck
"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useStartClone, useCloneJobs } from "@/hooks/useApi";
import { Copy, Play, Eye, CheckCircle, XCircle } from "lucide-react";

type Step = "source" | "options" | "preview" | "execute";

export default function ClonePage() {
  const [step, setStep] = useState<Step>("source");
  const [config, setConfig] = useState({
    source_catalog: "",
    destination_catalog: "",
    clone_type: "DEEP" as "DEEP" | "SHALLOW",
    load_type: "FULL" as "FULL" | "INCREMENTAL",
    dry_run: false,
    max_workers: 4,
    parallel_tables: 1,
    copy_permissions: true,
    copy_ownership: true,
    copy_tags: true,
    copy_properties: true,
    copy_security: true,
    copy_constraints: true,
    copy_comments: true,
    enable_rollback: true,
    validate_after_clone: false,
    exclude_schemas: ["information_schema", "default"],
    include_schemas: [] as string[],
    location: "",
  });

  const startClone = useStartClone();
  const jobs = useCloneJobs();

  const handleClone = (dryRun: boolean) => {
    startClone.mutate({ ...config, dry_run: dryRun });
    setStep("execute");
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Clone Catalog</h1>
        <p className="text-gray-500 mt-1">Clone a Unity Catalog catalog with full control</p>
      </div>

      {/* Step indicators */}
      <div className="flex gap-2">
        {(["source", "options", "preview", "execute"] as Step[]).map((s, i) => (
          <Badge
            key={s}
            variant={step === s ? "default" : "outline"}
            className={`cursor-pointer ${step === s ? "bg-blue-600" : ""}`}
            onClick={() => setStep(s)}
          >
            {i + 1}. {s.charAt(0).toUpperCase() + s.slice(1)}
          </Badge>
        ))}
      </div>

      {/* Step 1: Source & Destination */}
      {step === "source" && (
        <Card>
          <CardHeader>
            <CardTitle>Source & Destination</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="text-sm font-medium">Source Catalog</label>
              <Input
                placeholder="e.g. production"
                value={config.source_catalog}
                onChange={(e) => setConfig({ ...config, source_catalog: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input
                placeholder="e.g. staging"
                value={config.destination_catalog}
                onChange={(e) => setConfig({ ...config, destination_catalog: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Storage Location (optional)</label>
              <Input
                placeholder="abfss://container@storage.dfs.core.windows.net/path"
                value={config.location}
                onChange={(e) => setConfig({ ...config, location: e.target.value })}
              />
              <p className="text-xs text-gray-400 mt-1">Required if workspace uses Default Storage</p>
            </div>
            <Button onClick={() => setStep("options")} disabled={!config.source_catalog || !config.destination_catalog}>
              Next: Options
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Step 2: Clone Options */}
      {step === "options" && (
        <Card>
          <CardHeader>
            <CardTitle>Clone Options</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium">Clone Type</label>
                <div className="flex gap-2 mt-1">
                  {(["DEEP", "SHALLOW"] as const).map((t) => (
                    <Button
                      key={t}
                      variant={config.clone_type === t ? "default" : "outline"}
                      size="sm"
                      onClick={() => setConfig({ ...config, clone_type: t })}
                    >
                      {t}
                    </Button>
                  ))}
                </div>
              </div>
              <div>
                <label className="text-sm font-medium">Load Type</label>
                <div className="flex gap-2 mt-1">
                  {(["FULL", "INCREMENTAL"] as const).map((t) => (
                    <Button
                      key={t}
                      variant={config.load_type === t ? "default" : "outline"}
                      size="sm"
                      onClick={() => setConfig({ ...config, load_type: t })}
                    >
                      {t}
                    </Button>
                  ))}
                </div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium">Max Workers (schemas)</label>
                <Input
                  type="number" min={1} max={16}
                  value={config.max_workers}
                  onChange={(e) => setConfig({ ...config, max_workers: parseInt(e.target.value) || 4 })}
                />
              </div>
              <div>
                <label className="text-sm font-medium">Parallel Tables</label>
                <Input
                  type="number" min={1} max={8}
                  value={config.parallel_tables}
                  onChange={(e) => setConfig({ ...config, parallel_tables: parseInt(e.target.value) || 1 })}
                />
              </div>
            </div>

            {/* Toggles */}
            <div>
              <label className="text-sm font-medium mb-2 block">Copy Options</label>
              <div className="grid grid-cols-3 gap-2">
                {([
                  ["copy_permissions", "Permissions"],
                  ["copy_ownership", "Ownership"],
                  ["copy_tags", "Tags"],
                  ["copy_properties", "Properties"],
                  ["copy_security", "Security"],
                  ["copy_constraints", "Constraints"],
                  ["copy_comments", "Comments"],
                  ["enable_rollback", "Rollback"],
                  ["validate_after_clone", "Validate"],
                ] as const).map(([key, label]) => (
                  <label key={key} className="flex items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={config[key] as boolean}
                      onChange={(e) => setConfig({ ...config, [key]: e.target.checked })}
                    />
                    {label}
                  </label>
                ))}
              </div>
            </div>

            <div className="flex gap-2">
              <Button variant="outline" onClick={() => setStep("source")}>Back</Button>
              <Button onClick={() => setStep("preview")}>Next: Preview</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Step 3: Preview */}
      {step === "preview" && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Eye className="h-5 w-5" />
              Preview Clone Configuration
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm space-y-1">
              <p>clone-catalog clone \</p>
              <p>  --source {config.source_catalog} --dest {config.destination_catalog} \</p>
              <p>  --clone-type {config.clone_type} --load-type {config.load_type} \</p>
              <p>  --max-workers {config.max_workers} --parallel-tables {config.parallel_tables} \</p>
              {!config.copy_permissions && <p>  --no-permissions \</p>}
              {!config.copy_tags && <p>  --no-tags \</p>}
              {!config.copy_security && <p>  --no-security \</p>}
              {config.enable_rollback && <p>  --enable-rollback \</p>}
              {config.validate_after_clone && <p>  --validate \</p>}
              {config.location && <p>  --location &quot;{config.location}&quot; \</p>}
              <p>  --progress</p>
            </div>

            <div className="flex gap-2">
              <Button variant="outline" onClick={() => setStep("options")}>Back</Button>
              <Button variant="outline" onClick={() => handleClone(true)}>
                <Eye className="h-4 w-4 mr-2" />
                Dry Run
              </Button>
              <Button onClick={() => handleClone(false)} className="bg-blue-600 hover:bg-blue-700">
                <Play className="h-4 w-4 mr-2" />
                Execute Clone
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Step 4: Execution */}
      {step === "execute" && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Copy className="h-5 w-5" />
              Clone Execution
            </CardTitle>
          </CardHeader>
          <CardContent>
            {startClone.isPending && (
              <p className="text-gray-500">Submitting clone job...</p>
            )}
            {startClone.isSuccess && (
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-green-600" />
                  <span>Clone job submitted successfully</span>
                </div>
                <pre className="bg-gray-100 p-3 rounded text-sm">
                  {JSON.stringify(startClone.data, null, 2)}
                </pre>
              </div>
            )}
            {startClone.isError && (
              <div className="flex items-center gap-2 text-red-600">
                <XCircle className="h-5 w-5" />
                <span>Error: {(startClone.error as Error).message}</span>
              </div>
            )}
            <div className="mt-4">
              <Button variant="outline" onClick={() => setStep("source")}>
                New Clone
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
