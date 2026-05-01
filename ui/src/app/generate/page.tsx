// @ts-nocheck
import { useState } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import { usePageJob } from "@/contexts/JobContext";
import { useDurableJob } from "@/hooks/useDurableJob";
import {
  Wand2, Loader2, Copy, Download, FileCode, Boxes, XCircle, Clock,
} from "lucide-react";

export default function GeneratePage() {
  // Workflow Generation — short-lived synchronous request, but still cached
  // in JobContext so navigating away and back keeps the generated content.
  const workflowJob = usePageJob("generate-workflow");
  const [workflowFormat, setWorkflowFormat] = useState("json");
  const workflowLoading = workflowJob.isRunning;
  const workflowResult: string | null = workflowJob.job?.data ?? null;

  // Terraform / Pulumi — submitted as a server-side job, tracked durably so
  // navigating away during generation doesn't lose the in-flight job.
  const [iacCatalog, setIacCatalog] = useState("");
  const [iacFormat, setIacFormat] = useState("terraform");
  const iacTracker = useDurableJob({
    key: "generate-iac",
    pollUrl: (id) => `/clone/${id}`,
    pollInterval: 2000,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
    notificationTitle: "IaC ready",
    onComplete: (d) => {
      if (d.status === "completed") toast.success(`${iacFormat} generated successfully`);
      else toast.error(d.error || "Generation failed");
    },
  });
  const iacJobId = iacTracker.jobId;
  const iacJob = iacTracker.entry?.data ?? null;
  const iacLoading = iacTracker.isRunning;
  const iacResult: string | null = (() => {
    const r = iacJob?.result;
    if (!r) return null;
    return r.content ?? (typeof r === "string" ? r : JSON.stringify(r, null, 2));
  })();

  const generateWorkflow = async () => {
    try {
      await workflowJob.run({ format: workflowFormat }, async () => {
        const res = await api.post("/generate/workflow", { format: workflowFormat });
        const content = typeof res === "string" ? res : (res.content ?? res.workflow ?? JSON.stringify(res, null, 2));
        toast.success("Workflow generated successfully");
        return content;
      });
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const generateIaC = async () => {
    try {
      await iacTracker.start({ source_catalog: iacCatalog, format: iacFormat }, async () => {
        const res = await api.post("/generate/terraform", {
          source_catalog: iacCatalog,
          format: iacFormat,
        });
        if (res.job_id) {
          toast.success(`${iacFormat} generation submitted (Job ${res.job_id})`);
          return res.job_id;
        }
        // Synchronous response (no job_id) — return whole payload, useDurableJob
        // will treat it as a completed result.
        toast.success("Infrastructure code generated successfully");
        return { result: res };
      });
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success("Copied to clipboard");
  };

  const downloadFile = (content: string, filename: string) => {
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Generate"
        icon={Wand2}
        description="Export clone operations as Terraform HCL, Pulumi code, or Databricks Workflow JSON — ready for CI/CD pipelines and infrastructure-as-code."
        breadcrumbs={["Operations", "Generate"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/dev-tools/terraform/"
        docsLabel="Databricks Terraform provider"
      />

      {/* Workflow Generation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Wand2 className="h-5 w-5" />
            Databricks Workflow
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div>
              <label className="text-sm font-medium">Format</label>
              <select
                value={workflowFormat}
                onChange={(e) => setWorkflowFormat(e.target.value)}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                <option value="json">JSON</option>
                <option value="yaml">YAML</option>
              </select>
            </div>
            <Button onClick={generateWorkflow} disabled={workflowLoading}>
              {workflowLoading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Wand2 className="h-4 w-4 mr-2" />
              )}
              {workflowLoading ? "Generating..." : "Generate Workflow"}
            </Button>
          </div>

          {workflowResult && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Badge variant="outline" className="text-xs">
                  {workflowFormat.toUpperCase()}
                </Badge>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => copyToClipboard(workflowResult)}
                  >
                    <Copy className="h-3 w-3 mr-1" />
                    Copy
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadFile(
                        workflowResult,
                        `workflow.${workflowFormat === "json" ? "json" : "yml"}`
                      )
                    }
                  >
                    <Download className="h-3 w-3 mr-1" />
                    Download
                  </Button>
                </div>
              </div>
              <pre className="bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto text-xs font-mono max-h-[400px] overflow-y-auto">
                {workflowResult}
              </pre>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Terraform / Pulumi Generation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Boxes className="h-5 w-5" />
            Infrastructure as Code
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Catalog Name</label>
              <CatalogPicker catalog={iacCatalog} onCatalogChange={setIacCatalog} showSchema={false} showTable={false} />
            </div>
            <div>
              <label className="text-sm font-medium">Format</label>
              <select
                value={iacFormat}
                onChange={(e) => setIacFormat(e.target.value)}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                <option value="terraform">Terraform</option>
                <option value="pulumi">Pulumi</option>
              </select>
            </div>
            <Button onClick={generateIaC} disabled={!iacCatalog || iacLoading}>
              {iacLoading ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <FileCode className="h-4 w-4 mr-2" />
              )}
              {iacLoading ? "Generating..." : "Generate"}
            </Button>
          </div>

          {/* Job progress */}
          {iacJobId && iacJob && !iacResult && (
            <div className="flex items-center gap-3 p-3 bg-blue-50 rounded-lg">
              {iacJob.status === "running" ? (
                <Loader2 className="h-5 w-5 text-blue-600 animate-spin" />
              ) : iacJob.status === "queued" ? (
                <Clock className="h-5 w-5 text-yellow-600" />
              ) : null}
              <div className="flex-1">
                <p className="text-sm font-medium">
                  {iacFormat === "terraform" ? "Terraform" : "Pulumi"} generation in progress...
                </p>
                <p className="text-xs text-gray-500">Job {iacJobId} — querying catalog metadata</p>
              </div>
              <Badge className="bg-blue-100 text-blue-800">{iacJob.status?.toUpperCase()}</Badge>
            </div>
          )}

          {/* Job failed */}
          {iacJob?.status === "failed" && !iacResult && (
            <div className="flex items-center gap-3 p-3 bg-red-50 rounded-lg">
              <XCircle className="h-5 w-5 text-red-600" />
              <div>
                <p className="text-sm font-medium text-red-800">Generation failed</p>
                <p className="text-xs text-red-600">{iacJob.error}</p>
              </div>
            </div>
          )}

          {iacResult && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Badge variant="outline" className="text-xs">
                  {iacFormat === "terraform" ? "HCL (Terraform)" : "Pulumi"}
                </Badge>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => copyToClipboard(iacResult)}
                  >
                    <Copy className="h-3 w-3 mr-1" />
                    Copy
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadFile(
                        iacResult,
                        iacFormat === "terraform" ? "main.tf" : "index.ts"
                      )
                    }
                  >
                    <Download className="h-3 w-3 mr-1" />
                    Download
                  </Button>
                </div>
              </div>
              <pre className="bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto text-xs font-mono max-h-[400px] overflow-y-auto">
                {iacResult}
              </pre>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
