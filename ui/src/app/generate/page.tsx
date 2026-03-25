// @ts-nocheck
import { useState, useEffect, useRef } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Wand2, Loader2, Copy, Download, FileCode, Boxes, CheckCircle, XCircle, Clock,
} from "lucide-react";

export default function GeneratePage() {
  // Workflow Generation state
  const [workflowFormat, setWorkflowFormat] = useState("json");
  const [workflowLoading, setWorkflowLoading] = useState(false);
  const [workflowResult, setWorkflowResult] = useState<string | null>(null);

  // Terraform / Pulumi state
  const [iacCatalog, setIacCatalog] = useState("");
  const [iacFormat, setIacFormat] = useState("terraform");
  const [iacLoading, setIacLoading] = useState(false);
  const [iacResult, setIacResult] = useState<string | null>(null);
  const [iacJobId, setIacJobId] = useState<string | null>(null);
  const [iacJob, setIacJob] = useState<any>(null);
  const iacPollRef = useRef<NodeJS.Timeout | null>(null);

  const generateWorkflow = async () => {
    setWorkflowLoading(true);
    setWorkflowResult(null);
    try {
      const res = await api.post("/generate/workflow", { format: workflowFormat });
      const content = typeof res === "string" ? res : (res.content ?? res.workflow ?? JSON.stringify(res, null, 2));
      setWorkflowResult(content);
      toast.success("Workflow generated successfully");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setWorkflowLoading(false);
  };

  const generateIaC = async () => {
    setIacLoading(true);
    setIacResult(null);
    setIacJob(null);
    setIacJobId(null);
    try {
      const res = await api.post("/generate/terraform", {
        source_catalog: iacCatalog,
        format: iacFormat,
      });
      if (res.job_id) {
        setIacJobId(res.job_id);
        toast.success(`${iacFormat} generation submitted (Job ${res.job_id})`);
      } else {
        const content = typeof res === "string" ? res : (res.content ?? JSON.stringify(res, null, 2));
        setIacResult(content);
        toast.success("Infrastructure code generated successfully");
        setIacLoading(false);
      }
    } catch (e) {
      toast.error((e as Error).message);
      setIacLoading(false);
    }
  };

  // Poll for IaC job status
  useEffect(() => {
    if (!iacJobId) return;
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${iacJobId}`);
        setIacJob(data);
        if (data.status === "completed") {
          if (iacPollRef.current) clearInterval(iacPollRef.current);
          const content = data.result?.content || JSON.stringify(data.result, null, 2);
          setIacResult(content);
          setIacLoading(false);
          toast.success(`${iacFormat} generated successfully`);
        } else if (data.status === "failed") {
          if (iacPollRef.current) clearInterval(iacPollRef.current);
          setIacLoading(false);
          toast.error(data.error || "Generation failed");
        }
      } catch {}
    };
    poll();
    iacPollRef.current = setInterval(poll, 2000);
    return () => { if (iacPollRef.current) clearInterval(iacPollRef.current); };
  }, [iacJobId]);

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
