// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useNavigate } from "react-router-dom";
import { LayoutTemplate, ArrowRight } from "lucide-react";

interface Template {
  name: string;
  description: string;
  clone_type?: string;
  validate?: boolean;
  dry_run?: boolean;
  parallel?: boolean;
  settings?: Record<string, unknown>;
}

const FALLBACK_TEMPLATES: Template[] = [
  { name: "production_mirror", description: "Full mirror of production catalog for DR or reporting", clone_type: "SHALLOW", validate: true },
  { name: "dev_sandbox", description: "Lightweight dev copy with schema and sample data", clone_type: "SHALLOW", validate: false, dry_run: true },
  { name: "dr_copy", description: "Disaster recovery copy with full validation", clone_type: "DEEP", validate: true },
  { name: "schema_only", description: "Clone schema structure without data", clone_type: "SHALLOW", validate: false },
  { name: "incremental_sync", description: "Sync only changed tables since last clone", clone_type: "SHALLOW", validate: true, parallel: true },
  { name: "full_refresh", description: "Complete deep clone replacing all destination data", clone_type: "DEEP", validate: true, parallel: true },
];

export default function TemplatesPage() {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const res = await api.get<Template[]>("/templates");
        setTemplates(Array.isArray(res) && res.length > 0 ? res : FALLBACK_TEMPLATES);
      } catch {
        setTemplates(FALLBACK_TEMPLATES);
        setError("Using built-in templates (API unavailable)");
      }
      setLoading(false);
    };
    load();
  }, []);

  const useTemplate = (t: Template) => {
    const params = new URLSearchParams();
    if (t.clone_type) params.set("clone_type", t.clone_type);
    if (t.validate != null) params.set("validate", String(t.validate));
    if (t.dry_run != null) params.set("dry_run", String(t.dry_run));
    if (t.parallel != null) params.set("parallel", String(t.parallel));
    navigate(`/clone?${params.toString()}`);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-foreground">Clone Templates</h1>
        <p className="text-muted-foreground mt-1">12 pre-built clone recipes — Production Mirror, Dev Sandbox, DR Copy, Compliance Snapshot, and more. Each template pre-fills optimal settings for its use case.</p>
        <p className="text-xs text-muted-foreground mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-create-table-clone" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Clone best practices</a>
        </p>
      </div>

      {error && (
        <Card className="border-yellow-500/50">
          <CardContent className="pt-6 text-yellow-600 text-sm">{error}</CardContent>
        </Card>
      )}

      {templates.length === 0 && !loading ? (
        <div className="text-center py-12 text-muted-foreground">
          <LayoutTemplate className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>No templates available</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {templates.map((t) => (
            <Card key={t.name} className="flex flex-col">
              <CardHeader>
                <CardTitle className="text-base font-semibold text-foreground">{t.name.replace(/_/g, " ")}</CardTitle>
              </CardHeader>
              <CardContent className="flex-1 flex flex-col justify-between gap-4">
                <div className="space-y-3">
                  <p className="text-sm text-muted-foreground">{t.description}</p>
                  <div className="flex flex-wrap gap-2">
                    {t.clone_type && <Badge variant="secondary">{t.clone_type}</Badge>}
                    {t.validate && <Badge variant="outline">Validated</Badge>}
                    {t.dry_run && <Badge variant="outline">Dry Run</Badge>}
                    {t.parallel && <Badge variant="outline">Parallel</Badge>}
                  </div>
                </div>
                <Button size="sm" onClick={() => useTemplate(t)} className="w-full">
                  Use Template <ArrowRight className="h-4 w-4 ml-1" />
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
