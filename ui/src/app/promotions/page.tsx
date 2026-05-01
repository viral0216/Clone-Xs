// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { GitBranch, ArrowRight, Lock, Eye, Play } from "lucide-react";

type Step = {
  name: string;
  source_catalog: string;
  dest_catalog: string;
  auto_mask_pii: boolean;
  require_approval: boolean;
};

type Plan = {
  key: string;
  name: string;
  description: string;
  steps: Step[];
};

type Hop = {
  name: string;
  source_catalog: string;
  dest_catalog: string;
  job_id: string | null;
  status: string;
};

export default function PromotionsPage() {
  const [plans, setPlans] = useState<Plan[]>([]);
  const [prefix, setPrefix] = useState("");
  const [warehouseId, setWarehouseId] = useState("");
  const [running, setRunning] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, Hop[]>>({});

  useEffect(() => {
    api.get<Plan[]>("/promotions/plans")
      .then(setPlans)
      .catch(() => setPlans([]));
  }, []);

  async function runPlan(key: string) {
    if (!prefix.trim() || !warehouseId.trim()) {
      toast.error("Catalog prefix and warehouse_id are required");
      return;
    }
    setRunning(key);
    try {
      const r = await api.post<{ hops: Hop[] }>(`/promotions/plans/${key}/run`, {
        prefix: prefix.trim(),
        warehouse_id: warehouseId.trim(),
        max_workers: 4,
      });
      setResults(prev => ({ ...prev, [key]: r.hops }));
      toast.success(`Plan ${key} started — first hop submitted`);
    } catch (e: any) {
      toast.error(e?.message ?? "Failed to start plan");
    } finally {
      setRunning(null);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Promotion Plans"
        icon={GitBranch}
        breadcrumbs={["Operations", "Promotions"]}
        description="Multi-hop catalog promotions (prod → staging → dev) with mandatory PII masking and approval gates per hop."
      />

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Plan parameters</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2">
          <div>
            <label className="text-xs font-medium block mb-1">
              Catalog name prefix
            </label>
            <Input
              placeholder="supplier_portal"
              value={prefix}
              onChange={e => setPrefix(e.target.value)}
            />
            <p className="text-xs text-muted-foreground mt-1">
              Substituted into <code>{"{prefix}"}</code> on each step. Example
              yields <code>supplier_portal_prod</code>.
            </p>
          </div>
          <div>
            <label className="text-xs font-medium block mb-1">
              SQL warehouse ID
            </label>
            <Input
              placeholder="abcd1234..."
              value={warehouseId}
              onChange={e => setWarehouseId(e.target.value)}
            />
            <p className="text-xs text-muted-foreground mt-1">
              Used by every hop unless the step overrides it.
            </p>
          </div>
        </CardContent>
      </Card>

      {plans.length === 0 && (
        <p className="text-muted-foreground text-sm">No plans available.</p>
      )}

      {plans.map(p => (
        <Card key={p.key}>
          <CardHeader>
            <div className="flex items-start justify-between gap-3">
              <div>
                <CardTitle className="text-base">{p.name}</CardTitle>
                <p className="text-sm text-muted-foreground mt-1">{p.description}</p>
              </div>
              <Button
                size="sm"
                onClick={() => runPlan(p.key)}
                disabled={running === p.key}
                className="bg-foreground hover:bg-gray-700 text-white"
              >
                <Play className="h-3 w-3 mr-1" />
                {running === p.key ? "Submitting..." : "Run plan"}
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              {p.steps.map((s, i) => (
                <div key={i} className="flex items-center gap-2">
                  <div className="border rounded-md px-3 py-2 bg-gray-50 text-xs space-y-1">
                    <div className="font-mono">
                      {s.source_catalog} → {s.dest_catalog}
                    </div>
                    <div className="flex gap-1">
                      {s.auto_mask_pii && (
                        <Badge variant="outline" className="text-[10px] gap-1">
                          <Eye className="h-2.5 w-2.5" /> mask PII
                        </Badge>
                      )}
                      {s.require_approval && (
                        <Badge variant="outline" className="text-[10px] gap-1">
                          <Lock className="h-2.5 w-2.5" /> approval
                        </Badge>
                      )}
                    </div>
                  </div>
                  {i < p.steps.length - 1 && (
                    <ArrowRight className="h-4 w-4 text-muted-foreground" />
                  )}
                </div>
              ))}
            </div>

            {results[p.key] && (
              <div className="border-t pt-3 space-y-2">
                <p className="text-xs font-medium">Last run</p>
                <div className="space-y-1">
                  {results[p.key].map((h, i) => (
                    <div key={i} className="flex items-center justify-between text-xs">
                      <span className="font-mono">
                        {h.source_catalog} → {h.dest_catalog}
                      </span>
                      <span className="flex items-center gap-2">
                        {h.job_id && (
                          <a
                            href={`/clone?jobId=${h.job_id}`}
                            className="underline text-muted-foreground"
                          >
                            {h.job_id}
                          </a>
                        )}
                        <Badge variant="outline" className="text-[10px]">
                          {h.status}
                        </Badge>
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
