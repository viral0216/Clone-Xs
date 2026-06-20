// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import {
  Database, TreePine, GitBranch, Network, Loader2,
  Table2, FileStack, FunctionSquare, BrainCircuit,
} from "lucide-react";

export default function InventoryPage() {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/assessment/inventory")
      .then(d => setInv(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // UCInventoryResult.to_dict() nests counts under `stats`
  const s = inv?.stats ?? {};
  const stats = [
    { label: "Catalogs", value: s.catalogs ?? 0, icon: Database },
    { label: "Schemas", value: s.schemas ?? 0, icon: FileStack },
    { label: "Tables", value: s.tables ?? 0, icon: Table2 },
    { label: "Views", value: s.views ?? 0, icon: Table2 },
    { label: "Volumes", value: s.volumes ?? 0, icon: FileStack },
    { label: "Functions", value: s.functions ?? 0, icon: FunctionSquare },
    { label: "Models", value: s.registered_models ?? 0, icon: BrainCircuit },
    { label: "Columns", value: s.columns ?? 0, icon: Table2 },
  ];

  const views = [
    {
      href: "/assessment/inventory/tree",
      label: "Tree View",
      icon: TreePine,
      desc: "Collapsible hierarchy with search and type filters",
    },
    {
      href: "/assessment/inventory/sunburst",
      label: "Sunburst View",
      icon: GitBranch,
      desc: "Zoomable concentric rings — click to drill into any level",
    },
    {
      href: "/assessment/inventory/hubspoke",
      label: "Hub & Spoke",
      icon: Network,
      desc: "Radial drill-down with breadcrumbs and jump-to picker",
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="UC Inventory"
        icon={Database}
        breadcrumbs={["Assessment", "UC Inventory"]}
        description="Complete Unity Catalog object tree — catalogs, schemas, tables, volumes, functions, registered models, grants, and column-level detail."
      />

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <span className="ml-2 text-sm text-muted-foreground">Loading inventory…</span>
        </div>
      )}

      {!loading && !inv && (
        <Card>
          <CardContent className="py-12 text-center text-sm text-muted-foreground">
            <Database className="h-10 w-10 mx-auto mb-3 opacity-30" />
            <p className="font-medium mb-1">No UC inventory available</p>
            <p>Re-run the scan with <strong>Include UC Inventory</strong> enabled.</p>
          </CardContent>
        </Card>
      )}

      {!loading && inv && (
        <>
          {/* Stats grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {stats.map(({ label, value, icon: Icon }) => (
              <Card key={label}>
                <CardContent className="pt-4 pb-3 flex items-center gap-3">
                  <Icon className="h-6 w-6 text-primary shrink-0" />
                  <div>
                    <p className="text-xl font-bold">{value.toLocaleString()}</p>
                    <p className="text-xs text-muted-foreground">{label}</p>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {inv.metastore && (
            <div className="text-sm text-muted-foreground bg-muted/30 rounded-md px-4 py-2">
              <span className="font-medium">Metastore:</span>{" "}
              {typeof inv.metastore === "string"
                ? inv.metastore
                : (inv.metastore?.name
                   || inv.metastore?.current_assignment?.metastore_name
                   || inv.metastore?.current_assignment?.metastore_id
                   || "—")}
              {inv.workspace_name && <span> · {inv.workspace_name}</span>}
              {inv.scanned_at && <span className="ml-2">· Scanned {new Date(inv.scanned_at).toLocaleString()}</span>}
            </div>
          )}
        </>
      )}

      {/* Interactive views */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-2">
        {views.map(({ href, label, icon: Icon, desc }) => (
          <Link key={href} to={href}>
            <Card className="hover:bg-accent/30 transition-colors cursor-pointer h-full">
              <CardContent className="pt-5 pb-4 flex flex-col items-center text-center gap-3">
                <Icon className="h-10 w-10 text-primary" />
                <div>
                  <p className="font-semibold text-sm">{label}</p>
                  <p className="text-xs text-muted-foreground mt-1">{desc}</p>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
