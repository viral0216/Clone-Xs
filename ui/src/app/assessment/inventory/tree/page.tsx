// @ts-nocheck
"use client";

import { useState } from "react";
import { ExternalLink, Loader2, TreePine } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import { Button } from "@/components/ui/button";

export default function TreeViewPage() {
  const [loaded, setLoaded] = useState(false);

  return (
    <div className="flex flex-col h-full space-y-2">
      <PageHeader
        title="Tree View"
        icon={TreePine}
        breadcrumbs={["Assessment", "UC Inventory", "Tree View"]}
        description="Collapsible catalog tree with search, type filters, owner filters, and column-level detail panels."
        actions={
          <Button size="sm" variant="outline" onClick={() => window.open("/api/assessment/html/tree", "_blank")}>
            <ExternalLink className="h-4 w-4 mr-1.5" />
            Open fullscreen
          </Button>
        }
      />
      <div className="relative flex-1 rounded-lg border border-border overflow-hidden" style={{ minHeight: "calc(100vh - 160px)" }}>
        {!loaded && (
          <div className="absolute inset-0 flex items-center justify-center bg-background z-10">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <span className="ml-2 text-sm text-muted-foreground">Loading tree view…</span>
          </div>
        )}
        <iframe
          src="/api/assessment/html/tree"
          className="w-full h-full border-0"
          title="UC Catalog Tree View"
          onLoad={() => setLoaded(true)}
          style={{ minHeight: "calc(100vh - 160px)" }}
        />
      </div>
    </div>
  );
}
