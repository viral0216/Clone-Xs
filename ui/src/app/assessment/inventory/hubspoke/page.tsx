// @ts-nocheck
"use client";

import { useState } from "react";
import { ExternalLink, Loader2, Network } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import { Button } from "@/components/ui/button";

export default function HubSpokeViewPage() {
  const [loaded, setLoaded] = useState(false);

  return (
    <div className="flex flex-col h-full space-y-2">
      <PageHeader
        title="Hub & Spoke View"
        icon={Network}
        breadcrumbs={["Assessment", "UC Inventory", "Hub & Spoke"]}
        description="Radial drill-down with breadcrumb navigation and a jump-to picker for large catalogs."
        actions={
          <Button size="sm" variant="outline" onClick={() => window.open("/api/assessment/html/hubspoke", "_blank")}>
            <ExternalLink className="h-4 w-4 mr-1.5" />
            Open fullscreen
          </Button>
        }
      />
      <div className="relative flex-1 rounded-lg border border-border overflow-hidden" style={{ minHeight: "calc(100vh - 160px)" }}>
        {!loaded && (
          <div className="absolute inset-0 flex items-center justify-center bg-background z-10">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <span className="ml-2 text-sm text-muted-foreground">Loading hub & spoke view…</span>
          </div>
        )}
        <iframe
          src="/api/assessment/html/hubspoke"
          className="w-full h-full border-0"
          title="UC Catalog Hub and Spoke View"
          onLoad={() => setLoaded(true)}
          style={{ minHeight: "calc(100vh - 160px)" }}
        />
      </div>
    </div>
  );
}
