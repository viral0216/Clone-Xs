// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import PageHeader from "@/components/PageHeader";
import { Store } from "lucide-react";

export default function MarketplacePage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Marketplace"
        description="Discover and install connectors, templates, and extensions"
        icon={Store}
      />

      <Card>
        <CardHeader>
          <CardTitle>Coming soon</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          The Marketplace is under construction. Browse curated integrations,
          data products, and community templates here.
        </CardContent>
      </Card>
    </div>
  );
}
