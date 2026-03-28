// @ts-nocheck
import { Card, CardContent } from "@/components/ui/card";
import PageHeader from "@/components/PageHeader";
import { Link } from "react-router-dom";
import { Server, Globe, Share2, Radio, ArrowRight } from "lucide-react";

export default function InfrastructureOverview() {
  return (
    <div className="space-y-4">
      <PageHeader
        title="Infrastructure Overview"
        icon={Server}
        breadcrumbs={["Infrastructure"]}
        description="Warehouse management, cross-workspace federation, and data sharing."
      />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {[
          { href: "/infrastructure/warehouse", label: "Warehouse", desc: "View, start, and manage SQL warehouses", icon: Server },
          { href: "/infrastructure/lakehouse-monitor", label: "Lakehouse Monitor", desc: "Monitor lakehouse table quality and metrics", icon: Radio },
          { href: "/infrastructure/federation", label: "Federation", desc: "Cross-workspace catalog federation", icon: Globe },
          { href: "/infrastructure/delta-sharing", label: "Delta Sharing", desc: "Share data across organizations", icon: Share2 },
        ].map((link) => (
          <Link key={link.href} to={link.href}>
            <Card className="hover:border-border dark:hover:border-border transition-colors cursor-pointer h-full">
              <CardContent className="pt-4 flex items-start gap-3">
                <link.icon className="h-5 w-5 mt-0.5 text-muted-foreground" />
                <div>
                  <p className="text-sm font-medium">{link.label}</p>
                  <p className="text-xs text-muted-foreground">{link.desc}</p>
                </div>
                <ArrowRight className="h-4 w-4 text-muted-foreground ml-auto mt-1" />
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
