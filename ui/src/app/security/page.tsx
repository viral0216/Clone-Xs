// @ts-nocheck
import { Card, CardContent } from "@/components/ui/card";
import PageHeader from "@/components/PageHeader";
import { Link } from "react-router-dom";
import { Shield, Fingerprint, CheckSquare, ArrowRight } from "lucide-react";

export default function SecurityOverview() {
  return (
    <div className="space-y-4">
      <PageHeader
        title="Security Overview"
        icon={Shield}
        breadcrumbs={["Security"]}
        description="Data protection, compliance validation, and pre-clone security checks."
      />

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { href: "/security/pii", label: "PII Scanner", desc: "Detect personally identifiable information across catalogs", icon: Fingerprint },
          { href: "/security/compliance", label: "Compliance", desc: "Generate governance and compliance reports", icon: Shield },
          { href: "/security/preflight", label: "Preflight Checks", desc: "Validate permissions and config before cloning", icon: CheckSquare },
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
