// @ts-nocheck
import { Card, CardContent } from "@/components/ui/card";
import PageHeader from "@/components/PageHeader";
import { Link } from "react-router-dom";
import { Zap, GitBranch, Briefcase, LayoutTemplate, ArrowRight } from "lucide-react";

export default function AutomationOverview() {
  return (
    <div className="space-y-4">
      <PageHeader
        title="Automation Overview"
        icon={Zap}
        breadcrumbs={["Automation"]}
        description="Pipelines, job scheduling, and reusable templates for automated catalog operations."
      />

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { href: "/automation/pipelines", label: "Pipelines", desc: "Chain operations into reusable workflows", icon: GitBranch },
          { href: "/automation/templates", label: "Templates", desc: "Pre-built clone configurations and recipes", icon: LayoutTemplate },
          { href: "/automation/create-job", label: "Create Job", desc: "Schedule persistent Databricks clone jobs", icon: Briefcase },
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
