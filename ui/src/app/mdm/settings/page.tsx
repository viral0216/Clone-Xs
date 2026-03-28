// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { Settings2, Save } from "lucide-react";

export default function MdmSettingsPage() {
  const [settings, setSettings] = useState({
    auto_merge_threshold: 95,
    review_threshold: 80,
    reject_threshold: 50,
    sla_days: 3,
    max_comparison_records: 1000,
    auto_vacuum_after_merge: true,
    require_approval_for_merge: false,
    enable_cross_domain_matching: false,
    notify_on_new_task: true,
    notify_on_sla_breach: true,
    notify_on_auto_merge: false,
    slack_webhook: "",
    retention_days: 365,
    default_entity_type: "Customer",
    default_survivorship: "most_trusted",
  });

  const update = (key: string, value: any) => setSettings(prev => ({ ...prev, [key]: value }));

  const sections = [
    { title: "Matching Thresholds", fields: [
      { key: "auto_merge_threshold", label: "Auto-merge threshold (%)", type: "number", desc: "Pairs scoring above this are merged automatically" },
      { key: "review_threshold", label: "Review threshold (%)", type: "number", desc: "Pairs between this and auto-merge go to stewardship queue" },
      { key: "reject_threshold", label: "Reject threshold (%)", type: "number", desc: "Pairs below this are ignored" },
      { key: "max_comparison_records", label: "Max records per detection run", type: "number", desc: "Limits N² comparisons for performance" },
    ]},
    { title: "Stewardship", fields: [
      { key: "sla_days", label: "SLA deadline (days)", type: "number", desc: "Tasks older than this are marked overdue" },
      { key: "require_approval_for_merge", label: "Require approval for manual merges", type: "toggle", desc: "If enabled, manual merges go through approval workflow" },
    ]},
    { title: "Automation", fields: [
      { key: "auto_vacuum_after_merge", label: "Auto VACUUM after merge", type: "toggle", desc: "Run VACUUM on affected tables after merge operations" },
      { key: "enable_cross_domain_matching", label: "Cross-domain matching", type: "toggle", desc: "Match across entity types (e.g., Customer ↔ Supplier)" },
    ]},
    { title: "Notifications", fields: [
      { key: "notify_on_new_task", label: "New stewardship task", type: "toggle" },
      { key: "notify_on_sla_breach", label: "SLA breach", type: "toggle" },
      { key: "notify_on_auto_merge", label: "Auto-merge completed", type: "toggle" },
      { key: "slack_webhook", label: "Slack webhook URL", type: "text", desc: "Leave empty to disable Slack notifications" },
    ]},
    { title: "Retention & Defaults", fields: [
      { key: "retention_days", label: "Audit log retention (days)", type: "number" },
      { key: "default_entity_type", label: "Default entity type", type: "select", options: ["Customer", "Product", "Supplier", "Employee"] },
      { key: "default_survivorship", label: "Default survivorship strategy", type: "select", options: ["most_trusted", "most_recent", "most_complete", "longest"] },
    ]},
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="MDM Settings" icon={Settings2} breadcrumbs={["MDM", "Settings"]}
        description="Configure matching thresholds, stewardship SLAs, notifications, and default behaviors." />

      {sections.map(section => (
        <Card key={section.title}>
          <CardHeader className="pb-2"><CardTitle className="text-sm">{section.title}</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {section.fields.map(field => (
              <div key={field.key} className="flex items-center justify-between">
                <div>
                  <p className="text-sm">{field.label}</p>
                  {field.desc && <p className="text-[10px] text-muted-foreground">{field.desc}</p>}
                </div>
                {field.type === "number" && (
                  <input type="number" className="w-24 px-2 py-1.5 text-sm bg-muted border border-border rounded-md text-right"
                    value={settings[field.key]} onChange={e => update(field.key, parseInt(e.target.value) || 0)} />
                )}
                {field.type === "text" && (
                  <input className="w-64 px-2 py-1.5 text-sm bg-muted border border-border rounded-md"
                    value={settings[field.key]} onChange={e => update(field.key, e.target.value)} placeholder="https://..." />
                )}
                {field.type === "toggle" && (
                  <button onClick={() => update(field.key, !settings[field.key])}
                    className={`w-10 h-5 rounded-full transition-colors ${settings[field.key] ? "bg-[#E8453C]" : "bg-muted"}`}>
                    <div className={`h-4 w-4 rounded-full bg-white shadow transition-transform ${settings[field.key] ? "translate-x-5" : "translate-x-0.5"}`} />
                  </button>
                )}
                {field.type === "select" && (
                  <select className="px-2 py-1.5 text-sm bg-muted border border-border rounded-md"
                    value={settings[field.key]} onChange={e => update(field.key, e.target.value)}>
                    {field.options?.map(o => <option key={o}>{o}</option>)}
                  </select>
                )}
              </div>
            ))}
          </CardContent>
        </Card>
      ))}

      <Button className="w-full"><Save className="h-4 w-4 mr-2" /> Save Settings</Button>
    </div>
  );
}
