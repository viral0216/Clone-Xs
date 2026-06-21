// @ts-nocheck
"use client";
import { BarChart2 } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

export default function SqlPage() {
  return (
    <WorkspaceCategoryPage
      title="SQL & Analytics"
      description="SQL warehouses, Lakeview dashboards, Genie spaces, and associated configuration."
      icon={BarChart2}
      breadcrumb="SQL"
      findingCategories={["SQL Warehouses", "Data Architecture", "Data Quality", "Governance Data Quality"]}
      sections={[
        {
          title: "SQL Warehouses",
          resourceType: "warehouses",
          emptyMsg: "No SQL warehouses found.",
          getLink: (r, ws) => r.id ? `${ws}/sql/warehouses/${r.id}` : null,
          columns: [
            { key: "id",               label: "ID",            width: "100px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.id}</span> },
            { key: "name",             label: "Name",          render: r => <span className="font-medium">{r.name}</span> },
            { key: "cluster_size",     label: "Size",          render: r => <span className="text-muted-foreground">{r.cluster_size ?? "—"}</span> },
            { key: "state",            label: "State",         render: r => <span className={`font-medium ${r.state === "RUNNING" ? "text-green-600" : r.state === "STOPPED" ? "text-muted-foreground" : "text-yellow-600"}`}>{r.state ?? "—"}</span> },
            { key: "channel",          label: "Channel",       render: r => <span className="text-muted-foreground">{r.channel?.name ?? "—"}</span> },
            { key: "auto_stop_mins",   label: "Auto-stop",     render: r => r.auto_stop_mins ? <span className="text-muted-foreground">{r.auto_stop_mins}m</span> : <span className="text-red-600 text-xs">Disabled ⚠</span> },
            { key: "num_clusters",     label: "Clusters",      render: r => <span className="text-muted-foreground">{r.num_clusters ?? 1}</span> },
            { key: "creator_name",     label: "Creator",       render: r => <span className="text-muted-foreground">{r.creator_name ?? "—"}</span> },
            { key: "tags",             label: "Tags",          render: r => {
              const tags = r.tags?.custom_tags ?? [];
              return tags.length ? <span className="text-muted-foreground text-[11px]">{tags.slice(0,2).map((t: any) => `${t.key}=${t.value}`).join(", ")}</span> : <span className="text-yellow-600 text-[11px]">No tags</span>;
            }},
          ],
        },
        {
          title: "Lakeview Dashboards",
          resourceType: "dashboards",
          emptyMsg: "No Lakeview dashboards found.",
          getLink: (r, ws) => r.dashboard_id ? `${ws}/dashboards/${r.dashboard_id}` : null,
          columns: [
            { key: "dashboard_id", label: "Dashboard ID", width: "120px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.dashboard_id}</span> },
            { key: "display_name", label: "Name",         render: r => <span className="font-medium">{r.display_name ?? r.name ?? "—"}</span> },
            { key: "owner",        label: "Owner",        render: r => <span className="text-muted-foreground">{r.owner_user_name ?? r.owner ?? "—"}</span> },
            { key: "lifecycle_state", label: "State",     render: r => <span className="text-muted-foreground">{r.lifecycle_state ?? "—"}</span> },
          ],
        },
        {
          title: "Genie Spaces",
          resourceType: "genie_spaces",
          emptyMsg: "No Genie spaces configured.",
          getLink: (r, ws) => r.space_id ? `${ws}/genie/rooms/${r.space_id}` : null,
          columns: [
            { key: "space_id",     label: "Space ID",    render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.space_id}</span> },
            { key: "title",        label: "Title",       render: r => <span className="font-medium">{r.title ?? "—"}</span> },
            { key: "owner",        label: "Owner",       render: r => <span className="text-muted-foreground">{r.owner ?? "—"}</span> },
            { key: "description",  label: "Description", render: r => <span className="text-muted-foreground truncate">{r.description ?? "—"}</span> },
          ],
        },
        {
          title: "SQL Queries",
          resourceType: "sql_queries",
          emptyMsg: "No saved SQL queries found.",
          getLink: (r, ws) => r.id ? `${ws}/sql/editor/${r.id}` : null,
          columns: [
            { key: "name",     label: "Name",          render: r => <span className="font-medium">{r.name ?? "—"}</span> },
            { key: "user",     label: "Owner",         render: r => <span className="text-muted-foreground">{r.user?.name ?? r.user_id ?? "—"}</span> },
            { key: "query",    label: "Query Preview", render: r => <span className="font-mono text-[10px] text-muted-foreground">{(r.query ?? "").slice(0, 80)}{r.query?.length > 80 ? "…" : ""}</span> },
            { key: "schedule", label: "Schedule",      render: r => r.schedule ? <span className="text-muted-foreground text-[11px]">{r.schedule.interval ?? "Scheduled"}</span> : <span className="text-muted-foreground/40">Manual</span> },
          ],
        },
        {
          title: "SQL Alerts",
          resourceType: "sql_alerts",
          emptyMsg: "No SQL alerts configured.",
          getLink: (r, ws) => r.id ? `${ws}/sql/alerts/${r.id}` : null,
          columns: [
            { key: "name",  label: "Name",     render: r => <span className="font-medium">{r.name ?? "—"}</span> },
            { key: "state", label: "State",    render: r => <span className={`text-xs font-medium ${r.state === "triggered" ? "text-red-600" : "text-muted-foreground"}`}>{r.state ?? "—"}</span> },
            { key: "user",  label: "Owner",    render: r => <span className="text-muted-foreground">{r.user?.name ?? "—"}</span> },
            { key: "rearm", label: "Rearm (s)",render: r => <span className="text-muted-foreground">{r.rearm ?? "—"}</span> },
          ],
        },
        {
          title: "Notification Destinations",
          resourceType: "notification_destinations",
          emptyMsg: "No notification destinations configured.",
          columns: [
            { key: "display_name", label: "Name", render: r => <span className="font-medium">{r.display_name ?? "—"}</span> },
            { key: "type",         label: "Type", render: r => <span className="text-muted-foreground">{r.destination_type ?? r.type ?? "—"}</span> },
            { key: "id",           label: "ID",   width: "220px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.id}</span> },
          ],
        },
      ]}
    />
  );
}
