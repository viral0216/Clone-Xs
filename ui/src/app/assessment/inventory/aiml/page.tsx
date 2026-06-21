// @ts-nocheck
"use client";
import { BrainCircuit } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

function ts(ms: number) {
  if (!ms) return "—";
  return new Date(ms).toLocaleDateString();
}

function endpointState(ep: any) {
  const ready = ep.state?.ready ?? ep.state;
  const color = ready === "READY" ? "text-green-600" : ready === "NOT_READY" ? "text-red-600" : "text-yellow-600";
  return <span className={`font-medium ${color}`}>{ready ?? "—"}</span>;
}

function servedModels(ep: any) {
  const models = ep.config?.served_models ?? ep.config?.served_entities ?? [];
  if (!models.length) return <span className="text-muted-foreground/40">—</span>;
  return (
    <span className="text-muted-foreground text-[11px]">
      {models.slice(0, 2).map((m: any) => m.model_name || m.name || m.entity_name || "model").join(", ")}
      {models.length > 2 ? ` +${models.length - 2}` : ""}
    </span>
  );
}

export default function AiMlPage() {
  return (
    <WorkspaceCategoryPage
      title="AI / ML"
      description="Serving endpoints, vector search, apps, Databricks Apps, and repos connected to this workspace."
      icon={BrainCircuit}
      breadcrumb="AI / ML"
      findingCategories={["AI / ML Governance", "Serverless Governance"]}
      sections={[
        {
          title: "Serving Endpoints",
          resourceType: "serving_endpoints",
          emptyMsg: "No serving endpoints deployed.",
          getLink: (r, ws) => r.name ? `${ws}/ml/endpoints/${r.name}` : null,
          columns: [
            { key: "name",       label: "Name",         render: r => <span className="font-medium">{r.name}</span> },
            { key: "state",      label: "State",        render: r => endpointState(r) },
            { key: "models",     label: "Models / Entities", render: r => servedModels(r) },
            { key: "ai_gateway", label: "AI Gateway",   render: r => r.ai_gateway ? <span className="text-green-600 text-xs">Enabled</span> : <span className="text-yellow-600 text-xs">None ⚠</span> },
            { key: "rate_limits",label: "Rate Limits",  render: r => {
              const rl = r.rate_limits ?? r.ai_gateway?.rate_limits ?? [];
              return rl.length > 0 ? <span className="text-green-600 text-xs">{rl.length} limit{rl.length !== 1 ? "s" : ""}</span> : <span className="text-yellow-600 text-xs">None ⚠</span>;
            }},
            { key: "creator",    label: "Creator",      render: r => <span className="text-muted-foreground">{r.creator ?? "—"}</span> },
            { key: "created",    label: "Created",      render: r => <span className="text-muted-foreground">{ts(r.creation_timestamp)}</span> },
          ],
        },
        {
          title: "Vector Search Endpoints",
          resourceType: "vector_search",
          emptyMsg: "No vector search endpoints configured.",
          getLink: (r, ws) => r.name ? `${ws}/compute/vector-search/${r.name}` : null,
          columns: [
            { key: "name",               label: "Name",    render: r => <span className="font-medium">{r.name}</span> },
            { key: "endpoint_type",      label: "Type",    render: r => <span className="text-muted-foreground">{r.endpoint_type ?? "—"}</span> },
            { key: "endpoint_status",    label: "Status",  render: r => <span className={`font-medium ${r.endpoint_status?.state === "ONLINE" ? "text-green-600" : "text-yellow-600"}`}>{r.endpoint_status?.state ?? "—"}</span> },
            { key: "creator",            label: "Creator", render: r => <span className="text-muted-foreground">{r.creator ?? "—"}</span> },
          ],
        },
        {
          title: "Databricks Apps",
          resourceType: "apps",
          emptyMsg: "No Databricks Apps deployed.",
          getLink: (r, ws) => r.name ? `${ws}/apps/${r.name}` : null,
          columns: [
            { key: "name",        label: "Name",    render: r => <span className="font-medium">{r.name}</span> },
            { key: "description", label: "Description", render: r => <span className="text-muted-foreground truncate">{r.description ?? "—"}</span> },
            { key: "state",       label: "State",   render: r => <span className={`font-medium ${r.app_status?.state === "RUNNING" ? "text-green-600" : "text-muted-foreground"}`}>{r.app_status?.state ?? "—"}</span> },
            { key: "creator",     label: "Creator", render: r => <span className="text-muted-foreground">{r.create_time ? r.creator ?? "—" : "—"}</span> },
          ],
        },
        {
          title: "Git Repos",
          resourceType: "repos",
          emptyMsg: "No Git repos connected.",
          getLink: (r, ws) => r.path ? `${ws}/#workspace${r.path}` : null,
          columns: [
            { key: "id",          label: "Repo ID", width: "80px",  render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.id}</span> },
            { key: "path",        label: "Path",                    render: r => <span className="font-medium">{r.path}</span> },
            { key: "url",         label: "Remote URL",              render: r => <span className="font-mono text-[10px] text-muted-foreground truncate">{r.url}</span> },
            { key: "provider",    label: "Provider",                render: r => <span className="text-muted-foreground">{r.provider ?? "—"}</span> },
            { key: "branch",      label: "Branch",                  render: r => <span className="text-muted-foreground font-mono text-[11px]">{r.branch ?? "—"}</span> },
          ],
        },
        {
          title: "Marketplace Listings (Consumer)",
          resourceType: "marketplace_listings",
          emptyMsg: "No marketplace listings installed.",
          columns: [
            { key: "id",       label: "Listing ID", width: "120px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.listing_id ?? r.id ?? "—"}</span> },
            { key: "name",     label: "Name",                       render: r => <span className="font-medium">{r.listing?.summary?.name ?? r.name ?? "—"}</span> },
            { key: "provider", label: "Provider",                   render: r => <span className="text-muted-foreground">{r.listing?.summary?.provider_region?.cloud ?? r.provider_name ?? "—"}</span> },
            { key: "status",   label: "Status",                     render: r => <span className="text-muted-foreground">{r.status ?? "—"}</span> },
          ],
        },
      ]}
    />
  );
}
