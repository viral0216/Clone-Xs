// @ts-nocheck
"use client";
import { Server } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

function ts(ms: number) {
  if (!ms) return "—";
  return new Date(ms).toLocaleDateString();
}

function clusterState(state: string) {
  const color = state === "RUNNING" ? "text-green-600" : state === "TERMINATED" ? "text-muted-foreground" : state === "ERROR" ? "text-red-600" : "text-yellow-600";
  return <span className={`font-medium ${color}`}>{state ?? "—"}</span>;
}

export default function ComputePage() {
  return (
    <WorkspaceCategoryPage
      title="Compute"
      description="Cluster policies, active clusters, instance pools, and global init scripts."
      icon={Server}
      breadcrumb="Compute"
      findingCategories={["Compute Security", "Performance", "Advanced Performance", "Spark Best Practices"]}
      sections={[
        {
          title: "Cluster Policies",
          resourceType: "cluster_policies",
          emptyMsg: "No cluster policies configured. Policies enforce standards on cluster creation.",
          getLink: (r, ws) => r.policy_id ? `${ws}/compute/policies/${r.policy_id}` : null,
          columns: [
            { key: "policy_id",  label: "Policy ID", width: "100px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.policy_id}</span> },
            { key: "name",       label: "Name",      render: r => <span className="font-medium">{r.name}</span> },
            { key: "definition", label: "Rules",     render: r => {
              try {
                const d = typeof r.definition === "string" ? JSON.parse(r.definition) : r.definition ?? {};
                const keys = Object.keys(d);
                return <span className="text-muted-foreground">{keys.length} constraint{keys.length !== 1 ? "s" : ""}: {keys.slice(0, 3).join(", ")}{keys.length > 3 ? "…" : ""}</span>;
              } catch { return <span className="text-muted-foreground">—</span>; }
            }},
            { key: "creator",    label: "Creator",   render: r => <span className="text-muted-foreground">{r.creator_user_name ?? "—"}</span> },
            { key: "created",    label: "Created",   render: r => <span className="text-muted-foreground">{ts(r.created_at_timestamp)}</span> },
          ],
        },
        {
          title: "Active Clusters",
          resourceType: "clusters",
          emptyMsg: "No clusters are currently active in this workspace.",
          getLink: (r, ws) => r.cluster_id ? `${ws}/compute/clusters/${r.cluster_id}` : null,
          columns: [
            { key: "cluster_id",     label: "Cluster ID",     width: "120px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.cluster_id}</span> },
            { key: "cluster_name",   label: "Name",           render: r => <span className="font-medium">{r.cluster_name}</span> },
            { key: "state",          label: "State",          render: r => clusterState(r.state) },
            { key: "spark_version",  label: "Runtime",        render: r => <span className="text-muted-foreground text-[11px]">{r.spark_version?.split("-")?.[0] ?? "—"}</span> },
            { key: "node_type_id",   label: "Node Type",      render: r => <span className="text-muted-foreground">{r.node_type_id ?? "—"}</span> },
            { key: "num_workers",    label: "Workers",        render: r => <span className="text-muted-foreground">{r.num_workers ?? (r.autoscale ? `${r.autoscale.min_workers}–${r.autoscale.max_workers}` : "—")}</span> },
            { key: "creator",        label: "Creator",        render: r => <span className="text-muted-foreground">{r.creator_user_name ?? "—"}</span> },
          ],
        },
        {
          title: "Instance Pools",
          resourceType: "instance_pools",
          emptyMsg: "No instance pools configured.",
          getLink: (r, ws) => r.instance_pool_id ? `${ws}/compute/pools/${r.instance_pool_id}` : null,
          columns: [
            { key: "instance_pool_id",   label: "Pool ID",     width: "120px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.instance_pool_id}</span> },
            { key: "instance_pool_name", label: "Name",        render: r => <span className="font-medium">{r.instance_pool_name}</span> },
            { key: "node_type_id",       label: "Node Type",   render: r => <span className="text-muted-foreground">{r.node_type_id ?? "—"}</span> },
            { key: "min_idle_instances", label: "Min Idle",    render: r => <span className="text-muted-foreground">{r.min_idle_instances ?? 0}</span> },
            { key: "state",              label: "State",       render: r => <span className="text-muted-foreground">{r.state ?? "—"}</span> },
          ],
        },
        {
          title: "Global Init Scripts",
          resourceType: "global_init_scripts",
          emptyMsg: "No global init scripts configured.",
          columns: [
            { key: "script_id",  label: "Script ID",   render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.script_id}</span> },
            { key: "name",       label: "Name",        render: r => <span className="font-medium">{r.name}</span> },
            { key: "enabled",    label: "Enabled",     render: r => <span className={r.enabled ? "text-green-600" : "text-muted-foreground"}>{r.enabled ? "Yes" : "No"}</span> },
            { key: "position",   label: "Position",    render: r => <span className="text-muted-foreground">{r.position ?? "—"}</span> },
            { key: "created_by", label: "Created By",  render: r => <span className="text-muted-foreground">{r.created_by ?? "—"}</span> },
          ],
        },
        {
          title: "DBFS Mounts",
          resourceType: "dbfs_mounts",
          emptyMsg: "No DBFS mounts found under /mnt.",
          columns: [
            { key: "path",      label: "Path",  render: r => <span className="font-medium font-mono text-[11px]">{r.path}</span> },
            { key: "is_dir",    label: "Type",  render: r => <span className="text-muted-foreground">{r.is_dir ? "Directory" : "File"}</span> },
            { key: "file_size", label: "Size",  render: r => r.file_size ? <span className="text-muted-foreground">{(r.file_size / 1024).toFixed(1)} KB</span> : <span className="text-muted-foreground/40">—</span> },
          ],
        },
      ]}
    />
  );
}
