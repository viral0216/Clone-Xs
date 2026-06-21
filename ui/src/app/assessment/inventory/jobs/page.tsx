// @ts-nocheck
"use client";
import { Workflow } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

function ts(ms: number) {
  if (!ms) return "—";
  return new Date(ms).toLocaleDateString();
}

function taskTypes(job: any) {
  const tasks = job.settings?.tasks ?? [];
  const types = [...new Set(tasks.map((t: any) => {
    if (t.notebook_task) return "Notebook";
    if (t.python_wheel_task) return "Python Wheel";
    if (t.spark_python_task) return "PySpark";
    if (t.sql_task) return "SQL";
    if (t.pipeline_task) return "DLT";
    if (t.run_job_task) return "Run Job";
    if (t.spark_jar_task) return "JAR";
    return "Other";
  }))];
  return types.join(", ") || "—";
}

export default function JobsPage() {
  return (
    <WorkspaceCategoryPage
      title="Jobs & Workflows"
      description="Databricks job definitions, schedules, recent runs, and DLT pipelines."
      icon={Workflow}
      breadcrumb="Jobs"
      findingCategories={["Operations", "Ops Excellence"]}
      sections={[
        {
          title: "Jobs",
          resourceType: "jobs",
          emptyMsg: "No jobs found in this workspace.",
          getLink: (r, ws) => r.job_id ? `${ws}/jobs/${r.job_id}` : null,
          columns: [
            { key: "job_id",   label: "Job ID", width: "70px", render: r => <span className="font-mono text-[11px]">{r.job_id}</span> },
            { key: "name",     label: "Name",   render: r => <span className="font-medium">{r.settings?.name ?? "—"}</span> },
            { key: "tasks",    label: "Task Types", render: r => <span className="text-muted-foreground">{taskTypes(r)}</span> },
            { key: "schedule", label: "Schedule", render: r => r.settings?.schedule?.quartz_cron_expression ? <span className="font-mono text-[10px] text-muted-foreground">{r.settings.schedule.quartz_cron_expression}</span> : <span className="text-muted-foreground">Manual</span> },
            { key: "creator",  label: "Creator", render: r => <span className="text-muted-foreground truncate">{r.creator_user_name ?? "—"}</span> },
            { key: "created",  label: "Created", render: r => <span className="text-muted-foreground">{ts(r.created_time)}</span> },
            { key: "tags",     label: "Tags", render: r => { const t = r.settings?.tags ?? {}; const keys = Object.keys(t); return keys.length ? <span className="text-xs text-muted-foreground">{keys.slice(0,3).map(k => `${k}=${t[k]}`).join(", ")}</span> : <span className="text-muted-foreground/40">—</span>; } },
          ],
        },
        {
          title: "Recent Job Runs",
          resourceType: "job_runs",
          emptyMsg: "No recent job runs found.",
          getLink: (r, ws) => r.run_page_url || (r.job_id && r.run_id ? `${ws}/jobs/${r.job_id}/runs/${r.run_id}` : null),
          columns: [
            { key: "run_id",      label: "Run ID",   width: "70px", render: r => <span className="font-mono text-[11px]">{r.run_id}</span> },
            { key: "job_id",      label: "Job ID",   width: "70px", render: r => <span className="font-mono text-[11px] text-muted-foreground">{r.job_id}</span> },
            { key: "run_name",    label: "Name",     render: r => <span className="font-medium">{r.run_name ?? r.run_page_url?.split("/").pop() ?? "—"}</span> },
            { key: "state",       label: "State",    render: r => {
              const lc = r.state?.life_cycle_state ?? "—";
              const rs = r.state?.result_state ?? "";
              const color = rs === "SUCCESS" ? "text-green-600" : rs === "FAILED" ? "text-red-600" : "text-muted-foreground";
              return <span className={`font-medium ${color}`}>{rs || lc}</span>;
            }},
            { key: "started",     label: "Started",  render: r => <span className="text-muted-foreground">{ts(r.start_time)}</span> },
            { key: "duration",    label: "Duration", render: r => {
              if (!r.execution_duration) return <span className="text-muted-foreground">—</span>;
              const sec = Math.round(r.execution_duration / 1000);
              return <span className="text-muted-foreground">{sec < 60 ? `${sec}s` : `${Math.round(sec/60)}m`}</span>;
            }},
            { key: "trigger",     label: "Trigger",  render: r => <span className="text-muted-foreground capitalize">{(r.trigger ?? "manual").toLowerCase()}</span> },
          ],
        },
        {
          title: "DLT Pipelines",
          resourceType: "pipelines",
          emptyMsg: "No DLT pipelines configured.",
          getLink: (r, ws) => r.pipeline_id ? `${ws}/pipelines/${r.pipeline_id}` : null,
          columns: [
            { key: "pipeline_id", label: "Pipeline ID", width: "120px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.pipeline_id}</span> },
            { key: "name",        label: "Name",        render: r => <span className="font-medium">{r.name ?? "—"}</span> },
            { key: "state",       label: "State",       render: r => <span className={`font-medium ${r.state === "RUNNING" ? "text-green-600" : r.state === "FAILED" ? "text-red-600" : "text-muted-foreground"}`}>{r.state ?? "—"}</span> },
            { key: "creator",     label: "Creator",     render: r => <span className="text-muted-foreground">{r.creator_user_name ?? "—"}</span> },
          ],
        },
      ]}
    />
  );
}
