// @ts-nocheck
"use client";
import { FileCode } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

const LANG_BADGE: Record<string, React.ReactNode> = {
  PYTHON: <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">Python</span>,
  SQL:    <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300">SQL</span>,
  SCALA:  <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">Scala</span>,
  R:      <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300">R</span>,
};

function notebookName(path: string) {
  return path?.split("/").pop() ?? path ?? "—";
}

export default function NotebooksPage() {
  return (
    <WorkspaceCategoryPage
      title="Notebooks & Files"
      description="Databricks notebooks, workspace files, and MLflow experiments in this workspace (2-level scan from root)."
      icon={FileCode}
      breadcrumb="Notebooks"
      findingCategories={[]}
      sections={[
        {
          title: "Notebooks",
          resourceType: "notebooks",
          emptyMsg: "No notebooks found. Notebooks may be in deeper subdirectories not covered by the 2-level scan.",
          transform: rows => rows.filter((r: any) => r.object_type === "NOTEBOOK"),
          getLink: (r, ws) => r.object_id ? `${ws}/#notebook/${r.object_id}` : r.path ? `${ws}/#workspace${r.path}` : null,
          columns: [
            { key: "language",  label: "Language",  width: "90px",  render: r => LANG_BADGE[r.language] ?? <span className="text-muted-foreground">{r.language ?? "—"}</span> },
            { key: "name",      label: "Name",                       render: r => <span className="font-medium">{notebookName(r.path)}</span> },
            { key: "path",      label: "Path",                       render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.path}</span> },
            { key: "object_id", label: "Object ID", width: "100px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.object_id}</span> },
          ],
        },
        {
          title: "Workspace Files",
          resourceType: "notebooks",
          emptyMsg: "No generic workspace files found in the 2-level scan.",
          transform: rows => rows.filter((r: any) => r.object_type === "FILE"),
          getLink: (r, ws) => r.path ? `${ws}/#workspace${r.path}` : null,
          columns: [
            { key: "name",      label: "Name",                       render: r => <span className="font-medium">{notebookName(r.path)}</span> },
            { key: "path",      label: "Path",                       render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.path}</span> },
            { key: "object_id", label: "Object ID", width: "100px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.object_id}</span> },
          ],
        },
        {
          title: "MLflow Experiments",
          resourceType: "experiments",
          emptyMsg: "No MLflow experiments found.",
          getLink: (r, ws) => r.experiment_id ? `${ws}/ml/experiments/${r.experiment_id}` : null,
          columns: [
            { key: "name",              label: "Name",              render: r => <span className="font-medium">{r.name}</span> },
            { key: "lifecycle_stage",   label: "State",             render: r => <span className={`text-xs font-medium ${r.lifecycle_stage === "active" ? "text-green-600" : "text-muted-foreground"}`}>{r.lifecycle_stage ?? "—"}</span> },
            { key: "artifact_location", label: "Artifact Location", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.artifact_location ?? "—"}</span> },
            { key: "experiment_id",     label: "Experiment ID",     width: "110px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.experiment_id}</span> },
          ],
        },
      ]}
    />
  );
}
