/**
 * Global running jobs indicator for the header bar.
 * Shows a spinning icon + count when jobs are active, links to Active Jobs page.
 */
import { Link } from "react-router-dom";
import { Loader2, CheckCircle } from "lucide-react";
import { useActiveJobs } from "@/contexts/ActiveJobsContext";

export default function ActiveJobsIndicator() {
  const { activeCount, activeJobs } = useActiveJobs();

  if (activeCount === 0) return null;

  const runningCount = activeJobs.filter(j => j.status === "running").length;
  const queuedCount = activeJobs.filter(j => j.status === "queued").length;

  const tooltip = [
    runningCount > 0 ? `${runningCount} running` : "",
    queuedCount > 0 ? `${queuedCount} queued` : "",
  ].filter(Boolean).join(", ");

  return (
    <Link
      to="/data-quality/jobs"
      className="relative flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium
                 bg-[#E8453C]/10 text-[#E8453C] hover:bg-[#E8453C]/20 transition-colors"
      title={`Active jobs: ${tooltip}`}
    >
      <Loader2 className="h-3.5 w-3.5 animate-spin" />
      <span>{activeCount} job{activeCount !== 1 ? "s" : ""}</span>
    </Link>
  );
}
