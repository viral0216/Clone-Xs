import { useAuthStatus, useCloneJobs } from "@/hooks/useApi";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Link } from "react-router-dom";
import { Copy, GitCompare, CheckCircle, Activity, Database, AlertCircle } from "lucide-react";

export default function Dashboard() {
  const auth = useAuthStatus();
  const jobs = useCloneJobs();

  return (
    <div className="space-y-8">
      <div className="border-l-4 border-[#FF3621] pl-4">
        <h1 className="text-3xl font-bold">Dashboard</h1>
        <p className="text-gray-500 mt-1">Overview of your Unity Catalog clone operations — active jobs, recent runs, success rates, and catalog health at a glance.</p>
        <p className="text-xs text-gray-400 mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Unity Catalog overview</a>
        </p>
      </div>

      {/* Auth Status */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Connection Status
          </CardTitle>
        </CardHeader>
        <CardContent>
          {auth.isLoading ? (
            <div className="space-y-3">
              <div className="flex items-center gap-4">
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-4 w-48" />
              </div>
              <Skeleton className="h-4 w-32" />
            </div>
          ) : auth.data?.authenticated ? (
            <div className="flex items-center gap-4">
              <Badge variant="default" className="bg-[#00A972]">Connected</Badge>
              <span className="text-sm text-gray-600">
                {auth.data.user} @ {auth.data.host}
              </span>
              <span className="text-xs text-gray-400">via {auth.data.auth_method}</span>
            </div>
          ) : (
            <div className="flex items-center gap-4">
              <Badge variant="destructive">Not Connected</Badge>
              <Link to="/settings">
                <Button variant="outline" size="sm">Configure Connection</Button>
              </Link>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Link to="/clone">
          <Card className="hover:border-[#FF3621] hover:translate-y-[-2px] transition-all duration-200 cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <Copy className="h-8 w-8 text-blue-600" />
              <div>
                <p className="font-semibold">Clone</p>
                <p className="text-xs text-gray-500">Clone a catalog</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link to="/diff">
          <Card className="hover:border-[#FF3621] hover:translate-y-[-2px] transition-all duration-200 cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <GitCompare className="h-8 w-8 text-purple-600" />
              <div>
                <p className="font-semibold">Diff</p>
                <p className="text-xs text-gray-500">Compare catalogs</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link to="/monitor">
          <Card className="hover:border-[#FF3621] hover:translate-y-[-2px] transition-all duration-200 cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <Activity className="h-8 w-8 text-green-600" />
              <div>
                <p className="font-semibold">Monitor</p>
                <p className="text-xs text-gray-500">Check sync status</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link to="/explore">
          <Card className="hover:border-[#FF3621] hover:translate-y-[-2px] transition-all duration-200 cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <CheckCircle className="h-8 w-8 text-orange-600" />
              <div>
                <p className="font-semibold">Explore</p>
                <p className="text-xs text-gray-500">Browse catalog</p>
              </div>
            </CardContent>
          </Card>
        </Link>
      </div>

      {/* Recent Jobs */}
      <Card>
        <CardHeader>
          <CardTitle>Recent Clone Jobs</CardTitle>
        </CardHeader>
        <CardContent>
          {jobs.isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <div key={i} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <Skeleton className="h-5 w-20" />
                    <Skeleton className="h-4 w-40" />
                    <Skeleton className="h-4 w-16" />
                  </div>
                  <Skeleton className="h-4 w-28" />
                </div>
              ))}
            </div>
          ) : !jobs.data?.length ? (
            <p className="text-gray-400">No clone jobs yet. Start one from the Clone page.</p>
          ) : (
            <div className="space-y-3">
              {jobs.data.slice(0, 10).map((job) => (
                <div key={job.job_id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <Badge
                      variant={
                        job.status === "completed" ? "default" :
                        job.status === "running" ? "secondary" :
                        job.status === "failed" ? "destructive" : "outline"
                      }
                      className={job.status === "completed" ? "bg-green-600" : ""}
                    >
                      {job.status}
                    </Badge>
                    <span className="text-sm font-medium">
                      {job.source_catalog} &rarr; {job.destination_catalog}
                    </span>
                    <span className="text-xs text-gray-400">{job.clone_type}</span>
                  </div>
                  <span className="text-xs text-gray-400">
                    {job.created_at ? new Date(job.created_at).toLocaleString() : ""}
                  </span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
