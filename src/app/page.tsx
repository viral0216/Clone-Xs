// @ts-nocheck
"use client";

import { useAuthStatus, useCloneJobs } from "@/hooks/useApi";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import { Copy, GitCompare, CheckCircle, Activity, Database, AlertCircle } from "lucide-react";

export default function Dashboard() {
  const auth = useAuthStatus();
  const jobs = useCloneJobs();

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold">Dashboard</h1>
        <p className="text-gray-500 mt-1">Unity Catalog Clone Utility</p>
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
            <p className="text-gray-400">Checking authentication...</p>
          ) : auth.data?.authenticated ? (
            <div className="flex items-center gap-4">
              <Badge variant="default" className="bg-green-600">Connected</Badge>
              <span className="text-sm text-gray-600">
                {auth.data.user} @ {auth.data.host}
              </span>
              <span className="text-xs text-gray-400">via {auth.data.auth_method}</span>
            </div>
          ) : (
            <div className="flex items-center gap-4">
              <Badge variant="destructive">Not Connected</Badge>
              <Link href="/settings">
                <Button variant="outline" size="sm">Configure Connection</Button>
              </Link>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Link href="/clone">
          <Card className="hover:border-blue-500 transition-colors cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <Copy className="h-8 w-8 text-blue-600" />
              <div>
                <p className="font-semibold">Clone</p>
                <p className="text-xs text-gray-500">Clone a catalog</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link href="/diff">
          <Card className="hover:border-blue-500 transition-colors cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <GitCompare className="h-8 w-8 text-purple-600" />
              <div>
                <p className="font-semibold">Diff</p>
                <p className="text-xs text-gray-500">Compare catalogs</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link href="/monitor">
          <Card className="hover:border-blue-500 transition-colors cursor-pointer">
            <CardContent className="pt-6 flex items-center gap-3">
              <Activity className="h-8 w-8 text-green-600" />
              <div>
                <p className="font-semibold">Monitor</p>
                <p className="text-xs text-gray-500">Check sync status</p>
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link href="/explore">
          <Card className="hover:border-blue-500 transition-colors cursor-pointer">
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
            <p className="text-gray-400">Loading jobs...</p>
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
