// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { useNavigate, Link } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Play, CheckCircle2, XCircle, Loader2, AlertTriangle, ShieldCheck,
  Clock, Bell,
} from "lucide-react";

function getStoredCreds() {
  try {
    return {
      host: localStorage.getItem("dbx_host") || "",
      token: localStorage.getItem("dbx_token") || "",
    };
  } catch {
    return { host: "", token: "" };
  }
}

export default function RunAssessmentPage() {
  const navigate = useNavigate();
  const creds = getStoredCreds();

  const [host, setHost] = useState(creds.host);
  const [token, setToken] = useState(creds.token);
  const [workspaceName, setWorkspaceName] = useState("");
  const [scanType, setScanType] = useState("full");

  const [jobId, setJobId] = useState("");
  const [jobStatus, setJobStatus] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const pollRef = useRef(null);

  // Schedule state
  const [schedule, setSchedule] = useState({ enabled: false, frequency: "daily", hour: "08", scan_type: "full" });
  const [schedSaving, setSchedSaving] = useState(false);
  const [schedSaved, setSchedSaved] = useState(false);

  function clearPoll() {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }

  useEffect(() => () => clearPoll(), []);

  // Load current schedule on mount
  useEffect(() => {
    api.get("/assessment/schedule").then(r => {
      if (r && typeof r === "object") {
        setSchedule(prev => ({
          ...prev,
          enabled: r.enabled || false,
          frequency: r.frequency || "daily",
          hour: r.hour || "08",
          scan_type: r.scan_type || "full",
        }));
      }
    }).catch(() => {});
  }, []);

  async function saveSchedule() {
    setSchedSaving(true);
    try {
      const payload = {
        ...schedule,
        host,
        token,
        workspace_name: workspaceName,
      };
      if (schedule.enabled) {
        // Compute first next_run at the configured hour today or tomorrow
        const now = new Date();
        const target = new Date(now);
        target.setUTCHours(parseInt(schedule.hour, 10), 0, 0, 0);
        if (target <= now) target.setDate(target.getDate() + 1);
        payload.next_run = target.toISOString();
      }
      await api.put("/assessment/schedule", payload, { headers: { "Content-Type": "application/json" } });
      setSchedSaved(true);
      setTimeout(() => setSchedSaved(false), 3000);
    } catch {
      // ignore
    } finally {
      setSchedSaving(false);
    }
  }

  async function disableSchedule() {
    await api.delete("/assessment/schedule").catch(() => {});
    setSchedule(prev => ({ ...prev, enabled: false }));
  }

  async function startScan() {
    if (!host || !token) {
      setError("Databricks host and token are required.");
      return;
    }
    setError("");
    setRunning(true);
    setJobStatus(null);
    try {
      const params = new URLSearchParams({
        workspace_name: workspaceName,
        scan_type: scanType,
      });
      const resp = await api.post(`/assessment/run?${params}`, {});
      setJobId(resp.job_id);
      // Start polling
      pollRef.current = setInterval(async () => {
        try {
          const status = await api.get(`/assessment/status/${resp.job_id}`);
          setJobStatus(status);
          if (status.status === "completed" || status.status === "error") {
            clearPoll();
            setRunning(false);
          }
        } catch {}
      }, 2000);
    } catch (e: any) {
      setError(e?.message ?? "Failed to start scan");
      setRunning(false);
    }
  }

  const isDone = jobStatus?.status === "completed";
  const isError = jobStatus?.status === "error";

  return (
    <div className="space-y-4">
      <PageHeader
        title="Run Assessment Scan"
        icon={Play}
        breadcrumbs={["Assessment", "Run Scan"]}
        description="Run a comprehensive security assessment against your Databricks workspace — 345 checks across identity, network, data protection, Unity Catalog governance, and more."
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Config form */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium">Scan Configuration</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1.5">Databricks Workspace URL</label>
              <input
                type="text"
                value={host}
                onChange={e => setHost(e.target.value)}
                placeholder="https://adb-xxxx.azuredatabricks.net"
                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                disabled={running}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Personal Access Token</label>
              <input
                type="password"
                value={token}
                onChange={e => setToken(e.target.value)}
                placeholder="dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                disabled={running}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Workspace Name (optional)</label>
              <input
                type="text"
                value={workspaceName}
                onChange={e => setWorkspaceName(e.target.value)}
                placeholder="My Production Workspace"
                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                disabled={running}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-2">Scan Type</label>
              <div className="space-y-2">
                {([
                  {
                    value: "full",
                    label: "Full Assessment",
                    desc: "345 security checks + UC inventory (Tree / Sunburst / Hub & Spoke views)",
                  },
                  {
                    value: "security",
                    label: "Security Only",
                    desc: "345 security checks — findings, scores, recommendations. No UC inventory.",
                  },
                  {
                    value: "inventory",
                    label: "UC Inventory Only",
                    desc: "Catalog tree, schemas, tables, volumes, functions, models — no security checks.",
                  },
                ] as const).map(opt => (
                  <label
                    key={opt.value}
                    className={`flex items-start gap-3 p-3 rounded-md border cursor-pointer transition-colors ${
                      scanType === opt.value
                        ? "border-primary bg-primary/5"
                        : "border-border hover:bg-muted/30"
                    } ${running ? "opacity-50 cursor-not-allowed" : ""}`}
                  >
                    <input
                      type="radio"
                      name="scan-type"
                      value={opt.value}
                      checked={scanType === opt.value}
                      onChange={() => setScanType(opt.value)}
                      disabled={running}
                      className="mt-0.5 h-4 w-4 accent-primary shrink-0"
                    />
                    <div>
                      <p className="text-sm font-medium leading-tight">{opt.label}</p>
                      <p className="text-xs text-muted-foreground mt-0.5">{opt.desc}</p>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {error && (
              <div className="flex items-center gap-2 text-destructive text-sm bg-destructive/10 rounded-md p-2.5">
                <AlertTriangle className="h-4 w-4 shrink-0" />
                {error}
              </div>
            )}

            <Button
              onClick={startScan}
              disabled={running || isDone}
              className="w-full"
            >
              {running ? (
                <><Loader2 className="h-4 w-4 mr-2 animate-spin" />Scanning…</>
              ) : isDone ? (
                <><CheckCircle2 className="h-4 w-4 mr-2" />Scan Complete</>
              ) : (
                <><Play className="h-4 w-4 mr-2" />Start Scan</>
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Progress / result */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              Scan Progress
              {running && <Badge variant="outline" className="text-xs animate-pulse">Running</Badge>}
              {isDone && <Badge className="text-xs bg-green-500 text-white">Complete</Badge>}
              {isError && <Badge variant="destructive" className="text-xs">Failed</Badge>}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {!jobStatus && !running && (
              <div className="py-12 text-center text-muted-foreground text-sm">
                <ShieldCheck className="h-10 w-10 mx-auto mb-3 opacity-30" />
                Configure your workspace credentials and click <strong>Start Scan</strong>.
              </div>
            )}

            {(running || jobStatus) && (
              <div className="space-y-4">
                {/* Progress line */}
                <div className="flex items-center gap-2 text-sm">
                  {running && <Loader2 className="h-4 w-4 animate-spin text-primary shrink-0" />}
                  {isDone && <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />}
                  {isError && <XCircle className="h-4 w-4 text-destructive shrink-0" />}
                  <span className="text-muted-foreground">{jobStatus?.progress ?? "Initialising…"}</span>
                </div>

                {/* Status info */}
                {jobStatus && (
                  <div className="text-xs text-muted-foreground space-y-1 border border-border rounded-md p-3 bg-muted/30">
                    <p><span className="font-medium">Job ID:</span> {jobStatus.job_id}</p>
                    <p><span className="font-medium">Status:</span> {jobStatus.status}</p>
                    {jobStatus.submitted_at && (
                      <p><span className="font-medium">Started:</span> {new Date(jobStatus.submitted_at).toLocaleTimeString()}</p>
                    )}
                    {jobStatus.inventory_error && (
                      <p className="text-yellow-600 dark:text-yellow-400">
                        <AlertTriangle className="inline h-3 w-3 mr-1" />
                        Inventory warning: {jobStatus.inventory_error}
                      </p>
                    )}
                  </div>
                )}

                {isError && (
                  <div className="text-sm text-destructive bg-destructive/10 rounded-md p-3">
                    <strong>Error:</strong> {jobStatus?.error}
                  </div>
                )}

                {isDone && (
                  <div className="space-y-2">
                    <p className="text-sm text-green-600 dark:text-green-400 font-medium">
                      ✓ Scan completed successfully
                    </p>
                    <div className="flex gap-2 flex-wrap">
                      {scanType !== "inventory" && (
                        <Button size="sm" onClick={() => navigate("/assessment")}>
                          View Results
                        </Button>
                      )}
                      {scanType !== "inventory" && (
                        <Button size="sm" variant="outline" onClick={() => navigate("/assessment/findings")}>
                          View Findings
                        </Button>
                      )}
                      {scanType !== "security" && (
                        <Button size="sm" variant={scanType === "inventory" ? "default" : "outline"} onClick={() => navigate("/assessment/inventory")}>
                          View Inventory
                        </Button>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Scan completion notification CTA */}
      {isDone && (
        <Card className="border-blue-200 dark:border-blue-900 bg-blue-50/30 dark:bg-blue-950/20">
          <CardContent className="pt-4 pb-3 flex items-start gap-3">
            <Bell className="h-4 w-4 text-blue-600 shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium">Get notified on future scans</p>
              <p className="text-xs text-muted-foreground mt-0.5">
                Configure webhook destinations to receive scan completion notifications automatically.
              </p>
            </div>
            <Link to="/settings/notifications">
              <Button size="sm" variant="outline" className="shrink-0">
                Configure Webhooks
              </Button>
            </Link>
          </CardContent>
        </Card>
      )}

      {/* Scan Scheduler */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            <Clock className="h-4 w-4" />
            Automatic Scan Schedule
            {schedule.enabled && (
              <Badge className="text-[10px] bg-green-500/10 text-green-700 border-green-200">Active</Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <label className="flex items-center gap-3 cursor-pointer">
            <input
              type="checkbox"
              checked={schedule.enabled}
              onChange={e => setSchedule(prev => ({ ...prev, enabled: e.target.checked }))}
              className="h-4 w-4 accent-primary"
            />
            <div>
              <p className="text-sm font-medium">Enable automatic scans</p>
              <p className="text-xs text-muted-foreground">Run scans automatically on the configured schedule</p>
            </div>
          </label>

          {schedule.enabled && (
            <div className="grid grid-cols-2 gap-3 pl-7">
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">Frequency</label>
                <select
                  value={schedule.frequency}
                  onChange={e => setSchedule(prev => ({ ...prev, frequency: e.target.value }))}
                  className="w-full px-3 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                >
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">Run at (UTC hour)</label>
                <select
                  value={schedule.hour}
                  onChange={e => setSchedule(prev => ({ ...prev, hour: e.target.value }))}
                  className="w-full px-3 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                >
                  {Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0")).map(h => (
                    <option key={h} value={h}>{h}:00 UTC</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">Scan type</label>
                <select
                  value={schedule.scan_type}
                  onChange={e => setSchedule(prev => ({ ...prev, scan_type: e.target.value }))}
                  className="w-full px-3 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
                >
                  <option value="full">Full Assessment</option>
                  <option value="security">Security Only</option>
                  <option value="inventory">Inventory Only</option>
                </select>
              </div>
            </div>
          )}

          <div className="flex gap-2 pt-1">
            <Button size="sm" onClick={saveSchedule} disabled={schedSaving}>
              {schedSaving ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : null}
              {schedSaved ? "Saved!" : "Save Schedule"}
            </Button>
            {schedule.enabled && (
              <Button size="sm" variant="outline" onClick={disableSchedule}>
                Disable
              </Button>
            )}
          </div>

          <p className="text-xs text-muted-foreground">
            Scheduled scans use the workspace credentials entered above.
            The backend checks every 60 seconds whether a scan is due.
          </p>
        </CardContent>
      </Card>

      {/* What will be scanned */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">
            {scanType === "inventory" ? "What gets catalogued" : "What gets scanned"}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {scanType === "inventory" ? (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
              {[
                "Catalogs", "Schemas", "Tables & Views", "Volumes",
                "Functions", "Registered Models", "Column detail", "Grants (coarse)",
              ].map(label => (
                <div key={label} className="flex items-center gap-2 text-muted-foreground">
                  <CheckCircle2 className="h-3.5 w-3.5 text-green-500 shrink-0" />
                  <span className="text-xs">{label}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
              {[
                { label: "Identity & Access", count: "15 checks" },
                { label: "Network Security", count: "10 checks" },
                { label: "Data Protection", count: "20 checks" },
                { label: "UC Governance", count: "36 checks" },
                { label: "AI / ML Governance", count: "19 checks" },
                { label: "Audit & Logging", count: "7 checks" },
                { label: "Cost & Performance", count: "10 checks" },
                { label: "34 more categories", count: "228 checks" },
              ].map(({ label, count }) => (
                <div key={label} className="flex items-center gap-2 text-muted-foreground">
                  <CheckCircle2 className="h-3.5 w-3.5 text-green-500 shrink-0" />
                  <span className="text-xs">{label} <span className="text-muted-foreground/60">({count})</span></span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
