// @ts-nocheck
import { useState, useEffect } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useVolumes } from "@/hooks/useApi";
import CatalogPicker from "@/components/CatalogPicker";
import {
  Briefcase, Loader2, CheckCircle, XCircle, ExternalLink, Copy, CalendarClock, Bell, RotateCcw, Clock, Tag, Settings2, Filter, Gauge,
} from "lucide-react";

function DestinationCatalogPicker({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [isNew, setIsNew] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.get<string[]>("/catalogs")
      .then((data) => setCatalogs(data || []))
      .catch(() => setCatalogs([]))
      .finally(() => setLoading(false));
  }, []);

  const selectClass =
    "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#FF3621]/30 focus:border-[#FF3621]";

  return (
    <div>
      <label className="text-sm font-medium mb-1 block">Destination Catalog</label>
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading catalogs...
        </div>
      ) : isNew ? (
        <div className="space-y-2">
          <div className="flex gap-2">
            <input
              className={selectClass}
              value={value}
              onChange={(e) => onChange(e.target.value)}
              placeholder="Enter new catalog name (e.g. my_catalog_clone)"
              autoFocus
            />
            <button
              onClick={() => { setIsNew(false); onChange(""); }}
              className="px-3 py-2 text-sm rounded-lg border border-border hover:bg-muted/50 text-muted-foreground whitespace-nowrap"
            >
              Cancel
            </button>
          </div>
          <p className="text-xs text-muted-foreground">
            This catalog will be created automatically during the clone operation
          </p>
        </div>
      ) : (
        <select
          className={selectClass}
          value={value}
          onChange={(e) => {
            if (e.target.value === "__NEW__") {
              setIsNew(true);
              onChange("");
            } else {
              onChange(e.target.value);
            }
          }}
        >
          <option value="">Select catalog...</option>
          <option value="__NEW__">+ Create New Catalog</option>
          {catalogs.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      )}
    </div>
  );
}

export default function CreateJobPage() {
  const [sourceCatalog, setSourceCatalog] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [jobName, setJobName] = useState("");
  const [volume, setVolume] = useState("");
  const [schedule, setSchedule] = useState("");
  const [timezone, setTimezone] = useState("UTC");
  const [notificationEmail, setNotificationEmail] = useState("");
  const [maxRetries, setMaxRetries] = useState(0);
  const [timeout, setTimeout] = useState(7200);
  const [tags, setTags] = useState("");
  const [updateJobId, setUpdateJobId] = useState("");

  // Clone configuration
  const [cloneType, setCloneType] = useState<"DEEP" | "SHALLOW">("DEEP");
  const [loadType, setLoadType] = useState<"FULL" | "INCREMENTAL">("FULL");
  const [maxWorkers, setMaxWorkers] = useState(4);
  const [parallelTables, setParallelTables] = useState(1);
  const [maxParallelQueries, setMaxParallelQueries] = useState(10);
  const [maxRps, setMaxRps] = useState(0);
  const [orderBySize, setOrderBySize] = useState<"" | "asc" | "desc">("");
  // Copy options
  const [copyPermissions, setCopyPermissions] = useState(true);
  const [copyOwnership, setCopyOwnership] = useState(true);
  const [copyTags, setCopyTags] = useState(true);
  const [copyProperties, setCopyProperties] = useState(true);
  const [copySecurity, setCopySecurity] = useState(true);
  const [copyConstraints, setCopyConstraints] = useState(true);
  const [copyComments, setCopyComments] = useState(true);
  // Features
  const [enableRollback, setEnableRollback] = useState(false);
  const [validateAfterClone, setValidateAfterClone] = useState(false);
  const [validateChecksum, setValidateChecksum] = useState(false);
  const [forceReclone, setForceReclone] = useState(false);
  const [showProgress, setShowProgress] = useState(true);
  // Filtering
  const [excludeSchemas, setExcludeSchemas] = useState("information_schema,default");
  const [includeSchemas, setIncludeSchemas] = useState("");
  const [includeTablesRegex, setIncludeTablesRegex] = useState("");
  const [excludeTablesRegex, setExcludeTablesRegex] = useState("");
  // Time travel
  const [asOfTimestamp, setAsOfTimestamp] = useState("");
  const [asOfVersion, setAsOfVersion] = useState("");

  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const volumes = useVolumes();

  const handleSubmit = async () => {
    if (!sourceCatalog || !destCatalog) {
      toast.error("Source and destination catalogs are required");
      return;
    }
    if (!volume) {
      toast.error("UC Volume is required for wheel upload");
      return;
    }

    setLoading(true);
    setResult(null);
    setError(null);

    try {
      const payload: any = {
        source_catalog: sourceCatalog,
        destination_catalog: destCatalog,
        volume,
        max_retries: maxRetries,
        timeout,
        timezone,
        // Clone configuration
        clone_type: cloneType,
        load_type: loadType,
        max_workers: maxWorkers,
        parallel_tables: parallelTables,
        max_parallel_queries: maxParallelQueries,
        max_rps: maxRps,
        order_by_size: orderBySize,
        // Copy options
        copy_permissions: copyPermissions,
        copy_ownership: copyOwnership,
        copy_tags: copyTags,
        copy_properties: copyProperties,
        copy_security: copySecurity,
        copy_constraints: copyConstraints,
        copy_comments: copyComments,
        // Features
        enable_rollback: enableRollback,
        validate_after_clone: validateAfterClone,
        validate_checksum: validateChecksum,
        force_reclone: forceReclone,
        show_progress: showProgress,
        // Filtering
        exclude_schemas: excludeSchemas ? excludeSchemas.split(",").map((s: string) => s.trim()) : [],
        include_schemas: includeSchemas ? includeSchemas.split(",").map((s: string) => s.trim()) : [],
        include_tables_regex: includeTablesRegex,
        exclude_tables_regex: excludeTablesRegex,
        // Time travel
        as_of_timestamp: asOfTimestamp,
        as_of_version: asOfVersion,
      };
      if (jobName) payload.job_name = jobName;
      if (schedule) payload.schedule = schedule;
      if (notificationEmail) {
        payload.notification_emails = notificationEmail.split(",").map((e: string) => e.trim()).filter(Boolean);
      }
      if (tags) {
        payload.tags = Object.fromEntries(
          tags.split(",").map((t: string) => t.trim().split("=")).filter(([k, v]: string[]) => k && v)
        );
      }
      if (updateJobId) payload.update_job_id = parseInt(updateJobId, 10);

      const res = await api.post("/generate/create-job", payload);
      setResult(res);
      toast.success(updateJobId ? "Job updated successfully" : "Job created successfully");
    } catch (e) {
      const msg = (e as Error).message;
      setError(msg);
      toast.error(msg);
    }
    setLoading(false);
  };

  const cronPresets = [
    { label: "Daily 6 AM", value: "0 0 6 * * ?" },
    { label: "Daily 12 AM", value: "0 0 0 * * ?" },
    { label: "Hourly", value: "0 0 * * * ?" },
    { label: "Every 6h", value: "0 0 */6 * * ?" },
    { label: "Weekdays 8 AM", value: "0 0 8 ? * MON-FRI" },
    { label: "Weekly Sun", value: "0 0 2 ? * SUN" },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Create Databricks Job</h1>
        <p className="text-gray-500 mt-1">
          Create a persistent job in Databricks to run Clone-Xs on a schedule — no CLI required.
        </p>
      </div>

      {/* Configuration */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Briefcase className="h-5 w-5" />
            Job Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Catalogs */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-end">
            <div>
              <label className="text-sm font-medium mb-1 block">Source Catalog</label>
              <CatalogPicker catalog={sourceCatalog} onCatalogChange={setSourceCatalog} showSchema={false} showTable={false} />
            </div>
            <DestinationCatalogPicker value={destCatalog} onChange={setDestCatalog} />
          </div>

          {/* Job Name */}
          <div>
            <label className="text-sm font-medium mb-1 block">Job Name (optional)</label>
            <Input
              placeholder={`Clone-Xs: ${sourceCatalog || "source"} -> ${destCatalog || "dest"}`}
              value={jobName}
              onChange={(e) => setJobName(e.target.value)}
            />
          </div>

          {/* Volume */}
          <div>
            <label className="text-sm font-medium mb-1 block">UC Volume (for wheel upload)</label>
            {volumes.isLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading volumes...
              </div>
            ) : volumes.isError ? (
              <Input
                placeholder="/Volumes/catalog/schema/volume"
                value={volume}
                onChange={(e) => setVolume(e.target.value)}
              />
            ) : (
              <select
                className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#FF3621]/30 focus:border-[#FF3621]"
                value={volume}
                onChange={(e) => setVolume(e.target.value)}
              >
                <option value="">Select a volume...</option>
                {(volumes.data || []).map((v: any) => (
                  <option key={v.path} value={v.path}>
                    {v.path} ({v.type})
                  </option>
                ))}
              </select>
            )}
          </div>

          {/* Update existing job */}
          <div>
            <label className="text-sm font-medium mb-1 block flex items-center gap-2">
              <RotateCcw className="h-3.5 w-3.5" />
              Update Existing Job ID (optional)
            </label>
            <Input
              placeholder="Leave blank to create a new job"
              value={updateJobId}
              onChange={(e) => setUpdateJobId(e.target.value)}
              type="number"
            />
          </div>
        </CardContent>
      </Card>

      {/* Schedule & Notifications */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <CalendarClock className="h-5 w-5" />
            Schedule & Notifications
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Schedule */}
          <div>
            <label className="text-sm font-medium mb-1 block">Cron Schedule (optional)</label>
            <div className="flex gap-2 mb-2 flex-wrap">
              {cronPresets.map((p) => (
                <button
                  key={p.value}
                  onClick={() => setSchedule(p.value)}
                  className={`text-xs px-2.5 py-1 rounded-md border transition-colors ${
                    schedule === p.value
                      ? "bg-[#FF3621]/10 border-[#FF3621] text-[#FF5540]"
                      : "border-border text-muted-foreground hover:border-[#FF3621]/50 hover:text-foreground"
                  }`}
                >
                  {p.label}
                </button>
              ))}
            </div>
            <Input
              placeholder="0 0 6 * * ? (Quartz cron)"
              value={schedule}
              onChange={(e) => setSchedule(e.target.value)}
            />
          </div>

          {/* Timezone */}
          <div>
            <label className="text-sm font-medium mb-1 block flex items-center gap-2">
              <Clock className="h-3.5 w-3.5" />
              Timezone
            </label>
            <select
              className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#FF3621]/30 focus:border-[#FF3621]"
              value={timezone}
              onChange={(e) => setTimezone(e.target.value)}
            >
              <option value="UTC">UTC</option>
              <option value="America/New_York">America/New_York (EST/EDT)</option>
              <option value="America/Chicago">America/Chicago (CST/CDT)</option>
              <option value="America/Denver">America/Denver (MST/MDT)</option>
              <option value="America/Los_Angeles">America/Los_Angeles (PST/PDT)</option>
              <option value="Europe/London">Europe/London (GMT/BST)</option>
              <option value="Europe/Berlin">Europe/Berlin (CET/CEST)</option>
              <option value="Asia/Tokyo">Asia/Tokyo (JST)</option>
              <option value="Asia/Kolkata">Asia/Kolkata (IST)</option>
              <option value="Australia/Sydney">Australia/Sydney (AEST/AEDT)</option>
            </select>
          </div>

          {/* Notifications */}
          <div>
            <label className="text-sm font-medium mb-1 block flex items-center gap-2">
              <Bell className="h-3.5 w-3.5" />
              Notification Emails (comma-separated)
            </label>
            <Input
              placeholder="team@company.com, admin@company.com"
              value={notificationEmail}
              onChange={(e) => setNotificationEmail(e.target.value)}
            />
          </div>

          {/* Advanced: retries, timeout, tags */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="text-sm font-medium mb-1 block">Max Retries</label>
              <Input
                type="number"
                min={0}
                max={10}
                value={maxRetries}
                onChange={(e) => setMaxRetries(parseInt(e.target.value, 10) || 0)}
              />
            </div>
            <div>
              <label className="text-sm font-medium mb-1 block">Timeout (seconds)</label>
              <Input
                type="number"
                min={300}
                value={timeout}
                onChange={(e) => setTimeout(parseInt(e.target.value, 10) || 7200)}
              />
            </div>
            <div>
              <label className="text-sm font-medium mb-1 block flex items-center gap-2">
                <Tag className="h-3.5 w-3.5" />
                Tags (key=value, comma-separated)
              </label>
              <Input
                placeholder="env=prod, team=data"
                value={tags}
                onChange={(e) => setTags(e.target.value)}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Clone Options */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings2 className="h-5 w-5" />
            Clone Options
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Clone Type & Load Type */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium">Clone Type</label>
              <div className="flex gap-2 mt-1">
                {(["DEEP", "SHALLOW"] as const).map((t) => (
                  <Button
                    key={t}
                    variant={cloneType === t ? "default" : "outline"}
                    size="sm"
                    onClick={() => setCloneType(t)}
                  >
                    {t}
                  </Button>
                ))}
              </div>
            </div>
            <div>
              <label className="text-sm font-medium">Load Type</label>
              <div className="flex gap-2 mt-1">
                {(["FULL", "INCREMENTAL"] as const).map((t) => (
                  <Button
                    key={t}
                    variant={loadType === t ? "default" : "outline"}
                    size="sm"
                    onClick={() => setLoadType(t)}
                  >
                    {t}
                  </Button>
                ))}
              </div>
            </div>
          </div>

          {/* Performance */}
          <div>
            <label className="text-sm font-medium mb-2 block flex items-center gap-2">
              <Gauge className="h-3.5 w-3.5" />
              Performance
            </label>
            <div className="grid grid-cols-4 gap-4">
              <div>
                <label className="text-xs text-gray-500">Max Workers (schemas)</label>
                <Input type="number" min={1} max={16} value={maxWorkers}
                  onChange={(e) => setMaxWorkers(parseInt(e.target.value) || 4)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Parallel Tables</label>
                <Input type="number" min={1} max={8} value={parallelTables}
                  onChange={(e) => setParallelTables(parseInt(e.target.value) || 1)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Max Parallel Queries</label>
                <Input type="number" min={1} max={50} value={maxParallelQueries}
                  onChange={(e) => setMaxParallelQueries(parseInt(e.target.value) || 10)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Max RPS (0=unlimited)</label>
                <Input type="number" min={0} max={100} value={maxRps}
                  onChange={(e) => setMaxRps(parseFloat(e.target.value) || 0)} />
              </div>
            </div>
            <div className="mt-3">
              <label className="text-xs text-gray-500">Order by Size</label>
              <div className="flex gap-1 mt-1">
                {(["", "asc", "desc"] as const).map((v) => (
                  <Button key={v || "none"} size="sm" variant={orderBySize === v ? "default" : "outline"}
                    onClick={() => setOrderBySize(v)}>
                    {v || "None"}
                  </Button>
                ))}
              </div>
            </div>
          </div>

          {/* Copy Options */}
          <div>
            <label className="text-sm font-medium mb-2 block">Copy Options</label>
            <div className="grid grid-cols-4 gap-2">
              {([
                [copyPermissions, setCopyPermissions, "Permissions"],
                [copyOwnership, setCopyOwnership, "Ownership"],
                [copyTags, setCopyTags, "Tags"],
                [copyProperties, setCopyProperties, "Properties"],
                [copySecurity, setCopySecurity, "Security"],
                [copyConstraints, setCopyConstraints, "Constraints"],
                [copyComments, setCopyComments, "Comments"],
              ] as const).map(([val, setter, label]) => (
                <label key={label} className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={val as boolean}
                    onChange={(e) => (setter as any)(e.target.checked)} />
                  {label}
                </label>
              ))}
            </div>
          </div>

          {/* Features */}
          <div>
            <label className="text-sm font-medium mb-2 block">Features</label>
            <div className="grid grid-cols-4 gap-2">
              {([
                [enableRollback, setEnableRollback, "Enable Rollback"],
                [validateAfterClone, setValidateAfterClone, "Validate After Clone"],
                [validateChecksum, setValidateChecksum, "Checksum Validation"],
                [forceReclone, setForceReclone, "Force Re-clone"],
                [showProgress, setShowProgress, "Show Progress"],
              ] as const).map(([val, setter, label]) => (
                <label key={label} className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={val as boolean}
                    onChange={(e) => (setter as any)(e.target.checked)} />
                  {label}
                </label>
              ))}
            </div>
          </div>

          {/* Filtering */}
          <div>
            <label className="text-sm font-medium mb-2 block flex items-center gap-2">
              <Filter className="h-3.5 w-3.5" />
              Filtering
            </label>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-xs text-gray-500">Include Schemas (comma-separated)</label>
                <Input placeholder="e.g. bronze,silver,gold" value={includeSchemas}
                  onChange={(e) => setIncludeSchemas(e.target.value)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Exclude Schemas (comma-separated)</label>
                <Input value={excludeSchemas}
                  onChange={(e) => setExcludeSchemas(e.target.value)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Include Tables Regex</label>
                <Input placeholder="e.g. ^fact_.*" value={includeTablesRegex}
                  onChange={(e) => setIncludeTablesRegex(e.target.value)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">Exclude Tables Regex</label>
                <Input placeholder="e.g. _tmp$|_backup$" value={excludeTablesRegex}
                  onChange={(e) => setExcludeTablesRegex(e.target.value)} />
              </div>
            </div>
          </div>

          {/* Time Travel */}
          <div>
            <label className="text-sm font-medium mb-2 block">Time Travel (optional)</label>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-xs text-gray-500">As-of Timestamp</label>
                <Input type="datetime-local" value={asOfTimestamp}
                  onChange={(e) => setAsOfTimestamp(e.target.value)} />
              </div>
              <div>
                <label className="text-xs text-gray-500">As-of Version</label>
                <Input type="number" min={0} placeholder="e.g. 5" value={asOfVersion}
                  onChange={(e) => setAsOfVersion(e.target.value)} />
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Submit */}
      <div className="flex gap-3">
        <Button
          onClick={handleSubmit}
          disabled={loading || !sourceCatalog || !destCatalog || !volume}
          size="lg"
        >
          {loading ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Briefcase className="h-4 w-4 mr-2" />
          )}
          {loading ? "Creating Job..." : updateJobId ? "Update Job" : "Create Databricks Job"}
        </Button>
      </div>

      {/* Result */}
      {result && (
        <Card className="border-green-500/30 bg-green-500/5">
          <CardContent className="pt-6 space-y-4">
            <div className="flex items-center gap-3">
              <CheckCircle className="h-6 w-6 text-green-500" />
              <div>
                <p className="font-semibold text-lg">
                  Job {updateJobId ? "Updated" : "Created"} Successfully
                </p>
                <p className="text-sm text-muted-foreground">{result.job_name}</p>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="bg-background rounded-lg p-3 border">
                <p className="text-xs text-muted-foreground">Job ID</p>
                <div className="flex items-center gap-2">
                  <p className="font-mono font-semibold">{result.job_id}</p>
                  <button
                    onClick={() => {
                      navigator.clipboard.writeText(String(result.job_id));
                      toast.success("Job ID copied");
                    }}
                    className="text-muted-foreground hover:text-foreground"
                  >
                    <Copy className="h-3.5 w-3.5" />
                  </button>
                </div>
              </div>
              <div className="bg-background rounded-lg p-3 border">
                <p className="text-xs text-muted-foreground">Notebook</p>
                <p className="font-mono text-sm truncate">{result.notebook_path}</p>
              </div>
              <div className="bg-background rounded-lg p-3 border">
                <p className="text-xs text-muted-foreground">Wheel</p>
                <p className="font-mono text-sm truncate">{result.volume_wheel_path}</p>
              </div>
              {result.schedule && (
                <div className="bg-background rounded-lg p-3 border">
                  <p className="text-xs text-muted-foreground">Schedule</p>
                  <p className="font-mono text-sm">{result.schedule}</p>
                </div>
              )}
            </div>

            <a
              href={result.job_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-[#FF3621] text-white text-sm font-medium hover:bg-[#e0301d] transition-colors"
            >
              <ExternalLink className="h-4 w-4" />
              Open in Databricks
            </a>
          </CardContent>
        </Card>
      )}

      {/* Error */}
      {error && !result && (
        <Card className="border-red-500/30 bg-red-500/5">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <XCircle className="h-6 w-6 text-red-500" />
              <div>
                <p className="font-semibold">Job Creation Failed</p>
                <p className="text-sm text-red-600">{error}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
