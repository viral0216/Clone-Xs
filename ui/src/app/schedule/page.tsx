// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { CalendarClock, Plus, RefreshCw, Pause, Play } from "lucide-react";

interface Schedule {
  id: string;
  name: string;
  source: string;
  destination: string;
  cron: string;
  next_run?: string;
  last_run?: string;
  status: string;
  template?: string;
}

export default function SchedulePage() {
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ name: "", source: "", destination: "", cron: "", template: "" });

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<Schedule[]>("/schedule");
      setSchedules(Array.isArray(res) ? res : []);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const createSchedule = async () => {
    setError("");
    try {
      await api.post("/schedule", form);
      setShowForm(false);
      setForm({ name: "", source: "", destination: "", cron: "", template: "" });
      load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const toggleStatus = async (id: string, current: string) => {
    try {
      await api.post(`/schedule/${id}/${current === "active" ? "pause" : "resume"}`);
      load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Schedule</h1>
          <p className="text-muted-foreground mt-1">Schedule clone operations on a cron-based schedule — daily, hourly, or custom intervals. Runs are tracked in the audit trail.</p>
          <p className="text-xs text-muted-foreground mt-1">
            <a href="https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/create-run-jobs" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Databricks Jobs</a>
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => setShowForm(!showForm)} variant={showForm ? "secondary" : "default"}>
            <Plus className="h-4 w-4 mr-1" /> Create Schedule
          </Button>
          <Button onClick={load} disabled={loading} variant="outline">
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      {showForm && (
        <Card>
          <CardHeader><CardTitle className="text-sm">New Schedule</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-foreground">Name</label>
                <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="nightly-sync" />
              </div>
              <div>
                <label className="text-sm font-medium text-foreground">Cron Expression</label>
                <Input value={form.cron} onChange={(e) => setForm({ ...form, cron: e.target.value })} placeholder="0 2 * * *" />
              </div>
              <div>
                <label className="text-sm font-medium text-foreground">Source Catalog</label>
                <Input value={form.source} onChange={(e) => setForm({ ...form, source: e.target.value })} placeholder="production" />
              </div>
              <div>
                <label className="text-sm font-medium text-foreground">Destination Catalog</label>
                <Input value={form.destination} onChange={(e) => setForm({ ...form, destination: e.target.value })} placeholder="staging" />
              </div>
              <div>
                <label className="text-sm font-medium text-foreground">Template (optional)</label>
                <Input value={form.template} onChange={(e) => setForm({ ...form, template: e.target.value })} placeholder="production_mirror" />
              </div>
            </div>
            <Button onClick={createSchedule} disabled={!form.name || !form.source || !form.destination || !form.cron}>
              Create
            </Button>
          </CardContent>
        </Card>
      )}

      {schedules.length === 0 && !loading && !error ? (
        <div className="text-center py-12 text-muted-foreground">
          <CalendarClock className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>No scheduled jobs</p>
        </div>
      ) : (
        <Card>
          <CardContent className="pt-6">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/50">
                    <th className="text-left py-2 px-3 font-medium text-foreground">Name</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Source &rarr; Dest</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Cron</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Next Run</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Last Run</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Status</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {schedules.map((s) => (
                    <tr key={s.id} className="border-b border-border hover:bg-muted/30">
                      <td className="py-2 px-3 font-medium text-foreground">{s.name}</td>
                      <td className="py-2 px-3 text-foreground">{s.source} &rarr; {s.destination}</td>
                      <td className="py-2 px-3 font-mono text-muted-foreground">{s.cron}</td>
                      <td className="py-2 px-3 text-muted-foreground">{s.next_run ?? "—"}</td>
                      <td className="py-2 px-3 text-muted-foreground">{s.last_run ?? "—"}</td>
                      <td className="py-2 px-3">
                        <Badge variant={s.status === "active" ? "default" : "secondary"}>{s.status}</Badge>
                      </td>
                      <td className="py-2 px-3">
                        <Button size="sm" variant="ghost" onClick={() => toggleStatus(s.id, s.status)}>
                          {s.status === "active" ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
