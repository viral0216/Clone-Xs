/**
 * DataProfilePanel — Deep data profiler panel for Data Lab.
 *
 * Modes:
 *  - tableFqn: profile a catalog table via POST /api/profile-table
 *  - querySql: profile query results via POST /api/profile-results
 *  - profileData: display pre-fetched profile data
 */
import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import { Loader2, X, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import ProfileSummaryHeader from "./ProfileSummaryHeader";
import ColumnProfileCard from "./ColumnProfileCard";

interface ColumnProfile {
  column_name: string;
  data_type: string;
  null_count: number;
  null_pct: number;
  distinct_count: number;
  distinct_pct: number;
  min?: any;
  max?: any;
  avg?: number | null;
  min_length?: number | null;
  max_length?: number | null;
  avg_length?: number | null;
  histogram?: any[] | null;
  top_values?: any[] | null;
}

interface ProfileResult {
  table_fqn: string;
  row_count: number;
  profiled_at: string;
  columns: ColumnProfile[];
  error?: string | null;
}

interface Props {
  tableFqn?: string;
  querySql?: string;
  profileData?: ProfileResult;
  onClose?: () => void;
}

export default function DataProfilePanel({ tableFqn, querySql, profileData: initialData, onClose }: Props) {
  const [profile, setProfile] = useState<ProfileResult | null>(initialData || null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function fetchProfile() {
    setLoading(true);
    setError(null);
    try {
      let res: ProfileResult;
      if (tableFqn) {
        res = await api.post("/profile-table", { table_fqn: tableFqn });
      } else if (querySql) {
        res = await api.post("/profile-results", { sql: querySql });
      } else {
        setLoading(false);
        return;
      }
      if (res.error) {
        setError(res.error);
      } else {
        setProfile(res);
      }
    } catch (e: any) {
      setError(e.message || "Profile request failed");
    }
    setLoading(false);
  }

  useEffect(() => {
    if (!initialData && (tableFqn || querySql)) {
      fetchProfile();
    }
  }, [tableFqn, querySql]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin text-[#E8453C]" />
        <span className="text-sm">Profiling {tableFqn || "query results"}...</span>
        <span className="text-[10px]">Computing column stats, histograms, and top values</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3">
        <p className="text-sm text-red-400">{error}</p>
        <Button size="sm" variant="outline" onClick={fetchProfile} className="gap-1.5">
          <RefreshCw className="h-3 w-3" /> Retry
        </Button>
      </div>
    );
  }

  if (!profile) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
        <p className="text-sm">Select a table or run a query to see its profile</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header bar */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b border-border bg-muted/20 shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold text-foreground">Data Profile</span>
          <span className="text-[10px] text-muted-foreground font-mono">{profile.table_fqn}</span>
        </div>
        <div className="flex items-center gap-1">
          <Button size="sm" variant="ghost" onClick={fetchProfile} title="Refresh profile" className="h-6 w-6 p-0">
            <RefreshCw className="h-3 w-3" />
          </Button>
          {onClose && (
            <Button size="sm" variant="ghost" onClick={onClose} title="Close profile" className="h-6 w-6 p-0">
              <X className="h-3 w-3" />
            </Button>
          )}
        </div>
      </div>

      {/* Summary KPIs */}
      <ProfileSummaryHeader
        rowCount={profile.row_count}
        columns={profile.columns}
        profiledAt={profile.profiled_at}
      />

      {/* Column profiles */}
      <div className="flex-1 overflow-y-auto">
        {profile.columns.map((col, i) => (
          <ColumnProfileCard key={col.column_name} profile={col} rowCount={profile.row_count} index={i} />
        ))}
      </div>
    </div>
  );
}
