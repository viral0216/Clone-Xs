import { useState, useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  LayoutDashboard, Zap, ShieldCheck, BarChart3, ClipboardCheck,
  Rows3, Columns3, Activity, ScanSearch, Shield, GitCompare,
  Fingerprint, CheckSquare, Search, SearchCode, History,
  Clock, Database, AlertTriangle, Bell, ClipboardList, Settings,
  DollarSign, PanelLeftClose, PanelLeftOpen, UserX,
} from "lucide-react";

const NAV_SECTIONS = [
  {
    title: "Overview",
    items: [
      { href: "/data-quality", label: "Dashboard", icon: LayoutDashboard },
    ],
  },
  {
    title: "Monitoring",
    items: [
      { href: "/data-quality/freshness", label: "Data Freshness", icon: Clock },
      { href: "/data-quality/volume", label: "Volume Monitor", icon: Database },
      { href: "/data-quality/anomalies", label: "Anomalies", icon: AlertTriangle },
      { href: "/data-quality/incidents", label: "Incidents", icon: Bell },
      { href: "/data-quality/monitoring", label: "Configuration", icon: Settings },
    ],
  },
  {
    title: "Rules & Checks",
    items: [
      { href: "/data-quality/dqx", label: "DQX Engine", icon: Zap },
      { href: "/data-quality/rules", label: "Rules Engine", icon: ShieldCheck },
      { href: "/data-quality/dashboard", label: "DQ Dashboard", icon: BarChart3 },
      { href: "/data-quality/results", label: "Results", icon: ClipboardCheck },
    ],
  },
  {
    title: "Suites",
    items: [
      { href: "/data-quality/expectations", label: "Expectation Suites", icon: ClipboardList },
    ],
  },
  {
    title: "Reconciliation",
    items: [
      { href: "/data-quality/reconciliation/row-level", label: "Row-Level", icon: Rows3 },
      { href: "/data-quality/reconciliation/column-level", label: "Column-Level", icon: Columns3 },
      { href: "/data-quality/reconciliation/deep", label: "Deep Diff", icon: SearchCode },
      { href: "/data-quality/reconciliation/history", label: "Run History", icon: History },
    ],
  },
  {
    title: "Profiling",
    items: [
      { href: "/data-quality/profiling", label: "Column Profiles", icon: Activity },
      { href: "/data-quality/schema-drift", label: "Schema Drift", icon: GitCompare },
      { href: "/data-quality/diff", label: "Diff & Compare", icon: Search },
    ],
  },
  {
    title: "Validation",
    items: [
      { href: "/data-quality/preflight", label: "Preflight Checks", icon: CheckSquare },
      { href: "/data-quality/compliance", label: "Compliance", icon: Shield },
      { href: "/data-quality/pii", label: "PII Scanner", icon: Fingerprint },
    ],
  },
  {
    title: "Compliance",
    items: [
      { href: "/data-quality/rtbf", label: "RTBF / Erasure", icon: UserX },
    ],
  },
];

const STORAGE_KEY = "clxs-dq-sidebar-collapsed";

export default function DataQualitySidebar() {
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(() => {
    try { return localStorage.getItem(STORAGE_KEY) === "true"; } catch { return false; }
  });

  const toggleCollapsed = () => {
    setCollapsed(prev => {
      const next = !prev;
      try { localStorage.setItem(STORAGE_KEY, String(next)); } catch {}
      return next;
    });
  };

  useEffect(() => {
    const handler = () => {
      try { setCollapsed(localStorage.getItem(STORAGE_KEY) === "true"); } catch {}
    };
    window.addEventListener("storage", handler);
    return () => window.removeEventListener("storage", handler);
  }, []);

  if (collapsed) {
    return (
      <aside className="w-11 shrink-0 border-r border-border bg-background hidden lg:flex flex-col">
        <div className="flex flex-col items-center h-full py-2">
          <nav className="flex-1 overflow-y-auto w-full flex flex-col items-center gap-0.5 pt-1" aria-label="Data Quality navigation">
            {NAV_SECTIONS.flatMap((section) =>
              section.items.map((item) => {
                const Icon = item.icon;
                const active = location.pathname === item.href ||
                  (item.href !== "/data-quality" && location.pathname.startsWith(item.href));
                return (
                  <Link
                    key={item.href}
                    to={item.href}
                    title={item.label}
                    aria-label={item.label}
                    aria-current={active ? "page" : undefined}
                    className={`flex items-center justify-center w-8 h-8 rounded-md transition-colors ${
                      active
                        ? "bg-[#E8453C]/10 text-[#E8453C]"
                        : "text-muted-foreground hover:bg-muted/50 hover:text-foreground"
                    }`}
                  >
                    <Icon className="h-4 w-4" />
                  </Link>
                );
              })
            )}
          </nav>
          <button
            onClick={toggleCollapsed}
            className="p-2 mt-2 rounded-lg hover:bg-muted/50 text-muted-foreground hover:text-foreground transition-colors"
            aria-label="Expand sidebar"
            title="Expand sidebar"
          >
            <PanelLeftOpen className="h-4 w-4" />
          </button>
        </div>
      </aside>
    );
  }

  return (
    <aside className="w-56 shrink-0 border-r border-border bg-background h-full flex flex-col">
      <div className="px-4 py-3 border-b border-border">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Data Quality Portal</p>
      </div>
      <nav className="py-2 flex-1 overflow-y-auto" aria-label="Data Quality navigation">
        {NAV_SECTIONS.map((section) => (
          <div key={section.title} className="mb-1">
            <p className="px-4 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">{section.title}</p>
            {section.items.map((item) => {
              const Icon = item.icon;
              const active = location.pathname === item.href ||
                (item.href !== "/data-quality" && location.pathname.startsWith(item.href));
              return (
                <Link
                  key={item.href}
                  to={item.href}
                  aria-current={active ? "page" : undefined}
                  className={`flex items-center gap-2.5 px-4 py-2 text-sm transition-colors ${
                    active
                      ? "bg-[#E8453C]/5 dark:bg-[#E8453C]/10 text-[#E8453C] dark:text-[#E8453C] font-medium border-r-2 border-[#E8453C]"
                      : "text-muted-foreground hover:text-foreground hover:bg-accent/50"
                  }`}
                >
                  <Icon className={`h-4 w-4 shrink-0 ${active ? "text-[#E8453C]" : ""}`} />
                  <span className="truncate">{item.label}</span>
                </Link>
              );
            })}
          </div>
        ))}
      </nav>
      <div className="px-3 py-2 border-t border-border shrink-0">
        <button
          onClick={toggleCollapsed}
          className="flex items-center gap-2 w-full px-2 py-1.5 rounded-md text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
          aria-label="Collapse sidebar"
          title="Collapse sidebar"
        >
          <PanelLeftClose className="h-3.5 w-3.5" />
          <span>Collapse</span>
        </button>
      </div>
    </aside>
  );
}
