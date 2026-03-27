import { useState, useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  LayoutDashboard, DollarSign, HardDrive, Zap, Server,
  TrendingDown, Target, BarChart3, Lightbulb, Receipt,
  Database, Clock, Settings, PieChart, Briefcase,
  PanelLeftClose, PanelLeftOpen,
} from "lucide-react";

const NAV_SECTIONS = [
  {
    title: "Overview",
    items: [
      { href: "/finops", label: "Dashboard", icon: LayoutDashboard },
    ],
  },
  {
    title: "Cost Analysis",
    items: [
      { href: "/finops/billing", label: "Billing & DBUs", icon: Receipt },
      { href: "/finops/storage", label: "Storage Costs", icon: HardDrive },
      { href: "/finops/compute", label: "Compute Costs", icon: Zap },
      { href: "/finops/breakdown", label: "Cost Breakdown", icon: PieChart },
    ],
  },
  {
    title: "Cost Attribution",
    items: [
      { href: "/finops/query-costs", label: "Query Costs", icon: Clock },
      { href: "/finops/job-costs", label: "Job Costs", icon: Briefcase },
    ],
  },
  {
    title: "Optimization",
    items: [
      { href: "/finops/recommendations", label: "Recommendations", icon: Lightbulb },
      { href: "/finops/warehouses", label: "Warehouse Efficiency", icon: Server },
      { href: "/finops/storage-optimization", label: "Storage Optimization", icon: TrendingDown },
    ],
  },
  {
    title: "Budgets & Alerts",
    items: [
      { href: "/finops/budgets", label: "Budget Tracker", icon: Target },
      { href: "/finops/trends", label: "Cost Trends", icon: BarChart3 },
    ],
  },
];

const STORAGE_KEY = "clxs-finops-sidebar-collapsed";

export default function FinOpsSidebar() {
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(() => {
    try { return localStorage.getItem(STORAGE_KEY) === "true"; } catch { return false; }
  });

  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, String(collapsed)); } catch {}
  }, [collapsed]);

  if (collapsed) {
    return (
      <aside className="w-12 border-r border-border bg-background shrink-0 flex flex-col items-center py-3 gap-1">
        <button onClick={() => setCollapsed(false)} className="p-1.5 rounded-md hover:bg-accent/50 mb-2" title="Expand sidebar">
          <PanelLeftOpen className="h-4 w-4 text-muted-foreground" />
        </button>
        {NAV_SECTIONS.flatMap(s => s.items).map(item => {
          const Icon = item.icon;
          const active = location.pathname === item.href;
          return (
            <Link key={item.href} to={item.href} title={item.label}
              className={`p-1.5 rounded-md transition-colors ${active ? "bg-green-50 dark:bg-green-950/30 text-green-600" : "text-muted-foreground hover:bg-accent/50"}`}>
              <Icon className="h-4 w-4" />
            </Link>
          );
        })}
      </aside>
    );
  }

  return (
    <aside className="w-56 border-r border-border bg-background shrink-0 overflow-y-auto">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">FinOps Portal</span>
        <button onClick={() => setCollapsed(true)} className="p-1 rounded-md hover:bg-accent/50" title="Collapse sidebar">
          <PanelLeftClose className="h-3.5 w-3.5 text-muted-foreground" />
        </button>
      </div>
      <nav className="py-2">
        {NAV_SECTIONS.map((section) => (
          <div key={section.title} className="mb-1">
            <p className="px-4 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {section.title}
            </p>
            {section.items.map((item) => {
              const Icon = item.icon;
              const active = location.pathname === item.href;
              return (
                <Link
                  key={item.href}
                  to={item.href}
                  className={`flex items-center gap-2.5 px-4 py-1.5 text-sm transition-colors ${
                    active
                      ? "bg-green-50 dark:bg-green-950/30 text-green-700 dark:text-green-400 font-medium border-r-2 border-green-600"
                      : "text-muted-foreground hover:text-foreground hover:bg-accent/50"
                  }`}
                >
                  <Icon className="h-4 w-4 shrink-0" />
                  {item.label}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>
    </aside>
  );
}
