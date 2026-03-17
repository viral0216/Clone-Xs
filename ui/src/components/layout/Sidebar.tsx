import { useState, useEffect, useCallback } from "react";
import { Link, useLocation } from "react-router-dom";
import ResizeHandle from "@/components/ResizeHandle";
import {
  LayoutDashboard, Briefcase, Copy, FolderTree, GitCompare, Activity,
  Settings2, FileText, Wrench, Shield, GitBranch, ClipboardCheck, RefreshCw,
  GitCompareArrows, Wand2, ChevronRight, ChevronDown, History, BarChart3,
  Undo2, LayoutTemplate, CalendarClock, CopyPlus, GitFork, Zap, Eye,
  ScanSearch, Calculator, ShieldCheck, Server, Lock, Puzzle, HardDrive,
  X, Plus,
} from "lucide-react";

export interface NavItem { href: string; label: string; icon: React.ComponentType<{ className?: string }>; }
export interface NavSection { title: string; items: NavItem[]; }

// Get disabled pages from localStorage
function getDisabledPages(): Set<string> {
  try {
    const saved = localStorage.getItem("clxs-disabled-pages");
    return saved ? new Set(JSON.parse(saved)) : new Set();
  } catch { return new Set(); }
}

export function getFilteredSections(): NavSection[] {
  const disabled = getDisabledPages();
  return allNavSections
    .map(s => ({ ...s, items: s.items.filter(i => !disabled.has(i.href)) }))
    .filter(s => s.items.length > 0);
}

export const allNavSections: NavSection[] = [
  {
    title: "Overview",
    items: [
      { href: "/", label: "Dashboard", icon: LayoutDashboard },
      { href: "/audit", label: "Audit Trail", icon: History },
      { href: "/metrics", label: "Metrics", icon: BarChart3 },
    ],
  },
  {
    title: "Operations",
    items: [
      { href: "/clone", label: "Clone", icon: Copy },
      { href: "/sync", label: "Sync", icon: RefreshCw },
      { href: "/incremental-sync", label: "Incremental Sync", icon: GitCompareArrows },
      { href: "/generate", label: "Generate", icon: Wand2 },
      { href: "/rollback", label: "Rollback", icon: Undo2 },
      { href: "/templates", label: "Templates", icon: LayoutTemplate },
      { href: "/create-job", label: "Create Job", icon: Briefcase },
      { href: "/multi-clone", label: "Multi-Clone", icon: CopyPlus },
    ],
  },
  {
    title: "Discovery",
    items: [
      { href: "/explore", label: "Explorer", icon: FolderTree },
      { href: "/diff", label: "Diff & Compare", icon: GitCompare },
      { href: "/config-diff", label: "Config Diff", icon: GitCompareArrows },
      { href: "/lineage", label: "Lineage", icon: GitFork },
      { href: "/view-deps", label: "Dependencies", icon: GitBranch },
      { href: "/impact", label: "Impact Analysis", icon: Zap },
      { href: "/preview", label: "Data Preview", icon: Eye },
    ],
  },
  {
    title: "Analysis",
    items: [
      { href: "/reports", label: "Reports", icon: FileText },
      { href: "/pii", label: "PII Scanner", icon: Shield },
      { href: "/schema-drift", label: "Schema Drift", icon: GitBranch },
      { href: "/profiling", label: "Profiling", icon: ScanSearch },
      { href: "/cost", label: "Cost Estimator", icon: Calculator },
      { href: "/storage-metrics", label: "Storage Metrics", icon: HardDrive },
      { href: "/compliance", label: "Compliance", icon: ShieldCheck },
    ],
  },
  {
    title: "Management",
    items: [
      { href: "/monitor", label: "Monitor", icon: Activity },
      { href: "/preflight", label: "Preflight", icon: ClipboardCheck },
      { href: "/config", label: "Config", icon: Wrench },
      { href: "/settings", label: "Settings", icon: Settings2 },
      { href: "/warehouse", label: "Warehouse", icon: Server },
      { href: "/rbac", label: "RBAC", icon: Lock },
      { href: "/plugins", label: "Plugins", icon: Puzzle },
    ],
  },
];

interface SidebarProps { mobileOpen?: boolean; onMobileClose?: () => void; }

export default function Sidebar({ mobileOpen, onMobileClose }: SidebarProps) {
  const { pathname } = useLocation();
  const [, forceUpdate] = useState(0);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    () => new Set(getFilteredSections().map((s) => s.title))
  );
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    try { return Number(localStorage.getItem("clxs-sidebar-width")) || 208; } catch { return 208; }
  });
  const handleSidebarResize = useCallback((w: number) => {
    setSidebarWidth(w);
    try { localStorage.setItem("clxs-sidebar-width", String(w)); } catch {}
  }, []);

  // Re-render when feature toggles change from Settings page
  useEffect(() => {
    const handler = () => forceUpdate(n => n + 1);
    window.addEventListener("storage", handler);
    window.addEventListener("clxs-features-changed", handler);
    return () => {
      window.removeEventListener("storage", handler);
      window.removeEventListener("clxs-features-changed", handler);
    };
  }, []);

  const toggleSection = (title: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(title)) next.delete(title); else next.add(title);
      return next;
    });
  };

  const sidebarContent = (
    <div className="flex flex-col h-full">
      {/* + New button — light salmon bg, red icon like Databricks */}
      <div className="px-3 pt-3 pb-1">
        <Link
          to="/clone"
          className="flex items-center gap-2 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors"
          style={{ background: '#FCE8E6', color: '#D93025' }}
        >
          <Plus className="h-5 w-5" style={{ color: '#D93025' }} />
          New
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto pt-1 scrollbar-thin">
        {getFilteredSections().map((section, sIdx) => {
          const isExpanded = expandedSections.has(section.title);
          return (
            <div key={section.title} className={sIdx > 0 ? "mt-3" : "mt-1"}>
              {/* Section label */}
              <button
                onClick={() => toggleSection(section.title)}
                className="w-full flex items-center justify-between px-4 py-1 text-[12px] font-medium transition-colors"
                style={{ color: '#5F6368' }}
              >
                <span>{section.title}</span>
                {isExpanded
                  ? <ChevronDown className="h-3 w-3" />
                  : <ChevronRight className="h-3 w-3" />
                }
              </button>

              {/* Items */}
              {isExpanded && section.items.map((item) => {
                const Icon = item.icon;
                const active = pathname === item.href;
                return (
                  <Link
                    key={item.href}
                    to={item.href}
                    onClick={onMobileClose}
                    className="flex items-center gap-3 px-4 py-[7px] text-sm transition-all rounded-r-full mr-2"
                    style={active ? {
                      background: '#E8F0FE',
                      color: '#1A73E8',
                      fontWeight: 500,
                    } : {
                      color: '#3C4043',
                    }}
                    onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = '#F1F3F4'; }}
                    onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = 'transparent'; }}
                  >
                    <Icon
                      className="h-5 w-5 shrink-0"
                      style={{ color: active ? '#1A73E8' : '#5F6368' }}
                    />
                    <span className="truncate">{item.label}</span>
                  </Link>
                );
              })}
            </div>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-4 py-2 border-t" style={{ borderColor: '#E8EAED' }}>
        <span className="text-[10px]" style={{ color: '#9AA0A6' }}>cloneXs v0.5.0</span>
      </div>

      {/* Mobile close */}
      {mobileOpen && onMobileClose && (
        <button onClick={onMobileClose} className="absolute top-3 right-3 p-1 rounded lg:hidden" style={{ color: '#5F6368' }}>
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );

  // Dark mode wrapper — in dark mode, override inline styles
  const darkSidebar = (
    <div className="flex flex-col h-full">
      {/* + New button — dark mode */}
      <div className="px-3 pt-3 pb-1">
        <Link
          to="/clone"
          className="flex items-center gap-2 px-3 py-2.5 rounded-lg text-sm font-medium bg-blue-600 hover:bg-blue-700 text-white transition-colors"
        >
          <Plus className="h-5 w-5" />
          New
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto pt-1 scrollbar-thin">
        {getFilteredSections().map((section, sIdx) => {
          const isExpanded = expandedSections.has(section.title);
          return (
            <div key={section.title} className={sIdx > 0 ? "mt-3" : "mt-1"}>
              <button
                onClick={() => toggleSection(section.title)}
                className="w-full flex items-center justify-between px-4 py-1 text-[12px] font-medium text-gray-500 hover:text-gray-400 transition-colors"
              >
                <span>{section.title}</span>
                {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
              </button>

              {isExpanded && section.items.map((item) => {
                const Icon = item.icon;
                const active = pathname === item.href;
                return (
                  <Link
                    key={item.href}
                    to={item.href}
                    onClick={onMobileClose}
                    className={`flex items-center gap-3 px-4 py-[7px] text-sm transition-all rounded-r-full mr-2 ${
                      active
                        ? "bg-blue-500/15 text-blue-400 font-medium"
                        : "text-gray-300 hover:bg-white/5"
                    }`}
                  >
                    <Icon className={`h-5 w-5 shrink-0 ${active ? "text-blue-400" : "text-gray-500"}`} />
                    <span className="truncate">{item.label}</span>
                  </Link>
                );
              })}
            </div>
          );
        })}
      </nav>

      <div className="px-4 py-2 border-t border-white/10">
        <span className="text-[10px] text-gray-600">cloneXs v0.5.0</span>
      </div>

      {mobileOpen && onMobileClose && (
        <button onClick={onMobileClose} className="absolute top-3 right-3 p-1 rounded text-gray-500 lg:hidden">
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );

  return (
    <>
      {mobileOpen && (
        <div className="fixed inset-0 bg-black/40 z-40 lg:hidden" onClick={onMobileClose} />
      )}

      {/* Mobile */}
      <aside className={`fixed inset-y-0 left-0 z-50 w-52 sidebar-bg flex flex-col relative transform transition-transform duration-200 lg:hidden ${
        mobileOpen ? "translate-x-0" : "-translate-x-full"
      }`}>
        <div className="hidden dark:block h-full">{darkSidebar}</div>
        <div className="dark:hidden h-full">{sidebarContent}</div>
      </aside>

      {/* Desktop */}
      <aside className="sidebar-bg flex-col hidden lg:flex relative shrink-0" style={{ width: sidebarWidth }}>
        <div className="hidden dark:block h-full">{darkSidebar}</div>
        <div className="dark:hidden h-full">{sidebarContent}</div>
      </aside>
      <div className="hidden lg:block">
        <ResizeHandle width={sidebarWidth} onResize={handleSidebarResize} min={160} max={320} side="right" />
      </div>
    </>
  );
}
