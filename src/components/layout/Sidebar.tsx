import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  Copy,
  FolderTree,
  GitCompare,
  Activity,
  Settings2,
  FileText,
  Wrench,
  Shield,
  GitBranch,
  ClipboardCheck,
  RefreshCw,
  GitCompareArrows,
  Wand2,
  ChevronRight,
  ChevronDown,
  PanelLeftClose,
  PanelLeft,
  History,
  BarChart3,
  Undo2,
  LayoutTemplate,
  CalendarClock,
  CopyPlus,
  GitFork,
  Zap,
  Eye,
  ScanSearch,
  Calculator,
  ShieldCheck,
  Server,
  Lock,
  Puzzle,
  X,
} from "lucide-react";

interface NavItem {
  href: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}

interface NavSection {
  title: string;
  items: NavItem[];
}

const navSections: NavSection[] = [
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
      { href: "/generate", label: "Generate", icon: Wand2 },
      { href: "/rollback", label: "Rollback", icon: Undo2 },
      { href: "/templates", label: "Templates", icon: LayoutTemplate },
      { href: "/schedule", label: "Schedule", icon: CalendarClock },
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

interface SidebarProps {
  mobileOpen?: boolean;
  onMobileClose?: () => void;
}

export default function Sidebar({ mobileOpen, onMobileClose }: SidebarProps) {
  const { pathname } = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    () => new Set(navSections.map((s) => s.title))
  );

  const toggleSection = (title: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      if (next.has(title)) {
        next.delete(title);
      } else {
        next.add(title);
      }
      return next;
    });
  };

  const sidebarContent = (
    <>
      {/* Sidebar Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
        {!collapsed && (
          <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
            Navigator
          </span>
        )}
        {/* Close button on mobile */}
        {mobileOpen && onMobileClose ? (
          <button
            onClick={onMobileClose}
            className="p-1.5 rounded-md text-gray-400 hover:text-white hover:bg-white/10 transition-all lg:hidden"
          >
            <X className="h-4 w-4" />
          </button>
        ) : (
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="p-1.5 rounded-md text-gray-400 hover:text-white hover:bg-white/10 transition-all hidden lg:block"
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? (
              <PanelLeft className="h-4 w-4" />
            ) : (
              <PanelLeftClose className="h-4 w-4" />
            )}
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto py-2 scrollbar-thin">
        {navSections.map((section) => {
          const isExpanded = expandedSections.has(section.title);
          const hasActiveItem = section.items.some(
            (item) => item.href === pathname
          );

          return (
            <div key={section.title} className="mb-1">
              {/* Section Header */}
              {!collapsed && (
                <button
                  onClick={() => toggleSection(section.title)}
                  className={`w-full flex items-center justify-between px-4 py-2 text-xs font-semibold uppercase tracking-wider transition-colors ${
                    hasActiveItem
                      ? "text-[#FF5540]"
                      : "text-gray-500 hover:text-gray-300"
                  }`}
                >
                  <span className="flex items-center gap-2">
                    {section.title}
                    <span className="text-[10px] font-normal text-gray-600">
                      ({section.items.length})
                    </span>
                  </span>
                  {isExpanded ? (
                    <ChevronDown className="h-3 w-3" />
                  ) : (
                    <ChevronRight className="h-3 w-3" />
                  )}
                </button>
              )}

              {/* Section Items */}
              {(collapsed || isExpanded) && (
                <div className={collapsed ? "px-2 space-y-1" : "px-2 space-y-0.5"}>
                  {section.items.map((item) => {
                    const Icon = item.icon;
                    const active = pathname === item.href;
                    return (
                      <Link
                        key={item.href}
                        to={item.href}
                        onClick={onMobileClose}
                        title={collapsed ? item.label : undefined}
                        className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-all ${
                          collapsed ? "justify-center" : ""
                        } ${
                          active
                            ? "bg-[#FF3621]/15 text-[#FF5540] font-medium"
                            : "text-gray-400 hover:bg-white/5 hover:text-white"
                        }`}
                      >
                        <Icon
                          className={`h-4 w-4 shrink-0 ${
                            active ? "text-[#FF5540]" : ""
                          }`}
                        />
                        {!collapsed && <span className="truncate">{item.label}</span>}
                        {!collapsed && active && (
                          <ChevronRight className="h-3 w-3 ml-auto shrink-0 text-[#FF5540]" />
                        )}
                      </Link>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-white/10">
        <span className="text-[10px] text-gray-600">
          {collapsed ? "v0.4" : "v0.4.0"}
        </span>
      </div>
    </>
  );

  return (
    <>
      {/* Mobile overlay */}
      {mobileOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={onMobileClose}
        />
      )}

      {/* Mobile sidebar (slide-in overlay) */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 w-64 bg-[#0d1117] text-white flex flex-col border-r border-white/10 transform transition-transform duration-200 lg:hidden ${
          mobileOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        {sidebarContent}
      </aside>

      {/* Desktop sidebar */}
      <aside
        className={`${
          collapsed ? "w-16" : "w-60"
        } bg-[#0d1117] text-white flex-col border-r border-white/10 transition-all duration-200 overflow-hidden hidden lg:flex`}
      >
        {sidebarContent}
      </aside>
    </>
  );
}
