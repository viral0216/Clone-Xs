import { useState, useEffect, useCallback } from "react";
import { Link, useLocation } from "react-router-dom";
import ResizeHandle from "@/components/ResizeHandle";
import {
  LayoutDashboard, Copy, FolderTree, Activity,
  Settings2, FileText, Wrench, GitBranch, RefreshCw,
  ChevronRight, ChevronDown, History, BarChart3,
  Undo2, GitFork, Zap, Layers,
  X, Plus, PanelLeftClose, PanelLeftOpen,
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
      { href: "/reports", label: "Reports", icon: FileText },
    ],
  },
  {
    title: "Operations",
    items: [
      { href: "/clone", label: "Clone", icon: Copy },
      { href: "/sync", label: "Sync", icon: RefreshCw },
      { href: "/rollback", label: "Rollback", icon: Undo2 },
      { href: "/dlt", label: "Delta Live Tables", icon: Zap },
    ],
  },
  {
    title: "Discovery",
    items: [
      { href: "/explore", label: "Explorer", icon: FolderTree },
      { href: "/lineage", label: "Lineage", icon: GitFork },
      { href: "/view-deps", label: "Dependencies", icon: GitBranch },
      { href: "/impact", label: "Impact Analysis", icon: Zap },
    ],
  },
  {
    title: "Management",
    items: [
      { href: "/monitor", label: "Monitor", icon: Activity },
      { href: "/config", label: "Config", icon: Wrench },
      { href: "/settings", label: "Settings", icon: Settings2 },
      { href: "/advanced-tables", label: "Advanced Tables", icon: Layers },
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
    try { return Number(localStorage.getItem("clxs-sidebar-width")) || 180; } catch { return 180; }
  });
  const handleSidebarResize = useCallback((w: number) => {
    setSidebarWidth(w);
    try { localStorage.setItem("clxs-sidebar-width", String(w)); } catch {}
  }, []);

  const [collapsed, setCollapsed] = useState(() => {
    try { return localStorage.getItem("clxs-sidebar-collapsed") === "true"; } catch { return false; }
  });
  const toggleCollapsed = () => {
    setCollapsed(prev => {
      const next = !prev;
      localStorage.setItem("clxs-sidebar-collapsed", String(next));
      window.dispatchEvent(new Event("clxs-sidebar-changed"));
      return next;
    });
  };

  // Sync collapsed state from Settings or other sources
  useEffect(() => {
    const handler = () => {
      const val = localStorage.getItem("clxs-sidebar-collapsed") === "true";
      setCollapsed(val);
    };
    window.addEventListener("clxs-sidebar-changed", handler);
    return () => window.removeEventListener("clxs-sidebar-changed", handler);
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
      <div className="px-2 pt-2 pb-1">
        <Link
          to="/clone"
          className="flex items-center gap-2 px-3 py-2 rounded-lg text-[13px] font-medium transition-colors"
          style={{ background: '#FCE8E6', color: '#D93025' }}
        >
          <Plus className="h-4 w-4" style={{ color: '#D93025' }} />
          New
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto pt-1 scrollbar-thin" aria-label="Main navigation">
        {getFilteredSections().map((section, sIdx) => {
          const isExpanded = expandedSections.has(section.title);
          const sectionId = `sidebar-section-${section.title.toLowerCase().replace(/\s/g, '-')}`;
          return (
            <div key={section.title} className={sIdx > 0 ? "mt-2" : "mt-1"}>
              {/* Section label */}
              <button
                onClick={() => toggleSection(section.title)}
                className="w-full flex items-center justify-between px-3 py-1.5 text-[11px] font-medium uppercase tracking-wider transition-colors"
                style={{ color: '#5F6368' }}
                aria-expanded={isExpanded}
                aria-controls={sectionId}
              >
                <span>{section.title}</span>
                {isExpanded
                  ? <ChevronDown className="h-3 w-3" />
                  : <ChevronRight className="h-3 w-3" />
                }
              </button>

              {/* Items */}
              <div id={sectionId}>
              {isExpanded && section.items.map((item) => {
                const Icon = item.icon;
                const active = pathname === item.href;
                return (
                  <Link
                    key={item.href}
                    to={item.href}
                    onClick={onMobileClose}
                    aria-current={active ? "page" : undefined}
                    className="flex items-center gap-2.5 mx-1 pl-2.5 pr-2 py-1.5 text-[13px] transition-all rounded-md"
                    style={active ? {
                      background: 'rgba(232,69,60,0.08)',
                      color: '#E8453C',
                      fontWeight: 500,
                    } : {
                      color: '#3C4043',
                    }}
                    onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = '#F1F3F4'; }}
                    onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = 'transparent'; }}
                  >
                    <Icon
                      className={`h-4 w-4 shrink-0 ${active ? 'text-[#E8453C]' : 'text-[#5F6368]'}`}
                    />
                    <span className="truncate">{item.label}</span>
                  </Link>
                );
              })}
              </div>
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
        <button onClick={onMobileClose} className="absolute top-3 right-3 p-1 rounded lg:hidden" aria-label="Close navigation menu" style={{ color: '#5F6368' }}>
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );

  // Dark mode wrapper — in dark mode, override inline styles
  const darkSidebar = (
    <div className="flex flex-col h-full">
      {/* + New button — dark mode */}
      <div className="px-2 pt-2 pb-1">
        <Link
          to="/clone"
          className="flex items-center gap-2 px-3 py-2 rounded-lg text-[13px] font-medium bg-sidebar-primary text-sidebar-primary-foreground hover:opacity-90 transition-colors"
        >
          <Plus className="h-4 w-4" />
          New
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto pt-1 scrollbar-thin" aria-label="Main navigation">
        {getFilteredSections().map((section, sIdx) => {
          const isExpanded = expandedSections.has(section.title);
          const sectionId = `sidebar-dark-section-${section.title.toLowerCase().replace(/\s/g, '-')}`;
          return (
            <div key={section.title} className={sIdx > 0 ? "mt-2" : "mt-1"}>
              <button
                onClick={() => toggleSection(section.title)}
                className="w-full flex items-center justify-between px-3 py-1.5 text-[11px] font-medium uppercase tracking-wider text-sidebar-foreground/40 hover:text-sidebar-foreground/60 transition-colors"
                aria-expanded={isExpanded}
                aria-controls={sectionId}
              >
                <span>{section.title}</span>
                {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
              </button>

              <div id={sectionId}>
              {isExpanded && section.items.map((item) => {
                const Icon = item.icon;
                const active = pathname === item.href;
                return (
                  <Link
                    key={item.href}
                    to={item.href}
                    onClick={onMobileClose}
                    aria-current={active ? "page" : undefined}
                    className={`flex items-center gap-2.5 mx-1 pl-2.5 pr-2 py-1.5 text-[13px] transition-all rounded-md ${
                      active
                        ? "bg-sidebar-accent text-sidebar-primary font-medium"
                        : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50"
                    }`}
                  >
                    <Icon className={`h-4 w-4 shrink-0 ${active ? "text-sidebar-primary" : "text-sidebar-foreground/40"}`} />
                    <span className="truncate">{item.label}</span>
                  </Link>
                );
              })}
              </div>
            </div>
          );
        })}
      </nav>

      <div className="px-4 py-2 border-t border-sidebar-border">
        <span className="text-[10px] text-sidebar-foreground/30">cloneXs v0.5.0</span>
      </div>

      {mobileOpen && onMobileClose && (
        <button onClick={onMobileClose} className="absolute top-3 right-3 p-1 rounded text-sidebar-foreground/40 lg:hidden" aria-label="Close navigation menu">
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );

  return (
    <>
      {mobileOpen && (
        <div className="fixed inset-0 bg-black/40 z-40 lg:hidden" onClick={onMobileClose} aria-hidden="true" />
      )}

      {/* Mobile */}
      <aside className={`fixed inset-y-0 left-0 z-50 w-52 sidebar-bg flex flex-col relative transform transition-transform duration-200 lg:hidden ${
        mobileOpen ? "translate-x-0" : "-translate-x-full"
      }`}>
        <div className="hidden dark:block h-full">{darkSidebar}</div>
        <div className="dark:hidden h-full">{sidebarContent}</div>
      </aside>

      {/* Desktop */}
      {collapsed ? (
        /* Collapsed rail — icons only */
        <aside className="sidebar-bg flex-col hidden lg:flex relative shrink-0 w-11 border-r border-border">
          <div className="flex flex-col items-center h-full py-2">
            {/* Icon-only nav */}
            <nav className="flex-1 overflow-y-auto w-full flex flex-col items-center gap-0.5 pt-1" aria-label="Main navigation">
              {getFilteredSections().flatMap((section) =>
                section.items.map((item) => {
                  const Icon = item.icon;
                  const active = pathname === item.href;
                  return (
                    <Link
                      key={item.href}
                      to={item.href}
                      title={item.label}
                      aria-label={item.label}
                      aria-current={active ? "page" : undefined}
                      className={`flex items-center justify-center w-8 h-8 rounded-md transition-colors ${
                        active
                          ? "bg-sidebar-accent text-sidebar-primary"
                          : "text-muted-foreground hover:bg-muted/50 hover:text-foreground"
                      }`}
                    >
                      <Icon className="h-4 w-4" />
                    </Link>
                  );
                })
              )}
            </nav>

            {/* Expand button — bottom */}
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
      ) : (
        /* Expanded sidebar */
        <>
          <aside className="sidebar-bg flex-col hidden lg:flex relative shrink-0" style={{ width: sidebarWidth }}>
            <div className="hidden dark:block flex-1 min-h-0">{darkSidebar}</div>
            <div className="dark:hidden flex-1 min-h-0">{sidebarContent}</div>
            {/* Collapse button — bottom */}
            <div className="px-3 py-2 border-t border-border dark:border-white/10 shrink-0">
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
          <div className="hidden lg:block">
            <ResizeHandle width={sidebarWidth} onResize={handleSidebarResize} min={140} max={280} side="right" />
          </div>
        </>
      )}
    </>
  );
}
