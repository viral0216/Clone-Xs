import { useState, useEffect, useRef } from "react";
import {
  Sun, Moon, Sparkles, Sunset, Contrast, Waves, TreePine, Eclipse, Flower2, Mountain,
  Settings2, Wifi, WifiOff, Menu, Search, Palette, Check, LogOut, HelpCircle,
} from "lucide-react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import NotificationPanel from "@/components/NotificationPanel";
import PortalSwitcher from "@/components/PortalSwitcher";
import CloneBuilder from "@/components/CloneBuilder";
import { api } from "@/lib/api-client";
import { useAiStatus } from "@/hooks/useAi";

const ALL_PAGES = [
  // Overview
  { href: "/", label: "Dashboard", keywords: "home overview" },
  { href: "/audit", label: "Audit Trail", keywords: "logs history operations" },
  { href: "/metrics", label: "Metrics", keywords: "performance analytics system insights" },
  // Operations
  { href: "/clone", label: "Clone", keywords: "copy catalog deep shallow multi batch" },
  { href: "/sync", label: "Sync", keywords: "synchronize two-way incremental delta changed" },
  { href: "/rollback", label: "Rollback", keywords: "undo restore revert" },
  { href: "/templates", label: "Templates", keywords: "recipes prebuilt production dev generate terraform pulumi iac" },
  { href: "/create-job", label: "Create Job", keywords: "databricks job persistent scheduled cron" },
  // Discovery
  { href: "/explore", label: "Explorer", keywords: "browse catalog schemas tables columns config diff preview sample" },
  { href: "/diff", label: "Diff & Compare", keywords: "compare catalogs objects" },
  { href: "/lineage", label: "Lineage", keywords: "data flow tracking" },
  { href: "/view-deps", label: "Dependencies", keywords: "views functions dependency graph" },
  { href: "/impact", label: "Impact Analysis", keywords: "downstream blast radius" },
  // Analysis
  { href: "/reports", label: "Reports", keywords: "history cost export" },
  // Management
  { href: "/monitor", label: "Monitor", keywords: "sync status continuous" },
  { href: "/preflight", label: "Preflight", keywords: "validate prerequisites check" },
  { href: "/config", label: "Config", keywords: "configuration settings yaml" },
  { href: "/settings", label: "Settings", keywords: "connection databricks warehouse pat plugins demo data" },
  { href: "/warehouse", label: "Warehouse", keywords: "sql warehouse manage" },
  // Advanced
  { href: "/advanced-tables", label: "Advanced Tables", keywords: "tables advanced" },
  { href: "/lakehouse-monitor", label: "Lakehouse Monitor", keywords: "lakehouse monitor" },
  { href: "/federation", label: "Federation", keywords: "federation cross workspace" },
  { href: "/delta-sharing", label: "Delta Sharing", keywords: "delta sharing" },
  { href: "/pipelines", label: "Pipelines", keywords: "pipeline workflow chain steps clone mask validate notify automation template" },
  { href: "/dlt", label: "Delta Live Tables", keywords: "dlt delta live tables pipeline expectations quality streaming etl medallion" },
  // Governance portal
  { href: "/governance", label: "Governance", keywords: "governance compliance certifications data dictionary" },
  { href: "/governance/rtbf", label: "RTBF / Erasure", keywords: "right to be forgotten gdpr erasure delete subject personal data compliance article 17" },
  { href: "/governance/dsar", label: "DSAR / Access", keywords: "dsar right of access gdpr article 15 export subject data request" },
  { href: "/governance/rbac", label: "RBAC", keywords: "access control roles permissions" },
  // Data Quality portal
  { href: "/data-quality", label: "Data Quality", keywords: "data quality dq rules checks" },
  { href: "/data-quality/observability", label: "Data Observability", keywords: "observability health score freshness volume anomaly sla incidents" },
  { href: "/data-quality/pii", label: "PII Scanner", keywords: "personal data email phone ssn" },
  { href: "/data-quality/schema-drift", label: "Schema Drift", keywords: "column changes detect" },
  // FinOps portal
  { href: "/finops", label: "FinOps", keywords: "finops cost billing compute storage" },
  { href: "/finops/cost-estimator", label: "Cost Estimator", keywords: "storage dbus price estimate" },
  { href: "/finops/storage-metrics", label: "Storage Metrics", keywords: "analyze vacuum optimize active vacuumable" },
  // Security portal
  { href: "/security", label: "Security", keywords: "security data protection" },
  { href: "/security/pii", label: "PII Scanner", keywords: "personal data email phone ssn pii" },
  { href: "/security/compliance", label: "Compliance", keywords: "governance permissions audit compliance" },
  { href: "/security/preflight", label: "Preflight Checks", keywords: "validate prerequisites check preflight" },
  // Automation portal
  { href: "/automation", label: "Automation", keywords: "automation workflows" },
  { href: "/automation/pipelines", label: "Pipelines", keywords: "pipeline workflow chain steps clone mask validate notify template" },
  { href: "/automation/templates", label: "Templates", keywords: "recipes prebuilt production dev generate terraform pulumi iac" },
  { href: "/automation/create-job", label: "Create Job", keywords: "databricks job persistent scheduled cron" },
  // Infrastructure portal
  { href: "/infrastructure", label: "Infrastructure", keywords: "infrastructure compute connectivity" },
  { href: "/infrastructure/warehouse", label: "Warehouse", keywords: "sql warehouse manage start stop" },
  { href: "/infrastructure/federation", label: "Federation", keywords: "federation cross workspace" },
  { href: "/infrastructure/delta-sharing", label: "Delta Sharing", keywords: "delta sharing" },
  { href: "/infrastructure/lakehouse-monitor", label: "Lakehouse Monitor", keywords: "lakehouse monitor quality" },
  // MDM portal
  { href: "/mdm", label: "MDM", keywords: "master data management" },
  { href: "/mdm/golden-records", label: "Golden Records", keywords: "golden records master entity single source truth" },
  { href: "/mdm/match-merge", label: "Match & Merge", keywords: "match merge duplicates dedup matching rules" },
  { href: "/mdm/stewardship", label: "Data Stewardship", keywords: "stewardship review queue approve reject manual" },
  { href: "/mdm/hierarchies", label: "Hierarchies", keywords: "hierarchy parent child tree structure organization" },
  { href: "/mdm/reference-data", label: "Reference Data", keywords: "code lists mapping tables country currency industry standardization" },
  { href: "/mdm/relationship-graph", label: "Relationships", keywords: "entity relationship graph network connections visual" },
  { href: "/mdm/merge-history", label: "Merge History", keywords: "merge split undo audit history decisions" },
  { href: "/mdm/audit-log", label: "MDM Audit Log", keywords: "audit trail log events who what when" },
  { href: "/mdm/templates", label: "Industry Templates", keywords: "healthcare financial retail manufacturing patient KYC customer 360" },
  { href: "/mdm/scorecards", label: "DQ Scorecards", keywords: "data quality scorecard completeness accuracy timeliness" },
  { href: "/mdm/negative-match", label: "Negative Match", keywords: "do not link block never merge negative" },
  { href: "/mdm/settings", label: "MDM Settings", keywords: "mdm settings thresholds sla notifications configuration" },
  { href: "/mdm/consent", label: "Consent Management", keywords: "gdpr consent marketing data processing agreement" },
  { href: "/mdm/cross-domain", label: "Cross-Domain", keywords: "cross domain matching customer supplier entity types" },
  { href: "/mdm/reports", label: "MDM Reports", keywords: "compliance report export entity merge stewardship" },
  { href: "/mdm/profiling", label: "MDM Profiling", keywords: "profile attributes completeness distinct values patterns" },
  // Other
  { href: "/help", label: "Help & Guides", keywords: "help documentation guide tutorial how to keyboard shortcuts" },
];

interface HeaderBarProps {
  onMenuToggle?: () => void;
}

type Theme = "light" | "dark" | "midnight" | "sunset" | "high-contrast" | "ocean" | "forest" | "solarized" | "rose" | "slate";

const THEMES: { id: Theme; label: string; icon: React.ComponentType<{ className?: string }>; classes: string[] }[] = [
  { id: "light", label: "Light", icon: Sun, classes: [] },
  { id: "dark", label: "Dark", icon: Moon, classes: ["dark"] },
  { id: "midnight", label: "Midnight", icon: Sparkles, classes: ["dark", "midnight"] },
  { id: "sunset", label: "Sunset", icon: Sunset, classes: ["dark", "sunset"] },
  { id: "high-contrast", label: "High Contrast", icon: Contrast, classes: ["dark", "high-contrast"] },
  { id: "ocean", label: "Ocean", icon: Waves, classes: ["dark", "ocean"] },
  { id: "forest", label: "Forest", icon: TreePine, classes: ["dark", "forest"] },
  { id: "solarized", label: "Solarized", icon: Eclipse, classes: ["dark", "solarized"] },
  { id: "rose", label: "Rose", icon: Flower2, classes: ["dark", "rose"] },
  { id: "slate", label: "Slate", icon: Mountain, classes: ["dark", "slate"] },
];

const ALL_THEME_CLASSES = ["dark", "midnight", "sunset", "high-contrast", "ocean", "forest", "solarized", "rose", "slate"];

export default function HeaderBar({ onMenuToggle }: HeaderBarProps) {
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem("theme") as Theme | null;
    if (saved && THEMES.some(t => t.id === saved)) return saved;
    return "light";
  });
  const [connected, setConnected] = useState(false);
  const [connInfo, setConnInfo] = useState<{user?: string; host?: string; auth_method?: string}>({});
  const [query, setQuery] = useState("");
  const [showResults, setShowResults] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

  const [themePickerOpen, setThemePickerOpen] = useState(false);
  const themePickerRef = useRef<HTMLDivElement>(null);
  const [showCloneBuilder, setShowCloneBuilder] = useState(false);
  const aiStatus = useAiStatus();

  useEffect(() => {
    const root = document.documentElement.classList;
    root.remove(...ALL_THEME_CLASSES);
    const cfg = THEMES.find(t => t.id === theme);
    if (cfg) cfg.classes.forEach(c => root.add(c));
    localStorage.setItem("theme", theme);
    window.dispatchEvent(new Event("clxs-theme-changed"));
  }, [theme]);

  // Sync theme when changed from Settings page
  useEffect(() => {
    const handler = () => {
      const saved = localStorage.getItem("theme") as Theme | null;
      if (saved && THEMES.some(t => t.id === saved) && saved !== theme) {
        setTheme(saved);
      }
    };
    window.addEventListener("clxs-theme-changed", handler);
    return () => window.removeEventListener("clxs-theme-changed", handler);
  }, [theme]);

  // Close theme picker on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (themePickerRef.current && !themePickerRef.current.contains(e.target as Node)) setThemePickerOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  useEffect(() => {
    const check = () => {
      api.get<{ status?: string }>("/health")
        .then(() => setConnected(true))
        .catch(() => setConnected(false));
      api.get<{ authenticated?: boolean; user?: string; host?: string; auth_method?: string }>("/auth/status")
        .then((d) => { if (d.authenticated) setConnInfo({ user: d.user, host: d.host, auth_method: d.auth_method }); })
        .catch(() => {});
    };
    check();
    const id = setInterval(check, 15000);
    return () => clearInterval(id);
  }, []);

  // Close results when navigating
  useEffect(() => { setShowResults(false); setQuery(""); }, [pathname]);

  // Close on click outside
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (resultsRef.current && !resultsRef.current.contains(e.target as Node) &&
          inputRef.current && !inputRef.current.contains(e.target as Node)) {
        setShowResults(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Filter pages
  const results = query.trim()
    ? ALL_PAGES.filter(p => {
        const q = query.toLowerCase();
        return p.label.toLowerCase().includes(q) || p.keywords.includes(q) || p.href.includes(q);
      })
    : [];

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIdx(i => Math.min(i + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIdx(i => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && results[selectedIdx]) {
      navigate(results[selectedIdx].href);
      setShowResults(false);
      setQuery("");
      inputRef.current?.blur();
    } else if (e.key === "Escape") {
      setShowResults(false);
      inputRef.current?.blur();
    }
  }

  const pageName = pathname === "/"
    ? "Dashboard"
    : pathname.slice(1).split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");

  const iconBtn = "p-1.5 min-h-[32px] min-w-[32px] flex items-center justify-center rounded-md text-gray-400 hover:text-gray-600 hover:bg-gray-100 dark:text-gray-500 dark:hover:text-gray-300 dark:hover:bg-white/10 transition-all";

  return (
    <header className="h-12 header-bg flex items-center justify-between px-4 shrink-0">
      {/* Left */}
      <div className="flex items-center gap-2">
        {onMenuToggle && (
          <button onClick={onMenuToggle} className={`lg:hidden ${iconBtn}`} aria-label="Toggle navigation menu">
            <Menu className="h-4 w-4" />
          </button>
        )}
        <Link to="/">
          <img src="/logo.svg" alt="Clone→Xs" className="h-9 dark:hidden" />
          <img src="/logo-dark.svg" alt="Clone→Xs" className="h-9 hidden dark:block" />
        </Link>
        <span className="text-gray-300 dark:text-gray-600">/</span>
        <span className="text-sm font-medium text-gray-600 dark:text-gray-300">{pageName}</span>
      </div>

      {/* Center — Search with results dropdown */}
      <div className="hidden md:flex items-center flex-1 max-w-md mx-8 relative">
        <div className="relative w-full">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-gray-400" aria-hidden="true" />
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => { setQuery(e.target.value); setShowResults(true); setSelectedIdx(0); }}
            onFocus={() => query && setShowResults(true)}
            onKeyDown={handleKeyDown}
            placeholder="Search pages, commands..."
            role="combobox"
            aria-expanded={showResults && results.length > 0}
            aria-autocomplete="list"
            aria-controls="header-search-results"
            aria-activedescendant={showResults && results[selectedIdx] ? `search-result-${selectedIdx}` : undefined}
            aria-label="Search pages"
            className="w-full pl-9 pr-3 py-1.5 text-sm text-center bg-gray-50 dark:bg-white/10 border border-gray-200 dark:border-white/10 rounded-lg text-gray-900 dark:text-white placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-[#E8453C]/20 focus:border-[#E8453C]"
          />
        </div>

        {/* Search Results Dropdown */}
        {showResults && results.length > 0 && (
          <div
            ref={resultsRef}
            id="header-search-results"
            role="listbox"
            aria-label="Search results"
            className="absolute top-full left-0 right-0 mt-1 bg-white dark:bg-[#1E1E1E] border border-gray-200 dark:border-white/10 rounded-lg shadow-lg overflow-hidden z-50"
          >
            {results.map((page, i) => (
              <Link
                key={page.href}
                to={page.href}
                id={`search-result-${i}`}
                role="option"
                aria-selected={i === selectedIdx}
                onClick={() => { setShowResults(false); setQuery(""); }}
                className={`flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
                  i === selectedIdx
                    ? "bg-[#E8453C]/5 text-[#E8453C] dark:bg-[#E8453C]/10 dark:text-[#E8453C]"
                    : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-white/5"
                }`}
              >
                <span className="font-medium">{page.label}</span>
                <span className="text-xs text-gray-400 ml-auto">{page.href}</span>
              </Link>
            ))}
          </div>
        )}

        {showResults && query && results.length === 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 bg-white dark:bg-[#1E1E1E] border border-gray-200 dark:border-white/10 rounded-lg shadow-lg p-4 text-sm text-gray-400 text-center z-50">
            No pages found for "{query}"
          </div>
        )}
      </div>

      {/* Right */}
      <div className="flex items-center gap-0.5">
        {aiStatus.data?.available && (
          <button
            onClick={() => setShowCloneBuilder(true)}
            className={iconBtn}
            title="AI Clone Builder"
            aria-label="Open AI Clone Builder"
          >
            <Sparkles className="h-4 w-4" />
          </button>
        )}
        {showCloneBuilder && <CloneBuilder onClose={() => setShowCloneBuilder(false)} />}
        <PortalSwitcher />
        <div className="w-px h-5 bg-gray-200 dark:bg-white/10 mx-1.5 hidden sm:block" />
        <div className="relative group">
          <div
            role="status"
            aria-label={connected ? "Connected to Databricks" : "Disconnected from Databricks"}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium cursor-default ${
              connected
                ? "bg-muted/30 text-foreground dark:bg-white/5 dark:text-gray-300"
                : "bg-red-50 text-red-600 dark:bg-red-500/10 dark:text-red-400"
            }`}
          >
            {connected ? <Wifi className="h-3 w-3" aria-hidden="true" /> : <WifiOff className="h-3 w-3" aria-hidden="true" />}
            <span className="hidden sm:inline">{connected ? "Connected" : "Offline"}</span>
          </div>
          {connected && connInfo.user && (
            <div className="absolute right-0 top-full mt-2 w-72 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg p-3 opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 z-50">
              <div className="text-xs space-y-1.5">
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">User</span>
                  <span className="font-medium text-gray-900 dark:text-gray-100">{connInfo.user}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Host</span>
                  <span className="font-medium text-gray-900 dark:text-gray-100 truncate ml-2 max-w-[180px]">{connInfo.host}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Auth</span>
                  <span className="font-medium text-gray-900 dark:text-gray-100">{connInfo.auth_method}</span>
                </div>
              </div>
            </div>
          )}
        </div>
        <div className="w-px h-5 bg-gray-200 dark:bg-white/10 mx-1.5 hidden sm:block" />
        <NotificationPanel />
        <div className="relative" ref={themePickerRef}>
          <button
            onClick={() => setThemePickerOpen(p => !p)}
            className={iconBtn}
            title="Change theme"
            aria-label="Change theme"
            aria-expanded={themePickerOpen}
            aria-haspopup="listbox"
          >
            <Palette className="h-4 w-4" />
          </button>
          {themePickerOpen && (
            <div className="absolute right-0 top-full mt-2 w-48 bg-card border border-border rounded-lg shadow-lg z-50 py-1 overflow-hidden" role="listbox" aria-label="Select theme">
              {THEMES.map(t => {
                const Icon = t.icon;
                const active = theme === t.id;
                return (
                  <button
                    key={t.id}
                    role="option"
                    aria-selected={active}
                    onClick={() => { setTheme(t.id); setThemePickerOpen(false); }}
                    className={`w-full flex items-center gap-3 px-3 py-2 text-sm transition-colors text-left ${active ? "bg-accent text-accent-foreground font-medium" : "text-muted-foreground hover:bg-accent/50 hover:text-foreground"}`}
                  >
                    <Icon className="h-4 w-4 shrink-0" />
                    <span className="flex-1">{t.label}</span>
                    {active && <Check className="h-3.5 w-3.5 shrink-0" />}
                  </button>
                );
              })}
            </div>
          )}
        </div>
        <Link to="/help" className={iconBtn} title="Help & Guides" aria-label="Help & Guides"><HelpCircle className="h-4 w-4" /></Link>
        <Link to="/settings" className={iconBtn} title="Settings" aria-label="Settings"><Settings2 className="h-4 w-4" /></Link>
        <button
          onClick={() => {
            api.post("/auth/logout").catch(() => {});
            localStorage.removeItem("dbx_host");
            localStorage.removeItem("dbx_token");
            localStorage.removeItem("dbx_warehouse_id");
            localStorage.removeItem("clxs_session_id");
            window.dispatchEvent(new Event("clxs-logout"));
          }}
          className={iconBtn}
          title="Logout"
          aria-label="Logout"
        >
          <LogOut className="h-4 w-4" />
        </button>
      </div>
    </header>
  );
}
