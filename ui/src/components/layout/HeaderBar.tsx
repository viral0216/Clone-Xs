import { useState, useEffect, useRef } from "react";
import {
  Sun, Moon, Settings2, Wifi, WifiOff, Menu, Search,
} from "lucide-react";
import { Link, useLocation, useNavigate } from "react-router-dom";

const ALL_PAGES = [
  { href: "/", label: "Dashboard", keywords: "home overview" },
  { href: "/audit", label: "Audit Trail", keywords: "logs history operations" },
  { href: "/metrics", label: "Metrics", keywords: "performance analytics" },
  { href: "/clone", label: "Clone", keywords: "copy catalog deep shallow" },
  { href: "/sync", label: "Sync", keywords: "synchronize two-way" },
  { href: "/incremental-sync", label: "Incremental Sync", keywords: "delta changed tables" },
  { href: "/generate", label: "Generate", keywords: "terraform workflow pulumi iac" },
  { href: "/rollback", label: "Rollback", keywords: "undo restore revert" },
  { href: "/templates", label: "Templates", keywords: "recipes prebuilt production dev" },
  { href: "/schedule", label: "Schedule", keywords: "cron recurring automated" },
  { href: "/create-job", label: "Create Job", keywords: "databricks job persistent scheduled" },
  { href: "/multi-clone", label: "Multi-Clone", keywords: "multiple workspaces parallel" },
  { href: "/explore", label: "Explorer", keywords: "browse catalog schemas tables columns" },
  { href: "/diff", label: "Diff & Compare", keywords: "compare catalogs objects" },
  { href: "/config-diff", label: "Config Diff", keywords: "compare configuration" },
  { href: "/lineage", label: "Lineage", keywords: "data flow tracking" },
  { href: "/view-deps", label: "Dependencies", keywords: "views functions dependency graph" },
  { href: "/impact", label: "Impact Analysis", keywords: "downstream blast radius" },
  { href: "/preview", label: "Data Preview", keywords: "sample compare rows" },
  { href: "/reports", label: "Reports", keywords: "history cost export" },
  { href: "/pii", label: "PII Scanner", keywords: "personal data email phone ssn" },
  { href: "/schema-drift", label: "Schema Drift", keywords: "column changes detect" },
  { href: "/profiling", label: "Profiling", keywords: "data quality nulls distinct" },
  { href: "/cost", label: "Cost Estimator", keywords: "storage dbus price" },
  { href: "/storage-metrics", label: "Storage Metrics", keywords: "analyze vacuum optimize active vacuumable" },
  { href: "/compliance", label: "Compliance", keywords: "governance permissions audit" },
  { href: "/monitor", label: "Monitor", keywords: "sync status continuous" },
  { href: "/preflight", label: "Preflight", keywords: "validate prerequisites check" },
  { href: "/config", label: "Config", keywords: "configuration settings yaml" },
  { href: "/settings", label: "Settings", keywords: "connection databricks warehouse pat" },
  { href: "/warehouse", label: "Warehouse", keywords: "sql warehouse manage" },
  { href: "/rbac", label: "RBAC", keywords: "access control roles permissions" },
  { href: "/plugins", label: "Plugins", keywords: "extend hooks" },
];

interface HeaderBarProps {
  onMenuToggle?: () => void;
}

export default function HeaderBar({ onMenuToggle }: HeaderBarProps) {
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    if (saved) return saved === "dark";
    return false;
  });
  const [connected, setConnected] = useState(false);
  const [query, setQuery] = useState("");
  const [showResults, setShowResults] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (dark) {
      document.documentElement.classList.add("dark");
      localStorage.setItem("theme", "dark");
    } else {
      document.documentElement.classList.remove("dark");
      localStorage.setItem("theme", "light");
    }
  }, [dark]);

  useEffect(() => {
    const check = () => {
      fetch("/api/health")
        .then((r) => setConnected(r.ok))
        .catch(() => setConnected(false));
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

  const iconBtn = "p-2 rounded-lg text-gray-400 hover:text-gray-600 hover:bg-gray-100 dark:text-gray-500 dark:hover:text-gray-300 dark:hover:bg-white/10 transition-all";

  return (
    <header className="h-20 header-bg flex items-center justify-between px-4 shrink-0">
      {/* Left */}
      <div className="flex items-center gap-3">
        {onMenuToggle && (
          <button onClick={onMenuToggle} className={`lg:hidden ${iconBtn}`}>
            <Menu className="h-5 w-5" />
          </button>
        )}
        <Link to="/">
          <span className="flex flex-col items-center" style={{lineHeight:1.1}}>
            <span className="text-[18px] font-bold tracking-tight"><span className="text-gray-900 dark:text-white">Clone</span><span className="text-[#E8453C] mx-0.5">→</span><span className="text-gray-900 dark:text-white">Xs</span></span>
            <span className="text-[8px] text-gray-400 dark:text-gray-500 tracking-[0.15em] uppercase">Unity Catalog Toolkit</span>
          </span>
        </Link>
        <span className="text-gray-300 dark:text-gray-600">/</span>
        <span className="text-sm font-medium text-gray-600 dark:text-gray-300">{pageName}</span>
      </div>

      {/* Center — Search with results dropdown */}
      <div className="hidden md:flex items-center flex-1 max-w-md mx-8 relative">
        <div className="relative w-full">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-gray-400" />
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => { setQuery(e.target.value); setShowResults(true); setSelectedIdx(0); }}
            onFocus={() => query && setShowResults(true)}
            onKeyDown={handleKeyDown}
            placeholder="Search pages, commands..."
            className="w-full pl-9 pr-3 py-1.5 text-sm bg-gray-50 dark:bg-white/10 border border-gray-200 dark:border-white/10 rounded-lg text-gray-900 dark:text-white placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
          />
        </div>

        {/* Search Results Dropdown */}
        {showResults && results.length > 0 && (
          <div
            ref={resultsRef}
            className="absolute top-full left-0 right-0 mt-1 bg-white dark:bg-[#1E1E1E] border border-gray-200 dark:border-white/10 rounded-lg shadow-lg overflow-hidden z-50"
          >
            {results.map((page, i) => (
              <Link
                key={page.href}
                to={page.href}
                onClick={() => { setShowResults(false); setQuery(""); }}
                className={`flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
                  i === selectedIdx
                    ? "bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-400"
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
        <div className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
          connected
            ? "bg-green-50 text-green-700 dark:bg-green-500/10 dark:text-green-400"
            : "bg-red-50 text-red-600 dark:bg-red-500/10 dark:text-red-400"
        }`}>
          {connected ? <Wifi className="h-3 w-3" /> : <WifiOff className="h-3 w-3" />}
          <span className="hidden sm:inline">{connected ? "Connected" : "Offline"}</span>
        </div>
        <div className="w-px h-5 bg-gray-200 dark:bg-white/10 mx-1.5 hidden sm:block" />
        <button onClick={() => setDark(p => !p)} className={iconBtn} title={dark ? "Light mode" : "Dark mode"}>
          {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
        <Link to="/settings" className={iconBtn} title="Settings"><Settings2 className="h-4 w-4" /></Link>
      </div>
    </header>
  );
}
