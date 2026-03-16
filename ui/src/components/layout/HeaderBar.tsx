import { useState, useEffect } from "react";
import {
  Sun, Moon, Settings2, Wifi, WifiOff, Menu,
  Github, BookOpen, Terminal,
} from "lucide-react";
import { Link, useLocation } from "react-router-dom";

interface HeaderBarProps {
  onMenuToggle?: () => void;
}

export default function HeaderBar({ onMenuToggle }: HeaderBarProps) {
  const { pathname } = useLocation();
  const [dark, setDark] = useState(() => {
    return localStorage.getItem("theme") === "dark";
  });
  const [connected, setConnected] = useState(false);
  const [runtime, setRuntime] = useState("");

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
    const saved = localStorage.getItem("theme");
    if (saved === "dark") {
      document.documentElement.classList.add("dark");
      setDark(true);
    }
  }, []);

  // Health check
  useEffect(() => {
    const check = () => {
      fetch("/api/health")
        .then((r) => {
          setConnected(r.ok);
          if (r.ok) return r.json();
        })
        .then((data) => {
          if (data?.runtime) setRuntime(data.runtime);
        })
        .catch(() => setConnected(false));
    };
    check();
    const id = setInterval(check, 15000);
    return () => clearInterval(id);
  }, []);

  // Derive current page name from pathname
  const pageName = pathname === "/"
    ? "Dashboard"
    : pathname
        .slice(1)
        .split("-")
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(" ");

  return (
    <header className="h-12 sm:h-14 bg-gradient-to-r from-[#1B3139] to-[#0F1419] text-white flex items-center justify-between px-2 sm:px-4 border-b border-white/10 shrink-0">
      {/* Left: Menu + Branding + Breadcrumb */}
      <div className="flex items-center gap-2 sm:gap-3 min-w-0">
        {/* Mobile menu button */}
        {onMenuToggle && (
          <button
            onClick={onMenuToggle}
            className="p-1.5 sm:p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all lg:hidden"
            title="Toggle menu"
          >
            <Menu className="h-5 w-5" />
          </button>
        )}

        {/* Logo */}
        <Link to="/" className="flex items-center gap-2 sm:gap-2.5 shrink-0">
          <div className="w-7 h-7 sm:w-8 sm:h-8 rounded-lg bg-gradient-to-br from-[#FF3621] to-[#E02F1B] flex items-center justify-center text-xs sm:text-sm font-bold shadow-lg shadow-red-900/20">
            CX
          </div>
          <div className="hidden sm:block">
            <h1 className="text-sm sm:text-base font-semibold leading-tight tracking-tight">
              Clone-Xs
            </h1>
          </div>
        </Link>

        {/* Breadcrumb / Current Page */}
        <div className="hidden md:flex items-center gap-1.5 text-xs text-gray-500 ml-1">
          <span>/</span>
          <span className="text-gray-300 font-medium truncate max-w-[200px]">
            {pageName}
          </span>
        </div>

        {/* Runtime badge */}
        {runtime === "databricks-app" && (
          <span className="hidden lg:inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium bg-blue-500/15 text-blue-400 border border-blue-500/20">
            <Terminal className="h-2.5 w-2.5" />
            Databricks App
          </span>
        )}
      </div>

      {/* Right: Status + Actions */}
      <div className="flex items-center gap-0.5 sm:gap-1.5">
        {/* Connection Status */}
        <div
          className={`flex items-center gap-1 sm:gap-1.5 px-2 sm:px-2.5 py-1 rounded-full text-[11px] sm:text-xs font-medium transition-colors ${
            connected
              ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/20"
              : "bg-red-500/15 text-red-400 border border-red-500/20"
          }`}
        >
          {connected ? (
            <Wifi className="h-3 w-3" />
          ) : (
            <WifiOff className="h-3 w-3" />
          )}
          <span className="hidden sm:inline">
            {connected ? "Connected" : "Disconnected"}
          </span>
        </div>

        {/* Divider */}
        <div className="hidden sm:block w-px h-5 bg-white/10 mx-1" />

        {/* Theme Toggle */}
        <button
          onClick={() => setDark((prev) => !prev)}
          className="p-1.5 sm:p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title={dark ? "Switch to light mode" : "Switch to dark mode"}
        >
          {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>

        {/* Docs link */}
        <a
          href="https://github.com/viral0216/clone-xs#readme"
          target="_blank"
          rel="noopener noreferrer"
          className="hidden sm:flex p-1.5 sm:p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title="Documentation"
        >
          <BookOpen className="h-4 w-4" />
        </a>

        {/* GitHub link */}
        <a
          href="https://github.com/viral0216/clone-xs"
          target="_blank"
          rel="noopener noreferrer"
          className="hidden sm:flex p-1.5 sm:p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title="GitHub"
        >
          <Github className="h-4 w-4" />
        </a>

        {/* Settings */}
        <Link
          to="/settings"
          className="p-1.5 sm:p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title="Settings"
        >
          <Settings2 className="h-4 w-4" />
        </Link>
      </div>
    </header>
  );
}
