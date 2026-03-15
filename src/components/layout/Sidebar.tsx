import { useState, useEffect } from "react";
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
  Sun,
  Moon,
  Shield,
  GitBranch,
  ClipboardCheck,
  RefreshCw,
} from "lucide-react";

const navItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/clone", label: "Clone", icon: Copy },
  { href: "/explore", label: "Explorer", icon: FolderTree },
  { href: "/diff", label: "Diff & Compare", icon: GitCompare },
  { href: "/monitor", label: "Monitor", icon: Activity },
  { href: "/config", label: "Config", icon: Wrench },
  { href: "/reports", label: "Reports", icon: FileText },
  { href: "/pii", label: "PII Scanner", icon: Shield },
  { href: "/schema-drift", label: "Schema Drift", icon: GitBranch },
  { href: "/preflight", label: "Preflight", icon: ClipboardCheck },
  { href: "/sync", label: "Sync", icon: RefreshCw },
  { href: "/settings", label: "Settings", icon: Settings2 },
];

export default function Sidebar() {
  const { pathname } = useLocation();
  const [dark, setDark] = useState(() => {
    return localStorage.getItem("theme") === "dark";
  });

  useEffect(() => {
    if (dark) {
      document.documentElement.classList.add("dark");
      localStorage.setItem("theme", "dark");
    } else {
      document.documentElement.classList.remove("dark");
      localStorage.setItem("theme", "light");
    }
  }, [dark]);

  // Apply theme on mount
  useEffect(() => {
    const saved = localStorage.getItem("theme");
    if (saved === "dark") {
      document.documentElement.classList.add("dark");
      setDark(true);
    }
  }, []);

  return (
    <aside className="w-64 bg-gray-900 text-white flex flex-col min-h-screen">
      <div className="p-6 border-b border-gray-700">
        <h1 className="text-xl font-bold">Clone-X</h1>
        <p className="text-xs text-gray-400 mt-1">Unity Catalog Clone Utility</p>
      </div>
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => {
          const Icon = item.icon;
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              to={item.href}
              className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                active
                  ? "bg-blue-600 text-white"
                  : "text-gray-300 hover:bg-gray-800 hover:text-white"
              }`}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="p-4 border-t border-gray-700 flex items-center justify-between">
        <span className="text-xs text-gray-500">v0.4.0</span>
        <button
          onClick={() => setDark((prev) => !prev)}
          className="p-2 rounded-lg text-gray-400 hover:text-white hover:bg-gray-800 transition-colors"
          title={dark ? "Switch to light mode" : "Switch to dark mode"}
        >
          {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
      </div>
    </aside>
  );
}
