import { useState, useEffect } from "react";
import { Sun, Moon, Settings2, Wifi, WifiOff, Menu } from "lucide-react";
import { Link } from "react-router-dom";

interface HeaderBarProps {
  onMenuToggle?: () => void;
}

export default function HeaderBar({ onMenuToggle }: HeaderBarProps) {
  const [dark, setDark] = useState(() => {
    return localStorage.getItem("theme") === "dark";
  });
  const [connected, setConnected] = useState(false);

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
        .then((r) => setConnected(r.ok))
        .catch(() => setConnected(false));
    };
    check();
    const id = setInterval(check, 15000);
    return () => clearInterval(id);
  }, []);

  return (
    <header className="h-14 bg-gradient-to-r from-[#1B3139] to-[#0F1419] text-white flex items-center justify-between px-3 sm:px-5 border-b border-white/10 shrink-0">
      {/* Left: Menu + Branding */}
      <div className="flex items-center gap-2 sm:gap-3">
        {/* Mobile menu button */}
        {onMenuToggle && (
          <button
            onClick={onMenuToggle}
            className="p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all lg:hidden"
            title="Toggle menu"
          >
            <Menu className="h-5 w-5" />
          </button>
        )}
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#FF3621] to-[#E02F1B] flex items-center justify-center text-sm font-bold shrink-0">
          CX
        </div>
        <div className="hidden xs:block">
          <h1 className="text-base font-semibold leading-tight">Clone-Xs</h1>
          <p className="text-[10px] text-gray-400 leading-tight hidden sm:block">
            Unity Catalog Clone Utility
          </p>
        </div>
      </div>

      {/* Right: Status + Actions */}
      <div className="flex items-center gap-1 sm:gap-2">
        {/* Connection Status */}
        <div
          className={`flex items-center gap-1.5 px-2 sm:px-3 py-1 rounded-full text-xs font-medium ${
            connected
              ? "bg-emerald-500/15 text-emerald-400"
              : "bg-red-500/15 text-red-400"
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

        {/* Theme Toggle */}
        <button
          onClick={() => setDark((prev) => !prev)}
          className="p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title={dark ? "Switch to light mode" : "Switch to dark mode"}
        >
          {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>

        {/* Settings */}
        <Link
          to="/settings"
          className="p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-all"
          title="Settings"
        >
          <Settings2 className="h-4 w-4" />
        </Link>
      </div>
    </header>
  );
}
