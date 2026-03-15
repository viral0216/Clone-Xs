"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Copy,
  FolderTree,
  GitCompare,
  Activity,
  Settings2,
  FileText,
  Wrench,
} from "lucide-react";

const navItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/clone", label: "Clone", icon: Copy },
  { href: "/explore", label: "Explorer", icon: FolderTree },
  { href: "/diff", label: "Diff & Compare", icon: GitCompare },
  { href: "/monitor", label: "Monitor", icon: Activity },
  { href: "/config", label: "Config", icon: Wrench },
  { href: "/reports", label: "Reports", icon: FileText },
  { href: "/settings", label: "Settings", icon: Settings2 },
];

export default function Sidebar() {
  const pathname = usePathname();

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
              href={item.href}
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
      <div className="p-4 border-t border-gray-700 text-xs text-gray-500">
        v0.4.0
      </div>
    </aside>
  );
}
