import { Link, useLocation } from "react-router-dom";
import {
  LayoutDashboard, BookOpen, Search, ShieldCheck, BarChart3, ClipboardCheck,
  Award, CheckSquare, Clock, FileText, History, FileCode, Zap,
} from "lucide-react";

const NAV_SECTIONS = [
  {
    title: "Governance",
    items: [
      { href: "/governance", label: "Overview", icon: LayoutDashboard },
    ],
  },
  {
    title: "Data Dictionary",
    items: [
      { href: "/governance/dictionary", label: "Business Glossary", icon: BookOpen },
      { href: "/governance/search", label: "Global Search", icon: Search },
    ],
  },
  {
    title: "Data Quality",
    items: [
      { href: "/governance/dqx", label: "DQX Engine", icon: Zap },
      { href: "/governance/dq-rules", label: "Rules Engine", icon: ShieldCheck },
      { href: "/governance/dq-dashboard", label: "DQ Dashboard", icon: BarChart3 },
      { href: "/governance/dq-results", label: "Results", icon: ClipboardCheck },
    ],
  },
  {
    title: "Certifications",
    items: [
      { href: "/governance/certifications", label: "Board", icon: Award },
      { href: "/governance/approvals", label: "Approvals", icon: CheckSquare },
    ],
  },
  {
    title: "Data Contracts",
    items: [
      { href: "/governance/odcs", label: "ODCS Contracts", icon: FileCode },
      { href: "/governance/contracts", label: "Legacy Contracts", icon: FileText },
    ],
  },
  {
    title: "SLA & Freshness",
    items: [
      { href: "/governance/sla", label: "SLA Dashboard", icon: Clock },
    ],
  },
  {
    title: "Audit",
    items: [
      { href: "/governance/changes", label: "Change History", icon: History },
    ],
  },
];

export default function GovernanceSidebar() {
  const location = useLocation();

  return (
    <aside className="w-56 shrink-0 border-r border-border bg-background overflow-y-auto h-full">
      <div className="px-4 py-3 border-b border-border">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Governance Portal</p>
      </div>
      <nav className="py-2" aria-label="Governance navigation">
        {NAV_SECTIONS.map((section) => (
          <div key={section.title} className="mb-1">
            <p className="px-4 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">{section.title}</p>
            {section.items.map((item) => {
              const Icon = item.icon;
              const active = location.pathname === item.href ||
                (item.href !== "/governance" && location.pathname.startsWith(item.href));
              return (
                <Link
                  key={item.href}
                  to={item.href}
                  aria-current={active ? "page" : undefined}
                  className={`flex items-center gap-2.5 px-4 py-2 text-sm transition-colors ${
                    active
                      ? "bg-[#E8453C]/5 dark:bg-[#E8453C]/10 text-[#E8453C] dark:text-[#E8453C] font-medium border-r-2 border-[#E8453C]"
                      : "text-muted-foreground hover:text-foreground hover:bg-accent/50"
                  }`}
                >
                  <Icon className={`h-4 w-4 shrink-0 ${active ? "text-[#E8453C]" : ""}`} />
                  <span className="truncate">{item.label}</span>
                </Link>
              );
            })}
          </div>
        ))}
      </nav>
    </aside>
  );
}
