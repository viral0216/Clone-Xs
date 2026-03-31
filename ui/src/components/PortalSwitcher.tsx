import { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Copy, Shield, ChevronDown, BarChart3, DollarSign, Lock, Zap, Server, Database } from "lucide-react";

const ALL_PORTALS = [
  { id: "clone-xs", label: "Clone \u2192 Xs", description: "Catalog cloning & management", icon: Copy, path: "/" },
  { id: "governance", label: "Governance", description: "Metadata management & contracts", icon: Shield, path: "/governance" },
  { id: "data-quality", label: "Data Quality", description: "Quality rules, profiling & reconciliation", icon: BarChart3, path: "/data-quality" },
  { id: "finops", label: "FinOps", description: "Cost management, billing & optimization", icon: DollarSign, path: "/finops" },
  { id: "security", label: "Security", description: "PII detection, compliance & validation", icon: Lock, path: "/security" },
  { id: "automation", label: "Automation", description: "Pipelines, jobs & templates", icon: Zap, path: "/automation" },
  { id: "infrastructure", label: "Infrastructure", description: "Warehouses, federation & sharing", icon: Server, path: "/infrastructure" },
  { id: "mdm", label: "MDM", description: "Master data management", icon: Database, path: "/mdm" },
];

function getDisabledPortals(): Set<string> {
  try {
    const saved = localStorage.getItem("clxs-disabled-portals");
    return saved ? new Set(JSON.parse(saved)) : new Set(["finops", "infrastructure", "mdm"]);
  } catch { return new Set(); }
}

function detectPortal(pathname: string) {
  const prefixes = ["/governance", "/data-quality", "/finops", "/security", "/automation", "/infrastructure", "/mdm"];
  for (const prefix of prefixes) {
    if (pathname.startsWith(prefix)) {
      return ALL_PORTALS.find(p => p.path === prefix)!;
    }
  }
  return ALL_PORTALS[0];
}

export default function PortalSwitcher() {
  const [open, setOpen] = useState(false);
  const [focusIdx, setFocusIdx] = useState(0);
  const [disabledPortals, setDisabledPortals] = useState(getDisabledPortals);
  const ref = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const navigate = useNavigate();
  const location = useLocation();

  // Re-read disabled portals when features change
  useEffect(() => {
    const handler = () => setDisabledPortals(getDisabledPortals());
    window.addEventListener("clxs-features-changed", handler);
    window.addEventListener("storage", handler);
    return () => {
      window.removeEventListener("clxs-features-changed", handler);
      window.removeEventListener("storage", handler);
    };
  }, []);

  const portals = ALL_PORTALS.filter(p => p.id === "clone-xs" || !disabledPortals.has(p.id));
  const current = detectPortal(location.pathname);

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectPortal = useCallback((idx: number) => {
    navigate(portals[idx].path);
    setOpen(false);
    btnRef.current?.focus();
  }, [navigate, portals]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (!open) {
      if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        setOpen(true);
        setFocusIdx(portals.findIndex(p => p.id === current.id));
      }
      return;
    }

    switch (e.key) {
      case "Escape":
        e.preventDefault();
        setOpen(false);
        btnRef.current?.focus();
        break;
      case "ArrowDown":
        e.preventDefault();
        setFocusIdx(i => (i + 1) % portals.length);
        break;
      case "ArrowUp":
        e.preventDefault();
        setFocusIdx(i => (i - 1 + portals.length) % portals.length);
        break;
      case "Enter":
      case " ":
        e.preventDefault();
        selectPortal(focusIdx);
        break;
    }
  }, [open, focusIdx, current.id, selectPortal, portals]);

  return (
    <div ref={ref} className="relative" onKeyDown={handleKeyDown}>
      <button
        ref={btnRef}
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-label={`Portal: ${current.label}`}
        className="flex items-center gap-2 px-3 py-1.5 min-h-[44px] rounded-lg hover:bg-accent/50 transition-colors"
      >
        <current.icon className="h-4 w-4 text-red-500" aria-hidden="true" />
        <span className="text-sm font-semibold text-foreground">{current.label}</span>
        <ChevronDown className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${open ? "rotate-180" : ""}`} aria-hidden="true" />
      </button>

      {open && (
        <div className="absolute top-full right-0 mt-1 w-72 bg-popover border border-border rounded-lg shadow-lg z-50 py-1 max-h-[70vh] overflow-y-auto" role="listbox" aria-label="Select portal">
          {portals.map((portal, idx) => {
            const Icon = portal.icon;
            const isActive = portal.id === current.id;
            const isFocused = idx === focusIdx;
            return (
              <button
                key={portal.id}
                role="option"
                aria-selected={isActive}
                onClick={() => selectPortal(idx)}
                className={`w-full flex items-start gap-3 px-4 py-3 text-left hover:bg-accent/50 transition-colors ${isActive ? "bg-accent/30" : ""} ${isFocused ? "bg-accent/50" : ""}`}
              >
                <Icon className={`h-5 w-5 mt-0.5 shrink-0 ${isActive ? "text-red-500" : "text-muted-foreground"}`} aria-hidden="true" />
                <div>
                  <p className={`text-sm font-medium ${isActive ? "text-foreground" : "text-muted-foreground"}`}>{portal.label}</p>
                  <p className="text-xs text-muted-foreground">{portal.description}</p>
                </div>
                {isActive && <div className="ml-auto mt-1 h-2 w-2 rounded-full bg-green-500 shrink-0" aria-label="Active" />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
