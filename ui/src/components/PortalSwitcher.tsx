import { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Copy, Shield, ChevronDown } from "lucide-react";

const PORTALS = [
  { id: "clone-xs", label: "Clone \u2192 Xs", description: "Catalog cloning & management", icon: Copy, path: "/" },
  { id: "governance", label: "Governance", description: "Metadata management & data quality", icon: Shield, path: "/governance" },
];

export default function PortalSwitcher() {
  const [open, setOpen] = useState(false);
  const [focusIdx, setFocusIdx] = useState(0);
  const ref = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const navigate = useNavigate();
  const location = useLocation();

  const isGovernance = location.pathname.startsWith("/governance");
  const current = isGovernance ? PORTALS[1] : PORTALS[0];

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectPortal = useCallback((idx: number) => {
    navigate(PORTALS[idx].path);
    setOpen(false);
    btnRef.current?.focus();
  }, [navigate]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (!open) {
      if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        setOpen(true);
        setFocusIdx(PORTALS.findIndex(p => p.id === current.id));
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
        setFocusIdx(i => (i + 1) % PORTALS.length);
        break;
      case "ArrowUp":
        e.preventDefault();
        setFocusIdx(i => (i - 1 + PORTALS.length) % PORTALS.length);
        break;
      case "Enter":
      case " ":
        e.preventDefault();
        selectPortal(focusIdx);
        break;
    }
  }, [open, focusIdx, current.id, selectPortal]);

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
        <div className="absolute top-full right-0 mt-1 w-72 bg-popover border border-border rounded-lg shadow-lg z-50 py-1" role="listbox" aria-label="Select portal">
          {PORTALS.map((portal, idx) => {
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
