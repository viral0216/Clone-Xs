// @ts-nocheck
/**
 * Global keyboard shortcuts for Clone-Xs.
 * Provides vi-style G-chord navigation and quick-action keys.
 *
 * Shortcuts:
 *   /         → focus main search input (data-search-input attribute)
 *   Escape    → blur focused element / close open panels
 *   G then L  → /assessment/inventory/lineage
 *   G then F  → /assessment/findings
 *   G then I  → /assessment/inventory
 *   G then A  → /ai-assistant
 *   ?         → show keyboard shortcuts modal
 */
import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

// Module-level event name for toggling the modal
const TOGGLE_MODAL_EVENT = "clxs-global-shortcuts-modal";

export const GLOBAL_SHORTCUTS = [
  { keys: "/", description: "Focus search" },
  { keys: "Escape", description: "Blur / close panel" },
  { keys: "G L", description: "Go to Lineage" },
  { keys: "G F", description: "Go to Findings" },
  { keys: "G I", description: "Go to Inventory" },
  { keys: "G A", description: "Go to AI Assistant" },
  { keys: "?", description: "Show this shortcuts modal" },
];

/**
 * Call this hook once at the App root level to register global shortcuts.
 */
export function useGlobalShortcuts() {
  const navigate = useNavigate();
  const gChordActiveRef = useRef(false);
  const gChordTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const clearGChord = () => {
      gChordActiveRef.current = false;
      if (gChordTimerRef.current !== null) {
        clearTimeout(gChordTimerRef.current);
        gChordTimerRef.current = null;
      }
    };

    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      const tag = target.tagName;
      const isEditable =
        tag === "INPUT" ||
        tag === "TEXTAREA" ||
        tag === "SELECT" ||
        target.isContentEditable;

      // --- G-chord second key (consume regardless of editable) ---
      if (gChordActiveRef.current) {
        clearGChord();
        if (isEditable) return;
        const key = e.key.toLowerCase();
        if (key === "l") {
          e.preventDefault();
          navigate("/assessment/inventory/lineage");
        } else if (key === "f") {
          e.preventDefault();
          navigate("/assessment/findings");
        } else if (key === "i") {
          e.preventDefault();
          navigate("/assessment/inventory");
        } else if (key === "a") {
          e.preventDefault();
          navigate("/ai-assistant");
        }
        return;
      }

      // Don't intercept typing shortcuts when user is in an input
      if (isEditable) return;

      // --- Single-key shortcuts ---
      if (e.key === "/" && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        const searchEl =
          document.querySelector<HTMLInputElement>("[data-search-input]") ||
          document.querySelector<HTMLInputElement>('input[placeholder*="Search"]');
        searchEl?.focus();
        return;
      }

      if (e.key === "Escape") {
        // Blur focused element
        (document.activeElement as HTMLElement)?.blur();
        // Dispatch a custom event so panels/drawers can listen and close
        window.dispatchEvent(new Event("clxs-close-panels"));
        return;
      }

      if (e.key === "?" && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        window.dispatchEvent(new Event(TOGGLE_MODAL_EVENT));
        return;
      }

      // --- G-chord first key ---
      if (e.key.toLowerCase() === "g" && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        gChordActiveRef.current = true;
        // Auto-cancel chord after 500 ms
        gChordTimerRef.current = setTimeout(clearGChord, 500);
        return;
      }
    };

    document.addEventListener("keydown", handler);
    return () => {
      document.removeEventListener("keydown", handler);
      clearGChord();
    };
  }, [navigate]);
}

/**
 * Render this once in the app tree (e.g. inside App).
 * It manages its own open/close state via the TOGGLE_MODAL_EVENT.
 */
export function ShortcutsModal() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const handler = () => setOpen((prev) => !prev);
    window.addEventListener(TOGGLE_MODAL_EVENT, handler);
    return () => window.removeEventListener(TOGGLE_MODAL_EVENT, handler);
  }, []);

  return (
    <Dialog open={open} onOpenChange={(v) => !v && setOpen(false)}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Global Keyboard Shortcuts</DialogTitle>
        </DialogHeader>
        <div className="mt-2 space-y-1">
          <p className="text-xs text-muted-foreground mb-3">
            These shortcuts work anywhere in the app (outside of text inputs).
          </p>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left pb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Shortcut
                </th>
                <th className="text-left pb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Action
                </th>
              </tr>
            </thead>
            <tbody>
              {GLOBAL_SHORTCUTS.map((s) => (
                <tr key={s.keys} className="border-b border-border/50 last:border-0">
                  <td className="py-2 pr-4">
                    <div className="flex items-center gap-1">
                      {s.keys.split(" ").map((k, i) => (
                        <kbd
                          key={i}
                          className="bg-muted rounded px-1.5 py-0.5 text-xs font-mono"
                        >
                          {k}
                        </kbd>
                      ))}
                    </div>
                  </td>
                  <td className="py-2 text-muted-foreground">{s.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DialogContent>
    </Dialog>
  );
}
