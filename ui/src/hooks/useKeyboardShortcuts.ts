/**
 * Global keyboard shortcut registry for Clone-Xs.
 */
import { useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";

interface Shortcut {
  key: string;
  meta?: boolean; // Cmd on Mac
  shift?: boolean;
  description: string;
  action: () => void;
}

export const SHORTCUTS_MAP = [
  { keys: "⌘ Shift C", description: "Go to Clone", route: "/clone" },
  { keys: "⌘ Shift E", description: "Go to Explorer", route: "/explore" },
  { keys: "⌘ Shift D", description: "Go to Diff", route: "/diff" },
  { keys: "⌘ .", description: "Go to Settings", route: "/settings" },
  { keys: "⌘ K", description: "Focus Search", route: null },
  { keys: "?", description: "Show Shortcuts", route: null },
];

export function useKeyboardShortcuts(onShowHelp: () => void) {
  const navigate = useNavigate();

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Don't trigger in inputs/textareas
      const tag = (e.target as HTMLElement).tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;

      if (e.key === "?" && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        onShowHelp();
        return;
      }

      const meta = e.metaKey || e.ctrlKey;

      if (meta && e.shiftKey && e.key.toLowerCase() === "c") {
        e.preventDefault();
        navigate("/clone");
      } else if (meta && e.shiftKey && e.key.toLowerCase() === "e") {
        e.preventDefault();
        navigate("/explore");
      } else if (meta && e.shiftKey && e.key.toLowerCase() === "d") {
        e.preventDefault();
        navigate("/diff");
      } else if (meta && e.key === ".") {
        e.preventDefault();
        navigate("/settings");
      } else if (meta && e.key.toLowerCase() === "k") {
        e.preventDefault();
        // Focus the search input in the header
        const searchInput = document.querySelector<HTMLInputElement>('input[placeholder*="Search"]');
        searchInput?.focus();
      }
    };

    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [navigate, onShowHelp]);
}
