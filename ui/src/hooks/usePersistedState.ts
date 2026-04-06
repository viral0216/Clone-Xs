/**
 * usePersistedState — Like useState but persists to sessionStorage.
 * State survives page navigation within the same tab.
 * Auto-cleans stale entries older than maxAge (default 30 min).
 */
import { useState, useCallback } from "react";

const STALE_MS = 30 * 60 * 1000; // 30 minutes

interface Envelope<T> {
  value: T;
  ts: number;
}

export function usePersistedState<T>(key: string, initialValue: T): [T, (v: T | ((prev: T) => T)) => void] {
  const storageKey = `clxs_ps_${key}`;

  const [state, setStateRaw] = useState<T>(() => {
    try {
      const raw = sessionStorage.getItem(storageKey);
      if (raw) {
        const envelope: Envelope<T> = JSON.parse(raw);
        if (Date.now() - envelope.ts < STALE_MS) {
          return envelope.value;
        }
        sessionStorage.removeItem(storageKey);
      }
    } catch { /* ignore */ }
    return initialValue;
  });

  const setState = useCallback((v: T | ((prev: T) => T)) => {
    setStateRaw(prev => {
      const next = typeof v === "function" ? (v as (prev: T) => T)(prev) : v;
      try {
        sessionStorage.setItem(storageKey, JSON.stringify({ value: next, ts: Date.now() }));
      } catch { /* quota exceeded */ }
      return next;
    });
  }, [storageKey]);

  return [state, setState];
}
