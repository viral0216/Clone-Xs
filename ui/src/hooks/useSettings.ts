import { useState, useEffect } from "react";

/**
 * Hook to read a boolean setting from localStorage.
 * Listens for "clxs-settings-changed" events to stay in sync.
 */
export function useSetting(key: string, defaultValue = true): boolean {
  const [value, setValue] = useState(() => {
    try {
      const saved = localStorage.getItem(key);
      return saved !== null ? saved !== "false" : defaultValue;
    } catch {
      return defaultValue;
    }
  });

  useEffect(() => {
    const handler = () => {
      try {
        const saved = localStorage.getItem(key);
        setValue(saved !== null ? saved !== "false" : defaultValue);
      } catch {
        setValue(defaultValue);
      }
    };
    window.addEventListener("clxs-settings-changed", handler);
    return () => window.removeEventListener("clxs-settings-changed", handler);
  }, [key, defaultValue]);

  return value;
}

/** Whether export/download buttons should be shown. */
export function useShowExports(): boolean {
  return useSetting("clxs-show-exports", true);
}

/** Whether the catalog browser panel should be shown. */
export function useShowCatalogBrowser(): boolean {
  return useSetting("clxs-show-catalog-browser", true);
}

const CURRENCY_SYMBOLS: Record<string, string> = {
  USD: "$", EUR: "\u20ac", GBP: "\u00a3", AUD: "A$", CAD: "C$",
  INR: "\u20b9", JPY: "\u00a5", CHF: "CHF", SEK: "kr", BRL: "R$",
};

/** Get the configured currency code and symbol. */
export function useCurrency(): { code: string; symbol: string } {
  const [code, setCode] = useState(() => {
    try { return localStorage.getItem("clxs-currency") || "USD"; } catch { return "USD"; }
  });

  useEffect(() => {
    const handler = () => {
      try { setCode(localStorage.getItem("clxs-currency") || "USD"); } catch {}
    };
    window.addEventListener("clxs-settings-changed", handler);
    return () => window.removeEventListener("clxs-settings-changed", handler);
  }, []);

  return { code, symbol: CURRENCY_SYMBOLS[code] || "$" };
}

/** Get the configured storage price per GB/month. */
export function useStoragePrice(): number {
  const [price, setPrice] = useState(() => {
    try { return parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023; } catch { return 0.023; }
  });

  useEffect(() => {
    const handler = () => {
      try { setPrice(parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023); } catch {}
    };
    window.addEventListener("clxs-settings-changed", handler);
    return () => window.removeEventListener("clxs-settings-changed", handler);
  }, []);

  return price;
}

/**
 * Hook to persist a numeric value (e.g., panel width) in localStorage.
 * Returns [value, setValue] tuple.
 */
export function usePersistedNumber(key: string, defaultValue: number): [number, (v: number) => void] {
  const [value, setValue] = useState(() => {
    try {
      const saved = localStorage.getItem(key);
      return saved ? Number(saved) : defaultValue;
    } catch {
      return defaultValue;
    }
  });

  const set = (v: number) => {
    setValue(v);
    try { localStorage.setItem(key, String(v)); } catch {}
  };

  return [value, set];
}
