import { useState, useCallback } from "react";

export interface FavoritePair {
  source: string;
  destination: string;
  cloneType?: string;
  addedAt: string;
}

const STORAGE_KEY = "clxs_favorite_pairs";

function loadFavorites(): FavoritePair[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveFavorites(favorites: FavoritePair[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(favorites));
}

export function useFavorites() {
  const [favorites, setFavorites] = useState<FavoritePair[]>(loadFavorites);

  const addFavorite = useCallback((source: string, destination: string, cloneType?: string) => {
    setFavorites((prev) => {
      const exists = prev.some((f) => f.source === source && f.destination === destination);
      if (exists) return prev;
      const next = [...prev, { source, destination, cloneType, addedAt: new Date().toISOString() }];
      saveFavorites(next);
      return next;
    });
  }, []);

  const removeFavorite = useCallback((source: string, destination: string) => {
    setFavorites((prev) => {
      const next = prev.filter((f) => !(f.source === source && f.destination === destination));
      saveFavorites(next);
      return next;
    });
  }, []);

  const isFavorite = useCallback(
    (source: string, destination: string) =>
      favorites.some((f) => f.source === source && f.destination === destination),
    [favorites],
  );

  return { favorites, addFavorite, removeFavorite, isFavorite };
}
