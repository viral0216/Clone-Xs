"""Thread-safe TTL cache for Databricks metadata SDK calls."""

import os
import threading
import time
from typing import Any

MISSING = object()

_DEFAULT_TTL = float(os.getenv("CLXS_CACHE_TTL", "300"))


class MetadataCache:
    """In-memory cache with per-entry TTL expiration.

    Thread-safe: the lock protects dict reads/writes only.
    SDK calls happen outside the lock to avoid blocking parallel workers.
    """

    def __init__(self, default_ttl: float = _DEFAULT_TTL):
        self._lock = threading.Lock()
        self._store: dict[tuple, tuple[float, Any]] = {}  # key -> (expiry, value)
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def get(self, key: tuple) -> Any:
        """Return cached value if present and not expired, else MISSING."""
        with self._lock:
            entry = self._store.get(key)
            if entry is not None:
                expiry, value = entry
                if time.monotonic() < expiry:
                    self._hits += 1
                    return value
                del self._store[key]
            self._misses += 1
            return MISSING

    def put(self, key: tuple, value: Any, ttl: float | None = None) -> None:
        """Store a value with optional per-entry TTL override."""
        expiry = time.monotonic() + (ttl if ttl is not None else self._default_ttl)
        with self._lock:
            self._store[key] = (expiry, value)

    def invalidate_catalog(self, catalog: str) -> int:
        """Remove all entries where catalog appears at key[1]."""
        with self._lock:
            keys = [k for k in self._store if len(k) > 1 and k[1] == catalog]
            for k in keys:
                del self._store[k]
            return len(keys)

    def clear(self) -> None:
        """Remove all entries and reset counters."""
        with self._lock:
            self._store.clear()
            self._hits = 0
            self._misses = 0

    def stats(self) -> dict:
        """Return cache statistics."""
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "size": len(self._store),
                "ttl_seconds": self._default_ttl,
            }


# Module-level singleton
metadata_cache = MetadataCache()
