"""Response caching utility for performance optimization."""

from datetime import datetime, timedelta
from typing import Any, Optional, Callable, Awaitable


class CacheEntry:
    """A single cache entry with TTL."""
    
    def __init__(self, data: Any, ttl_seconds: int):
        self.data = data
        self.created_at = datetime.utcnow()
        self.ttl_seconds = ttl_seconds
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        elapsed = (datetime.utcnow() - self.created_at).total_seconds()
        return elapsed > self.ttl_seconds
    
    def get(self) -> Optional[Any]:
        """Get cached data if not expired."""
        if self.is_expired():
            return None
        return self.data


class Cache:
    """Simple in-memory cache with TTL support."""
    
    def __init__(self):
        self._cache: dict[str, CacheEntry] = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if exists and not expired."""
        if key not in self._cache:
            return None
        
        entry = self._cache[key]
        if entry.is_expired():
            del self._cache[key]
            return None
        
        return entry.get()
    
    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        """Set value in cache with TTL."""
        self._cache[key] = CacheEntry(value, ttl_seconds)
    
    def clear(self, key: str) -> None:
        """Clear a specific cache key."""
        if key in self._cache:
            del self._cache[key]
    
    def clear_all(self) -> None:
        """Clear all cache."""
        self._cache.clear()
    
    async def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Awaitable[Any]],
        ttl_seconds: int = 300
    ) -> Any:
        """Get from cache or compute value if not cached."""
        cached = self.get(key)
        if cached is not None:
            return cached
        
        result = await compute_fn()
        self.set(key, result, ttl_seconds)
        return result


# Global cache instance
_global_cache = Cache()


def get_cache() -> Cache:
    """Get global cache instance."""
    return _global_cache
