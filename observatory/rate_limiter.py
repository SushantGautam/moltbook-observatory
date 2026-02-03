"""Rate limiter for API calls with support for per-key rotation."""

import asyncio
from time import time
from collections import deque
from typing import List, Dict, Optional
from observatory.config import config


class RateLimiter:
    """Token bucket rate limiter for API calls (single key)."""
    
    def __init__(self, calls_per_minute: int):
        """
        Initialize the rate limiter.
        
        Args:
            calls_per_minute: Maximum number of API calls allowed per minute
        """
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # Seconds between calls
        self.call_times: deque = deque()
        self._lock = asyncio.Lock()
    
    async def wait_if_needed(self) -> None:
        """
        Wait if necessary to maintain rate limit.
        """
        async with self._lock:
            now = time()
            
            # Remove calls older than or exactly 60 seconds ago
            while self.call_times and self.call_times[0] <= now - 60:
                self.call_times.popleft()
            
            # If we've hit the limit, wait until the oldest call drops out of the window
            if len(self.call_times) >= self.calls_per_minute:
                sleep_time = 60 - (now - self.call_times[0])
                if sleep_time > 0:
                    print(f"Rate limit reached. Waiting {sleep_time:.2f}s before next API call...")
                    await asyncio.sleep(sleep_time)
            
            # Record this call
            self.call_times.append(time())

    async def try_acquire_now(self) -> bool:
        """Try to acquire a slot now without waiting. Return True if acquired."""
        async with self._lock:
            now = time()
            while self.call_times and self.call_times[0] <= now - 60:
                self.call_times.popleft()
            if len(self.call_times) < self.calls_per_minute:
                self.call_times.append(now)
                return True
            return False

    def get_usage(self, now: Optional[float] = None) -> tuple[int, int]:
        """Return (used, available) counts in the current 60s window."""
        if now is None:
            now = time()
        # count timestamps strictly newer than now - 60 (we prune <= now-60 elsewhere)
        used = sum(1 for t in self.call_times if t > now - 60)
        available = max(0, self.calls_per_minute - used)
        return used, available

    def status(self, now: Optional[float] = None) -> dict:
        """Return a dict with used, available, and limit for this limiter."""
        used, available = self.get_usage(now)
        return {"used": used, "available": available, "limit": self.calls_per_minute}


class KeyedRateLimiter:
    """Rate limiter that rotates among multiple API keys.

    Each key has its own RateLimiter (with the same calls_per_minute limit).
    The limiter will try to pick a key that can be used immediately; if none
    are available it sleeps the minimal required time across keys.
    """

    def __init__(self, keys: List[str], calls_per_minute: int):
        if not keys:
            raise ValueError("At least one API key is required")
        self.keys = keys
        self._limiters: Dict[str, RateLimiter] = {k: RateLimiter(calls_per_minute) for k in keys}
        self._idx = 0
        self._lock = asyncio.Lock()
        self.calls_per_minute = calls_per_minute

    async def wait_and_get_key(self) -> str:
        """Wait if needed and return an API key that can be used for the next call."""
        n = len(self.keys)
        while True:
            async with self._lock:
                now = time()
                # Try to find a key that can be acquired immediately
                for i in range(n):
                    idx = (self._idx + i) % n
                    key = self.keys[idx]
                    limiter = self._limiters[key]
                    if await limiter.try_acquire_now():
                        # move pointer forward for next rotation
                        self._idx = (idx + 1) % n
                        return key

                # None available immediately - compute minimal sleep needed across keys
                min_sleep = None
                for key in self.keys:
                    limiter = self._limiters[key]
                    async with limiter._lock:
                        if not limiter.call_times:
                            wait = 0
                        else:
                            wait = 60 - (now - limiter.call_times[0]) if len(limiter.call_times) >= limiter.calls_per_minute else 0
                        if wait > 0 and (min_sleep is None or wait < min_sleep):
                            min_sleep = wait

                # compute aggregate status for nicer logging
                total_used = 0
                total_capacity = self.calls_per_minute * len(self.keys)
                total_available = 0
                for key in self.keys:
                    used, available = self._limiters[key].get_usage(now)
                    total_used += used
                    total_available += available
                status = {"total_used": total_used, "capacity": total_capacity, "total_available": total_available}
                # If for some reason min_sleep is None (shouldn't happen), default to small sleep
                if min_sleep is None:
                    min_sleep = 0.1
                print(f"All keys exhausted. Waiting {min_sleep:.2f}s before retrying... total_used={status['total_used']}/{status['capacity']}, total_available={status['total_available']}")
            await asyncio.sleep(min_sleep)

    def status(self, now: Optional[float] = None) -> dict:
        """Return status for all keys: total_used, total_available, capacity, and per-key breakdown."""
        if now is None:
            now = time()
        per_key = {k: self._limiters[k].status(now) for k in self.keys}
        total_used = sum(v['used'] for v in per_key.values())
        total_available = sum(v['available'] for v in per_key.values())
        capacity = sum(v['limit'] for v in per_key.values())
        return {
            'total_used': total_used,
            'total_available': total_available,
            'capacity': capacity,
            'per_key': per_key,
        }


# Global rate limiter instance (KeyedRateLimiter)
_rate_limiter: KeyedRateLimiter | None = None


async def get_rate_limiter() -> KeyedRateLimiter:
    """Get the global keyed rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        keys = config.MOLTBOOK_API_KEYS
        _rate_limiter = KeyedRateLimiter(keys=keys, calls_per_minute=config.MOLTBOOK_API_RATE_LIMIT)
    return _rate_limiter
