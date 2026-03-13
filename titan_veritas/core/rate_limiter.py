"""Shared rate limiter for all scrapers and API clients."""

import asyncio
import random
from typing import Tuple

# Rotating user agents for ethical scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


class RateLimiter:
    """Async-aware rate limiter with per-domain semaphore and random jitter."""

    def __init__(self, requests_per_minute: int = 10,
                 jitter_range: Tuple[float, float] = (1.0, 3.0)):
        self._semaphore = asyncio.Semaphore(requests_per_minute)
        self._jitter = jitter_range

    async def acquire(self):
        await self._semaphore.acquire()
        await asyncio.sleep(random.uniform(*self._jitter))

    def release(self):
        self._semaphore.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


# Pre-configured rate limiters for different source types
RATE_LIMITS = {
    "fratellanze": RateLimiter(requests_per_minute=10, jitter_range=(1.0, 3.0)),
    "archive": RateLimiter(requests_per_minute=3, jitter_range=(5.0, 10.0)),
    "youth_league": RateLimiter(requests_per_minute=5, jitter_range=(2.0, 5.0)),
    "default": RateLimiter(requests_per_minute=10, jitter_range=(1.0, 2.0)),
}
