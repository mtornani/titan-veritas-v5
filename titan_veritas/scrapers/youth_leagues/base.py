"""Abstract base class for regional youth league scrapers."""

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
import logging

import httpx

from ...core.models import PlayerProfile
from ...core.rate_limiter import RateLimiter, get_random_user_agent

logger = logging.getLogger(__name__)


class AsyncLeagueScraper(ABC):
    """Base class for all regional youth league scrapers.

    Each concrete implementation must define the target league's URL patterns,
    HTML selectors, and parsing logic.
    """

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rate_limiter = rate_limiter or RateLimiter(
            requests_per_minute=5, jitter_range=(2.0, 5.0)
        )

    @property
    @abstractmethod
    def league_name(self) -> str:
        """Human-readable name of the league."""
        ...

    @property
    @abstractmethod
    def country(self) -> str:
        """ISO country code or name."""
        ...

    @property
    @abstractmethod
    def base_url(self) -> str:
        """Base URL of the league website."""
        ...

    @abstractmethod
    async def search_by_surname(self, surname: str) -> List[PlayerProfile]:
        """Search the league registry for players matching the given surname."""
        ...

    @abstractmethod
    async def search_by_age_range(self, min_age: int, max_age: int) -> List[PlayerProfile]:
        """Search for players in the given age range (U14-U20 focus)."""
        ...

    async def _fetch(self, client: httpx.AsyncClient, url: str,
                     method: str = "GET", **kwargs) -> Optional[httpx.Response]:
        """Rate-limited HTTP request with error handling."""
        try:
            async with self.rate_limiter:
                headers = kwargs.pop("headers", {})
                headers.setdefault("User-Agent", get_random_user_agent())
                if method == "GET":
                    response = await client.get(url, headers=headers, **kwargs)
                else:
                    response = await client.post(url, headers=headers, **kwargs)

            if response.status_code == 200:
                return response
            else:
                logger.warning(f"[{self.league_name}] HTTP {response.status_code} for {url}")
                return None
        except httpx.TimeoutException:
            logger.warning(f"[{self.league_name}] Timeout for {url}")
            return None
        except Exception as e:
            logger.warning(f"[{self.league_name}] Error fetching {url}: {e}")
            return None
