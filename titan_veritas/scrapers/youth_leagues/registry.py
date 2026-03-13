"""Registry and factory for youth league scrapers."""

import logging
from typing import Dict, List, Tuple, Type, Optional

from ...core.models import PlayerProfile
from ...core.rate_limiter import RateLimiter
from .base import AsyncLeagueScraper

logger = logging.getLogger(__name__)


class LeagueRegistry:
    """Central registry for all youth league scrapers."""

    def __init__(self):
        self._scrapers: Dict[str, AsyncLeagueScraper] = {}

    def register(self, scraper: AsyncLeagueScraper) -> None:
        key = f"{scraper.country}:{scraper.league_name}"
        self._scrapers[key] = scraper
        logger.info(f"[Registry] Registered scraper: {key}")

    def get_scrapers_for_country(self, country: str) -> List[AsyncLeagueScraper]:
        return [s for k, s in self._scrapers.items() if s.country.lower() == country.lower()]

    def get_all_scrapers(self) -> List[AsyncLeagueScraper]:
        return list(self._scrapers.values())

    async def search_all(self, surnames: List[str],
                         age_range: Tuple[int, int] = (14, 20),
                         country: Optional[str] = None) -> List[PlayerProfile]:
        """Search all registered scrapers (or filtered by country) for candidates."""
        scrapers = (
            self.get_scrapers_for_country(country) if country
            else self.get_all_scrapers()
        )

        all_players = []
        for scraper in scrapers:
            # Search by surname
            for surname in surnames:
                try:
                    players = await scraper.search_by_surname(surname)
                    all_players.extend(players)
                except NotImplementedError:
                    logger.info(
                        f"[Registry] {scraper.league_name}: surname search not implemented"
                    )
                    break
                except Exception as e:
                    logger.warning(
                        f"[Registry] Error searching {scraper.league_name} for {surname}: {e}"
                    )

            # Search by age range
            try:
                players = await scraper.search_by_age_range(*age_range)
                all_players.extend(players)
            except NotImplementedError:
                pass
            except Exception as e:
                logger.warning(
                    f"[Registry] Error in age search for {scraper.league_name}: {e}"
                )

        # Deduplicate by (first_name, last_name, source)
        seen = set()
        unique = []
        for p in all_players:
            key = (p.first_name.lower(), p.last_name.lower(), p.source)
            if key not in seen:
                seen.add(key)
                unique.append(p)

        logger.info(f"[Registry] Total unique candidates found: {len(unique)}")
        return unique


def create_default_registry(rate_limiter: Optional[RateLimiter] = None) -> LeagueRegistry:
    """Create a registry with all available scrapers."""
    from .michigan_sysa import MichiganSYSAScraper
    from .pergamino import PergaminoLeagueScraper
    from .cordoba import CordobaLeagueScraper

    registry = LeagueRegistry()
    registry.register(MichiganSYSAScraper(rate_limiter))
    registry.register(PergaminoLeagueScraper(rate_limiter))
    registry.register(CordobaLeagueScraper(rate_limiter))
    return registry
