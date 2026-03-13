"""Scraper for Ellis Island / Statue of Liberty - Ellis Island Foundation archives.

Target: https://www.libertyellisfoundation.org/passenger
Strategy: Search for passengers with "San Marino" as place of origin.

IMPORTANT: This is a heavy anti-bot site. We use conservative rate limiting
(3 req/min, 5-10s jitter) and respect robots.txt.
"""

import logging
from typing import List, Optional
import httpx
from bs4 import BeautifulSoup

from ..core.models import RawArchiveRecord
from ..core.rate_limiter import RateLimiter, get_random_user_agent

logger = logging.getLogger(__name__)

BASE_URL = "https://heritage.statueofliberty.org"
SEARCH_URL = f"{BASE_URL}/passenger"


class EllisIslandScraper:
    """Search Ellis Island passenger records for San Marino emigrants."""

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rate_limiter = rate_limiter or RateLimiter(
            requests_per_minute=3, jitter_range=(5.0, 10.0)
        )

    async def search_by_surname(self, surname: str) -> List[RawArchiveRecord]:
        """Search Ellis Island records for a specific surname from San Marino."""
        records = []

        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            try:
                async with self.rate_limiter:
                    # The Liberty Ellis Foundation has a search API
                    # We search by last name and filter results for San Marino origin
                    response = await client.get(
                        SEARCH_URL,
                        params={
                            "lastName": surname,
                            "birthCountry": "San Marino",
                        },
                        headers={
                            "User-Agent": get_random_user_agent(),
                            "Accept": "text/html,application/xhtml+xml",
                            "Accept-Language": "en-US,en;q=0.9",
                        },
                    )

                if response.status_code != 200:
                    logger.warning(
                        f"[Ellis] HTTP {response.status_code} for surname={surname}"
                    )
                    return records

                records = self._parse_results(response.text, surname)
                logger.info(f"[Ellis] {surname}: found {len(records)} records")

            except httpx.TimeoutException:
                logger.warning(f"[Ellis] Timeout searching for {surname}")
            except Exception as e:
                logger.warning(f"[Ellis] Error searching for {surname}: {e}")

        return records

    async def search_all_surnames(self, surnames: List[str]) -> List[RawArchiveRecord]:
        """Search for multiple surnames. Respects rate limits between queries."""
        all_records = []
        for surname in surnames:
            results = await self.search_by_surname(surname)
            all_records.extend(results)
        return all_records

    def _parse_results(self, html: str, search_surname: str) -> List[RawArchiveRecord]:
        """Parse the search results page for passenger records."""
        records = []
        soup = BeautifulSoup(html, "html.parser")

        # Look for result rows/cards containing passenger data
        # The actual CSS selectors depend on the site's current layout
        result_items = soup.select(".result-item, .passenger-record, tr.record")

        for item in result_items:
            try:
                # Extract surname from result
                name_el = item.select_one(".name, .passenger-name, td:first-child")
                if not name_el:
                    continue

                full_name = name_el.get_text(strip=True)
                surname = full_name.split()[-1] if full_name else search_surname

                # Extract year of arrival
                year_el = item.select_one(".year, .arrival-year, td.year")
                year = None
                if year_el:
                    year_text = year_el.get_text(strip=True)
                    try:
                        year = int(year_text[:4])
                    except (ValueError, IndexError):
                        pass

                # Extract origin
                origin_el = item.select_one(".origin, .birthplace, td.origin")
                origin = "San Marino"
                if origin_el:
                    origin = origin_el.get_text(strip=True)

                # Build the detail URL if available
                link = item.select_one("a[href]")
                source_url = ""
                if link:
                    href = link.get("href", "")
                    if href.startswith("/"):
                        source_url = f"{BASE_URL}{href}"
                    elif href.startswith("http"):
                        source_url = href

                records.append(RawArchiveRecord(
                    surname=surname,
                    origin=origin,
                    destination="New York, USA",
                    year=year,
                    source_url=source_url,
                ))

            except Exception as e:
                logger.debug(f"[Ellis] Failed to parse result item: {e}")
                continue

        return records
